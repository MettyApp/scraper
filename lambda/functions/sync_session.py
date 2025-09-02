from datetime import datetime, timedelta
import json
import logging
import os
import time
import boto3
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.utils.job import job_dir

logger = logging.getLogger(__name__)

dynamodb_client = None
s3_client = None
if "AWS_ACCESS_KEY_ID" in os.environ:
    dynamodb_client = boto3.client("dynamodb")
    s3_client = boto3.client("s3")
    sqs_client = boto3.client("sqs")


class FugueSync:
    def __init__(self, session_id, iteration_count, domains, jobdir):
        self.session_id = session_id
        self.iteration_count = iteration_count
        self.domains = domains
        self.iteration_date = datetime.now().isoformat()
        self.state_file = jobdir + "/fugue_sync.pkl"
        self.start_time = time.time()
        self.start_datetime = datetime.now().isoformat()
        self.items = []
        self.downloaded_bytes = 0
        self.request_count = 0
        self.error_count = 0
        self.stats = {}
        self.backends = set({})

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.get("FUGUE_SESSION_ID"):
            raise NotConfigured
        jobdir = job_dir(crawler.settings)
        if not jobdir:
            raise NotConfigured
        ext = cls(
            crawler.settings.get("FUGUE_SESSION_ID"),
            crawler.settings.getint("FUGUE_ITER_COUNT"),
            crawler.settings.getlist("FUGUE_DOMAINS"),
            jobdir,
        )

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.bytes_received, signal=signals.bytes_received)

        return ext

    def spider_opened(self, spider):
        logger.info("fugueSync: opened spider %s", spider.name)
        if dynamodb_client and "DDB_STATE_TABLE" in os.environ:
            dynamodb_client.update_item(
                TableName=os.environ["DDB_STATE_TABLE"],
                ReturnValues="UPDATED_NEW",
                Key={
                    "pk": {
                        "S": f"CRAWLING_SESSION#{self.session_id}",
                    },
                },
                ConditionExpression="attribute_not_exists(pk) or (sessionIterationCount < :maxIteration and sessionIterationCount = :sessionIterationCount and sessionState = :awaiting)",
                UpdateExpression="SET crawlingSessionId = :crawlingSessionId, crawlingSessionDomains = :domains, sessionState = :sessionState, sessionDate = :sessionIterationStartedAt, #ttl = :ttl, sessionSpiders = :spiders, sessionIterationStartedAt = :sessionIterationStartedAt, sessionSpiderVersion = :crawlerVersion ADD sessionIterationCount :one",
                ExpressionAttributeNames={
                    "#ttl": "_ttl",
                },
                ExpressionAttributeValues={
                    ":sessionState": {"S": "RUNNING"},
                    ":crawlingSessionId": {"S": self.session_id},
                    ":ttl": {
                        "N": str(int((datetime.now() + timedelta(days=7)).timestamp()))
                    },
                    ":spiders": {"SS": [spider.name]},
                    ":domains": {"SS": self.domains},
                    ":awaiting": {"S": "AWAITING_SCHEDULING"},
                    ":one": {"N": "1"},
                    ":maxIteration": {"N": "30"},
                    ":sessionIterationStartedAt": {"S": self.iteration_date},
                    ":sessionIterationCount": {"N": str(self.iteration_count)},
                    ":crawlerVersion": {"S": os.getenv("GIT_SHORT_HASH", "_snapshot")},
                },
            )
            self.iteration_count += 1

    def spider_closed(self, spider, reason):
        logging.info(json.dumps(self.stats))
        elapsed_time = time.time() - self.start_time
        finished = reason == "finished"
        logging.info(f"crawling stopped: reason={reason}")
        if dynamodb_client and "DDB_STATE_TABLE" in os.environ:
            dynamodb_client.update_item(
                TableName=os.environ["DDB_STATE_TABLE"],
                ReturnValues="UPDATED_NEW",
                Key={
                    "pk": {
                        "S": f"CRAWLING_SESSION#{self.session_id}",
                    },
                },
                ConditionExpression="attribute_exists(pk) and sessionIterationCount < :maxIteration and sessionIterationCount = :sessionIterationCount",
                UpdateExpression=" ".join(
                    elt
                    for elt in [
                        "SET",
                        "sessionState = :state",
                        "ADD",
                        ",".join(
                            [
                                "sessionIterationCount :one",
                                "sessionDurationTime :elapsed",
                                "sessionItemScraped :sessionItemScraped",
                                "crawlingSessionDownloadedBytes :crawlingSessionDownloadedBytes",
                                "crawlingSessionRequests :crawlingSessionRequests",
                                "crawlingSessionErrors :crawlingSessionErrors",
                                "sessionItemScrapedDetailed :sessionItemScrapedDetailed"
                                if len(self.items) > 0
                                else "",
                                "sessionBackends :sessionBackends"
                                if len(self.backends) > 0
                                else "",
                            ]
                        ),
                    ]
                    if elt is not None
                ),
                ExpressionAttributeValues={
                    k: v
                    for k, v in {
                        ":state": {
                            "S": "COMPLETED" if finished else "AWAITING_SCHEDULING"
                        },
                        ":one": {"N": "1"},
                        ":maxIteration": {"N": "30"},
                        ":sessionIterationCount": {"N": str(self.iteration_count)},
                        ":elapsed": {"N": str(elapsed_time)},
                        ":sessionItemScraped": {"N": str(len(self.items))},
                        ":sessionItemScrapedDetailed": {
                            "SS": list(json.dumps(e) for e in self.items)
                        }
                        if len(self.items) > 0
                        else None,
                        ":sessionBackends": {"SS": list(self.backends)}
                        if len(self.backends) > 0
                        else None,
                        ":crawlingSessionDownloadedBytes": {
                            "N": str(self.downloaded_bytes)
                        },
                        ":crawlingSessionRequests": {"N": str(self.request_count)},
                        ":crawlingSessionErrors": {"N": str(self.error_count)},
                    }.items()
                    if v is not None
                },
            )
            if not finished:
                if sqs_client and "QUEUE_URL" in os.environ:
                    logging.info("asking crawling to resume")
                    sqs_client.send_message(
                        QueueUrl=os.environ["QUEUE_URL"],
                        DelaySeconds=10,
                        MessageBody=json.dumps(
                            {
                                "session_id": self.session_id,
                                "iteration_count": self.iteration_count + 1,
                                "domains": self.domains,
                            }
                        ),
                    )

    def item_scraped(self, item, spider):
        if "product_url" in item:
            self.items.append(
                {
                    "url": item.get("product_url"),
                    "predicted_category": item.get("predicted_category"),
                }
            )
        if "backend" in item and item.get("backend") not in self.backends:
            self.backends.add(item.get("backend"))
        pass

    def bytes_received(self, data: bytes, request, spider) -> None:
        self.downloaded_bytes += len(data)

    def response_received(self, response, request, spider) -> None:
        if response.status not in self.stats:
            self.stats[response.status] = 0
        self.stats[response.status] += 1
        if response.status >= 400:
            self.error_count += 1
        self.request_count += 1
