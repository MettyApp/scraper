import logging
import os
import json
import sys

import boto3

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from crochet import setup, wait_for

from ulid import ULID
import shutil

setup()

dynamodb_client = boto3.client("dynamodb")
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


data_root_dir = os.environ.get("DATA_ROOT_DIR", "./data/synced")

sessions_dir = f"{data_root_dir}/sessions"
crawled_dir = f"{data_root_dir}/crawled/v3"
parsed_dir = f"{data_root_dir}/parsed/v1"


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


class RestoreScrapyJobdir:
    def __init__(self, session_id):
        if session_id is not None:
            self.session_id = session_id
        else:
            self.session_id = str(ULID())

    def __enter__(self):
        try:
            shutil.rmtree(f"{crawled_dir}/{self.session_id}", ignore_errors=True)
            os.makedirs(f"{crawled_dir}/{self.session_id}")
        except Exception as err:
            logging.error(f"failed to setup directory layout: {err}")
            pass
        try:
            s3_client.download_file(
                os.environ["PAGE_S3_BUCKET"],
                f"crawled/v3/{self.session_id}.tar.gz",
                f"{crawled_dir}/{self.session_id}.tar.gz",
            )
            shutil.unpack_archive(
                f"{crawled_dir}/{self.session_id}.tar.gz",
                f"{crawled_dir}/{self.session_id}",
            )
            size_bytes = os.path.getsize(f"{crawled_dir}/{self.session_id}.tar.gz")
            os.unlink(f"{crawled_dir}/{self.session_id}.tar.gz")
            size_mb = size_bytes / (1024 * 1024)
            logging.info(f"Session restored - tar size: {size_mb:.2f} MB")
        except Exception as err:
            logging.error(f"failed to restore session {self.session_id}: {err}")
            pass
        try:
            s3_client.download_file(
                os.environ["PAGE_S3_BUCKET"],
                "crawled/v4/cache.tar.gz",
                "/tmp/cache.tar.gz",
            )
            shutil.unpack_archive("/tmp/cache.tar.gz", "/tmp/cache")
            size_bytes = os.path.getsize("/tmp/cache.tar.gz")
            os.unlink("/tmp/cache.tar.gz")
            size_mb = size_bytes / (1024 * 1024)
            logging.info(f"Cache restored - tar size: {size_mb:.2f} MB")
        except Exception as err:
            logging.error(f"failed to restore cache: {err}")
            pass
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        try:
            shutil.make_archive(
                f"{crawled_dir}/{self.session_id}",
                "gztar",
                f"{crawled_dir}/{self.session_id}",
            )
            s3_client.upload_file(
                f"{crawled_dir}/{self.session_id}.tar.gz",
                os.environ["PAGE_S3_BUCKET"],
                f"crawled/v3/{self.session_id}.tar.gz",
            )
            size_bytes = os.path.getsize(f"{crawled_dir}/{self.session_id}.tar.gz")
            os.unlink(f"{crawled_dir}/{self.session_id}.tar.gz")
            size_mb = size_bytes / (1024 * 1024)
            logging.info(
                f"session {self.session_id} saved - tar size: {size_mb:.2f} MB"
            )
        except Exception as err:
            logging.error(f"failed to save session: {err}")
            pass
        try:
            shutil.make_archive("/tmp/cache", "gztar", "/tmp/cache")
            s3_client.upload_file(
                "/tmp/cache.tar.gz",
                os.environ["PAGE_S3_BUCKET"],
                "crawled/v4/cache.tar.gz",
            )
            size_bytes = os.path.getsize("/tmp/cache.tar.gz")
            os.unlink("/tmp/cache.tar.gz")
            size_mb = size_bytes / (1024 * 1024)
            logging.info(f"cache saved - tar size: {size_mb:.2f} MB")
        except Exception as err:
            logging.error(f"failed to save cache: {err}")
            pass


@wait_for(timeout=300)
async def payload_handler(session_id, itercount, domains):
    settings = get_project_settings()
    settings.set(name="HTTPCACHE_DIR", value="/tmp/cache", priority="cmdline")

    settings.set(
        name="EXTENSIONS",
        value={
            "scrapy.extensions.closespider.CloseSpider": 500,
            "lambda.functions.sync_session.FugueSync": 0,
        },
        priority="cmdline",
    )

    settings.set(name="CLOSESPIDER_PAGECOUNT", value=500, priority="cmdline")
    settings.set(name="CLOSESPIDER_TIMEOUT", value=100, priority="cmdline")
    settings.set("FEED_EXPORT_BATCH_ITEM_COUNT", value=20, priority="cmdline")
    settings.set(
        name="FEEDS",
        value={
            f"s3://{os.environ['PAGE_S3_BUCKET']}/parsed/v3/{session_id}/%(batch_time)s-%(batch_id)05d.json.gz": {
                "format": "jsonlines",
                "postprocessing": ["scrapy.extensions.postprocessing.GzipPlugin"],
                "store_empty": False,
                "overwrite": True,
                "indent": 0,
                "item_export_kwargs": {
                    "export_empty_fields": True,
                },
            }
        },
        priority="cmdline",
    )
    settings.set(
        name="IMAGES_STORE",
        value=f"s3://{os.environ['PAGE_S3_BUCKET']}/assets/v2/",
        priority="cmdline",
    )

    settings.set(
        name="JOBDIR", value=f"{crawled_dir}/{session_id}/", priority="cmdline"
    )
    settings.set("FUGUE_SESSION_ID", session_id)
    settings.set("FUGUE_ITER_COUNT", itercount)
    settings.set("FUGUE_DOMAINS", domains)
    settings.set("TWISTED_REACTOR", "twisted.internet.epollreactor.EPollReactor")
    runner = CrawlerRunner(settings)
    if len(domains) > 0:
        await runner.crawl("products", domains=",".join(domains))
    else:
        await runner.crawl("products")


def lambda_handler(event, context):
    for record in event["Records"]:
        config = json.loads(record["body"])
        if "session_id" in config:
            logging.info(
                f"resuming session {config.get('session_id')}: itercount={config.get('iteration_count')}"
            )
        with RestoreScrapyJobdir(config.get("session_id")) as state:  # type: ignore
            logging.info(f"crawling session id: {state.session_id}")
            payload_handler(
                state.session_id,
                config.get("iteration_count", 0),
                config.get("domains", []),
            )  # type: ignore


if __name__ == "__main__":
    if len(sys.argv) == 3:
        with RestoreScrapyJobdir(sys.argv[1]) as state:
            payload_handler(state.session_id, sys.argv[2], [])  # type: ignore
    else:
        with RestoreScrapyJobdir(None) as state:
            payload_handler(state.session_id, 0, [])  # type: ignore
