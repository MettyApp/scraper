import gzip
import json
import logging
import os
from urllib.parse import urlparse
import boto3

from urllib.parse import unquote

s3_client = boto3.client("s3")


new_queue_url = os.getenv("NEW_URLS_QUEUE")


class DownloadedPageNotFound(Exception):
    pass


class InvalidBucketException(Exception):
    pass


def lambda_handler(event, context):
    for record in event["Records"]:
        message = json.loads(record["body"])
        for record in message["Records"]:
            object_key = unquote(record["s3"]["object"]["key"])
            bucket_name = record["s3"]["bucket"]["name"]
            s3_file_handler(f"s3://{bucket_name}/{object_key}")


def download_content(url_str):
    url = urlparse(url_str)
    if url.hostname != os.environ["PAGE_S3_BUCKET"] or url.scheme != "s3":
        logging.error(f"invalid url received: {url}")
        raise InvalidBucketException()
    key = url.path.removeprefix("/")
    print("downloading from S3", key)
    try:
        resp = s3_client.get_object(Bucket=os.environ["PAGE_S3_BUCKET"], Key=key)
        with gzip.GzipFile(fileobj=resp["Body"]) as gz:
            for line in gz:
                yield json.loads(line)
    except Exception as err:
        print("failed to download", key, ":", err)
        raise DownloadedPageNotFound()


def s3_file_handler(s3_url):
    # llm = MistralExtractor()
    for item in download_content(s3_url):
        prediction = item.get("predicted_category")
        if prediction == "roasted-beans":
            print(item.get("title"), item.get("image_url"))


if __name__ == "__main__":
    s3_file_handler(
        "s3://fugue-crawler-s3bucket-wfpbhlliaf63/parsed/v3/01K3DP52YCAHN4XEQW31CE6YKR/2025-08-24T09-22-12.711399+00-00-00001.json.gz"
    )
