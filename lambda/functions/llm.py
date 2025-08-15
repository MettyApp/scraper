#!/usr/bin/env python
import base64
import gzip
import json
import logging
import os
from urllib.parse import urlparse

import boto3

from extractors.mistral import MistralExtractor
from validators import validate


class DownloadedPageNotFound(Exception):
    pass


class LLMFailedException(Exception):
    pass


class InvalidBucketException(Exception):
    pass


s3_client = boto3.client("s3")

extractor = MistralExtractor()


def download_gz_content(url_str):
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


def b64_decode(encoded_value):
    return base64.b64decode(encoded_value).decode("utf-8")


def lambda_handler(event, context):
    required_params = ["product_url", "crawled_page_url"]
    missing_params = [param for param in required_params if param not in event]

    if missing_params:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {"error": "Missing parameters", "missing": missing_params}
            ),
        }

    for item in download_gz_content(event.get("crawled_page_url")):
        if item.get("product_url") == event.get("product_url"):
            parsed = extractor.parse(event["product_url"], b64_decode(item["content"]))[
                0
            ]
            if parsed:
                parsed = validate(parsed)
                for key, value in parsed.items():
                    if isinstance(value, list):
                        parsed[key] = [item for item in value if item is not None]

            return {"statusCode": 200, "body": json.dumps(parsed)}
    return {"statusCode": 200, "body": json.dumps({"error": "item not found"})}
