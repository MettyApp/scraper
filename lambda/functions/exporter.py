import base64
from datetime import datetime, timedelta
import gzip
import json
import logging
import os
from urllib.parse import unquote, urlparse
import boto3
from gql import GraphQLRequest, gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.appsync_auth import AppSyncIAMAuthentication

from extractors.mistral import MistralExtractor
from validators import validate

llm = MistralExtractor()

s3_client = boto3.client("s3")

auth = AppSyncIAMAuthentication(
    host="jizfjk7ncnb6rbq7lvu3k2izoe.appsync-api.eu-west-1.amazonaws.com",
)

transport = AIOHTTPTransport(
    url="https://jizfjk7ncnb6rbq7lvu3k2izoe.appsync-api.eu-west-1.amazonaws.com/graphql",
    auth=auth,
)
get_bean = gql("""
query getBean($id: String!) {
    getRoastedBean(id: $id) {
             id
    }
}
""")

get_bean_by_name = gql("""
query GetBean($roaster: String!, $name: String!) {
  getRoastedBeanByName(
    name: $name
    roasterId: $roaster
  ) {
    id
  }
}""")

import_bean = gql("""
    mutation import(
        $roaster: String!,
        $name: String!,
        $imageUrl: String,
        $pageUrl: String,
        $pageS3Url: String!,
        $sessionId: String!,
        $originCountry: [String!]!,
        $originRegion: [String!]!,
        $originFarm: [String!]!,
        $producer: [String!]!,
        $washingStation: [String!]!,
        $varieties: [String!]!,
        $processes: [String!]!,
        $flavorNotes: [String!]!,
        $altitude: [Int!]!
    ) {
    adminCreateRoastedBean(
        input: {
        metadata: {
            crawlingSessionId: $sessionId,
            crawledPageUrl: $pageS3Url
        },
        roasterId: $roaster,
        imageUrl: $imageUrl,
        pageUrl: $pageUrl,
        name: $name,
        originCountry: $originCountry,
        originRegion: $originRegion,
        originFarm: $originFarm,
        producer: $producer,
        washingStation: $washingStation,
        varieties: $varieties,
        processes: $processes,
        flavorNotes: $flavorNotes,
        altitude: $altitude
        }
    ) {
        id
    }
    }
""")

client = Client(transport=transport, fetch_schema_from_transport=False)
dynamodb = boto3.client("dynamodb")
logging.basicConfig(level=os.getenv("LOG_LEVEL", logging.WARNING))

class DownloadedPageNotFound(Exception):
    pass


class LLMFailedException(Exception):
    pass


class InvalidBucketException(Exception):
    pass


def download_gz_content(url_str):
    url = urlparse(url_str)
    if url.hostname != os.environ["PAGE_S3_BUCKET"] or url.scheme != "s3":
        logging.error(f"invalid url received: {url}")
        raise InvalidBucketException()
    key = url.path.removeprefix("/")
    logging.info(f"downloading from S3 {key}")
    try:
        resp = s3_client.get_object(Bucket=os.environ["PAGE_S3_BUCKET"], Key=key)
        with gzip.GzipFile(fileobj=resp["Body"]) as gz:
            for line in gz:
                yield json.loads(line)
    except Exception as err:
        logging.error(f"failed to download {key}: {err}")
        raise DownloadedPageNotFound()


def download_content(object_key):
    logging.info(f"downloading from S3 {object_key}")
    try:
        resp = s3_client.get_object(Bucket=os.environ["PAGE_S3_BUCKET"], Key=object_key)
        return json.load(resp["Body"])
    except Exception as err:
        logging.error(f"failed to download {object_key}: {err}")
        raise DownloadedPageNotFound()


def b64_decode(encoded_value):
    return base64.b64decode(encoded_value).decode("utf-8")


def create_bean(export_rule, parsed, s3Url, session_id):
    llm_parsed = None
    encoded = parsed.get("content")
    if encoded is None:
        raise Exception(f"no content found")
    logging.info(f"will import {parsed.get('title')} from session {session_id}")
    try:
        content = b64_decode(encoded)
        if content is not None:
            resp = llm.parse(parsed.get("product_url"), content)
            if len(resp) > 0:
                llm_parsed = validate(
                    resp[0]
                )
            else:
                logging.error(f"no llm parsed found for {parsed.get('title')} from session {session_id}")
                raise LLMFailedException()
    except Exception as err:
        logging.error(f"llm parsing failed: {err}")
        raise LLMFailedException()
    req = GraphQLRequest(
        import_bean,
        variable_values={
            "roaster": export_rule["roasterId"],
            "imageUrl": parsed.get("image_url"),
            "pageUrl": parsed.get("product_url"),
            "name": parsed["title"],
            "pageS3Url": s3Url,
            "sessionId": session_id,
            "originCountry": llm_parsed.get("origin_countries", []),
            "originRegion": llm_parsed.get("origin_regions", []),
            "originFarm": llm_parsed.get("origin_farms", []),
            "producer": llm_parsed.get("coffee_producers", []),
            "washingStation": llm_parsed.get("origin_washing_station", []),
            "varieties": llm_parsed.get("varieties", []),
            "processes": llm_parsed.get("processes", []),
            "flavorNotes": llm_parsed.get("tasting_notes", []),
            "altitude": llm_parsed.get("altitude", []),
        },
    )

    out = client.execute(req)
    created_id = out["adminCreateRoastedBean"]["id"]
    expires_at = int((datetime.now(datetime.UTC) + timedelta(days=30)).timestamp())
    dynamodb.put_item(
        TableName=os.environ["STATE_DDB"],
        Item={
            "pk": {"S": parsed.get("product_url")},
            "createdAt": {"S": datetime.now().isoformat()},
            "sessionId": {"S": session_id},
            "exportedId": {"S": created_id},
            "ttl": {"N": str(expires_at)},
        },
    )
    logging.info(f"exported bean {parsed.get('title')} from session {session_id}")


def s3_file_handler(s3_url):
    session_id = s3_url.split("/")[-2]
    cached_rules = {}
    for item in download_gz_content(s3_url):
        prediction = item.get("predicted_category")
        if prediction == "roasted-beans":
            try:
                if item["host"] not in cached_rules:
                    cached_rules[item["host"]] = download_content(
                        f"exports/v1/{item['host']}.json"
                    )
                export_rule = cached_rules[item["host"]]
                if not export_rule["active"]:
                    continue
                try:
                    resp = dynamodb.get_item(
                        TableName=os.environ["STATE_DDB"],
                        Key={"pk": {"S": item.get("product_url")}},
                    )
                    if "Item" not in resp:
                        create_bean(export_rule, item, s3_url, session_id)
                except LLMFailedException:
                    raise LLMFailedException()
                except Exception as err:
                    logging.error(f"failed to export bean {item.get('title')} from session {session_id}: {err}")
            except DownloadedPageNotFound:
                pass


def lambda_handler(event, context):
    for record in event["Records"]:
        message = json.loads(record["body"])
        for record in message["Records"]:
            object_key = unquote(record["s3"]["object"]["key"])
            bucket_name = record["s3"]["bucket"]["name"]
            s3_file_handler(f"s3://{bucket_name}/{object_key}")


if __name__ == "__main__":
    s3_file_handler(
        "s3://fugue-crawler-s3bucket-wfpbhlliaf63/parsed/v3/01K4AA8SY2F9922RJ50AXZH16F/2025-09-04T12-12-48.679772+00-00-00001.json.gz"
    )
