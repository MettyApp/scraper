import json
from urllib.parse import urlparse


def lambda_handler(event, context):
    with open("roasters.json", "r") as r:
        roasters = json.load(r)
        domains = [urlparse(url).hostname for url in roasters]
        return {"domains": list(set(domains))}


if __name__ == "__main__":
    print(lambda_handler(None, None))
