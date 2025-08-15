import hashlib
import json
import os
from urllib.parse import urlparse
from scrapy.exceptions import DropItem
from classifier.train import ProductClassifier


def md5(*values):
    return hashlib.md5(json.dumps(values).encode("utf-8")).hexdigest()


class EnrichItem:
    model = ProductClassifier()
    run_predictions = os.getenv("LAMBDA_TASK_ROOT") is not None

    def validate_list(self, item, key):
        if key not in item:
            return {key: []}
        return {key: list([c for c in item[key] if c and len(c) > 0])}

    def process_item(self, item, spider):
        host = urlparse(item["product_url"]).hostname
        predicted_category = "_unknown"
        if self.run_predictions:
            predicted_category = self.model.predict([item])[0]
            if predicted_category != "roasted-beans":
                raise DropItem("not roasted bean")
        out = {
            **item,
            "id": md5(item["id"], host),
            "predicted_category": predicted_category,
            "spider": spider.name,
            "host": host,
            **self.validate_list(item, "categories"),
            **self.validate_list(item, "variants"),
            **self.validate_list(item, "options"),
            **self.validate_list(item, "tags"),
        }

        if "image_url" in out and out["image_url"] is not None:
            out["image_urls"] = [item["image_url"]]

        return out
