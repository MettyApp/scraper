import json
import re

from text import strip_accents


class ProducerValidator:
    unmatched_items_file = "./unmatched_producers.json"
    unmatched_items = set({})

    def __init__(self):
        with open("origin_regions.json", "r") as r:
            self.config = json.load(r)

    def prepare_str(self, s):
        return strip_accents(s).lower().strip()

    def add_unmatched(self, item):
        if item not in self.unmatched_items and len(item.split(" ")) < 5:
            self.unmatched_items.add(item)
        with open(self.unmatched_items_file, "w") as w:
            json.dump(list(self.unmatched_items), w)

    def validate(self, text, record_unmatched=True):
        elt = self.prepare_str(text)
        for country in self.config:
            for region in country.get("region", []):
                if "farms" in region:
                    for farm in region.get("farms", []):
                        if "producer" in farm:
                            producer = farm["producer"]
                            if len(producer["keys"]) > 0:
                                r = re.compile(
                                    r"\b("
                                    + "|".join(producer.get("keys", []))
                                    + r")\b",
                                    re.IGNORECASE,
                                )
                                if r.search(strip_accents(elt)) or text == producer.get(
                                    "name"
                                ):
                                    return (
                                        country.get("country"),
                                        region.get("name"),
                                        farm.get("name"),
                                        producer.get("name"),
                                    )
        if record_unmatched:
            return (None, None, None, elt)
            # self.add_unmatched(elt)
