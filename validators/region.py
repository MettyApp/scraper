import json
import re

from text import strip_accents


class RegionValidator:
    def key(self):
        return "COFFEE_ORIGIN_REGION"

    def __init__(self):
        with open("origin_regions.json", "r") as r:
            self.config = json.load(r)

    def prepare_str(self, s):
        return strip_accents(s).lower().strip()

    def validate(self, elt):
        elt = self.prepare_str(elt)
        for country in self.config:
            for region in country.get("region", []):
                if len(region.get("keys", [])) > 0:
                    r = re.compile(
                        r"\b(" + "|".join(region.get("keys", [])) + r")\b",
                        re.IGNORECASE,
                    )
                    if r.search(strip_accents(elt)) or elt == self.prepare_str(
                        region.get("name")
                    ):
                        return (country.get("country"), region.get("name"))
        return None, elt

    def find(self, candidate, truth):
        out = []
        elt = self.prepare_str(candidate)
        for country in self.config:
            for region in country.get("region", []):
                for key in region.get("keys", []):
                    if self.prepare_str(key) in elt:
                        if region.get("name") in truth:
                            pos = strip_accents(candidate.lower()).find(
                                self.prepare_str(key)
                            )
                            if pos > -1:
                                text = candidate[pos : pos + len(self.prepare_str(key))]
                                out.append(
                                    {
                                        "text": text,
                                        "start_token": pos,
                                        "end_token": pos + len(self.prepare_str(key)),
                                    }
                                )
                                break
        return out
