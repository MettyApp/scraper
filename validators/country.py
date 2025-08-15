import json
import re
from text import strip_accents


class CountryValidator:
    producers = []

    def __init__(self):
        with open("origin_regions.json", "r") as r:
            self.config = json.load(r)

    def prepare_str(self, s):
        return strip_accents(s).lower().strip()

    def validate(self, elt):
        elt = self.prepare_str(elt)
        for country in self.config:
            if len(country.get("keys", [])) > 0:
                r = re.compile(
                    r"\b(" + "|".join(country.get("keys", [])) + r")\b", re.IGNORECASE
                )
                if (
                    r.search(strip_accents(elt))
                    or elt == country.get("country").lower()
                ):
                    return (country.get("country"),)
