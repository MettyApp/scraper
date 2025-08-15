import re
from text import strip_accents


class ProcessesValidator:
    config = [
        ["YEAST", "yeast", "levu"],
        ["CARBONIC", "carboni", "maceratio"],
        ["ANAEROBIC", "anaerobique"],
        ["KOJI", "koji"],
        ["HONEY", "hon"],
        ["NATURAL", "natur"],
        ["WASHED", "lav"],
    ]
    regexp = re.compile(
        r"((?:carboni|koji|lavado|lavato|washed|anaerobi(?:c|que)|natur[ea]l|naturelle|lavee?|honey|yeast|levure|wet|hulled|honey|carboni(?:c|que)))",
        re.IGNORECASE,
    )

    def key(self):
        return "PROCESSING"

    def validate(self, elt):
        items = set(
            [
                v.lower().strip()
                for v in self.regexp.findall(strip_accents(elt))
                if len(v) > 0
            ]
        )
        for keys in self.config:
            for key in keys:
                for elt in items:
                    if key.lower() in elt:
                        return keys[0]

    def find(self, candidate, truth=None):
        matches = []
        candidate_clean = strip_accents(candidate)

        for match in re.finditer(self.regexp, candidate_clean):
            match_info = {
                "text": match.group(0),
                "start_token": match.start(),
                "end_token": match.end(),
            }
            items = set(
                [
                    v.lower().strip()
                    for v in self.regexp.findall(strip_accents(candidate))
                    if len(v) > 0
                ]
            )
            for keys in self.config:
                for key in keys:
                    for elt in items:
                        if (
                            key.lower() in elt
                            and (truth is None or keys[0] in truth)
                            and match_info not in matches
                        ):
                            matches.append(match_info)
                            break

        return matches
