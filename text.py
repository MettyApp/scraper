import hashlib
import json
import re
import unicodedata

normalizers = [
    # ['’', '\''],
    # ['&#8217;', '\''],
    # ["&gt;", ">"],
    ["&nbsp;", " "],
    # ['&#39;', '\''],
    # ['&quot;', '\''],
    ["&amp;", "&"],
    ["&euro;", "€"],
]


def md5(value):
    return hashlib.md5(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def strip_accents(s):
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def dedup_newlines(s):
    return "\n".join(line.strip() for line in s.split("\n") if len(line.strip()) > 0)


def normalize_str(s):
    for n in normalizers:
        s = s.replace(n[0], n[1])
    return s


def degrade_string_to_first_word(s, limit=1):
    s = (
        s.strip()
        .replace("\n", " ")
        .replace("\t", " ")
        .replace('"', " ")
        .replace("'", " ")
        .split(" ")
    )
    limit = min(len(s), limit)
    return " ".join(s[0:limit]).replace(" ", "").upper().strip()


def fix_space(txt):
    r = re.sub(r"([A-Z]+)([A-Z][a-z]+)", r"\1\n\2", txt)
    r = re.sub(r"([a-z])([A-Z]+)", r"\1\n\2", r)
    r = re.sub(r"([a-zA-Z]+\.)([a-zA-Z]+)", r"\1\n\2", r)
    r = re.sub(r"([0-9]+)([a-zA-Z])", r"\1 \2", r)
    r = re.sub(r"([a-zA-Z]+\.)([a-zA-Z]+)", r"\1\n\2", r)
    r = re.sub(r":([a-zA-Z]+)", r": \1", r)
    r = re.sub(r"\.([A-ZÉÈÀ])", r".\n\1", r)
    r = re.sub(r"([^.:])(\s)\s+([A-Z])", r"\1\2\3", r)
    r = re.sub(r"\s+\n", "", r)
    r = re.sub(r"(\s){2,}", r"\1", r)
    r = re.sub(r"([a-z])\.([A-Z])", r"\1.\n\2", r)
    r = re.sub(r"([a-zA-Z])([0-9])", r"\1 \2", r)
    return r
