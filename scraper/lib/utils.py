import base64

from bs4 import BeautifulSoup


def b64(value):
    base64_encoded = base64.b64encode(value.encode("utf-8"))
    result = base64_encoded.decode("utf-8")
    return result


def shrink_html(html):
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(
        [
            "style",
            "svg",
        ]
    ):
        tag.extract()
    return str(soup)
