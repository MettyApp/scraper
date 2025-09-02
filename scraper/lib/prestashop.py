import json
from urllib.parse import urlparse
import scrapy

from scraper.lib.utils import b64, shrink_html


class PrestaShopScraper:
    def __init__(self, _parse_sitemap):
        self._parse_sitemap = _parse_sitemap

    def start(self, host, rules=None, response=None):
        if rules and rules.get("ignore_sitemap"):
            yield from self.start_content(response)
        else:
            yield scrapy.Request(
                f"https://{host}/robots.txt", callback=self._parse_sitemap
            )

    def start_content(self, response):
        """Démarre le scraping via le contenu des pages PrestaShop"""
        link_extractor = scrapy.linkextractors.lxmlhtml.LxmlLinkExtractor(
            allow_domains=urlparse(response.url).hostname,
        )
        yield from self.parse_product(response)
        for link in link_extractor.extract_links(response):
            yield scrapy.Request(
                link.url.split("?")[0].split("#")[0], callback=self.start_content
            )

    def parse_product(self, response):
        """Parse une page produit PrestaShop"""
        # PS 1.7+ : pas de changement
        if response.css("#product-details::attr(data-product)").get():
            raw_json = response.css("#product-details::attr(data-product)").get()
            data = json.loads(raw_json) if raw_json else {}
            yield {
                "id": "prestashop1.7:"
                + urlparse(response.url).hostname
                + ":"
                + response.css("#product_page_product_id::attr(value)").get(),
                "product_url": data.get("link"),
                "product_image": response.css(".product-cover img::attr(src)").get()
                or response.css(".product-covers img::attr(src)").get(),
                "backend": "prestashop1.7",
                "title": data.get("name"),
                "options": [],
                "categories": [data.get("category_name")],
                "variants": list(
                    [
                        a.get("name")
                        for a in data.get("attributes").values()
                        if "name" in a
                    ]
                )
                if ("attributes" in data and len(data.get("attributes")) > 0)
                else [],
            }
            return

        # PS 1.6 : vérifie application/ld+json
        ld_json_raw_all = response.css(
            'script[type="application/ld+json"]::text'
        ).getall()
        ld_json_objs = []
        for raw in ld_json_raw_all:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    ld_json_objs.extend(parsed)
                else:
                    ld_json_objs.append(parsed)
            except json.JSONDecodeError:
                continue

        product_data = next(
            (o for o in ld_json_objs if o.get("@type") == "Product"), None
        )
        breadcrumb_data = next(
            (o for o in ld_json_objs if o.get("@type") == "BreadcrumbList"), None
        )

        if product_data:
            name = response.css(".product_name::text").get()
            if breadcrumb_data and isinstance(
                breadcrumb_data.get("itemListElement"), list
            ):
                categories = [
                    i.get("name")
                    for i in breadcrumb_data["itemListElement"]
                    if isinstance(i, dict) and i.get("name")
                ]
            else:
                categories = response.css(".breadcrumb a::text").getall()
                categories = [c.strip() for c in categories if c.strip()]

            yield {
                "id": "prestashop1.6:"
                + urlparse(response.url).hostname
                + ":"
                + response.css("#product_page_product_id::attr(value)").get(),
                "product_url": response.url,
                "product_image": response.css(".product-cover img::attr(src)").get(),
                "backend": "prestashop1.6",
                "options": [],
                "title": name,
                "content": b64(shrink_html(response.css("body").get())),
                "categories": categories,
                "variants": response.css(
                    ".product-variants .control-label::text"
                ).getall(),
            }
