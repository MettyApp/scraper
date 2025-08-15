import logging
from urllib.parse import urlparse
import scrapy
import json
from scrapy.spiders import SitemapSpider
from scraper.lib.shopify import ShopifyScraper
from scraper.lib.woocommerce import WoocommerceScraper
from scraper.lib.prestashop import PrestaShopScraper


def get_backend(html):
    matchers = [
        (
            "shopify",
            (
                "shopify",
                "web-pixels-manager-setup",
            ),
        ),
        ("woocommerce", ("woocommerce",)),
        ("prestashop", ("prestashop",)),
        ("wix", ("wix",)),
        ("bigcartel", ("bigcartel",)),
        ("magento", ("magento",)),
    ]

    backend = "Custom"
    for name, keywords in matchers:
        for keyword in keywords:
            if keyword in html:
                return name
    return backend


class ProductsSpider(SitemapSpider):
    name = "products"
    roasters_urls = []
    domains = ""
    backends = ""

    sitemap_rules = [
        ("/.*/", "parse_html_from_sitemap"),
    ]

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        with open("rules.json", "r") as f:
            self.rules = json.load(f)

        if len(self.domains) == 0:
            with open("roasters.json", "r") as f:
                self.roasters_urls = list(
                    [f"https://{urlparse(u).hostname}/" for u in json.load(f)]
                )
        else:
            self.roasters_urls = [
                f"https://{urlparse(d).hostname or d.strip()}/"
                for d in self.domains.split(",")
            ]
        logging.info(f"urls loaded: {json.dumps(self.roasters_urls)}")

    async def start(self):
        logging.info("spider starting")
        async for item_or_request in super().start():
            yield item_or_request
        for url in self.roasters_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        backend = get_backend(response.text)
        if len(self.backends) == 0 or any(
            b in backend for b in self.backends.split(",")
        ):
            if backend == "shopify":
                yield from ShopifyScraper().start(urlparse(response.url).hostname)
            if backend == "woocommerce":
                yield from WoocommerceScraper(urlparse(response.url).hostname).start()
            if backend == "prestashop":
                host = urlparse(response.url).hostname
                rules = self.rules.get(host, {})
                yield from PrestaShopScraper(self._parse_sitemap).start(
                    host, rules, response
                )

    def parse_html_from_sitemap(self, response):
        yield from PrestaShopScraper(self._parse_sitemap).parse_product(response)
