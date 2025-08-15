import json
import logging
import scrapy

from scraper.lib.utils import b64, shrink_html
from scraper.lib.woocommerce_model import (
    Product,
    ProductCategory,
    ProductMedia,
    ProductTag,
)


class WoocommerceScraper:
    categories_map = {}
    medias_map = {}
    tags_map = {}

    def __init__(self, host) -> None:
        self.host = host

    def paginate_type(
        self, response, base_url, item_cb, finished_cb, page=1, per_page=100
    ):
        # logging.error("{}: page={}".format(base_url, page))
        if response is not None:
            try:
                data = json.loads(response.text)
                if data:
                    yield from [item_cb(x) for x in data]
                if not data or len(data) == 0 or len(data) < per_page:
                    if finished_cb:
                        yield from finished_cb()
                    return

            except json.JSONDecodeError as err:
                if len(response.text) == 0 and per_page >= 5:
                    logging.warning(
                        f"server returned an empty page: reducing per_page size to {per_page / 2}"
                    )
                    yield scrapy.Request(
                        url=f"{base_url}?page={page - 1}&per_page={per_page / 2}",
                        callback=self.paginate_type,
                        cb_kwargs={
                            "base_url": base_url,
                            "item_cb": item_cb,
                            "finished_cb": finished_cb,
                            "page": page,
                            "per_page": per_page / 2,
                        },
                    )
                else:
                    logging.error(
                        f"failed to decode response: {base_url} status={response.status}: {err}"
                    )
                return
        yield scrapy.Request(
            url=f"{base_url}?page={page}&per_page={per_page}",
            callback=self.paginate_type,
            cb_kwargs={
                "base_url": base_url,
                "item_cb": item_cb,
                "finished_cb": finished_cb,
                "page": page + 1,
                "per_page": per_page,
            },
        )

    def add_category(self, item):
        category = ProductCategory.from_json(item)
        self.categories_map[category.id] = category.name

    def add_tag(self, item):
        tag = ProductTag.from_json(item)
        self.tags_map[tag.id] = tag.name

    def add_media(self, item):
        tag = ProductMedia.from_json(item)
        self.medias_map[tag.id] = tag.guid.rendered

    def start(self):
        yield from self.paginate_type(
            None,
            f"https://{self.host}/wp-json/wp/v2/product_cat",
            self.add_category,
            self.start_tags,
        )

    def start_tags(self):
        yield from self.paginate_type(
            None,
            f"https://{self.host}/wp-json/wp/v2/product_tag",
            self.add_tag,
            self.start_medias,
        )

    def start_medias(self):
        yield from self.paginate_type(
            None,
            f"https://{self.host}/wp-json/wp/v2/media",
            self.add_media,
            self.start_products,
        )

    def start_products(self):
        yield from self.paginate_type(
            None, f"https://{self.host}/wp-json/wp/v2/product", self.parse_product, None
        )

    def parse_product(self, data):
        product = Product.from_json(data)
        item = {
            "backend": "woocommerce",
        }
        item["id"] = product.guid.rendered
        item["title"] = product.title.rendered
        item["product_url"] = product.link
        image = product.featured_media or product.links.wp_featuredmedia
        item["image_url"] = self.medias_map.get(image)

        if product.product_cat:
            item["categories"] = [
                self.categories_map.get(cat, None)
                for cat in product.product_cat
                if cat in self.categories_map
            ]
        else:
            item["categories"] = []

        if product.product_tag:
            item["tags"] = [
                self.tags_map.get(tag, None)
                for tag in product.product_tag
                if tag in self.tags_map
            ]
        else:
            item["tags"] = []

        return scrapy.Request(
            url=item["product_url"],
            callback=self.load_product_options,
            cb_kwargs={
                "product": item,
            },
        )

    def load_product_options(self, response, product):
        options = response.css('[id^="pa_"]::attr(id)').getall()
        yield {
            "content": b64(shrink_html(response.css("body").get())),
            "options": list([e.removeprefix("pa_") for e in options]),
            **product,
        }
