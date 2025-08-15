import json
import scrapy

from scraper.lib.utils import b64, shrink_html


class ShopifyScraper:
    graphql_query = """
    query getProducts($cursor: String) {
      products(first: 100, after: $cursor) {
        edges {
          node {
            id
            title
            handle
            productType
            descriptionHtml
            tags
            images(first: 1) {
              nodes {
                url
              }
            }
            category {
              name
            }
            options(first: 10) {
              name
            }
            variants(first: 10) {
              nodes {
                title
              }
            }
            collections(first: 10) {
              edges {
                node {
                  title
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
"""

    def start(self, host):
        url = f"https://{host}/api/2025-07/graphql.json"
        payload = {"query": self.graphql_query, "variables": {"cursor": None}}
        yield scrapy.Request(
            url,
            method="POST",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            callback=self.parse_shopify_products,
            cb_kwargs={"host": host},
            meta={"download_slot": "shopify"},
        )

    def parse_product(self, response, host):
        for edge in response.get("edges", []):
            node = edge.get("node", {})
            yield {
                "id": node.get("id"),
                "backend": "shopify",
                "content": b64(shrink_html(node.get("descriptionHtml"))),
                "title": node.get("title"),
                "image_url": list(
                    [c["url"] for c in node.get("images", {}).get("nodes", [])]
                )[0]
                if len(node.get("images", {}).get("nodes", [])) > 0
                else None,
                "product_url": f"https://{host}/products/{node.get('handle')}",
                "variants": (
                    [c["title"] for c in node.get("variants", {}).get("nodes", [])]
                    if node.get("variants")
                    else []
                ),
                "options": (
                    [c["name"] for c in node.get("options", [])]
                    if node.get("options")
                    else []
                ),
                "tags": node.get("tags", []),
                "categories": [
                    cat
                    for cat in (
                        [
                            c["node"]["title"]
                            for c in node.get("collections", {}).get("edges", [])
                        ]
                        + (
                            [node.get("category", {}).get("name")]
                            if node.get("category")
                            else []
                        )
                        + [node.get("productType")]
                    )
                    if cat is not None and len(cat) > 0
                ],
            }

    def parse_shopify_products(self, response, host):
        data = json.loads(response.text)
        products_data = data.get("data", {}).get("products", {})
        yield from self.parse_product(products_data, host)
        if products_data.get("pageInfo", {}).get("hasNextPage"):
            payload = {
                "query": self.graphql_query,
                "variables": {
                    "cursor": products_data.get("pageInfo", {}).get("endCursor")
                },
            }
            yield scrapy.Request(
                f"https://{host}/api/2025-07/graphql.json",
                method="POST",
                body=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                callback=self.parse_shopify_products,
                cb_kwargs={"host": host},
            )
