import json
import pytest
from scrapy.http import Request
from scrapy.http import TextResponse
from scraper.lib.shopify import ShopifyScraper


class TestShopifyScraper:
    @pytest.fixture
    def scraper(self):
        return ShopifyScraper()

    @pytest.fixture
    def sample_host(self):
        return "example.myshopify.com"

    @pytest.fixture
    def sample_graphql_response(self):
        return {
            "data": {
                "products": {
                    "edges": [
                        {
                            "node": {
                                "id": "gid://shopify/Product/123456789",
                                "title": "Café Arabica Premium",
                                "handle": "cafe-arabica-premium",
                                "category": {"name": "test"},
                                "collections": {
                                    "edges": [
                                        {"node": {"title": "Cafés Arabica"}},
                                        {"node": {"title": "Cafés Premium"}},
                                    ]
                                },
                            }
                        },
                        {
                            "node": {
                                "id": "gid://shopify/Product/987654321",
                                "title": "Café Robusta",
                                "handle": "cafe-robusta",
                                "category": None,
                                "collections": {
                                    "edges": [{"node": {"title": "Cafés Robusta"}}]
                                },
                            }
                        },
                    ],
                    "pageInfo": {
                        "hasNextPage": True,
                        "endCursor": "eyJsYXN0X2lkIjo5ODc2NTQzMjF9",
                    },
                }
            }
        }

    @pytest.fixture
    def sample_graphql_response_last_page(self):
        return {
            "data": {
                "products": {
                    "edges": [
                        {
                            "node": {
                                "id": "gid://shopify/Product/111111111",
                                "title": "Café Dernière Page",
                                "handle": "cafe-derniere-page",
                                "collections": {"edges": []},
                            }
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }

    def test_parse_shopify_products_extracts_products(
        self, scraper, sample_host, sample_graphql_response
    ):
        response = TextResponse(
            url=f"https://{sample_host}/api/2025-07/graphql.json",
            body=json.dumps(sample_graphql_response).encode("utf-8"),
        )
        products = list(scraper.parse_shopify_products(response, sample_host))
        assert len(products) == 3
        first_product = products[0]
        assert first_product["id"] == "gid://shopify/Product/123456789"
        assert first_product["backend"] == "shopify"
        assert first_product["title"] == "Café Arabica Premium"
        assert (
            first_product["product_url"]
            == f"https://{sample_host}/products/cafe-arabica-premium"
        )
        assert first_product["categories"] == ["Cafés Arabica", "Cafés Premium", "test"]
        second_product = products[1]
        assert second_product["id"] == "gid://shopify/Product/987654321"
        assert second_product["backend"] == "shopify"
        assert second_product["title"] == "Café Robusta"
        assert (
            second_product["product_url"]
            == f"https://{sample_host}/products/cafe-robusta"
        )
        assert second_product["categories"] == ["Cafés Robusta"]

    def test_parse_shopify_products_creates_next_request_when_has_next_page(
        self, scraper, sample_host, sample_graphql_response
    ):
        response = TextResponse(
            url=f"https://{sample_host}/api/2025-07/graphql.json",
            body=json.dumps(sample_graphql_response).encode("utf-8"),
        )
        results = list(scraper.parse_shopify_products(response, sample_host))
        assert len(results) == 3
        assert all(isinstance(result, dict) for result in results[:2])
        next_request = results[2]
        assert isinstance(next_request, Request)
        payload = json.loads(next_request.body)
        expected_cursor = "eyJsYXN0X2lkIjo5ODc2NTQzMjF9"
        assert payload["variables"]["cursor"] == expected_cursor


if __name__ == "__main__":
    pytest.main([__file__])
