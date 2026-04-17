"""lib.shopify 通用客户端与分页逻辑自测。"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from lib.shopify.client import ShopifyAdminClient
from lib.shopify.config import ShopifyAdminConfig
from lib.shopify.errors import ShopifyGraphQLError
from lib.shopify.pager import iter_connection_nodes


class ShopifyLibTestCase(unittest.TestCase):
    """验证 GraphQL execute 与 connection 分页。"""

    def test_execute_rejects_mutation_before_http(self) -> None:
        config = ShopifyAdminConfig(
            store_domain="demo.myshopify.com",
            access_token="token",
            api_version="2024-10",
        )
        client = ShopifyAdminClient(config)

        with patch("lib.shopify.client.requests.post") as mock_post:
            with self.assertRaises(ShopifyGraphQLError):
                client.execute(
                    "\n  mutation Fake($id: ID!) { productDelete(input: {id: $id}) { deletedProductId } }",
                    {"id": "x"},
                )

        mock_post.assert_not_called()

    def test_execute_raises_on_graphql_errors(self) -> None:
        config = ShopifyAdminConfig(
            store_domain="demo.myshopify.com",
            access_token="token",
            api_version="2024-10",
        )
        client = ShopifyAdminClient(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"errors": [{"message": "boom"}]}

        with patch("lib.shopify.client.requests.post", return_value=mock_response):
            with self.assertRaises(ShopifyGraphQLError):
                client.execute("query { shop { name } }", {})

    def test_iter_connection_nodes_pages_until_end(self) -> None:
        config = ShopifyAdminConfig(
            store_domain="demo.myshopify.com",
            access_token="token",
            api_version="2024-10",
        )
        client = ShopifyAdminClient(config)

        first_payload = {
            "customers": {
                "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                "edges": [{"node": {"legacyResourceId": "1"}}],
            }
        }
        second_payload = {
            "customers": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "edges": [{"node": {"legacyResourceId": "2"}}],
            }
        }

        with patch.object(ShopifyAdminClient, "execute", side_effect=[first_payload, second_payload]):
            nodes = list(
                iter_connection_nodes(
                    client,
                    "query CustomersPage($first: Int!, $after: String) { customers { ... } }",
                    "customers",
                    page_size=2,
                )
            )

        self.assertEqual([n.get("legacyResourceId") for n in nodes], ["1", "2"])


if __name__ == "__main__":
    unittest.main()
