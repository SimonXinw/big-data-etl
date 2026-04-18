"""lib.shopify Admin 客户端自测。"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from lib.shopify.admin import ShopifyAdminClient
from lib.shopify.config import ShopifyAdminConfig
from lib.shopify.errors import ShopifyGraphQLError


class ShopifyLibTestCase(unittest.TestCase):
    """验证 GraphQL execute。"""

    def test_execute_rejects_mutation_before_http(self) -> None:
        config = ShopifyAdminConfig(
            store_domain="demo.myshopify.com",
            access_token="token",
            api_version="2024-10",
        )
        client = ShopifyAdminClient(config)

        with patch("lib.shopify.admin.requests.post") as mock_post:
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

        with patch("lib.shopify.admin.requests.post", return_value=mock_response):
            with self.assertRaises(ShopifyGraphQLError):
                client.execute("query { shop { name } }", {})


if __name__ == "__main__":
    unittest.main()
