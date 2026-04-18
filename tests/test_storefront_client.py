"""Storefront API 客户端自测。"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from lib.shopify.errors import ShopifyConfigError, ShopifyGraphQLError
from lib.shopify.storefront import (
    ShopifyStorefrontClient,
    ShopifyStorefrontConfig,
    read_storefront_config_from_env,
)


class StorefrontClientTestCase(unittest.TestCase):
    """验证 Storefront 配置与 HTTP 封装。"""

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "demo.myshopify.com",
            "SHOPIFY_STOREFRONT_ACCESS_TOKEN": "sf-token",
            "SHOPIFY_STOREFRONT_API_VERSION": "2025-10",
        },
        clear=False,
    )
    def test_read_storefront_config_from_env(self) -> None:
        cfg = read_storefront_config_from_env()
        self.assertEqual(cfg.store_domain, "demo.myshopify.com")
        self.assertEqual(cfg.storefront_access_token, "sf-token")
        self.assertEqual(cfg.api_version, "2025-10")

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "",
            "SHOPIFY_STOREFRONT_ACCESS_TOKEN": "",
        },
        clear=False,
    )
    def test_read_storefront_config_missing_raises(self) -> None:
        with self.assertRaises(ShopifyConfigError):
            read_storefront_config_from_env()

    def test_execute_success(self) -> None:
        cfg = ShopifyStorefrontConfig(
            store_domain="demo.myshopify.com",
            storefront_access_token="t",
            api_version="2025-10",
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {
                "products": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [{"node": {"id": "1", "title": "A", "handle": "a"}}],
                }
            }
        }

        with patch("lib.shopify.storefront.requests.post", return_value=mock_resp) as mock_post:
            client = ShopifyStorefrontClient(cfg)
            data = client.execute("query { __typename }", {})

        self.assertIn("products", data)
        mock_post.assert_called_once()
        call_kw = mock_post.call_args.kwargs
        self.assertIn("X-Shopify-Storefront-Access-Token", call_kw["headers"])
        self.assertEqual(
            call_kw["headers"]["X-Shopify-Storefront-Access-Token"],
            "t",
        )

    def test_execute_http_error_raises(self) -> None:
        cfg = ShopifyStorefrontConfig(
            store_domain="demo.myshopify.com",
            storefront_access_token="t",
            api_version="2025-10",
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "unauthorized"

        with patch("lib.shopify.storefront.requests.post", return_value=mock_resp):
            client = ShopifyStorefrontClient(cfg)
            with self.assertRaises(ShopifyGraphQLError):
                client.execute("query { __typename }", {})
