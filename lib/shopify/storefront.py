"""Shopify Storefront API：配置、GraphQL 客户端与商品目录分页遍历。"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, cast

import requests

from lib.shopify.errors import ShopifyConfigError, ShopifyGraphQLError
from lib.shopify.queries.products import STOREFRONT_PRODUCTS_FULL_PAGE_QUERY

# Storefront 单次 `products` 连接最大 first 一般为 250
_DEFAULT_PAGE_SIZE = 250
_DEFAULT_MAX_PAGES = 200


@dataclass(frozen=True)
class ShopifyStorefrontConfig:
    """Storefront GraphQL：`https://{shop}/api/{version}/graphql.json`。"""

    store_domain: str
    storefront_access_token: str
    api_version: str


def read_storefront_config_from_env() -> ShopifyStorefrontConfig:
    """从环境变量读取店面 API 配置（`SHOPIFY_STORE_DOMAIN` + `SHOPIFY_STOREFRONT_ACCESS_TOKEN`）。"""

    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN", "").strip()
    token = os.getenv("SHOPIFY_STOREFRONT_ACCESS_TOKEN", "").strip()
    api_version = (
        os.getenv("SHOPIFY_STOREFRONT_API_VERSION", "").strip()
        or os.getenv("SHOPIFY_API_VERSION", "").strip()
        or "2025-10"
    )

    if not store_domain or not token:
        expected_env = Path(__file__).resolve().parent.parent.parent / ".env"
        raise ShopifyConfigError(
            "Storefront 探测需要 SHOPIFY_STORE_DOMAIN 与 SHOPIFY_STOREFRONT_ACCESS_TOKEN。"
            f"（期望 .env: {expected_env}，存在: {expected_env.is_file()}）"
        )

    normalized = store_domain.lower().replace("https://", "").replace("http://", "").strip("/")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*\.myshopify\.com", normalized):
        raise ShopifyConfigError(
            "SHOPIFY_STORE_DOMAIN 应为 your-store.myshopify.com（不要带协议与路径）。"
        )

    return ShopifyStorefrontConfig(
        store_domain=normalized,
        storefront_access_token=token,
        api_version=api_version,
    )


class ShopifyStorefrontClient:
    """对 `https://{shop}/api/{version}/graphql.json` 的薄封装。"""

    def __init__(self, config: ShopifyStorefrontConfig) -> None:
        self._config = config

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"https://{self._config.store_domain}/api/{self._config.api_version}/graphql.json"
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Storefront-Access-Token": self._config.storefront_access_token,
                "Accept": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=120,
        )

        if response.status_code >= 400:
            raise ShopifyGraphQLError(
                f"Storefront HTTP {response.status_code}: {response.text[:800]}"
            )

        payload = response.json()
        if not isinstance(payload, dict):
            raise ShopifyGraphQLError("Storefront 响应不是 JSON 对象。")

        errors = payload.get("errors")
        if errors:
            raise ShopifyGraphQLError(f"Storefront GraphQL errors: {errors!s}"[:2000])

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ShopifyGraphQLError("Storefront 响应缺少 data 字段。")

        return cast(dict[str, Any], data)


def iter_storefront_product_nodes(
    client: ShopifyStorefrontClient,
    *,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_pages: int = _DEFAULT_MAX_PAGES,
) -> Iterator[dict[str, Any]]:
    """分页遍历店面全部商品，逐个 yield GraphQL `node` 字典。"""

    after: str | None = None
    pages = 0

    while pages < max_pages:
        pages += 1
        data = client.execute(
            STOREFRONT_PRODUCTS_FULL_PAGE_QUERY,
            {"first": page_size, "after": after},
        )
        products = data.get("products")
        if not isinstance(products, dict):
            break

        edges = products.get("edges")
        if not isinstance(edges, list):
            break

        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if isinstance(node, dict):
                yield node

        page_info = products.get("pageInfo")
        has_next = False
        end_cursor: str | None = None
        if isinstance(page_info, dict):
            has_next = bool(page_info.get("hasNextPage"))
            raw_cursor = page_info.get("endCursor")
            end_cursor = raw_cursor if isinstance(raw_cursor, str) else None

        batch = len(edges)
        if not has_next or batch == 0:
            break

        after = end_cursor
        if not after:
            break
