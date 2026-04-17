"""Shopify Storefront API GraphQL 客户端（与 Admin `/admin/api/...` 独立）。"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import requests

from lib.shopify.errors import ShopifyConfigError, ShopifyGraphQLError


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
