"""Shopify Admin 连接配置（从环境变量读取）。"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from lib.shopify.errors import ShopifyConfigError


@dataclass(frozen=True)
class ShopifyAdminConfig:
    """Admin GraphQL 所需的最小配置。"""

    store_domain: str
    access_token: str
    api_version: str


def read_shopify_admin_config_from_env() -> ShopifyAdminConfig:
    """读取 `SHOPIFY_STORE_DOMAIN`、`SHOPIFY_ACCESS_TOKEN` 等环境变量。"""

    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN", "").strip()
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10").strip() or "2024-10"

    if not store_domain or not access_token:
        raise ShopifyConfigError(
            "需要设置环境变量 SHOPIFY_STORE_DOMAIN 与 SHOPIFY_ACCESS_TOKEN（Admin API access token）。"
        )

    normalized_domain = store_domain.lower().replace("https://", "").replace("http://", "").strip("/")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*\.myshopify\.com", normalized_domain):
        raise ShopifyConfigError(
            "SHOPIFY_STORE_DOMAIN 应为类似 your-store.myshopify.com 的店铺主域名（不要带协议与路径）。"
        )

    return ShopifyAdminConfig(
        store_domain=normalized_domain,
        access_token=access_token,
        api_version=api_version,
    )
