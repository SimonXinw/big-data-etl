"""Shopify Admin 连接配置（从环境变量读取）。"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from lib.shopify.errors import ShopifyConfigError


@dataclass(frozen=True)
class ShopifyAdminConfig:
    """Admin GraphQL 所需的最小配置。"""

    store_domain: str
    access_token: str
    api_version: str
    storefront_access_token: str | None = None


def _admin_access_token_from_env() -> str:
    """优先 `SHOPIFY_ADMIN_ACCESS_TOKEN`，兼容旧名 `SHOPIFY_ACCESS_TOKEN`。"""

    admin = os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN", "").strip()
    if admin:
        return admin

    return os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()


def read_shopify_admin_config_from_env() -> ShopifyAdminConfig:
    """读取 `SHOPIFY_STORE_DOMAIN`、`SHOPIFY_ADMIN_ACCESS_TOKEN` 等环境变量。"""

    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN", "").strip()
    access_token = _admin_access_token_from_env()
    api_version = (
        os.getenv("SHOPIFY_ADMIN_API_VERSION", "").strip()
        or os.getenv("SHOPIFY_API_VERSION", "2025-04").strip()
        or "2025-04"
    )
    storefront = os.getenv("SHOPIFY_STOREFRONT_ACCESS_TOKEN", "").strip() or None

    if not store_domain or not access_token:
        expected_env = Path(__file__).resolve().parent.parent.parent / ".env"
        raise ShopifyConfigError(
            "需要设置环境变量 SHOPIFY_STORE_DOMAIN 与 SHOPIFY_ADMIN_ACCESS_TOKEN（Admin API access token，旧名 SHOPIFY_ACCESS_TOKEN 仍兼容）。"
            f" 程序会从项目根目录加载 .env（期望路径: {expected_env}，文件存在: {expected_env.is_file()}）。"
            " 请确认键名完全一致、等号两侧无多余空格、值为 UTF-8；店铺域名形如 your-store.myshopify.com。"
            " 说明：SHOPIFY_STOREFRONT_ACCESS_TOKEN 为 Storefront API，本仓库 Admin GraphQL 拉数不使用该值。"
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
        storefront_access_token=storefront,
    )
