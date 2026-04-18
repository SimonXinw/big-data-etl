"""Shopify Admin / Storefront GraphQL 冒烟测试（验证凭据与 API 版本）。"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv


def run_smoke_cli() -> int:
    """返回 0=Admin 成功；Storefront 为可选，失败仅打印不影响退出码（除非 Admin 已失败）。"""

    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(project_root / ".env", encoding="utf-8-sig", override=True)

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except OSError:
            pass

    exit_code = 0

    print("—— Shopify Admin GraphQL ——")

    try:
        from lib.shopify.admin import ShopifyAdminClient
        from lib.shopify.config import read_shopify_admin_config_from_env

        admin_config = read_shopify_admin_config_from_env()
        client = ShopifyAdminClient(admin_config)
        payload = client.execute(
            "query SmokeAdmin { shop { name primaryDomain { host } } }"
        )
        shop = payload.get("shop")
        if not isinstance(shop, dict):
            raise RuntimeError("响应缺少 shop")

        name = shop.get("name")
        host = (shop.get("primaryDomain") or {}).get("host") if isinstance(shop.get("primaryDomain"), dict) else None
        print(
            f"Admin OK | api_version={admin_config.api_version} | "
            f"store_domain={admin_config.store_domain} | shop_name={name} | host={host}"
        )
    except Exception as exc:
        print(f"Admin FAIL | {exc}")
        exit_code = 1

    print("—— Shopify Storefront GraphQL（可选，需 SHOPIFY_STOREFRONT_ACCESS_TOKEN）——")

    try:
        from lib.shopify.storefront import ShopifyStorefrontClient, read_storefront_config_from_env

        storefront_config = read_storefront_config_from_env()
        sf_client = ShopifyStorefrontClient(storefront_config)
        sf_payload = sf_client.execute("query SmokeSf { shop { name } }", {})
        sf_shop = sf_payload.get("shop")
        sf_name = sf_shop.get("name") if isinstance(sf_shop, dict) else None
        print(
            f"Storefront OK | api_version={storefront_config.api_version} | "
            f"endpoint=https://{storefront_config.store_domain}/api/{storefront_config.api_version}/graphql.json | "
            f"shop_name={sf_name}"
        )
    except Exception as exc:
        print(f"Storefront SKIP/FAIL | {exc}")

    return exit_code
