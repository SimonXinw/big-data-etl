"""将店面全部商品导出为项目 `data/` 下的 JSON（便于检视结构或下游处理）。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from etl_project.config import ProjectPaths, build_paths
from lib.shopify.storefront import (
    ShopifyStorefrontClient,
    iter_storefront_product_nodes,
    read_storefront_config_from_env,
)


@dataclass(frozen=True)
class StorefrontProductsExportSummary:
    """导出结果摘要。"""

    output_path: Path
    product_count: int
    store_domain: str
    storefront_api_version: str


def export_storefront_products_json(
    *,
    paths: ProjectPaths | None = None,
    output_path: Path | None = None,
) -> StorefrontProductsExportSummary:
    """分页拉取店面全部商品并写入 JSON。

    默认写入 `{project_root}/data/products/storefront_products.json`（见 `ProjectPaths.storefront_products_json_file`）。
    """

    resolved_paths = paths or build_paths()
    target = output_path or resolved_paths.storefront_products_json_file
    target.parent.mkdir(parents=True, exist_ok=True)

    config = read_storefront_config_from_env()
    client = ShopifyStorefrontClient(config)
    products: list[dict[str, Any]] = []
    for node in iter_storefront_product_nodes(client):
        products.append(node)

    payload = {
        "meta": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "store_domain": config.store_domain,
            "storefront_api_version": config.api_version,
            "product_count": len(products),
        },
        "products": products,
    }

    with open(target, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    return StorefrontProductsExportSummary(
        output_path=target,
        product_count=len(products),
        store_domain=config.store_domain,
        storefront_api_version=config.api_version,
    )


def run_export_cli() -> int:
    """命令行入口：打印摘要并返回退出码。"""

    try:
        summary = export_storefront_products_json()
    except Exception as exc:  # noqa: BLE001 — CLI 面向人工可读输出
        print(f"导出失败: {exc}")
        return 1

    print(
        f"已写入: {summary.output_path.as_posix()} "
        f"(商品数={summary.product_count}, 店面={summary.store_domain}, "
        f"Storefront API={summary.storefront_api_version})"
    )
    return 0
