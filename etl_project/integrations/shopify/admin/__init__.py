"""Shopify Admin API：订单宽表同步与字段映射。"""

from etl_project.integrations.shopify.admin.wide_sync import (
    ShopifyWideSyncSummary,
    rebuild_narrow_raw_tables_from_shopify,
    sync_shopify_orders_incremental,
)

__all__ = [
    "ShopifyWideSyncSummary",
    "rebuild_narrow_raw_tables_from_shopify",
    "sync_shopify_orders_incremental",
]
