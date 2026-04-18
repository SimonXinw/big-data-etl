"""兼容旧测试路径：Shopify Admin 配置别名与客户标签粗映射。

订单同步请使用 `etl_project.integrations.shopify.admin.wide_sync.sync_shopify_orders_incremental`（宽表 UPSERT）。
"""

from __future__ import annotations

from typing import Any

from lib.shopify.config import ShopifyAdminConfig, read_shopify_admin_config_from_env


def read_shopify_config_from_env() -> ShopifyAdminConfig:
    """从环境变量读取 Shopify Admin 配置（委托 `lib.shopify.config`）。"""

    return read_shopify_admin_config_from_env()


def _normalize_customer_level(tags: Any) -> str:
    """把 Shopify tags 粗映射到学习项目里的 customer_level。"""

    def _tags_as_string(raw: Any) -> str | None:
        if raw is None:
            return None

        if isinstance(raw, list):
            joined = ",".join(str(item).strip() for item in raw if str(item).strip())
            return joined or None

        text = str(raw).strip()
        return text or None

    raw = _tags_as_string(tags)
    if not raw:
        return "standard"

    lowered = raw.lower()
    if "vip" in lowered:
        return "vip"
    if "gold" in lowered:
        return "gold"
    if "silver" in lowered:
        return "silver"
    if "bronze" in lowered:
        return "bronze"

    first = next((part.strip() for part in raw.split(",") if part.strip()), "")
    if not first:
        return "standard"

    return first.lower().replace(" ", "_")
