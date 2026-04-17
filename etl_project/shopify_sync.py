"""从 Shopify 拉取订单与客户，并写入项目约定的 CSV 形态。

数据获取：通过 `lib.shopify` 的 **Admin GraphQL 只读** 能力分页拉取。
数据清洗与分层：仍由 `load.py` / `transform.py` 在 DuckDB 中完成（raw -> stg -> dw -> mart）。
报表：由导出 CSV 或 PostgreSQL + Metabase 完成，与本模块解耦。

本文件只负责「源系统 JSON -> 统一 raw 列」的映射。
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, cast

from etl_project.config import ProjectPaths
from lib.shopify.client import ShopifyAdminClient
from lib.shopify.config import ShopifyAdminConfig, read_shopify_admin_config_from_env
from lib.shopify.resource_fetchers import iter_customer_nodes, iter_order_nodes


@dataclass(frozen=True)
class ShopifySyncSummary:
    """一次同步的统计信息。"""

    customer_rows: int
    order_rows: int


_GUEST_CUSTOMER_ID_OFFSET = 5_000_000_000_000


def read_shopify_config_from_env() -> ShopifyAdminConfig:
    """从环境变量读取 Shopify 配置（委托 `lib.shopify.config`）。"""

    return read_shopify_admin_config_from_env()


def _as_int(value: Any) -> int:
    if value is None:
        return 0

    return int(value)


def _tags_as_string(tags: Any) -> str | None:
    if tags is None:
        return None

    if isinstance(tags, list):
        joined = ",".join(str(item).strip() for item in tags if str(item).strip())
        return joined or None

    text = str(tags).strip()
    return text or None


def _normalize_customer_level(tags: Any) -> str:
    """把 Shopify tags 粗映射到学习项目里的 customer_level。"""

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


def _build_customer_name(node: dict[str, Any]) -> str:
    first = (node.get("firstName") or "").strip()
    last = (node.get("lastName") or "").strip()
    name = f"{first} {last}".strip()
    if name:
        return name

    email = (node.get("email") or "").strip()
    if email:
        return email

    return f"customer_{node.get('legacyResourceId')}"


def _build_city(node: dict[str, Any]) -> str:
    default_address = node.get("defaultAddress")
    if isinstance(default_address, dict):
        city = (default_address.get("city") or "").strip()
        if city:
            return city

    return "unknown"


def _parse_order_date(created_at: str | None) -> date:
    if not created_at:
        return date.today()

    normalized = created_at.replace("Z", "+00:00") if created_at.endswith("Z") else created_at
    parsed = datetime.fromisoformat(normalized)
    return parsed.date()


def _guest_customer_id(order_id: int) -> int:
    return _GUEST_CUSTOMER_ID_OFFSET + int(order_id)


def _order_amount(node: dict[str, Any]) -> float:
    price_set = node.get("currentTotalPriceSet")
    if isinstance(price_set, dict):
        shop_money = price_set.get("shopMoney")
        if isinstance(shop_money, dict):
            amount = shop_money.get("amount")
            if amount is not None:
                return float(amount)

    return 0.0


def _customer_stub_from_order_customer(customer: dict[str, Any]) -> dict[str, Any]:
    customer_id = _as_int(customer.get("legacyResourceId"))
    return {
        "customer_id": customer_id,
        "customer_name": _build_customer_name(customer),
        "city": _build_city(customer),
        "customer_level": _normalize_customer_level(customer.get("tags")),
    }


def _collect_customers_and_orders(config: ShopifyAdminConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """拉取全量客户与订单，并整理为内部行结构（供 raw CSV 使用）。"""

    client = ShopifyAdminClient(config)
    customers_by_id: dict[int, dict[str, Any]] = {}

    for node in iter_customer_nodes(client):
        customer_id = _as_int(node.get("legacyResourceId"))
        if customer_id == 0:
            continue

        customers_by_id[customer_id] = {
            "customer_id": customer_id,
            "customer_name": _build_customer_name(node),
            "city": _build_city(node),
            "customer_level": _normalize_customer_level(node.get("tags")),
        }

    order_rows: list[dict[str, Any]] = []

    for node in iter_order_nodes(client):
        order_id = _as_int(node.get("legacyResourceId"))
        if order_id == 0:
            continue

        order_amount = _order_amount(node)
        order_date = _parse_order_date(node.get("createdAt") if isinstance(node.get("createdAt"), str) else None)

        customer_blob = node.get("customer")
        if isinstance(customer_blob, dict) and _as_int(customer_blob.get("legacyResourceId")) > 0:
            customer_id = _as_int(customer_blob.get("legacyResourceId"))
            if customer_id not in customers_by_id:
                customers_by_id[customer_id] = _customer_stub_from_order_customer(customer_blob)
        else:
            customer_id = _guest_customer_id(order_id)
            if customer_id not in customers_by_id:
                customers_by_id[customer_id] = {
                    "customer_id": customer_id,
                    "customer_name": f"guest_order_{order_id}",
                    "city": "unknown",
                    "customer_level": "guest",
                }

        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_amount": order_amount,
                "order_date": order_date.isoformat(),
            }
        )

    customer_rows = sorted(customers_by_id.values(), key=lambda row: row["customer_id"])
    order_rows = sorted(order_rows, key=lambda row: row["order_id"])

    return customer_rows, order_rows


def _write_customers_csv(path: str, rows: list[dict[str, Any]]) -> None:
    fieldnames = ("customer_id", "customer_name", "city", "customer_level")
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cast(Any, rows))


def _write_orders_csv(path: str, rows: list[dict[str, Any]]) -> None:
    fieldnames = ("order_id", "customer_id", "order_amount", "order_date")
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cast(Any, rows))


def sync_shopify_to_csv(paths: ProjectPaths) -> ShopifySyncSummary:
    """从 Shopify 拉取数据并写入 `data/raw` 下的 customers.csv / orders.csv。"""

    config = read_shopify_admin_config_from_env()
    customer_rows, order_rows = _collect_customers_and_orders(config)

    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    _write_customers_csv(paths.customers_file.as_posix(), customer_rows)
    _write_orders_csv(paths.orders_file.as_posix(), order_rows)

    return ShopifySyncSummary(customer_rows=len(customer_rows), order_rows=len(order_rows))
