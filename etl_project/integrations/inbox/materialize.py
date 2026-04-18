"""将 `data/inbox/` 下的 JSON 或 CSV 规范化为 `data/raw/*.csv`，作为本地「中转」再进入 DuckDB。

约定（任选其一，按优先级）：

- 客户：`data/inbox/customers.json` 优先于 `data/inbox/customers.csv`
- 订单：`data/inbox/orders.json` 优先于 `data/inbox/orders.csv`

写出的 `data/raw/customers.csv` / `orders.csv` 列名与示例 CSV 完全一致，后续 `load` / `transform` 不变。
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

from etl_project.config import ProjectPaths


@dataclass(frozen=True)
class InboxSyncSummary:
    """一次 inbox 物化到 raw 的统计。"""

    customer_rows: int
    order_rows: int


class InboxSourceError(FileNotFoundError):
    """inbox 缺少可读文件或 JSON 结构不符合预期时抛出。"""


def _pick_inbox_file(inbox_dir: Path, base: str) -> Path | None:
    """返回 `base.json` 或 `base.csv` 中第一个存在的路径（JSON 优先）。"""

    json_path = inbox_dir / f"{base}.json"
    csv_path = inbox_dir / f"{base}.csv"
    if json_path.is_file():
        return json_path
    if csv_path.is_file():
        return csv_path

    return None


def _load_json_array(path: Path) -> list[dict[str, Any]]:
    raw_text = path.read_text(encoding="utf-8")
    payload = json.loads(raw_text)

    if isinstance(payload, dict):
        stem = path.stem.lower()
        inner: Any = None

        if stem == "customers" and isinstance(payload.get("customers"), list):
            inner = payload["customers"]
        elif stem == "orders" and isinstance(payload.get("orders"), list):
            inner = payload["orders"]
        elif isinstance(payload.get("data"), list):
            inner = payload["data"]
        else:
            list_values = [value for value in payload.values() if isinstance(value, list)]
            if len(list_values) == 1:
                inner = list_values[0]

        if inner is None:
            raise InboxSourceError(
                f"JSON 需为数组，或根对象内含 customers/orders/data 等数组字段: {path}"
            )

        payload = inner

    if not isinstance(payload, list):
        raise InboxSourceError(f"JSON 解析结果应为对象数组: {path}")

    return [row for row in payload if isinstance(row, dict)]


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _parse_date(value: Any) -> str:
    if value is None:
        return date.today().isoformat()

    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return date.today().isoformat()

    if "T" in text or text.endswith("Z"):
        normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
        return datetime.fromisoformat(normalized).date().isoformat()

    return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()


def _normalize_customer_row(row: dict[str, Any]) -> dict[str, Any]:
    cid = row.get("customer_id", row.get("id"))
    name = row.get("customer_name", row.get("name", ""))
    city = row.get("city", row.get("defaultCity", "unknown"))
    level = row.get("customer_level", row.get("level", "standard"))

    if cid is None:
        raise InboxSourceError(f"客户行缺少 customer_id / id: {row!r}")

    return {
        "customer_id": int(cid),
        "customer_name": str(name).strip() or f"customer_{cid}",
        "city": str(city).strip() or "unknown",
        "customer_level": str(level).strip().lower() or "standard",
    }


def _normalize_order_row(row: dict[str, Any]) -> dict[str, Any]:
    oid = row.get("order_id", row.get("id"))
    cid = row.get("customer_id", row.get("customerId"))
    amount = row.get("order_amount", row.get("amount", row.get("total")))
    od = row.get("order_date", row.get("date", row.get("createdAt")))

    if oid is None or cid is None:
        raise InboxSourceError(f"订单行缺少 order_id 或 customer_id: {row!r}")

    if amount is None:
        raise InboxSourceError(f"订单行缺少 order_amount: {row!r}")

    return {
        "order_id": int(oid),
        "customer_id": int(cid),
        "order_amount": float(amount),
        "order_date": _parse_date(od),
    }


def _load_customer_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        rows = _load_json_array(path)
    else:
        rows = _load_csv_rows(path)

    return [_normalize_customer_row(row) for row in rows]


def _load_order_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        rows = _load_json_array(path)
    else:
        rows = _load_csv_rows(path)

    return [_normalize_order_row(row) for row in rows]


def materialize_raw_from_inbox(paths: ProjectPaths) -> InboxSyncSummary:
    """读取 inbox 文件并写入 `paths.customers_file` / `paths.orders_file`。"""

    inbox_dir = paths.inbox_dir
    if not inbox_dir.is_dir():
        raise InboxSourceError(
            f"缺少 inbox 目录: {inbox_dir}。请在 data/inbox 下放置 customers/orders 的 json 或 csv。"
        )

    customer_path = _pick_inbox_file(inbox_dir, "customers")
    order_path = _pick_inbox_file(inbox_dir, "orders")

    if customer_path is None:
        raise InboxSourceError(f"未找到 customers.json 或 customers.csv: {inbox_dir}")
    if order_path is None:
        raise InboxSourceError(f"未找到 orders.json 或 orders.csv: {inbox_dir}")

    customer_rows = _load_customer_records(customer_path)
    order_rows = _load_order_records(order_path)

    paths.raw_dir.mkdir(parents=True, exist_ok=True)

    with paths.customers_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("customer_id", "customer_name", "city", "customer_level"),
        )
        writer.writeheader()
        writer.writerows(cast(Any, customer_rows))

    with paths.orders_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("order_id", "customer_id", "order_amount", "order_date"),
        )
        writer.writeheader()
        writer.writerows(cast(Any, order_rows))

    return InboxSyncSummary(customer_rows=len(customer_rows), order_rows=len(order_rows))
