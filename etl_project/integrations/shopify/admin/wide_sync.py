"""按 bi-database 字段拉取 Admin 订单：按 UTC 自然日拆分 `updated_at` 窗口，逐日分页拉取后写入 DuckDB / PostgreSQL。"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from etl_project.config import ProjectPaths, read_postgres_dsn
from etl_project.models import PipelineOptions
from etl_project.etl.sync_logging import log_timing
from etl_project.utils.pipeline_logging import configure_pipeline_logger, get_pipeline_logger
from etl_project.integrations.shopify.admin.order_mapping import (
    SHOPIFY_ORDER_COLUMNS,
    graph_order_node_to_row,
    row_dict_to_tuple,
)
from etl_project.integrations.shopify.admin.orders_bi import build_orders_query, updated_at_day_filter
from lib.shopify.admin import ShopifyAdminClient
from lib.shopify.config import read_shopify_admin_config_from_env
PHASE_ORDERS_WIDE = "shopify.orders.wide_sync"

# 固定写入 ProjectPaths.orders_dir / 该文件名（不按环境变量改路径）
DEFAULT_SHOPIFY_ORDERS_SNAPSHOT_FILENAME = "shopify_orders_snapshot.json"


def _is_shopify_orders_json_snapshot_enabled() -> bool:
    """默认开启写 JSON；环境变量 ``ETL_SHOPIFY_JSON_SNAPSHOT_ENABLE`` 设为 0/false/no/off 时关闭。"""

    raw = os.getenv("ETL_SHOPIFY_JSON_SNAPSHOT_ENABLE", "1").strip().lower()

    return raw not in ("0", "false", "no", "off")


def _shopify_orders_json_snapshot_file(paths: ProjectPaths) -> Path | None:
    """返回写死的快照文件路径；若开关关闭则返回 ``None``。"""

    if not _is_shopify_orders_json_snapshot_enabled():
        return None

    return paths.orders_dir / DEFAULT_SHOPIFY_ORDERS_SNAPSHOT_FILENAME


_DUCKDB_CREATE_SHOPIFY_ORDERS = """
CREATE TABLE IF NOT EXISTS raw_shopify_orders (
    shopify_gid VARCHAR PRIMARY KEY,
    shop_name VARCHAR NOT NULL,
    order_id VARCHAR,
    legacy_order_id BIGINT,
    customer_legacy_id BIGINT,
    created_at VARCHAR,
    beijing_created_at VARCHAR,
    use_created_at VARCHAR,
    berlin_created_at VARCHAR,
    order_status VARCHAR,
    product_quantity INTEGER,
    total_product_amount DOUBLE,
    shippingfee DOUBLE,
    total_product_discount DOUBLE,
    discountfee DOUBLE,
    tax DOUBLE,
    logistics_status VARCHAR,
    marketing_intention INTEGER,
    currency_code VARCHAR,
    total_price DOUBLE,
    product_sales DOUBLE,
    totalrefunded DOUBLE,
    discount_method VARCHAR,
    customer_display_name VARCHAR,
    customer_email VARCHAR,
    customer_phone VARCHAR,
    bill_country VARCHAR,
    shipping_address1 VARCHAR,
    shipping_address2 VARCHAR,
    shipping_city VARCHAR,
    shipping_province VARCHAR,
    shipping_country VARCHAR,
    shipping_zip VARCHAR,
    shipping_phone VARCHAR,
    shipping_name VARCHAR,
    discount_code VARCHAR,
    updated_at VARCHAR,
    cancelled_at VARCHAR,
    cancel_reason VARCHAR,
    product_details VARCHAR,
    discount_information VARCHAR,
    billing_address VARCHAR,
    refunds VARCHAR,
    etl_synced_at TIMESTAMP
);
"""


def _ensure_wide_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(_DUCKDB_CREATE_SHOPIFY_ORDERS)


def _placeholder_sql(n: int) -> str:
    return ", ".join(["?"] * n)


def _duckdb_shopify_wide_upsert_sql() -> str:
    """同一 ``shopify_gid`` 再次写入时用新快照覆盖旧行（保持一行一单的最新副本）。"""

    cols = ", ".join(SHOPIFY_ORDER_COLUMNS)
    ph = _placeholder_sql(len(SHOPIFY_ORDER_COLUMNS))

    return f"INSERT OR REPLACE INTO raw_shopify_orders ({cols}) VALUES ({ph})"


def _postgres_shopify_wide_upsert_sql() -> str:
    """PostgreSQL 宽表：``ON CONFLICT (shopify_gid) DO UPDATE`` 与 DuckDB UPSERT 语义对齐。"""

    cols = list(SHOPIFY_ORDER_COLUMNS)
    insert_cols = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_cols = [c for c in cols if c != "shopify_gid"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    return (
        f"INSERT INTO raw.big_data_etl_shopify_orders ({insert_cols}) VALUES ({placeholders}) "
        f"ON CONFLICT (shopify_gid) DO UPDATE SET {set_clause}"
    )


_DUCKDB_SHOPIFY_WIDE_UPSERT_SQL = _duckdb_shopify_wide_upsert_sql()
_POSTGRES_SHOPIFY_WIDE_UPSERT_SQL = _postgres_shopify_wide_upsert_sql()


def rebuild_narrow_raw_tables_from_shopify(connection: duckdb.DuckDBPyConnection) -> None:
    """由宽表衍生学习用 `raw_orders` / `raw_customers`（供后续 transform）。"""

    connection.execute(
        """
        CREATE OR REPLACE TABLE raw_orders AS
        SELECT DISTINCT
            CAST(legacy_order_id AS BIGINT) AS order_id,
            CAST(customer_legacy_id AS BIGINT) AS customer_id,
            CAST(total_price AS DOUBLE) AS order_amount,
            CAST(
                strptime(created_at, '%Y-%m-%d %H:%M:%S') AS DATE
            ) AS order_date,
            TRIM(COALESCE(NULLIF(currency_code, ''), 'UNKNOWN')) AS currency_code,
            TRIM(COALESCE(shipping_country, '')) AS ship_country,
            CAST(COALESCE(totalrefunded, 0) AS DOUBLE) AS refund_amount
        FROM raw_shopify_orders
        WHERE legacy_order_id IS NOT NULL
          AND customer_legacy_id IS NOT NULL
          AND created_at IS NOT NULL
          AND total_price IS NOT NULL
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE TABLE raw_customers AS
        SELECT
            customer_id,
            customer_name,
            city,
            customer_level
        FROM (
            SELECT
                CAST(customer_legacy_id AS BIGINT) AS customer_id,
                COALESCE(NULLIF(TRIM(customer_display_name), ''), 'guest') AS customer_name,
                UPPER(TRIM(COALESCE(NULLIF(shipping_city, ''), 'unknown'))) AS city,
                'standard' AS customer_level
            FROM raw_shopify_orders
            WHERE customer_legacy_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(customer_legacy_id AS BIGINT)
                ORDER BY legacy_order_id DESC NULLS LAST, shopify_gid DESC
            ) = 1
        )
        """
    )


def _daterange(start_d: date, end_d: date) -> list[date]:
    span = (end_d - start_d).days + 1
    return [start_d + timedelta(days=offset) for offset in range(span)]


def _sync_days_from_options(options: PipelineOptions | None) -> int:
    if options is not None and options.shopify_sync_days is not None:
        return max(1, int(options.shopify_sync_days))

    raw = os.getenv("ETL_SHOPIFY_SYNC_DAYS", "14").strip()

    try:
        return max(1, int(raw))
    except ValueError:
        return 14


def _append_orders_snapshot(
    path: Path,
    *,
    new_rows: list[dict[str, Any]],
    meta: dict[str, Any],
    log_label: str = "",
) -> None:
    """将订单追加到 JSON 的 ``orders`` 数组末尾；若文件不存在则新建。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    existing_orders: list[dict[str, Any]] = []
    existing_meta: dict[str, Any] = {}

    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            if isinstance(payload, dict):
                eo = payload.get("orders")
                em = payload.get("meta")

                if isinstance(eo, list):
                    existing_orders = [r for r in eo if isinstance(r, dict)]

                if isinstance(em, dict):
                    existing_meta = em
        except json.JSONDecodeError:
            log = get_pipeline_logger()
            log.warn(
                PHASE_ORDERS_WIDE,
                "快照 JSON 解析失败，将视为空文件并重新追加: %s",
                path.as_posix(),
            )

    combined = existing_orders + new_rows
    merged_meta = {**existing_meta, **meta, "orders_total": len(combined)}
    payload = {"meta": merged_meta, "orders": combined}

    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    log = get_pipeline_logger()
    suffix = f" | {log_label}" if log_label else ""
    log.success(
        PHASE_ORDERS_WIDE,
        "已追加订单 JSON 快照: %s（本次 +%s 条，累计 %s 条）%s",
        path.as_posix(),
        len(new_rows),
        len(combined),
        suffix,
    )


def _shop_label() -> str:
    explicit = os.getenv("ETL_SHOPIFY_SHOP_NAME", "").strip()
    if explicit:
        return explicit

    domain = os.getenv("SHOPIFY_STORE_DOMAIN", "").strip().lower()
    if domain.endswith(".myshopify.com"):
        return domain.replace(".myshopify.com", "")

    return domain or "shopify_store"


def _fetch_day_pages(
    client: ShopifyAdminClient,
    *,
    day_str: str,
    shop_label: str,
) -> tuple[int, list[dict[str, Any]]]:
    """单日 UTC：该日 `updated_at` 窗口内分页拉取订单。"""

    search_filter = updated_at_day_filter(day_str)
    total = 0
    rows: list[dict[str, Any]] = []
    after_cursor: str | None = None
    page_number = 1

    while True:
        query = build_orders_query(search_filter=search_filter, after_cursor=after_cursor)
        data = client.execute(query)
        payload = data.get("orders")
        if not isinstance(payload, dict):
            break

        edges = payload.get("edges") or []
        if page_number == 1 and not edges:
            break

        for edge in edges:
            if not isinstance(edge, dict):
                continue

            node = edge.get("node")
            if not isinstance(node, dict):
                continue

            row = graph_order_node_to_row(node, shop_name=shop_label)
            rows.append(row)
            total += 1

        page_info = payload.get("pageInfo") or {}
        has_next = bool(page_info.get("hasNextPage")) if isinstance(page_info, dict) else False

        if has_next and edges:
            last_cursor = edges[-1].get("cursor") if isinstance(edges[-1], dict) else None
            after_cursor = str(last_cursor) if last_cursor else None
            page_number += 1
            time.sleep(0.25)
            continue

        break

    return total, rows


def _merge_rows_duckdb(connection: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    synced = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    batch = [row_dict_to_tuple(row, etl_synced_at=synced) for row in rows]

    connection.executemany(_DUCKDB_SHOPIFY_WIDE_UPSERT_SQL, batch)

    return len(batch)


def _require_psycopg():
    try:
        import psycopg  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("写入 PostgreSQL 需要 psycopg，请 pip install psycopg[binary]") from exc


def _insert_postgres(rows: list[dict[str, Any]], postgres_dsn: str) -> int:
    if not rows or not postgres_dsn:
        return 0

    _require_psycopg()
    import psycopg

    sql = _POSTGRES_SHOPIFY_WIDE_UPSERT_SQL

    synced = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    tuples = [row_dict_to_tuple(row, etl_synced_at=synced) for row in rows]

    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, tuples)

        conn.commit()

    return len(tuples)


@dataclass(frozen=True)
class ShopifyWideSyncSummary:
    """Shopify 宽表同步统计。"""

    days_requested: int
    shop_label: str
    orders_merged: int
    duckdb_rows_touched: int
    postgres_rows_touched: int


def sync_shopify_orders_incremental(
    connection: duckdb.DuckDBPyConnection,
    paths: ProjectPaths,
    options: PipelineOptions | None = None,
) -> ShopifyWideSyncSummary:
    """按 UTC 日历日回溯 N 天：每日单独查询「该日 `updated_at` 窗口」内的订单（游标分页）。

    不在此轮开始前清空宽表：DuckDB ``INSERT OR REPLACE``、PostgreSQL ``ON CONFLICT (shopify_gid) DO UPDATE``，
    已存在的 ``shopify_gid`` 用本轮 API 快照**覆盖**为最新一行。
    默认将每日拉取的订单**当日写入一次** JSON（``data/orders/shopify_orders_snapshot.json``，可用 ``ETL_SHOPIFY_JSON_SNAPSHOT_ENABLE`` 关闭），按日追加到 ``orders`` 数组末尾。
    """

    log = configure_pipeline_logger("etl_shopify_sync", paths.root_dir / "logs")

    log.info(PHASE_ORDERS_WIDE, "项目根目录: %s", paths.root_dir)

    days = _sync_days_from_options(options)
    shop_label = _shop_label()
    end_d = date.today()
    start_d = end_d - timedelta(days=days - 1)

    postgres_dsn = ""
    if options is not None and options.postgres_dsn:
        postgres_dsn = options.postgres_dsn.strip()
    else:
        postgres_dsn = read_postgres_dsn().strip()

    config = read_shopify_admin_config_from_env()
    client = ShopifyAdminClient(config)

    merged_total = 0
    pg_total = 0
    snapshot_target = _shopify_orders_json_snapshot_file(paths)

    if snapshot_target:
        log.info(PHASE_ORDERS_WIDE, "JSON 快照（按日追加）: %s", snapshot_target.as_posix())
    else:
        log.info(PHASE_ORDERS_WIDE, "JSON 快照已关闭（ETL_SHOPIFY_JSON_SNAPSHOT_ENABLE=0）")

    with log_timing(
        log.logger,
        f"Shopify 订单按日拉取 | 店铺={shop_label} | 回溯自然日={days} | UTC 日期 {start_d}～{end_d}",
        phase="shopify.orders.timing",
    ):
        log.info(
            PHASE_ORDERS_WIDE,
            "GraphQL Admin API 版本: %s | 店铺域名: %s | PostgreSQL 写入: %s",
            config.api_version,
            config.store_domain,
            "是" if postgres_dsn else "否（未配置 ETL_POSTGRES_DSN）",
        )

        _ensure_wide_table(connection)

        log.info(
            PHASE_ORDERS_WIDE,
            "宽表写入：shopify_gid 存在则覆盖为最新快照（DuckDB INSERT OR REPLACE / Postgres ON CONFLICT DO UPDATE）",
        )

        for day in _daterange(start_d, end_d):
            day_str = day.strftime("%Y-%m-%d")
            day_label = f"{day_str}（当日 UTC `updated_at` 窗口）"

            count, row_dicts = _fetch_day_pages(client, day_str=day_str, shop_label=shop_label)

            if count == 0:
                log.info(PHASE_ORDERS_WIDE, "%s | 无订单", day_label)
            else:
                log.info(PHASE_ORDERS_WIDE, "%s | 拉取 %s 条", day_label, count)

            if row_dicts:
                merged_total += _merge_rows_duckdb(connection, row_dicts)

                if postgres_dsn:
                    pg_total += _insert_postgres(row_dicts, postgres_dsn)

                if snapshot_target:
                    _append_orders_snapshot(
                        snapshot_target,
                        new_rows=row_dicts,
                        meta={
                            "shop_label": shop_label,
                            "store_domain": config.store_domain,
                            "admin_api_version": config.api_version,
                            "sync_days": days,
                            "date_range_utc": {"start": str(start_d), "end": str(end_d)},
                            "snapshot_day_utc": day_str,
                            "snapshot_day_order_count": len(row_dicts),
                            "snapshot_day_saved_at_utc": datetime.now(timezone.utc).isoformat(),
                        },
                        log_label=f"UTC 日 {day_str}",
                    )

        log.info(
            PHASE_ORDERS_WIDE,
            "宽表写入完成 | DuckDB 本轮尝试插入(累计行数)=%s | PostgreSQL 本轮尝试插入(累计行数)=%s",
            merged_total,
            pg_total,
        )

        with log_timing(log.logger, "由 raw_shopify_orders 衍生 raw_orders / raw_customers", phase="shopify.orders.timing"):
            rebuild_narrow_raw_tables_from_shopify(connection)

        narrow_orders = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
        narrow_customers = connection.execute("SELECT COUNT(*) FROM raw_customers").fetchone()
        wide_cnt = connection.execute("SELECT COUNT(*) FROM raw_shopify_orders").fetchone()

        log.info(
            PHASE_ORDERS_WIDE,
            "衍生表行数 | raw_shopify_orders=%s | raw_orders=%s | raw_customers=%s",
            int(wide_cnt[0]) if wide_cnt else 0,
            int(narrow_orders[0]) if narrow_orders else 0,
            int(narrow_customers[0]) if narrow_customers else 0,
        )

    return ShopifyWideSyncSummary(
        days_requested=days,
        shop_label=shop_label,
        orders_merged=merged_total,
        duckdb_rows_touched=merged_total,
        postgres_rows_touched=pg_total,
    )
