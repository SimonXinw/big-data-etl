"""把 DuckDB 中的结果发布到 PostgreSQL / Supabase。

为了保持学习项目简单：
- DuckDB 仍然作为本地转换引擎
- PostgreSQL / Supabase 作为可选的落库目标
- 这样可以避免同时维护两套转换逻辑
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb

try:
    import psycopg
    from psycopg import sql
except ImportError:  # pragma: no cover - 只有真的走 postgres 目标时才需要该依赖
    psycopg = None
    sql = None


@dataclass(frozen=True)
class PublishPlan:
    """描述一张 DuckDB 表如何同步到 PostgreSQL。"""

    target_table: str
    select_sql: str
    columns: tuple[str, ...]


DELETE_ORDER = (
    "mart.big_data_etl_daily_sales",
    "mart.big_data_etl_city_sales",
    "mart.big_data_etl_customer_level_sales",
    "mart.big_data_etl_sales_summary",
    "dw.big_data_etl_fact_orders",
    "dw.big_data_etl_dim_customers",
    "stg.big_data_etl_orders_clean",
    "stg.big_data_etl_customers_clean",
    "raw.big_data_etl_orders_raw",
    "raw.big_data_etl_customers_raw",
)


PUBLISH_PLANS = (
    PublishPlan(
        target_table="raw.big_data_etl_customers_raw",
        select_sql="SELECT customer_id, customer_name, city, customer_level FROM raw_customers ORDER BY customer_id",
        columns=("customer_id", "customer_name", "city", "customer_level"),
    ),
    PublishPlan(
        target_table="raw.big_data_etl_orders_raw",
        select_sql=(
            "SELECT order_id, customer_id, order_amount, order_date, currency_code, ship_country, refund_amount "
            "FROM raw_orders ORDER BY order_id"
        ),
        columns=(
            "order_id",
            "customer_id",
            "order_amount",
            "order_date",
            "currency_code",
            "ship_country",
            "refund_amount",
        ),
    ),
    PublishPlan(
        target_table="stg.big_data_etl_customers_clean",
        select_sql="SELECT customer_id, customer_name, city, customer_level FROM stg_customers ORDER BY customer_id",
        columns=("customer_id", "customer_name", "city", "customer_level"),
    ),
    PublishPlan(
        target_table="stg.big_data_etl_orders_clean",
        select_sql=(
            "SELECT order_id, customer_id, order_amount, order_date, order_month, currency_code, ship_country, "
            "refund_amount, net_amount FROM stg_orders ORDER BY order_id"
        ),
        columns=(
            "order_id",
            "customer_id",
            "order_amount",
            "order_date",
            "order_month",
            "currency_code",
            "ship_country",
            "refund_amount",
            "net_amount",
        ),
    ),
    PublishPlan(
        target_table="dw.big_data_etl_dim_customers",
        select_sql="SELECT customer_id, customer_name, city, customer_level FROM dim_customers ORDER BY customer_id",
        columns=("customer_id", "customer_name", "city", "customer_level"),
    ),
    PublishPlan(
        target_table="dw.big_data_etl_fact_orders",
        select_sql=(
            "SELECT order_id, customer_id, order_amount, order_date, order_month, currency_code, ship_country, "
            "refund_amount, net_amount FROM fact_orders ORDER BY order_id"
        ),
        columns=(
            "order_id",
            "customer_id",
            "order_amount",
            "order_date",
            "order_month",
            "currency_code",
            "ship_country",
            "refund_amount",
            "net_amount",
        ),
    ),
    PublishPlan(
        target_table="mart.big_data_etl_sales_summary",
        select_sql=(
            "SELECT summary_key, city, customer_level, order_month, currency_code, order_count, gross_sales, "
            "total_refunds, net_sales, avg_order_amount FROM mart_sales_summary ORDER BY summary_key"
        ),
        columns=(
            "summary_key",
            "city",
            "customer_level",
            "order_month",
            "currency_code",
            "order_count",
            "gross_sales",
            "total_refunds",
            "net_sales",
            "avg_order_amount",
        ),
    ),
    PublishPlan(
        target_table="mart.big_data_etl_daily_sales",
        select_sql=(
            "SELECT order_date, order_month, currency_code, order_count, customer_count, gross_merchandise_value, "
            "total_refunds, net_sales, average_order_value FROM mart_daily_sales ORDER BY order_date, currency_code"
        ),
        columns=(
            "order_date",
            "order_month",
            "currency_code",
            "order_count",
            "customer_count",
            "gross_merchandise_value",
            "total_refunds",
            "net_sales",
            "average_order_value",
        ),
    ),
    PublishPlan(
        target_table="mart.big_data_etl_city_sales",
        select_sql=(
            "SELECT city, order_month, currency_code, order_count, customer_count, gross_merchandise_value, "
            "total_refunds, net_sales, average_order_value FROM mart_city_sales ORDER BY city, order_month, currency_code"
        ),
        columns=(
            "city",
            "order_month",
            "currency_code",
            "order_count",
            "customer_count",
            "gross_merchandise_value",
            "total_refunds",
            "net_sales",
            "average_order_value",
        ),
    ),
    PublishPlan(
        target_table="mart.big_data_etl_customer_level_sales",
        select_sql=(
            "SELECT customer_level, order_month, currency_code, order_count, customer_count, gross_merchandise_value, "
            "total_refunds, net_sales, average_order_value FROM mart_customer_level_sales ORDER BY "
            "customer_level, order_month, currency_code"
        ),
        columns=(
            "customer_level",
            "order_month",
            "currency_code",
            "order_count",
            "customer_count",
            "gross_merchandise_value",
            "total_refunds",
            "net_sales",
            "average_order_value",
        ),
    ),
)


def _build_table_identifier(table_name: str) -> Any:
    """把 schema.table 形式的字符串安全地转成 psycopg SQL 对象。"""

    schema_name, relation_name = table_name.split(".", maxsplit=1)
    assert sql is not None
    return sql.SQL("{}.{}").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
    )


def _require_psycopg() -> None:
    if psycopg is None or sql is None:
        raise RuntimeError(
            "当前环境未安装 psycopg。若要写入 PostgreSQL / Supabase，请先执行: "
            "python -m pip install -r requirements.txt"
        )


def _fetch_rows(connection: duckdb.DuckDBPyConnection, select_sql: str) -> list[tuple[Any, ...]]:
    rows = connection.execute(select_sql).fetchall()
    return [tuple(row) for row in rows]


def publish_to_postgres(connection: duckdb.DuckDBPyConnection, postgres_dsn: str) -> list[str]:
    """把 DuckDB 中的分层结果同步到 PostgreSQL / Supabase。"""

    if not postgres_dsn:
        raise ValueError("当 target=postgres 时，必须提供 ETL_POSTGRES_DSN")

    _require_psycopg()
    assert psycopg is not None
    assert sql is not None

    published_tables: list[str] = []

    with psycopg.connect(postgres_dsn) as postgres_connection:
        with postgres_connection.cursor() as cursor:
            for table_name in DELETE_ORDER:
                cursor.execute(f"DELETE FROM {table_name}")

            for plan in PUBLISH_PLANS:
                rows = _fetch_rows(connection, plan.select_sql)
                if not rows:
                    continue

                column_sql = sql.SQL(", ").join(sql.Identifier(column_name) for column_name in plan.columns)
                placeholder_sql = sql.SQL(", ").join(sql.Placeholder() for _ in plan.columns)
                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    _build_table_identifier(plan.target_table),
                    column_sql,
                    placeholder_sql,
                )
                cursor.executemany(
                    insert_sql,
                    rows,
                )
                published_tables.append(plan.target_table)

        postgres_connection.commit()

    return published_tables
