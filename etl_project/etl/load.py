"""加载阶段。

负责：
1. 创建 DuckDB 连接
2. 把原始 CSV 读取到 raw 表
3. 导出最终结果到文件
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from etl_project.config import ProjectPaths


def ensure_directories(paths: ProjectPaths) -> None:
    """确保运行过程中需要的目录存在。"""

    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.inbox_dir.mkdir(parents=True, exist_ok=True)
    paths.products_dir.mkdir(parents=True, exist_ok=True)
    paths.orders_dir.mkdir(parents=True, exist_ok=True)
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    paths.warehouse_dir.mkdir(parents=True, exist_ok=True)


def open_connection(database_file: Path) -> duckdb.DuckDBPyConnection:
    """打开 DuckDB 连接。"""

    return duckdb.connect(str(database_file))


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """判断 DuckDB 中某张表是否已经存在。"""

    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def _replace_raw_customers(connection: duckdb.DuckDBPyConnection, customer_path: str) -> None:
    """全量刷新客户原始表。

    对学习项目来说，客户主数据量通常不大，
    直接全量替换更容易理解。
    """

    connection.execute(
        f"""
        CREATE OR REPLACE TABLE raw_customers AS
        SELECT *
        FROM read_csv_auto('{customer_path}', header = true, delim = ',')
        """
    )


def _replace_raw_orders(connection: duckdb.DuckDBPyConnection, order_path: str) -> int:
    """全量刷新订单原始表，并返回本次写入行数。"""

    connection.execute(
        f"""
        CREATE OR REPLACE TABLE raw_orders AS
        SELECT
            CAST(order_id AS BIGINT) AS order_id,
            CAST(customer_id AS BIGINT) AS customer_id,
            CAST(order_amount AS DOUBLE) AS order_amount,
            CAST(order_date AS DATE) AS order_date,
            'CNY' AS currency_code,
            CAST('UNKNOWN' AS VARCHAR) AS ship_country,
            CAST(0.0 AS DOUBLE) AS refund_amount
        FROM read_csv_auto('{order_path}', header = true, delim = ',')
        """
    )
    row = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
    return int(row[0]) if row else 0


def _append_incremental_orders(connection: duckdb.DuckDBPyConnection, order_path: str) -> int:
    """仅把新订单追加到 raw_orders。

    简化规则：
    - 以 `order_id` 作为增量唯一键
    - 原始 CSV 里已经存在但数据库里没有的订单会被追加
    - 已经处理过的订单不会重复插入
    """

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE incoming_orders AS
        SELECT
            CAST(order_id AS BIGINT) AS order_id,
            CAST(customer_id AS BIGINT) AS customer_id,
            CAST(order_amount AS DOUBLE) AS order_amount,
            CAST(order_date AS DATE) AS order_date,
            'CNY' AS currency_code,
            CAST('UNKNOWN' AS VARCHAR) AS ship_country,
            CAST(0.0 AS DOUBLE) AS refund_amount
        FROM read_csv_auto('{order_path}', header = true, delim = ',')
        """
    )

    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM incoming_orders AS incoming
        LEFT JOIN raw_orders AS existing
            ON CAST(incoming.order_id AS BIGINT) = CAST(existing.order_id AS BIGINT)
        WHERE existing.order_id IS NULL
        """
    ).fetchone()
    inserted_order_rows = int(row[0]) if row else 0

    connection.execute(
        """
        INSERT INTO raw_orders
        SELECT incoming.*
        FROM incoming_orders AS incoming
        LEFT JOIN raw_orders AS existing
            ON CAST(incoming.order_id AS BIGINT) = CAST(existing.order_id AS BIGINT)
        WHERE existing.order_id IS NULL
        """
    )
    return inserted_order_rows


def load_raw_tables(
    connection: duckdb.DuckDBPyConnection,
    paths: ProjectPaths,
    incremental_load: bool = False,
) -> int:
    """将原始 CSV 加载为 raw 层表。

    raw 层的意义是：
    - 保留最接近原始数据的内容
    - 为后续清洗和转换提供可追溯的输入
    """

    customer_path = paths.customers_file.as_posix()
    order_path = paths.orders_file.as_posix()

    _replace_raw_customers(connection, customer_path)

    has_raw_orders = _table_exists(connection, "raw_orders")
    if incremental_load and has_raw_orders:
        return _append_incremental_orders(connection, order_path)

    return _replace_raw_orders(connection, order_path)


def export_summary(connection: duckdb.DuckDBPyConnection, paths: ProjectPaths) -> None:
    """导出分析结果，方便学习者直接查看最终产物。"""

    export_path = paths.export_file.as_posix()

    connection.execute(
        f"""
        COPY (
            SELECT *
            FROM mart_sales_summary
            ORDER BY order_month, city, customer_level, currency_code
        )
        TO '{export_path}'
        WITH (HEADER, DELIMITER ',')
        """
    )
