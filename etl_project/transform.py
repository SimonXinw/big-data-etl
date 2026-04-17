"""转换阶段。

这里使用 DuckDB SQL 来完成清洗、建模和聚合，
让学习者能直接看到典型的数据仓库分层思路。
"""

from __future__ import annotations

import duckdb


def run_transformations(connection: duckdb.DuckDBPyConnection) -> None:
    """执行核心转换逻辑。"""

    # stg 层：对客户原始数据做基础清洗和去重。
    connection.execute(
        """
        CREATE OR REPLACE TABLE stg_customers AS
        SELECT DISTINCT
            CAST(customer_id AS BIGINT) AS customer_id,
            TRIM(customer_name) AS customer_name,
            UPPER(TRIM(city)) AS city,
            LOWER(TRIM(customer_level)) AS customer_level
        FROM raw_customers
        """
    )

    # stg 层：对订单原始数据做类型转换、过滤和月份衍生。
    connection.execute(
        """
        CREATE OR REPLACE TABLE stg_orders AS
        SELECT DISTINCT
            CAST(order_id AS BIGINT) AS order_id,
            CAST(customer_id AS BIGINT) AS customer_id,
            CAST(order_amount AS DOUBLE) AS order_amount,
            CAST(order_date AS DATE) AS order_date,
            strftime(CAST(order_date AS DATE), '%Y-%m') AS order_month
        FROM raw_orders
        WHERE CAST(order_amount AS DOUBLE) > 0
        """
    )

    # 维度表：维表通常来自清洗后的 stg 层。
    connection.execute(
        """
        CREATE OR REPLACE TABLE dim_customers AS
        SELECT
            customer_id,
            customer_name,
            city,
            customer_level
        FROM stg_customers
        """
    )

    # 事实表：事实表来自清洗后的订单明细。
    connection.execute(
        """
        CREATE OR REPLACE TABLE fact_orders AS
        SELECT
            order_id,
            customer_id,
            order_amount,
            order_date,
            order_month
        FROM stg_orders
        """
    )

    # 汇总层 1：城市 + 客户等级 + 月份维度的销售汇总。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_sales_summary AS
        SELECT
            dc.city || '|' || dc.customer_level || '|' || fo.order_month AS summary_key,
            dc.city,
            dc.customer_level,
            fo.order_month,
            COUNT(*) AS order_count,
            ROUND(SUM(fo.order_amount), 2) AS total_sales,
            ROUND(AVG(fo.order_amount), 2) AS avg_order_amount
        FROM fact_orders AS fo
        INNER JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        GROUP BY dc.city, dc.customer_level, fo.order_month
        """
    )

    # 汇总层 2：按天观察 GMV、订单数、客单价。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_daily_sales AS
        SELECT
            CAST(order_date AS DATE) AS order_date,
            order_month,
            COUNT(*) AS order_count,
            COUNT(DISTINCT customer_id) AS customer_count,
            ROUND(SUM(order_amount), 2) AS gross_merchandise_value,
            ROUND(AVG(order_amount), 2) AS average_order_value
        FROM fact_orders
        GROUP BY CAST(order_date AS DATE), order_month
        """
    )

    # 汇总层 3：按城市观察销售贡献。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_city_sales AS
        SELECT
            dc.city,
            fo.order_month,
            COUNT(*) AS order_count,
            COUNT(DISTINCT fo.customer_id) AS customer_count,
            ROUND(SUM(fo.order_amount), 2) AS gross_merchandise_value,
            ROUND(AVG(fo.order_amount), 2) AS average_order_value
        FROM fact_orders AS fo
        INNER JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        GROUP BY dc.city, fo.order_month
        """
    )

    # 汇总层 4：按客户等级观察业务结构。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_customer_level_sales AS
        SELECT
            dc.customer_level,
            fo.order_month,
            COUNT(*) AS order_count,
            COUNT(DISTINCT fo.customer_id) AS customer_count,
            ROUND(SUM(fo.order_amount), 2) AS gross_merchandise_value,
            ROUND(AVG(fo.order_amount), 2) AS average_order_value
        FROM fact_orders AS fo
        INNER JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        GROUP BY dc.customer_level, fo.order_month
        """
    )
