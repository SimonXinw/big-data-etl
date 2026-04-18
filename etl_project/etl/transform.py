"""转换阶段。

这里使用 DuckDB SQL 来完成清洗、建模和聚合，
让学习者能直接看到典型的数据仓库分层思路。

Shopify 路径下，订单金额来自 Admin GraphQL 宽表 ``total_price`` / ``totalRefundedSet``（映射为 ``refund_amount``），
币种来自 ``currencyCode``；跨境场景按 ``currency_code`` 分组汇总，避免多币种直接相加。

汇总 CSV（``mart_sales_summary``）常见电商指标含义：
- ``gross_sales``：下单 GMV（订单总金额之和）
- ``total_refunds``：退款金额之和（店铺结算货币口径）
- ``net_sales``：净销售额（GMV − 退款，单行 clamp 为非负后再汇总）
- ``avg_order_amount``：净客单价（按 ``net_sales`` / ``order_count``）
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
            strftime(CAST(order_date AS DATE), '%Y-%m') AS order_month,
            UPPER(TRIM(COALESCE(NULLIF(TRIM(currency_code), ''), 'UNKNOWN'))) AS currency_code,
            UPPER(TRIM(COALESCE(NULLIF(TRIM(ship_country), ''), 'UNKNOWN'))) AS ship_country,
            CAST(COALESCE(refund_amount, 0) AS DOUBLE) AS refund_amount,
            GREATEST(
                CAST(order_amount AS DOUBLE) - CAST(COALESCE(refund_amount, 0) AS DOUBLE),
                0
            ) AS net_amount
        FROM raw_orders
        WHERE CAST(order_amount AS DOUBLE) > 0
        """
    )

    # 维度表：raw 层允许同一 customer_id 多行（不同订单/地址），此处按 customer_id 保留一行以满足唯一性。
    connection.execute(
        """
        CREATE OR REPLACE TABLE dim_customers AS
        SELECT customer_id, customer_name, city, customer_level
        FROM stg_customers
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY customer_name, city, customer_level
        ) = 1
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
            order_month,
            currency_code,
            ship_country,
            refund_amount,
            net_amount
        FROM stg_orders
        """
    )

    # 汇总层 1：城市 + 客户等级 + 月份 + 币种（跨境常用拆分）。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_sales_summary AS
        SELECT
            dc.city || '|' || dc.customer_level || '|' || fo.order_month || '|' || fo.currency_code AS summary_key,
            dc.city,
            dc.customer_level,
            fo.order_month,
            fo.currency_code,
            COUNT(*) AS order_count,
            ROUND(SUM(fo.order_amount), 2) AS gross_sales,
            ROUND(SUM(fo.refund_amount), 2) AS total_refunds,
            ROUND(SUM(fo.net_amount), 2) AS net_sales,
            ROUND(AVG(fo.net_amount), 2) AS avg_order_amount
        FROM fact_orders AS fo
        INNER JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        GROUP BY dc.city, dc.customer_level, fo.order_month, fo.currency_code
        """
    )

    # 汇总层 2：按天 + 币种观察 GMV、订单数、净销售额与客单价。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_daily_sales AS
        SELECT
            CAST(order_date AS DATE) AS order_date,
            order_month,
            currency_code,
            COUNT(*) AS order_count,
            COUNT(DISTINCT customer_id) AS customer_count,
            ROUND(SUM(order_amount), 2) AS gross_merchandise_value,
            ROUND(SUM(refund_amount), 2) AS total_refunds,
            ROUND(SUM(net_amount), 2) AS net_sales,
            ROUND(AVG(net_amount), 2) AS average_order_value
        FROM fact_orders
        GROUP BY CAST(order_date AS DATE), order_month, currency_code
        """
    )

    # 汇总层 3：按城市 + 币种观察销售贡献。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_city_sales AS
        SELECT
            dc.city,
            fo.order_month,
            fo.currency_code,
            COUNT(*) AS order_count,
            COUNT(DISTINCT fo.customer_id) AS customer_count,
            ROUND(SUM(fo.order_amount), 2) AS gross_merchandise_value,
            ROUND(SUM(fo.refund_amount), 2) AS total_refunds,
            ROUND(SUM(fo.net_amount), 2) AS net_sales,
            ROUND(AVG(fo.net_amount), 2) AS average_order_value
        FROM fact_orders AS fo
        INNER JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        GROUP BY dc.city, fo.order_month, fo.currency_code
        """
    )

    # 汇总层 4：按客户等级 + 币种观察业务结构。
    connection.execute(
        """
        CREATE OR REPLACE TABLE mart_customer_level_sales AS
        SELECT
            dc.customer_level,
            fo.order_month,
            fo.currency_code,
            COUNT(*) AS order_count,
            COUNT(DISTINCT fo.customer_id) AS customer_count,
            ROUND(SUM(fo.order_amount), 2) AS gross_merchandise_value,
            ROUND(SUM(fo.refund_amount), 2) AS total_refunds,
            ROUND(SUM(fo.net_amount), 2) AS net_sales,
            ROUND(AVG(fo.net_amount), 2) AS average_order_value
        FROM fact_orders AS fo
        INNER JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        GROUP BY dc.customer_level, fo.order_month, fo.currency_code
        """
    )
