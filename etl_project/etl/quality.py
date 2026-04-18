"""数据质量校验模块。

这个模块故意保持简单：
- 用少量但关键的规则覆盖学习项目最常见的问题
- 让学习者知道 ETL 跑完以后，不只是“有结果”，还要“结果可信”
"""

# pyright: reportMissingImports=false

from __future__ import annotations

import json

import duckdb

from etl_project.config import ProjectPaths
from etl_project.models import QualityCheckResult, QualityReport


class DataQualityError(RuntimeError):
    """当关键质量校验失败时抛出。"""


def _fetch_scalar(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None:
        return 0
    return int(row[0])


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
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


def run_quality_checks(connection: duckdb.DuckDBPyConnection) -> QualityReport:
    """执行内置的数据质量校验。"""

    checks = [
        QualityCheckResult(
            name="raw_customers_not_empty",
            passed=_fetch_scalar(connection, "SELECT COUNT(*) FROM raw_customers") > 0,
            message="raw_customers 不能为空。",
        ),
        QualityCheckResult(
            name="raw_orders_not_empty",
            passed=_fetch_scalar(connection, "SELECT COUNT(*) FROM raw_orders") > 0,
            message="raw_orders 不能为空。",
        ),
    ]

    invalid_stg_orders = _fetch_scalar(
        connection,
        "SELECT COUNT(*) FROM stg_orders WHERE order_amount <= 0 OR customer_id IS NULL",
    )
    checks.append(
        QualityCheckResult(
            name="stg_orders_positive_amount",
            passed=invalid_stg_orders == 0,
            message="stg_orders 中不应存在金额小于等于 0 或 customer_id 为空的数据。",
            failed_rows=invalid_stg_orders,
        )
    )

    duplicate_customers = _fetch_scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM (
            SELECT customer_id
            FROM dim_customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ) AS duplicate_rows
        """,
    )
    checks.append(
        QualityCheckResult(
            name="dim_customers_unique_customer_id",
            passed=duplicate_customers == 0,
            message="dim_customers 中 customer_id 应唯一。",
            failed_rows=duplicate_customers,
        )
    )

    orphan_fact_orders = _fetch_scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM fact_orders AS fo
        LEFT JOIN dim_customers AS dc
            ON fo.customer_id = dc.customer_id
        WHERE dc.customer_id IS NULL
        """,
    )
    checks.append(
        QualityCheckResult(
            name="fact_orders_customer_reference_valid",
            passed=orphan_fact_orders == 0,
            message="fact_orders 中的 customer_id 必须能在 dim_customers 中找到。",
            failed_rows=orphan_fact_orders,
        )
    )

    mart_rows = (
        _fetch_scalar(connection, "SELECT COUNT(*) FROM mart_sales_summary")
        if _table_exists(connection, "mart_sales_summary")
        else 0
    )
    checks.append(
        QualityCheckResult(
            name="mart_sales_summary_not_empty",
            passed=mart_rows > 0,
            message="mart_sales_summary 至少要有一行聚合结果。",
            failed_rows=0 if mart_rows > 0 else 1,
        )
    )

    daily_mart_rows = (
        _fetch_scalar(connection, "SELECT COUNT(*) FROM mart_daily_sales")
        if _table_exists(connection, "mart_daily_sales")
        else 0
    )
    checks.append(
        QualityCheckResult(
            name="mart_daily_sales_not_empty",
            passed=daily_mart_rows > 0,
            message="mart_daily_sales 至少要有一行日级别聚合结果。",
            failed_rows=0 if daily_mart_rows > 0 else 1,
        )
    )

    return QualityReport(checks=checks)


def write_quality_report(paths: ProjectPaths, report: QualityReport) -> None:
    """把质量报告写到 output 目录，方便学习者查看。"""

    payload = {
        "total_checks": report.total_checks,
        "passed_checks": report.passed_checks,
        "failed_checks": report.failed_checks,
        "is_successful": report.is_successful,
        "checks": [
            {
                "name": check.name,
                "passed": check.passed,
                "message": check.message,
                "failed_rows": check.failed_rows,
            }
            for check in report.checks
        ],
    }

    paths.quality_report_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def raise_for_failed_quality_checks(report: QualityReport) -> None:
    """当存在失败规则时，抛出可读的异常。"""

    if report.is_successful:
        return

    failed_checks = [
        f"{check.name}: {check.message} (failed_rows={check.failed_rows})"
        for check in report.checks
        if not check.passed
    ]
    raise DataQualityError("; ".join(failed_checks))
