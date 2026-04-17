"""最小自测，验证项目可以完整跑通。"""

from __future__ import annotations

import shutil
import tempfile
from importlib import import_module
from pathlib import Path
import unittest

import duckdb

build_paths = import_module("etl_project.config").build_paths
ProjectPaths = import_module("etl_project.config").ProjectPaths
PipelineOptions = import_module("etl_project.models").PipelineOptions
DataQualityError = import_module("etl_project.quality").DataQualityError
raise_for_failed_quality_checks = import_module("etl_project.quality").raise_for_failed_quality_checks
run_quality_checks = import_module("etl_project.quality").run_quality_checks
run_pipeline = import_module("etl_project.pipeline").run_pipeline


class PipelineTestCase(unittest.TestCase):
    """验证 ETL 主链路。"""

    def _build_temp_paths(self):
        """创建一套隔离的测试目录，避免测试之间互相影响。"""

        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)

        root_dir = Path(temp_dir.name)
        raw_dir = root_dir / "data" / "raw"
        inbox_dir = root_dir / "data" / "inbox"
        output_dir = root_dir / "data" / "output"
        warehouse_dir = root_dir / "data" / "warehouse"
        sql_dir = root_dir / "sql"
        docs_dir = root_dir / "docs"
        bi_dir = root_dir / "bi"

        raw_dir.mkdir(parents=True, exist_ok=True)
        inbox_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        sql_dir.mkdir(parents=True, exist_ok=True)
        docs_dir.mkdir(parents=True, exist_ok=True)
        bi_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "postgres").mkdir(parents=True, exist_ok=True)

        source_paths = build_paths()
        shutil.copy2(source_paths.customers_file, raw_dir / "customers.csv")
        shutil.copy2(source_paths.orders_file, raw_dir / "orders.csv")
        shutil.copy2(source_paths.postgres_init_file, sql_dir / "postgres" / "init_warehouse.sql")

        return ProjectPaths(
            root_dir=root_dir,
            raw_dir=raw_dir,
            inbox_dir=inbox_dir,
            output_dir=output_dir,
            warehouse_dir=warehouse_dir,
            sql_dir=sql_dir,
            docs_dir=docs_dir,
            bi_dir=bi_dir,
            customers_file=raw_dir / "customers.csv",
            orders_file=raw_dir / "orders.csv",
            database_file=warehouse_dir / "etl_learning.duckdb",
            export_file=output_dir / "sales_summary.csv",
            quality_report_file=output_dir / "quality_report.json",
            postgres_init_file=sql_dir / "postgres" / "init_warehouse.sql",
        )

    def test_pipeline_outputs_expected_artifacts(self) -> None:
        paths = self._build_temp_paths()

        result = run_pipeline(paths, PipelineOptions(target="duckdb"))

        self.assertTrue(paths.database_file.exists())
        self.assertTrue(paths.export_file.exists())
        self.assertTrue(paths.quality_report_file.exists())
        self.assertEqual(result.loaded_sources, ["customers", "orders"])
        self.assertEqual(result.target, "duckdb")
        self.assertEqual(result.published_tables, [])
        self.assertIsNotNone(result.quality_report)

        quality_report = result.quality_report
        assert quality_report is not None
        self.assertTrue(quality_report.is_successful)
        self.assertEqual(quality_report.failed_checks, 0)

        connection = duckdb.connect(str(paths.database_file))
        try:
            stg_count_row = connection.execute(
                "SELECT COUNT(*) FROM stg_orders"
            ).fetchone()
            mart_count_row = connection.execute(
                "SELECT COUNT(*) FROM mart_sales_summary"
            ).fetchone()
            daily_mart_count_row = connection.execute(
                "SELECT COUNT(*) FROM mart_daily_sales"
            ).fetchone()
            city_metric_row = connection.execute(
                "SELECT gross_merchandise_value FROM mart_city_sales WHERE city = 'HANGZHOU' AND order_month = '2026-04'"
            ).fetchone()
            customer_level_metric_row = connection.execute(
                "SELECT customer_count FROM mart_customer_level_sales WHERE customer_level = 'gold' AND order_month = '2026-04'"
            ).fetchone()
            summary_key_row = connection.execute(
                "SELECT summary_key FROM mart_sales_summary ORDER BY summary_key LIMIT 1"
            ).fetchone()
            total_sales_row = connection.execute(
                "SELECT ROUND(SUM(total_sales), 2) FROM mart_sales_summary"
            ).fetchone()
        finally:
            connection.close()

        if (
            stg_count_row is None
            or mart_count_row is None
            or daily_mart_count_row is None
            or city_metric_row is None
            or customer_level_metric_row is None
            or summary_key_row is None
            or total_sales_row is None
        ):
            raise AssertionError("汇总查询没有返回结果")

        stg_count = stg_count_row[0]
        mart_count = mart_count_row[0]
        daily_mart_count = daily_mart_count_row[0]
        hangzhou_gmv = city_metric_row[0]
        gold_customer_count = customer_level_metric_row[0]
        summary_key = summary_key_row[0]
        total_sales = total_sales_row[0]

        self.assertEqual(stg_count, 6)
        self.assertEqual(mart_count, 4)
        self.assertEqual(daily_mart_count, 3)
        self.assertEqual(hangzhou_gmv, 710.0)
        self.assertEqual(gold_customer_count, 2)
        self.assertEqual(summary_key, "HANGZHOU|silver|2026-04")
        self.assertEqual(total_sales, 2658.4)

    def test_pipeline_allows_skip_export(self) -> None:
        paths = self._build_temp_paths()

        result = run_pipeline(paths, PipelineOptions(target="duckdb", export_summary=False))

        self.assertEqual(result.target, "duckdb")
        self.assertEqual(result.loaded_sources, ["customers", "orders"])
        self.assertTrue(paths.quality_report_file.exists())

    def test_pipeline_incremental_load_only_appends_new_orders(self) -> None:
        paths = self._build_temp_paths()

        first_result = run_pipeline(paths, PipelineOptions(target="duckdb"))
        self.assertFalse(first_result.incremental_load)
        self.assertEqual(first_result.inserted_order_rows, 6)

        original_content = paths.orders_file.read_text(encoding="utf-8").strip()
        paths.orders_file.write_text(
            original_content + "\n1006,2,450.00,2026-04-03\n1007,3,300.00,2026-04-04\n",
            encoding="utf-8",
        )

        incremental_result = run_pipeline(
            paths,
            PipelineOptions(target="duckdb", incremental_load=True),
        )

        self.assertTrue(incremental_result.incremental_load)
        self.assertEqual(incremental_result.inserted_order_rows, 1)

        connection = duckdb.connect(str(paths.database_file))
        try:
            raw_count_row = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
            fact_count_row = connection.execute("SELECT COUNT(*) FROM fact_orders").fetchone()
            new_order_row = connection.execute(
                "SELECT COUNT(*) FROM fact_orders WHERE order_id = 1007"
            ).fetchone()
        finally:
            connection.close()

        if raw_count_row is None or fact_count_row is None or new_order_row is None:
            raise AssertionError("增量查询没有返回结果")

        self.assertEqual(raw_count_row[0], 7)
        self.assertEqual(fact_count_row[0], 7)
        self.assertEqual(new_order_row[0], 1)

    def test_quality_checks_detect_invalid_fact_orders(self) -> None:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                "CREATE TABLE raw_customers (customer_id INTEGER, customer_name VARCHAR, city VARCHAR, customer_level VARCHAR)"
            )
            connection.execute(
                "CREATE TABLE raw_orders (order_id INTEGER, customer_id INTEGER, order_amount DOUBLE, order_date DATE)"
            )
            connection.execute(
                "CREATE TABLE stg_orders (order_id INTEGER, customer_id INTEGER, order_amount DOUBLE, order_date DATE, order_month VARCHAR)"
            )
            connection.execute(
                "CREATE TABLE dim_customers (customer_id INTEGER, customer_name VARCHAR, city VARCHAR, customer_level VARCHAR)"
            )
            connection.execute(
                "CREATE TABLE fact_orders (order_id INTEGER, customer_id INTEGER, order_amount DOUBLE, order_date DATE, order_month VARCHAR)"
            )
            connection.execute(
                "CREATE TABLE mart_sales_summary (summary_key VARCHAR, city VARCHAR, customer_level VARCHAR, order_month VARCHAR, order_count INTEGER, total_sales DOUBLE, avg_order_amount DOUBLE)"
            )

            connection.execute("INSERT INTO raw_customers VALUES (1, 'Alice', 'Shanghai', 'gold')")
            connection.execute("INSERT INTO raw_orders VALUES (1001, 1, 100, DATE '2026-04-01')")
            connection.execute("INSERT INTO stg_orders VALUES (1001, 999, 100, DATE '2026-04-01', '2026-04')")
            connection.execute("INSERT INTO dim_customers VALUES (1, 'Alice', 'SHANGHAI', 'gold')")
            connection.execute("INSERT INTO fact_orders VALUES (1001, 999, 100, DATE '2026-04-01', '2026-04')")
            connection.execute(
                "INSERT INTO mart_sales_summary VALUES ('SHANGHAI|gold|2026-04', 'SHANGHAI', 'gold', '2026-04', 1, 100, 100)"
            )

            report = run_quality_checks(connection)
            self.assertFalse(report.is_successful)
            self.assertGreater(report.failed_checks, 0)

            with self.assertRaises(DataQualityError):
                raise_for_failed_quality_checks(report)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
