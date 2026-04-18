"""data/inbox 物化与 inbox 数据源 ETL 自测。"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from importlib import import_module
from pathlib import Path

build_paths = import_module("etl_project.config").build_paths
ProjectPaths = import_module("etl_project.config").ProjectPaths
PipelineOptions = import_module("etl_project.models").PipelineOptions
InboxSourceError = import_module("etl_project.integrations.inbox.materialize").InboxSourceError
materialize_raw_from_inbox = import_module(
    "etl_project.integrations.inbox.materialize"
).materialize_raw_from_inbox
run_pipeline = import_module("etl_project.etl.pipeline").run_pipeline


class InboxSyncTestCase(unittest.TestCase):
    """验证 inbox → raw → pipeline。"""

    def _build_temp_paths(self) -> ProjectPaths:
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
        shutil.copy2(source_paths.postgres_init_file, sql_dir / "postgres" / "init_warehouse.sql")

        (inbox_dir / "customers.json").write_text(
            json.dumps(
                [
                    {"customer_id": 1, "customer_name": "A", "city": "X", "customer_level": "gold"},
                    {"customer_id": 2, "customer_name": "B", "city": "Y", "customer_level": "silver"},
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (inbox_dir / "orders.json").write_text(
            json.dumps(
                [
                    {"order_id": 10, "customer_id": 1, "order_amount": 100.0, "order_date": "2026-04-01"},
                    {"order_id": 11, "customer_id": 2, "order_amount": 50.0, "order_date": "2026-04-02"},
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        data_dir = root_dir / "data"
        products_dir = data_dir / "products"
        orders_dir = data_dir / "orders"

        return ProjectPaths(
            root_dir=root_dir,
            data_dir=data_dir,
            raw_dir=raw_dir,
            inbox_dir=inbox_dir,
            products_dir=products_dir,
            orders_dir=orders_dir,
            output_dir=output_dir,
            warehouse_dir=warehouse_dir,
            sql_dir=sql_dir,
            docs_dir=docs_dir,
            bi_dir=bi_dir,
            customers_file=raw_dir / "customers.csv",
            orders_file=raw_dir / "orders.csv",
            storefront_products_json_file=products_dir / "storefront_products.json",
            database_file=warehouse_dir / "etl_learning.duckdb",
            export_file=output_dir / "sales_summary.csv",
            quality_report_file=output_dir / "quality_report.json",
            postgres_init_file=sql_dir / "postgres" / "init_warehouse.sql",
        )

    def test_materialize_raw_from_inbox_writes_csv(self) -> None:
        paths = self._build_temp_paths()
        summary = materialize_raw_from_inbox(paths)

        self.assertEqual(summary.customer_rows, 2)
        self.assertEqual(summary.order_rows, 2)
        self.assertTrue(paths.customers_file.exists())
        self.assertTrue(paths.orders_file.exists())

    def test_pipeline_with_inbox_source(self) -> None:
        paths = self._build_temp_paths()

        result = run_pipeline(paths, PipelineOptions(target="duckdb", data_source="inbox"))

        self.assertEqual(result.data_source, "inbox")
        self.assertEqual(result.inbox_customer_rows, 2)
        self.assertEqual(result.inbox_order_rows, 2)
        self.assertTrue(result.quality_report is not None and result.quality_report.is_successful)

    def test_materialize_raises_when_inbox_missing_files(self) -> None:
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

        data_dir = root_dir / "data"
        products_dir = data_dir / "products"
        orders_dir = data_dir / "orders"

        paths = ProjectPaths(
            root_dir=root_dir,
            data_dir=data_dir,
            raw_dir=raw_dir,
            inbox_dir=inbox_dir,
            products_dir=products_dir,
            orders_dir=orders_dir,
            output_dir=output_dir,
            warehouse_dir=warehouse_dir,
            sql_dir=sql_dir,
            docs_dir=docs_dir,
            bi_dir=bi_dir,
            customers_file=raw_dir / "customers.csv",
            orders_file=raw_dir / "orders.csv",
            storefront_products_json_file=products_dir / "storefront_products.json",
            database_file=warehouse_dir / "etl_learning.duckdb",
            export_file=output_dir / "sales_summary.csv",
            quality_report_file=output_dir / "quality_report.json",
            postgres_init_file=sql_dir / "postgres" / "init_warehouse.sql",
        )

        with self.assertRaises(InboxSourceError):
            materialize_raw_from_inbox(paths)


if __name__ == "__main__":
    unittest.main()
