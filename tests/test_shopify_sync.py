"""Shopify 同步模块的单元测试。"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from importlib import import_module
from pathlib import Path
from unittest.mock import patch

build_paths = import_module("etl_project.config").build_paths
ShopifyAdminConfig = import_module("lib.shopify.config").ShopifyAdminConfig
ProjectPaths = import_module("etl_project.config").ProjectPaths
ShopifyConfigError = import_module("lib.shopify.errors").ShopifyConfigError
ShopifySyncSummary = import_module("etl_project.shopify_sync").ShopifySyncSummary
read_shopify_config_from_env = import_module("etl_project.shopify_sync").read_shopify_config_from_env
_normalize_customer_level = import_module("etl_project.shopify_sync")._normalize_customer_level
sync_shopify_to_csv = import_module("etl_project.shopify_sync").sync_shopify_to_csv


class ShopifySyncTestCase(unittest.TestCase):
    """验证 Shopify CSV 映射与配置校验。"""

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

    def test_normalize_customer_level_maps_tags(self) -> None:
        self.assertEqual(_normalize_customer_level("VIP, wholesale"), "vip")
        self.assertEqual(_normalize_customer_level("Gold Member"), "gold")
        self.assertEqual(_normalize_customer_level(""), "standard")
        self.assertEqual(_normalize_customer_level("  "), "standard")

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "demo-store.myshopify.com",
            "SHOPIFY_ADMIN_ACCESS_TOKEN": "admin-token",
            "SHOPIFY_ACCESS_TOKEN": "",
            "SHOPIFY_API_VERSION": "2024-10",
            "SHOPIFY_STOREFRONT_ACCESS_TOKEN": "sf-token",
        },
        clear=False,
    )
    def test_read_shopify_config_from_env_success(self) -> None:
        config = read_shopify_config_from_env()
        self.assertEqual(config.store_domain, "demo-store.myshopify.com")
        self.assertEqual(config.access_token, "admin-token")
        self.assertEqual(config.api_version, "2024-10")
        self.assertEqual(config.storefront_access_token, "sf-token")

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "",
            "SHOPIFY_ADMIN_ACCESS_TOKEN": "",
            "SHOPIFY_ACCESS_TOKEN": "",
        },
        clear=False,
    )
    def test_read_shopify_config_from_env_missing_raises(self) -> None:
        with self.assertRaises(ShopifyConfigError):
            read_shopify_config_from_env()

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "demo-store.myshopify.com",
            "SHOPIFY_ADMIN_ACCESS_TOKEN": "",
            "SHOPIFY_ACCESS_TOKEN": "legacy-only",
        },
        clear=False,
    )
    def test_read_shopify_config_legacy_access_token_only(self) -> None:
        config = read_shopify_config_from_env()
        self.assertEqual(config.access_token, "legacy-only")

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "demo-store.myshopify.com",
            "SHOPIFY_ADMIN_ACCESS_TOKEN": "admin-wins",
            "SHOPIFY_ACCESS_TOKEN": "legacy",
        },
        clear=False,
    )
    def test_read_shopify_config_admin_token_preferred_over_legacy(self) -> None:
        config = read_shopify_config_from_env()
        self.assertEqual(config.access_token, "admin-wins")

    @patch.dict(
        "os.environ",
        {
            "SHOPIFY_STORE_DOMAIN": "not-a-shopify-domain.com",
            "SHOPIFY_ADMIN_ACCESS_TOKEN": "token",
        },
        clear=False,
    )
    def test_read_shopify_config_from_env_invalid_domain_raises(self) -> None:
        with self.assertRaises(ShopifyConfigError):
            read_shopify_config_from_env()

    @patch("etl_project.shopify_sync.read_shopify_admin_config_from_env")
    @patch("etl_project.shopify_sync._collect_customers_and_orders")
    def test_sync_shopify_to_csv_writes_expected_files(self, mock_collect, mock_read_config) -> None:
        mock_read_config.return_value = ShopifyAdminConfig(
            store_domain="demo.myshopify.com",
            access_token="token",
            api_version="2024-10",
        )
        mock_collect.return_value = (
            [
                {
                    "customer_id": 7001,
                    "customer_name": "Test User",
                    "city": "Shanghai",
                    "customer_level": "gold",
                }
            ],
            [
                {
                    "order_id": 8001,
                    "customer_id": 7001,
                    "order_amount": 12.34,
                    "order_date": "2026-04-01",
                }
            ],
        )

        paths = self._build_temp_paths()
        summary = sync_shopify_to_csv(paths)

        self.assertEqual(summary, ShopifySyncSummary(customer_rows=1, order_rows=1))
        self.assertTrue(paths.customers_file.exists())
        self.assertTrue(paths.orders_file.exists())

        customers_text = paths.customers_file.read_text(encoding="utf-8").strip().splitlines()
        orders_text = paths.orders_file.read_text(encoding="utf-8").strip().splitlines()

        self.assertEqual(customers_text[0], "customer_id,customer_name,city,customer_level")
        self.assertEqual(customers_text[1], "7001,Test User,Shanghai,gold")
        self.assertEqual(orders_text[0], "order_id,customer_id,order_amount,order_date")
        self.assertEqual(orders_text[1], "8001,7001,12.34,2026-04-01")


class ShopifyPipelineIntegrationTestCase(unittest.TestCase):
    """验证编排层在 shopify 数据源下能跑通主链路。"""

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

    def test_pipeline_runs_with_shopify_source_when_sync_writes_csv(self) -> None:
        PipelineOptions = import_module("etl_project.models").PipelineOptions
        run_pipeline = import_module("etl_project.pipeline").run_pipeline
        ShopifySyncSummary = import_module("etl_project.shopify_sync").ShopifySyncSummary

        paths = self._build_temp_paths()
        sample_paths = build_paths()

        def fake_sync(target_paths: ProjectPaths) -> ShopifySyncSummary:
            shutil.copy2(sample_paths.customers_file, target_paths.customers_file)
            shutil.copy2(sample_paths.orders_file, target_paths.orders_file)
            return ShopifySyncSummary(customer_rows=4, order_rows=6)

        with patch("etl_project.pipeline.sync_shopify_to_csv", side_effect=fake_sync):
            result = run_pipeline(paths, PipelineOptions(target="duckdb", data_source="shopify"))

        self.assertEqual(result.data_source, "shopify")
        self.assertEqual(result.shopify_customer_rows, 4)
        self.assertEqual(result.shopify_order_rows, 6)
        self.assertTrue(paths.database_file.exists())
        self.assertTrue(result.quality_report is not None and result.quality_report.is_successful)


if __name__ == "__main__":
    unittest.main()
