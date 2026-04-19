"""Shopify 同步模块的单元测试。"""

from __future__ import annotations

import csv
import os
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
ShopifyWideSyncSummary = import_module(
    "etl_project.integrations.shopify.admin.wide_sync"
).ShopifyWideSyncSummary
read_shopify_config_from_env = import_module("etl_project.integrations.shopify.compat").read_shopify_config_from_env
_normalize_customer_level = import_module("etl_project.integrations.shopify.compat")._normalize_customer_level
read_shopify_admin_config_from_env = import_module("lib.shopify.config").read_shopify_admin_config_from_env


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

        data_dir = root_dir / "data"
        products_dir = data_dir / "products"
        orders_dir = data_dir / "orders"
        backup_dir = data_dir / "backup"

        return ProjectPaths(
            root_dir=root_dir,
            data_dir=data_dir,
            raw_dir=raw_dir,
            inbox_dir=inbox_dir,
            products_dir=products_dir,
            orders_dir=orders_dir,
            backup_dir=backup_dir,
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

    def test_default_shopify_admin_api_version_is_2025_04(self) -> None:
        env_keys = (
            "SHOPIFY_STORE_DOMAIN",
            "SHOPIFY_ADMIN_ACCESS_TOKEN",
            "SHOPIFY_ADMIN_API_VERSION",
            "SHOPIFY_API_VERSION",
        )
        saved = {key: os.environ.get(key) for key in env_keys}

        try:
            os.environ.pop("SHOPIFY_ADMIN_API_VERSION", None)
            os.environ.pop("SHOPIFY_API_VERSION", None)
            os.environ["SHOPIFY_STORE_DOMAIN"] = "demo-store.myshopify.com"
            os.environ["SHOPIFY_ADMIN_ACCESS_TOKEN"] = "token-for-default-version-test"
            cfg = read_shopify_admin_config_from_env()
            self.assertEqual(cfg.api_version, "2025-04")
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

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

        data_dir = root_dir / "data"
        products_dir = data_dir / "products"
        orders_dir = data_dir / "orders"
        backup_dir = data_dir / "backup"

        return ProjectPaths(
            root_dir=root_dir,
            data_dir=data_dir,
            raw_dir=raw_dir,
            inbox_dir=inbox_dir,
            products_dir=products_dir,
            orders_dir=orders_dir,
            backup_dir=backup_dir,
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

    def test_pipeline_runs_with_shopify_source_when_sync_writes_csv(self) -> None:
        PipelineOptions = import_module("etl_project.models").PipelineOptions
        run_pipeline = import_module("etl_project.etl.pipeline").run_pipeline
        load_raw_tables = import_module("etl_project.etl.load").load_raw_tables

        paths = self._build_temp_paths()
        sample_paths = build_paths()

        def fake_wide_sync(connection, target_paths: ProjectPaths, options: object) -> ShopifyWideSyncSummary:
            shutil.copy2(sample_paths.customers_file, target_paths.customers_file)
            shutil.copy2(sample_paths.orders_file, target_paths.orders_file)
            load_raw_tables(connection, target_paths, incremental_load=False)

            return ShopifyWideSyncSummary(
                days_requested=14,
                shop_label="demo",
                orders_merged=6,
                duckdb_rows_touched=6,
                postgres_rows_touched=0,
            )

        with patch("etl_project.etl.pipeline.sync_shopify_orders_incremental", side_effect=fake_wide_sync):
            result = run_pipeline(paths, PipelineOptions(target="duckdb", data_source="shopify"))

        with sample_paths.customers_file.open(newline="", encoding="utf-8") as customer_handle:
            expected_customer_rows = sum(1 for _ in csv.DictReader(customer_handle))
        with sample_paths.orders_file.open(newline="", encoding="utf-8") as order_handle:
            expected_order_rows = sum(1 for _ in csv.DictReader(order_handle))

        self.assertEqual(result.data_source, "shopify")
        self.assertEqual(result.shopify_customer_rows, expected_customer_rows)
        self.assertEqual(result.shopify_order_rows, expected_order_rows)
        self.assertEqual(result.shopify_wide_orders_merged, 6)
        self.assertEqual(result.loaded_sources, ["shopify_orders_wide"])
        self.assertTrue(paths.database_file.exists())
        self.assertTrue(result.quality_report is not None and result.quality_report.is_successful)


if __name__ == "__main__":
    unittest.main()
