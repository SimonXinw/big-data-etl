"""命令行入口逻辑。"""

# pyright: reportMissingImports=false

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

from etl_project.models import PipelineOptions
from etl_project.etl.pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="运行学习型 ETL 项目")
    parser.add_argument(
        "--target",
        choices=("duckdb", "postgres"),
        default="duckdb",
        help="选择 ETL 的最终落库目标。duckdb 表示只跑本地学习版；postgres 可用于 PostgreSQL / Supabase。",
    )
    parser.add_argument(
        "--source",
        choices=("csv", "shopify", "inbox"),
        default="csv",
        help=(
            "数据来源。csv=直接用 data/raw 下 CSV；inbox=先把 data/inbox 下 json/csv 物化到 data/raw 再走 ETL；"
            "shopify=Admin GraphQL 按日拉订单宽表→DuckDB（可选 PostgreSQL UPSERT），再衍生 raw 表走转换。"
        ),
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="跳过本地 CSV 导出（仅 mart 汇总 sales_summary.csv）。",
    )
    parser.add_argument(
        "--skip-layer-backup",
        action="store_true",
        help="跳过分层备份（默认写入 data/backup/<raw|stg|dw|mart>/ 下各表 CSV）。",
    )
    parser.add_argument(
        "--postgres-dsn",
        default="",
        help="可直接传入 PostgreSQL / Supabase 的连接串。若不传，则读取 ETL_POSTGRES_DSN。",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="使用简单的增量模式加载订单数据。当前规则会按 order_id 追加新订单，避免重复插入。",
    )
    parser.add_argument(
        "--shopify-days",
        type=int,
        default=None,
        help=(
            "仅 --source shopify：从「今天」起向前共 N 个 UTC 自然日，按日拆分 `updated_at` 窗口拉取（默认 14）；"
            "也可用 ETL_SHOPIFY_SYNC_DAYS。"
        ),
    )
    parser.add_argument(
        "--smoke-shopify-apis",
        action="store_true",
        help="仅验证 Shopify Admin GraphQL 与（若配置了）Storefront API 是否可用，不跑 ETL。",
    )
    parser.add_argument(
        "--probe-storefront-products",
        action="store_true",
        help="仅调用 Storefront GraphQL 分页拉取全部商品（需 SHOPIFY_STOREFRONT_ACCESS_TOKEN），不跑 DuckDB ETL。",
    )
    parser.add_argument(
        "--export-storefront-products-json",
        action="store_true",
        help="将店面全部商品（较完整字段）写入 data/products/storefront_products.json（需 SHOPIFY_STOREFRONT_ACCESS_TOKEN）。",
    )
    return parser


def main() -> None:
    """执行命令行 ETL。"""

    project_root = Path(__file__).resolve().parent.parent
    # UTF-8 BOM 在 Windows 编辑器中常见；override=True 避免系统里空字符串占位导致 .env 不生效
    load_dotenv(project_root / ".env", encoding="utf-8-sig", override=True)

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except OSError:
            pass

    parser = build_parser()
    args = parser.parse_args()

    if args.export_storefront_products_json:
        from etl_project.integrations.shopify.storefront.export_storefront_products_json import (
            run_export_cli,
        )

        raise SystemExit(run_export_cli())

    if args.probe_storefront_products:
        from etl_project.integrations.shopify.storefront.products_probe import run_probe

        raise SystemExit(run_probe())

    if args.smoke_shopify_apis:
        from etl_project.integrations.shopify.api_smoke import run_smoke_cli

        raise SystemExit(run_smoke_cli())

    result = run_pipeline(
        options=PipelineOptions(
            target=args.target,
            data_source=args.source,
            export_summary=not args.skip_export,
            export_layer_backups=not args.skip_layer_backup,
            postgres_dsn=args.postgres_dsn,
            incremental_load=args.incremental,
            shopify_sync_days=args.shopify_days,
        )
    )

    print("ETL 运行完成")
    print(f"数据来源: {result.data_source}")
    if result.shopify_customer_rows is not None and result.shopify_order_rows is not None:
        print(f"Shopify 同步: customers={result.shopify_customer_rows}, orders={result.shopify_order_rows}")
    if result.shopify_wide_orders_merged is not None:
        print(f"Shopify 宽表尝试插入行数（累计）: {result.shopify_wide_orders_merged}")
    if result.inbox_customer_rows is not None and result.inbox_order_rows is not None:
        print(f"本地 inbox 物化: customers={result.inbox_customer_rows}, orders={result.inbox_order_rows}")
    print(f"运行目标: {result.target}")
    print(f"增量模式: {result.incremental_load}")
    print(f"数据库文件: {result.database_path}")
    print(f"导出文件: {result.export_path}")
    if result.layer_backup_paths:
        print(f"分层备份 CSV 共 {len(result.layer_backup_paths)} 个（目录: {Path(result.layer_backup_paths[0]).parent.parent}）")
    print(f"质量报告: {result.quality_report_path}")
    print(f"已加载数据源: {', '.join(result.loaded_sources)}")
    print(f"本次新增订单行数: {result.inserted_order_rows}")

    if result.quality_report is not None:
        print(
            "数据质量检查: "
            f"{result.quality_report.passed_checks}/{result.quality_report.total_checks} 通过"
        )

    if result.published_tables:
        print(f"已发布到 PostgreSQL / Supabase 的表: {', '.join(result.published_tables)}")
