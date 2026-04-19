"""ETL 流程编排模块。

数据源约定：
- ``csv`` / ``inbox``：依赖 ``data/raw`` 下 CSV（inbox 会先物化）。
- ``shopify``：由 ``integrations.shopify.admin.wide_sync`` 写宽表并衍生 narrow raw，**不再**先行写 CSV。

详见仓库 ``data/README.md`` 与 ``docs/shopify-and-bi-pipeline.md``。
"""

# pyright: reportMissingImports=false

from __future__ import annotations

from etl_project.config import ProjectPaths, build_paths, read_postgres_dsn
from etl_project.etl.extract import validate_sources
from etl_project.etl.load import (
    ensure_directories,
    export_layer_backups,
    export_summary,
    load_raw_tables,
    open_connection,
)
from etl_project.etl.postgres_loader import publish_to_postgres
from etl_project.etl.quality import raise_for_failed_quality_checks, run_quality_checks, write_quality_report
from etl_project.etl.transform import run_transformations
from etl_project.integrations.inbox.materialize import materialize_raw_from_inbox
from etl_project.integrations.shopify.admin.wide_sync import sync_shopify_orders_incremental
from etl_project.models import PipelineOptions, PipelineResult


def run_pipeline(
    paths: ProjectPaths | None = None,
    options: PipelineOptions | None = None,
) -> PipelineResult:
    """执行完整 ETL 流程。

    默认行为：
    - 使用 DuckDB 完成本地 ETL
    - 导出一份 mart 汇总 CSV
    - 将 raw / stg / dw / mart 各层表备份为 ``data/backup/<layer>/*.csv``

    可选行为：
    - 把 DuckDB 中已经构建好的分层结果发布到 PostgreSQL / Supabase
    """

    resolved_paths = paths or build_paths()
    resolved_options = options or PipelineOptions()
    ensure_directories(resolved_paths)

    shopify_customer_rows: int | None = None
    shopify_order_rows: int | None = None
    shopify_wide_orders_merged: int | None = None
    inbox_customer_rows: int | None = None
    inbox_order_rows: int | None = None
    loaded_sources: list[str] = []
    published_tables: list[str] = []
    quality_report = None
    inserted_order_rows = 0
    layer_backup_paths: list[str] = []

    connection = open_connection(resolved_paths.database_file)
    try:
        if resolved_options.data_source == "shopify":
            wide_summary = sync_shopify_orders_incremental(connection, resolved_paths, resolved_options)
            shopify_wide_orders_merged = wide_summary.orders_merged
            customer_count_row = connection.execute("SELECT COUNT(*) FROM raw_customers").fetchone()
            order_count_row = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
            shopify_customer_rows = int(customer_count_row[0]) if customer_count_row else 0
            shopify_order_rows = int(order_count_row[0]) if order_count_row else 0

        if resolved_options.data_source == "inbox":
            inbox_summary = materialize_raw_from_inbox(resolved_paths)
            inbox_customer_rows = inbox_summary.customer_rows
            inbox_order_rows = inbox_summary.order_rows

        if resolved_options.data_source == "shopify":
            loaded_sources = ["shopify_orders_wide"]
        else:
            sources = validate_sources(resolved_paths)
            loaded_sources = [source.name for source in sources]

        if resolved_options.data_source != "shopify":
            inserted_order_rows = load_raw_tables(
                connection,
                resolved_paths,
                incremental_load=resolved_options.incremental_load,
            )
        else:
            count_row = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
            inserted_order_rows = int(count_row[0]) if count_row else 0

        run_transformations(connection)
        quality_report = run_quality_checks(connection)
        write_quality_report(resolved_paths, quality_report)

        if resolved_options.fail_on_quality_error:
            raise_for_failed_quality_checks(quality_report)

        if resolved_options.export_summary:
            export_summary(connection, resolved_paths)

        if resolved_options.export_layer_backups:
            layer_backup_paths = export_layer_backups(connection, resolved_paths)

        if resolved_options.target == "postgres":
            postgres_dsn = resolved_options.postgres_dsn or read_postgres_dsn()
            published_tables = publish_to_postgres(connection, postgres_dsn)
    finally:
        connection.close()

    return PipelineResult(
        target=resolved_options.target,
        database_path=str(resolved_paths.database_file),
        export_path=str(resolved_paths.export_file),
        loaded_sources=loaded_sources,
        published_tables=published_tables,
        layer_backup_paths=layer_backup_paths,
        quality_report=quality_report,
        quality_report_path=str(resolved_paths.quality_report_file),
        incremental_load=resolved_options.incremental_load,
        inserted_order_rows=inserted_order_rows,
        data_source=resolved_options.data_source,
        shopify_customer_rows=shopify_customer_rows,
        shopify_order_rows=shopify_order_rows,
        shopify_wide_orders_merged=shopify_wide_orders_merged,
        inbox_customer_rows=inbox_customer_rows,
        inbox_order_rows=inbox_order_rows,
    )
