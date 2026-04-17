"""ETL 流程编排模块。"""

# pyright: reportMissingImports=false

from __future__ import annotations

from etl_project.config import ProjectPaths, build_paths, read_postgres_dsn
from etl_project.extract import validate_sources
from etl_project.load import ensure_directories, export_summary, load_raw_tables, open_connection
from etl_project.models import PipelineOptions, PipelineResult
from etl_project.postgres_loader import publish_to_postgres
from etl_project.quality import raise_for_failed_quality_checks, run_quality_checks, write_quality_report
from etl_project.data_inbox_sync import materialize_raw_from_inbox
from etl_project.shopify_sync import sync_shopify_to_csv
from etl_project.transform import run_transformations


def run_pipeline(
    paths: ProjectPaths | None = None,
    options: PipelineOptions | None = None,
) -> PipelineResult:
    """执行完整 ETL 流程。

    默认行为：
    - 使用 DuckDB 完成本地 ETL
    - 导出一份 mart 汇总 CSV

    可选行为：
    - 把 DuckDB 中已经构建好的分层结果发布到 PostgreSQL / Supabase
    """

    resolved_paths = paths or build_paths()
    resolved_options = options or PipelineOptions()
    ensure_directories(resolved_paths)

    shopify_customer_rows: int | None = None
    shopify_order_rows: int | None = None
    inbox_customer_rows: int | None = None
    inbox_order_rows: int | None = None

    if resolved_options.data_source == "shopify":
        shopify_summary = sync_shopify_to_csv(resolved_paths)
        shopify_customer_rows = shopify_summary.customer_rows
        shopify_order_rows = shopify_summary.order_rows

    if resolved_options.data_source == "inbox":
        inbox_summary = materialize_raw_from_inbox(resolved_paths)
        inbox_customer_rows = inbox_summary.customer_rows
        inbox_order_rows = inbox_summary.order_rows

    sources = validate_sources(resolved_paths)
    loaded_sources = [source.name for source in sources]
    published_tables: list[str] = []
    quality_report = None
    inserted_order_rows = 0

    connection = open_connection(resolved_paths.database_file)
    try:
        inserted_order_rows = load_raw_tables(
            connection,
            resolved_paths,
            incremental_load=resolved_options.incremental_load,
        )
        run_transformations(connection)
        quality_report = run_quality_checks(connection)
        write_quality_report(resolved_paths, quality_report)

        if resolved_options.fail_on_quality_error:
            raise_for_failed_quality_checks(quality_report)

        if resolved_options.export_summary:
            export_summary(connection, resolved_paths)

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
        quality_report=quality_report,
        quality_report_path=str(resolved_paths.quality_report_file),
        incremental_load=resolved_options.incremental_load,
        inserted_order_rows=inserted_order_rows,
        data_source=resolved_options.data_source,
        shopify_customer_rows=shopify_customer_rows,
        shopify_order_rows=shopify_order_rows,
        inbox_customer_rows=inbox_customer_rows,
        inbox_order_rows=inbox_order_rows,
    )
