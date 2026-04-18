"""常用链路的一次性入口（便于脚本 / Airflow / 其它模块直接 import 调用）。

链路划分：

1. **本地中转（data/inbox → data/raw → DuckDB → 导出）** — 不落远程库。
2. **本地中转 + Supabase/PostgreSQL** — 与 1 相同物化与转换，最后 `publish_to_postgres`。
3. **其它数据源**仍通过 `run_pipeline` + `PipelineOptions` 组合；本文件只封装最高频的两条 inbox 链路。
"""

from __future__ import annotations

from etl_project.config import ProjectPaths, build_paths, read_postgres_dsn
from etl_project.models import PipelineOptions, PipelineResult
from etl_project.etl.pipeline import run_pipeline


def run_inbox_to_duckdb(
    paths: ProjectPaths | None = None,
    *,
    export_summary: bool = True,
    incremental_load: bool = False,
    fail_on_quality_error: bool = True,
) -> PipelineResult:
    """`data/inbox`（json/csv）→ `data/raw` → DuckDB mart → 本地导出。"""

    return run_pipeline(
        paths,
        PipelineOptions(
            target="duckdb",
            data_source="inbox",
            export_summary=export_summary,
            incremental_load=incremental_load,
            fail_on_quality_error=fail_on_quality_error,
        ),
    )


def run_inbox_to_supabase(
    paths: ProjectPaths | None = None,
    *,
    postgres_dsn: str = "",
    export_summary: bool = True,
    incremental_load: bool = False,
    fail_on_quality_error: bool = True,
) -> PipelineResult:
    """`data/inbox` → raw → DuckDB → **PostgreSQL / Supabase**（连接串同 `ETL_POSTGRES_DSN`）。"""

    resolved_dsn = postgres_dsn.strip() or read_postgres_dsn()

    return run_pipeline(
        paths,
        PipelineOptions(
            target="postgres",
            data_source="inbox",
            export_summary=export_summary,
            postgres_dsn=resolved_dsn,
            incremental_load=incremental_load,
            fail_on_quality_error=fail_on_quality_error,
        ),
    )


def run_csv_to_supabase(
    paths: ProjectPaths | None = None,
    *,
    postgres_dsn: str = "",
    export_summary: bool = True,
    incremental_load: bool = False,
    fail_on_quality_error: bool = True,
) -> PipelineResult:
    """直接使用 `data/raw` 已有 CSV，转换后发布到 PostgreSQL / Supabase。"""

    resolved_dsn = postgres_dsn.strip() or read_postgres_dsn()

    return run_pipeline(
        paths,
        PipelineOptions(
            target="postgres",
            data_source="csv",
            export_summary=export_summary,
            postgres_dsn=resolved_dsn,
            incremental_load=incremental_load,
            fail_on_quality_error=fail_on_quality_error,
        ),
    )


def default_paths() -> ProjectPaths:
    """返回默认项目路径（与 `python -m etl_project` 一致）。"""

    return build_paths()
