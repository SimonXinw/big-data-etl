"""项目配置。

这里统一管理路径和运行时配置，
让代码里只关心“做什么”，而不是到处拼接路径或读取环境变量。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """集中管理项目中会反复使用的路径。"""

    root_dir: Path
    raw_dir: Path
    inbox_dir: Path
    output_dir: Path
    warehouse_dir: Path
    sql_dir: Path
    docs_dir: Path
    bi_dir: Path
    customers_file: Path
    orders_file: Path
    database_file: Path
    export_file: Path
    quality_report_file: Path
    postgres_init_file: Path


def build_paths() -> ProjectPaths:
    """构建项目路径对象。

    通过当前文件所在位置反推项目根目录，因此不依赖运行命令的当前目录。
    """

    root_dir = Path(__file__).resolve().parent.parent
    raw_dir = root_dir / "data" / "raw"
    inbox_dir = root_dir / "data" / "inbox"
    output_dir = root_dir / "data" / "output"
    warehouse_dir = root_dir / "data" / "warehouse"
    sql_dir = root_dir / "sql"
    docs_dir = root_dir / "docs"
    bi_dir = root_dir / "bi"

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


def read_postgres_dsn() -> str:
    """读取 PostgreSQL / Supabase 连接串。

    这里使用一个统一变量名，便于同一套 Python ETL
    同时兼容普通 PostgreSQL 和 Supabase(Postgres) 场景。
    """

    return os.getenv("ETL_POSTGRES_DSN", "").strip()
