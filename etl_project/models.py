"""项目里会复用的数据模型。

把简单的数据结构集中放在一个文件里，
可以让配置、抽取、编排模块的职责更清晰。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SourceFile:
    """描述一个原始数据文件。"""

    name: str
    path: Path


@dataclass(frozen=True)
class PipelineOptions:
    """描述一次 ETL 运行的目标配置。"""

    target: str = "duckdb"
    data_source: str = "csv"
    export_summary: bool = True
    postgres_dsn: str = ""
    fail_on_quality_error: bool = True
    incremental_load: bool = False
    shopify_sync_days: int | None = None


@dataclass(frozen=True)
class QualityCheckResult:
    """描述一项数据质量检查的结果。"""

    name: str
    passed: bool
    message: str
    failed_rows: int = 0


@dataclass(frozen=True)
class QualityReport:
    """汇总一次 ETL 运行中的数据质量结果。"""

    checks: list[QualityCheckResult]

    @property
    def total_checks(self) -> int:
        return len(self.checks)

    @property
    def passed_checks(self) -> int:
        return sum(1 for check in self.checks if check.passed)

    @property
    def failed_checks(self) -> int:
        return sum(1 for check in self.checks if not check.passed)

    @property
    def is_successful(self) -> bool:
        return self.failed_checks == 0


@dataclass(frozen=True)
class PipelineResult:
    """记录一次 ETL 运行的关键结果。"""

    target: str
    database_path: str
    export_path: str
    loaded_sources: list[str]
    published_tables: list[str] = field(default_factory=list)
    quality_report: QualityReport | None = None
    quality_report_path: str = ""
    incremental_load: bool = False
    inserted_order_rows: int = 0
    data_source: str = "csv"
    shopify_customer_rows: int | None = None
    shopify_order_rows: int | None = None
    shopify_wide_orders_merged: int | None = None
    inbox_customer_rows: int | None = None
    inbox_order_rows: int | None = None
