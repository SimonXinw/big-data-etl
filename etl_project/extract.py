"""抽取阶段。

本项目的抽取阶段保持简单：
只负责确认原始文件存在，并把输入数据源信息组织出来。
"""

# pyright: reportMissingImports=false

from __future__ import annotations

from etl_project.config import ProjectPaths
from etl_project.models import SourceFile


def get_source_files(paths: ProjectPaths) -> list[SourceFile]:
    """返回当前项目使用的原始数据文件列表。"""

    return [
        SourceFile(name="customers", path=paths.customers_file),
        SourceFile(name="orders", path=paths.orders_file),
    ]


def validate_sources(paths: ProjectPaths) -> list[SourceFile]:
    """校验原始文件是否存在。

    真实项目中，这里通常还会做文件格式、字段、日期分区等校验。
    """

    sources = get_source_files(paths)
    missing_files = [source.path for source in sources if not source.path.exists()]

    if missing_files:
        missing_text = ", ".join(str(path) for path in missing_files)
        raise FileNotFoundError(f"缺少原始数据文件: {missing_text}")

    return sources
