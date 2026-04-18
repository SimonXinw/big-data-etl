"""ETL / 同步任务用的统一日志（控制台、开始结束、耗时）。"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator


def configure_sync_logger(name: str = "etl_shopify_sync") -> logging.Logger:
    """获取已配置控制台 Handler 的 logger（重复调用不会重复挂载）。"""

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


@contextmanager
def log_timing(
    logger: logging.Logger,
    label: str,
    *,
    phase: str = "pipeline.timing",
) -> Iterator[None]:
    """记录一段逻辑的「开始 → 结束 → 耗时（秒）」。

    `phase` 传入 `logging` 的 `extra`，供 `PhaseFormatter`（见 `utils.pipeline_logging`）输出「阶段」列。
    """

    started_at = datetime.now().isoformat(timespec="seconds")
    t0 = time.perf_counter()
    logger.info("%s | 开始 | now=%s", label, started_at, extra={"phase": phase})

    try:
        yield
    finally:
        elapsed = time.perf_counter() - t0
        ended_at = datetime.now().isoformat(timespec="seconds")
        logger.info("%s | 结束 | now=%s | 耗时 %.2f 秒", label, ended_at, elapsed, extra={"phase": phase})
