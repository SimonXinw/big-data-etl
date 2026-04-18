"""管线级结构化日志：控制台彩色输出 + 写入项目根目录 `logs/`。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

SUCCESS_LEVEL_NUM = 25


def _ensure_success_level_registered() -> None:
    if logging.getLevelName(SUCCESS_LEVEL_NUM) == "Level 25":
        logging.addLevelName(SUCCESS_LEVEL_NUM, "SUCCESS")


_ensure_success_level_registered()


class PhaseFormatter(logging.Formatter):
    """支持 `phase` 字段；控制台可选 ANSI 着色。"""

    _LEVEL_COLORS = {
        "DEBUG": "\033[90m",
        "INFO": "\033[36m",
        "SUCCESS": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
        "RESET": "\033[0m",
    }

    def __init__(
        self,
        *,
        fmt: str,
        datefmt: str | None,
        colorize: bool,
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._colorize = bool(colorize)

    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "phase"):
            record.phase = "-"

        text = super().format(record)

        if not self._colorize:
            return text

        level_name = record.levelname
        prefix = self._LEVEL_COLORS.get(level_name, "")
        reset = self._LEVEL_COLORS["RESET"]

        if prefix and sys.stdout.isatty():
            return f"{prefix}{text}{reset}"

        return text


_PIPELINE_SINGLETON: PipelineLogger | None = None


class PipelineLogger:
    """结构化日志：`时间 | 级别 | 阶段 | 消息`；SUCCESS 为独立级别（绿色）。"""

    def __init__(self, name: str, underlying: logging.Logger) -> None:
        self._name = name
        self._logger = underlying

    @property
    def logger(self) -> logging.Logger:
        """供 `log_timing` 等使用标准 `logging.Logger` API。"""

        return self._logger

    def _emit(self, level: int, phase: str, msg: str, args: tuple[Any, ...]) -> None:
        text = msg % args if args else msg
        self._logger.log(level, text, extra={"phase": phase})

    def debug(self, phase: str, msg: str, *args: Any) -> None:
        self._emit(logging.DEBUG, phase, msg, args)

    def info(self, phase: str, msg: str, *args: Any) -> None:
        self._emit(logging.INFO, phase, msg, args)

    def success(self, phase: str, msg: str, *args: Any) -> None:
        self._emit(SUCCESS_LEVEL_NUM, phase, msg, args)

    def warn(self, phase: str, msg: str, *args: Any) -> None:
        self._emit(logging.WARNING, phase, msg, args)

    def error(self, phase: str, msg: str, *args: Any) -> None:
        self._emit(logging.ERROR, phase, msg, args)


def configure_pipeline_logger(name: str, logs_dir: Path) -> PipelineLogger:
    """创建带控制台色与文件输出的 logger（同名重复调用返回同一套单例包装）。"""

    global _PIPELINE_SINGLETON

    logs_dir.mkdir(parents=True, exist_ok=True)

    underlying = logging.getLogger(name)

    if not underlying.handlers:
        underlying.setLevel(logging.DEBUG)
        underlying.propagate = False

        fmt = "%(asctime)s | %(levelname)-7s | %(phase)s | %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"

        console_fmt = PhaseFormatter(fmt=fmt, datefmt=datefmt, colorize=True)
        file_fmt = PhaseFormatter(fmt=fmt, datefmt=datefmt, colorize=False)

        stream = logging.StreamHandler(sys.stdout)
        stream.setLevel(logging.DEBUG)
        stream.setFormatter(console_fmt)

        log_path = logs_dir / f"{name}.log"
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_fmt)

        underlying.addHandler(stream)
        underlying.addHandler(file_handler)

    _PIPELINE_SINGLETON = PipelineLogger(name, underlying)

    return _PIPELINE_SINGLETON


def get_pipeline_logger() -> PipelineLogger:
    """获取 `configure_pipeline_logger` 已创建的单例；若未配置则回退到项目默认 `logs/`。"""

    global _PIPELINE_SINGLETON

    if _PIPELINE_SINGLETON is not None:
        return _PIPELINE_SINGLETON

    from etl_project.config import build_paths

    root = build_paths().root_dir

    return configure_pipeline_logger("etl_shopify_sync", root / "logs")
