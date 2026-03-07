from __future__ import annotations

import json
import logging
import sys
import traceback
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator, Mapping

_LOG_CONTEXT: ContextVar[dict[str, str]] = ContextVar("cryptoinsider_log_context", default={})


@dataclass(frozen=True)
class LoggingOptions:
    level: str
    fmt: str
    directory: str
    max_bytes: int
    backup_count: int


def build_logging_options(settings: Any) -> LoggingOptions:
    return LoggingOptions(
        level=str(getattr(settings, "log_level", "INFO") or "INFO"),
        fmt=str(getattr(settings, "log_format", "text") or "text"),
        directory=str(getattr(settings, "log_directory", "") or "").strip(),
        max_bytes=max(1, int(getattr(settings, "log_file_max_bytes", 10_485_760) or 10_485_760)),
        backup_count=max(1, int(getattr(settings, "log_file_backup_count", 5) or 5)),
    )


def new_trace_id(prefix: str = "evt") -> str:
    clean = "".join(ch for ch in prefix.strip().lower() if ch.isalnum()) or "evt"
    return f"{clean}-{uuid.uuid4().hex[:12]}"


@contextmanager
def bind_log_context(**items: Any) -> Iterator[None]:
    current = dict(_LOG_CONTEXT.get())
    for key, value in items.items():
        if value is None:
            continue
        clean_key = str(key).strip()
        if not clean_key:
            continue
        current[clean_key] = str(value)
    token = _LOG_CONTEXT.set(current)
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)


class _ContextFilter(logging.Filter):
    def __init__(self, service_name: str):
        super().__init__()
        self._service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self._service_name
        record.context = dict(_LOG_CONTEXT.get())
        return True


class _TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3] + "Z"
        base = (
            f"{timestamp} | {record.levelname} | {getattr(record, 'service', '-')}"
            f" | {record.name} | {record.getMessage()}"
        )
        context = getattr(record, "context", {})
        if isinstance(context, Mapping) and context:
            context_str = " ".join(
                f"{key}={value}"
                for key, value in sorted(context.items(), key=lambda item: item[0])
            )
            base = f"{base} | {context_str}"
        if record.exc_info:
            return f"{base}\n{self.formatException(record.exc_info)}"
        return base


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", "-"),
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "pid": record.process,
            "thread": record.threadName,
        }
        context = getattr(record, "context", {})
        if isinstance(context, Mapping) and context:
            payload["context"] = dict(context)
        if record.exc_info:
            payload["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else "Exception",
                "message": str(record.exc_info[1]),
                "traceback": "".join(traceback.format_exception(*record.exc_info)),
            }
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _build_formatter(fmt: str) -> logging.Formatter:
    if fmt.strip().lower() == "json":
        return _JsonFormatter()
    return _TextFormatter()


def _normalize_level(level: str) -> int:
    normalized = str(level or "INFO").strip().upper()
    if normalized.isdigit():
        return int(normalized)
    resolved = logging.getLevelName(normalized)
    if isinstance(resolved, int):
        return resolved
    return logging.INFO


def _service_log_filename(service_name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in service_name.strip().lower())
    return f"{cleaned or 'service'}.log"


def setup_logging(*, service_name: str, options: LoggingOptions) -> None:
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    level = _normalize_level(options.level)
    root_logger.setLevel(level)
    root_logger.propagate = False
    logging.captureWarnings(True)

    formatter = _build_formatter(options.fmt)
    context_filter = _ContextFilter(service_name=service_name)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(context_filter)
    root_logger.addHandler(stream_handler)

    if options.directory:
        try:
            log_dir = Path(options.directory)
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                filename=log_dir / _service_log_filename(service_name),
                maxBytes=options.max_bytes,
                backupCount=options.backup_count,
                encoding="utf-8",
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(context_filter)
            root_logger.addHandler(file_handler)
        except Exception as exc:
            root_logger.error(
                "Failed to initialize file logging directory=%s error=%s",
                options.directory,
                exc,
            )

    logging.getLogger("aiohttp.access").setLevel(max(level, logging.INFO))
    logging.getLogger("aiohttp.server").setLevel(max(level, logging.INFO))
    logging.getLogger("aiohttp.client").setLevel(max(level, logging.WARNING))
    logging.getLogger("asyncio").setLevel(max(level, logging.WARNING))
