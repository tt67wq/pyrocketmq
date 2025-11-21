"""
JSON格式化器

提供结构化的JSON日志输出功能，便于日志分析和监控。
"""

import json
import logging
import time
from typing import Any, Literal


class JsonFormatter(logging.Formatter):
    """
    JSON格式化器

    将日志记录转换为JSON格式，便于机器解析和分析。
    """

    def __init__(
        self,
        include_extra: bool = True,
        include_timestamp: bool = True,
        include_level: bool = True,
        include_logger: bool = False,
        include_module: bool = True,
        include_function: bool = True,
        include_line: bool = True,
        indent: bool = False,
        ensure_ascii: bool = False,
        **extra_fields: Any,
    ) -> None:
        """
        初始化JSON格式化器

        Args:
            include_extra: 是否包含extra字段
            include_timestamp: 是否包含时间戳
            include_level: 是否包含日志级别
            include_logger: 是否包含logger名称
            include_module: 是否包含模块名
            include_function: 是否包含函数名
            include_line: 是否包含行号
            indent: 是否缩进JSON输出
            ensure_ascii: 是否确保ASCII编码
            **extra_fields: 额外的固定字段
        """
        super().__init__()
        self.include_extra: bool = include_extra
        self.include_timestamp: bool = include_timestamp
        self.include_level: bool = include_level
        self.include_logger: bool = include_logger
        self.include_module: bool = include_module
        self.include_function: bool = include_function
        self.include_line: bool = include_line
        self.indent: bool = indent
        self.ensure_ascii: bool = ensure_ascii
        self.extra_fields: dict[str, Any] = extra_fields

    def format(self, record: logging.LogRecord) -> str:
        """
        格式化日志记录为JSON

        Args:
            record: 日志记录

        Returns:
            str: JSON格式的日志字符串
        """
        log_entry: dict[str, Any] = {}

        # 添加固定字段
        log_entry.update(self.extra_fields)

        # 添加时间戳
        if self.include_timestamp:
            # log_entry["timestamp"] = record.created
            log_entry["asctime"] = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(record.created)
            )
            # log_entry["msecs"] = record.msecs

        # 添加日志级别
        if self.include_level:
            log_entry["level"] = record.levelname
            # log_entry["levelno"] = record.levelno

        # 添加logger信息
        if self.include_logger:
            log_entry["logger"] = record.name

        # 添加代码位置信息
        if self.include_module and hasattr(record, "module"):
            log_entry["module"] = record.module
        elif hasattr(record, "pathname"):
            log_entry["module"] = record.pathname.split("/")[-1].replace(".py", "")

        if self.include_function:
            log_entry["function"] = record.funcName

        if self.include_line:
            log_entry["line"] = record.lineno

        # 添加消息
        log_entry["message"] = record.getMessage()

        # 添加异常信息
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            log_entry["exception"] = record.exc_text

        # 添加堆栈信息
        if record.stack_info:
            log_entry["stack"] = self.formatStack(record.stack_info)

        # 添加额外字段
        if self.include_extra and hasattr(record, "__dict__"):
            # 过滤掉标准字段
            excluded_fields: set[str] = {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "getMessage",
                "exc_info",
                "exc_text",
                "stack_info",
                "exc_info",
                "message",
            }
            extra_fields: dict[str, Any] = {
                k: v for k, v in record.__dict__.items() if k not in excluded_fields
            }
            log_entry.update(extra_fields)

        # 序列化为JSON
        try:
            indent_value: int | None = 2 if self.indent else None
            separators_value: tuple[str, str] | None = (
                (",", ":") if not self.indent else None
            )

            return json.dumps(
                log_entry,
                ensure_ascii=self.ensure_ascii,
                indent=indent_value,
                separators=separators_value,
                default=str,
            )
        except (TypeError, ValueError) as e:
            # 如果序列化失败，回退到基本格式
            fallback_entry: dict[str, Any] = {
                "timestamp": record.created,
                "level": record.levelname,
                "message": record.getMessage(),
                "serialization_error": str(e),
            }
            return json.dumps(fallback_entry, ensure_ascii=self.ensure_ascii)


class ExtraAwareFormatter(logging.Formatter):
    """
    支持extra字段的普通格式化器

    在普通文本格式中也能显示extra信息，同时支持彩色输出。
    """

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%"] = "%",
        colored_output: bool = True,
    ) -> None:
        """
        初始化支持extra字段的格式化器

        Args:
            fmt: 日志格式字符串
            datefmt: 日期格式
            style: 格式风格
            colored_output: 是否启用彩色输出
        """
        super().__init__(fmt, datefmt, style)
        self.colored_output: bool = colored_output

    def format(self, record: logging.LogRecord) -> str:
        """
        格式化日志记录，包含extra字段

        Args:
            record: 日志记录

        Returns:
            str: 格式化后的日志字符串
        """
        # 调用父类格式化
        formatted: str = super().format(record)

        # 检查是否有extra字段
        extra_fields: dict[str, Any] = {}
        if hasattr(record, "__dict__"):
            # 过滤掉标准logging字段，只保留用户自定义的extra字段
            standard_fields: set[str] = {
                # 基础字段
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "message",
                # 位置信息
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                # 时间信息
                "created",
                "msecs",
                "relativeCreated",
                "asctime",
                # 线程进程信息
                "thread",
                "threadName",
                "process",
                "processName",
                # 异常和堆栈信息
                "exc_info",
                "exc_text",
                "stack_info",
                # 方法引用
                "getMessage",
            }
            extra_fields = {
                k: v
                for k, v in record.__dict__.items()
                if k not in standard_fields and v is not None
            }

        # 如果有extra字段，追加到格式化消息后面
        if extra_fields:
            extra_str: str = " | ".join([f"{k}={v}" for k, v in extra_fields.items()])
            formatted = f"{formatted} [{extra_str}]"

        return formatted


class StructuredJsonFormatter(JsonFormatter):
    """
    结构化JSON格式化器

    提供更结构化的日志字段，便于特定场景的日志分析。
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        格式化为结构化JSON

        Args:
            record: 日志记录

        Returns:
            str: 结构化JSON格式的日志字符串
        """
        # 创建基础结构
        log_entry: dict[str, Any] = {
            "@timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime(record.created)
            ),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "source": {
                "logger": record.name,
                "module": getattr(
                    record, "module", record.pathname.split("/")[-1].replace(".py", "")
                ),
                "function": record.funcName,
                "line": record.lineno,
            },
        }

        # 添加进程和线程信息
        log_entry["process"] = {"id": record.process, "name": record.processName}
        log_entry["thread"] = {"id": record.thread, "name": record.threadName}

        # 添加异常信息
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }

        # 添加额外字段
        if self.include_extra and hasattr(record, "__dict__"):
            excluded_fields: set[str] = {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "getMessage",
                "exc_info",
                "exc_text",
                "stack_info",
            }
            extra_fields: dict[str, Any] = {
                k: v for k, v in record.__dict__.items() if k not in excluded_fields
            }
            if extra_fields:
                log_entry["fields"] = extra_fields

        # 添加自定义字段
        log_entry.update(self.extra_fields)

        # 序列化为JSON
        try:
            return json.dumps(
                log_entry,
                ensure_ascii=self.ensure_ascii,
                indent=None,
                separators=(",", ":"),
                default=str,
            )
        except (TypeError, ValueError) as e:
            # 如果序列化失败，回退到基本格式
            fallback_entry: dict[str, Any] = {
                "@timestamp": time.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime(record.created)
                ),
                "level": record.levelname.lower(),
                "message": record.getMessage(),
                "serialization_error": str(e),
            }
            return json.dumps(fallback_entry, ensure_ascii=self.ensure_ascii)
