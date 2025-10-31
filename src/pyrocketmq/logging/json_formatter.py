"""
JSON格式化器

提供结构化的JSON日志输出功能，便于日志分析和监控。
"""

import json
import logging
import time
from typing import Any


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
        include_logger: bool = True,
        include_module: bool = True,
        include_function: bool = True,
        include_line: bool = True,
        indent: bool = False,
        ensure_ascii: bool = False,
        **extra_fields: Any,
    ):
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
        self.include_extra = include_extra
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_logger = include_logger
        self.include_module = include_module
        self.include_function = include_function
        self.include_line = include_line
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.extra_fields = extra_fields

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
            log_entry["timestamp"] = record.created
            log_entry["asctime"] = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(record.created)
            )
            log_entry["msecs"] = record.msecs

        # 添加日志级别
        if self.include_level:
            log_entry["level"] = record.levelname
            log_entry["levelno"] = record.levelno

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
            extra_fields = {
                k: v
                for k, v in record.__dict__.items()
                if k
                not in {
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
            }
            log_entry.update(extra_fields)

        # 序列化为JSON
        try:
            return json.dumps(
                log_entry,
                ensure_ascii=self.ensure_ascii,
                indent=2 if self.indent else None,
                separators=(",", ":") if not self.indent else None,
                default=str,
            )
        except (TypeError, ValueError) as e:
            # 如果序列化失败，回退到基本格式
            fallback_entry = {
                "timestamp": record.created,
                "level": record.levelname,
                "message": record.getMessage(),
                "serialization_error": str(e),
            }
            return json.dumps(fallback_entry, ensure_ascii=self.ensure_ascii)


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
        log_entry = {
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
            extra_fields = {
                k: v
                for k, v in record.__dict__.items()
                if k
                not in {
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
            fallback_entry = {
                "@timestamp": time.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime(record.created)
                ),
                "level": record.levelname.lower(),
                "message": record.getMessage(),
                "serialization_error": str(e),
            }
            return json.dumps(fallback_entry, ensure_ascii=self.ensure_ascii)
