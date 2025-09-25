"""
pyrocketmq 日志模块

提供简单易用的日志记录功能，支持配置管理和便捷接口。
"""

import logging
from typing import Optional

from .config import LoggingConfig
from .logger import LoggerFactory

__all__ = [
    "LoggingConfig",
    "LoggerFactory",
    "get_logger",
    "setup_logging",
    "get_config",
]


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的logger实例

    Args:
        name: logger名称，通常使用模块名

    Returns:
        logging.Logger: 配置好的logger实例

    Example:
        >>> logger = get_logger("transport")
        >>> logger.info("连接成功")
    """
    return LoggerFactory.get_logger(name)


def setup_logging(config: Optional[LoggingConfig] = None) -> None:
    """
    设置全局日志配置

    Args:
        config: 日志配置，如果为None则使用默认配置

    Example:
        >>> config = LoggingConfig(level="DEBUG")
        >>> setup_logging(config)
    """
    LoggerFactory.setup_default_config(config or LoggingConfig())


def get_config() -> LoggingConfig:
    """
    获取当前日志配置

    Returns:
        LoggingConfig: 当前使用的配置
    """
    return LoggerFactory.get_current_config()
