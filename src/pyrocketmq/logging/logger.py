"""
Logger工厂类

提供logger创建和配置管理功能。
"""

import logging
import sys

from pyrocketmq.logging.json_formatter import JsonFormatter

from .config import LoggingConfig


class LoggerFactory:
    """
    Logger工厂类

    负责创建和管理logger实例，确保配置的一致性。
    """

    # 类变量，保存全局配置
    _config: LoggingConfig | None = None
    _configured_loggers: set[str] = set()
    _default_formatter: logging.Formatter | None = None

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        获取指定名称的logger实例

        Args:
            name: logger名称

        Returns:
            logging.Logger: 配置好的logger实例
        """
        # 如果还没有配置，使用默认配置
        if cls._config is None:
            cls.setup_default_config(LoggingConfig())

        logger = logging.getLogger(name)

        # 如果这个logger还没有被配置过
        if name not in cls._configured_loggers:
            cls._configure_logger(logger)
            cls._configured_loggers.add(name)

        return logger

    @classmethod
    def setup_default_config(cls, config: LoggingConfig) -> None:
        """
        设置默认配置

        Args:
            config: 日志配置
        """
        cls._config = config
        cls._default_formatter = cls._create_formatter(config)

        # 配置根logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, config.level))

        # 清除现有的处理器
        root_logger.handlers.clear()

        # 创建JSON格式化器
        json_formatter = JsonFormatter(
            include_timestamp=True,
            include_level=True,
            include_logger=True,
            include_module=True,
            include_function=True,
            include_line=True,
            include_extra=True,
        )

        # 添加处理器
        if config.console_output:
            console_handler = cls._create_console_handler(config)
            root_logger.addHandler(console_handler)
            if config.json_output:
                console_handler.setFormatter(json_formatter)

        if config.file_path:
            file_handler = cls._create_file_handler(config)
            root_logger.addHandler(file_handler)

            if config.json_output:
                file_handler.setFormatter(json_formatter)

        # 重置已配置的logger集合
        cls._configured_loggers.clear()

    @classmethod
    def get_current_config(cls) -> LoggingConfig:
        """
        获取当前配置

        Returns:
            LoggingConfig: 当前配置
        """
        if cls._config is None:
            cls.setup_default_config(LoggingConfig())
            if cls._config is None:
                raise RuntimeError("LoggerFactory配置未正确初始化")
        return cls._config

    @classmethod
    def _configure_logger(cls, logger: logging.Logger) -> None:
        """
        配置单个logger

        Args:
            logger: 要配置的logger实例
        """
        # 设置日志级别
        if cls._config:
            logger.setLevel(getattr(logging, cls._config.level))

        # 确保logger能将消息传播到根logger
        logger.propagate = True

    @classmethod
    def _create_formatter(cls, config: LoggingConfig) -> logging.Formatter:
        """
        创建日志格式化器

        Args:
            config: 日志配置

        Returns:
            logging.Formatter: 格式化器实例
        """
        if config.colored_output and config.console_output:
            # 彩色输出格式
            return ColoredFormatter(config.format, datefmt=config.date_format)
        else:
            # 普通格式
            return logging.Formatter(config.format, datefmt=config.date_format)

    @classmethod
    def _create_console_handler(cls, config: LoggingConfig) -> logging.Handler:
        """
        创建控制台处理器

        Args:
            config: 日志配置

        Returns:
            logging.Handler: 控制台处理器
        """
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, config.level))
        handler.setFormatter(cls._default_formatter)
        return handler

    @classmethod
    def _create_file_handler(cls, config: LoggingConfig) -> logging.Handler:
        """
        创建文件处理器

        Args:
            config: 日志配置

        Returns:
            logging.Handler: 文件处理器
        """
        # 如果需要文件轮转，使用RotatingFileHandler
        if config.max_file_size and config.backup_count > 0:
            try:
                from logging.handlers import RotatingFileHandler

                return RotatingFileHandler(
                    config.file_path or "pyrocketmq.log",
                    maxBytes=config.max_file_size,
                    backupCount=config.backup_count,
                    encoding=config.file_encoding,
                    mode=config.file_mode,
                )
            except ImportError:
                # 如果无法导入，使用普通FileHandler
                pass

        # 普通文件处理器
        return logging.FileHandler(
            config.file_path or "pyrocketmq.log",
            encoding=config.file_encoding,
            mode=config.file_mode,
        )


class ColoredFormatter(logging.Formatter):
    """
    彩色日志格式化器

    为不同级别的日志添加颜色显示。
    """

    # ANSI颜色代码
    COLORS = {
        "DEBUG": "\033[36m",  # 青色
        "INFO": "\033[32m",  # 绿色
        "WARNING": "\033[33m",  # 黄色
        "ERROR": "\033[31m",  # 红色
        "CRITICAL": "\033[35m",  # 紫色
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        """
        格式化日志记录，添加颜色

        Args:
            record: 日志记录

        Returns:
            str: 格式化后的日志消息
        """
        # 添加颜色
        if record.levelname in self.COLORS:
            record.levelname = (
                f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
            )

        # 调用父类的format方法
        return super().format(record)
