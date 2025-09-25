"""
日志配置管理

定义日志配置相关的数据类和配置管理功能。
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class LoggingConfig:
    """
    日志配置类

    提供完整的日志配置选项，支持文件输出和控制台输出。
    """

    # 日志级别
    level: str = "INFO"

    # 日志格式
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 日期格式
    date_format: str = "%Y-%m-%d %H:%M:%S"

    # 文件输出配置
    file_path: Optional[str] = None
    file_mode: str = "a"
    file_encoding: str = "utf-8"

    # 控制台输出配置
    console_output: bool = True

    # 高级配置
    max_file_size: Optional[int] = None  # 字节数，None表示不限制
    backup_count: int = 0  # 备份文件数量

    # 是否捕获异常信息
    capture_exceptions: bool = True

    # 是否显示日志级别颜色
    colored_output: bool = True

    @classmethod
    def from_env(cls) -> "LoggingConfig":
        """
        从环境变量创建配置实例

        支持的环境变量：
        - PYROCKETMQ_LOG_LEVEL: 日志级别
        - PYROCKETMQ_LOG_FILE: 日志文件路径
        - PYROCKETMQ_LOG_CONSOLE: 是否输出到控制台 (true/false)

        Returns:
            LoggingConfig: 配置实例
        """
        config = cls()

        # 从环境变量读取配置
        if log_level := os.getenv("PYROCKETMQ_LOG_LEVEL"):
            config.level = log_level.upper()

        if log_file := os.getenv("PYROCKETMQ_LOG_FILE"):
            config.file_path = log_file

        if console_output := os.getenv("PYROCKETMQ_LOG_CONSOLE"):
            config.console_output = console_output.lower() in (
                "true",
                "1",
                "yes",
            )

        return config

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            "level": self.level,
            "format": self.format,
            "date_format": self.date_format,
            "file_path": self.file_path,
            "file_mode": self.file_mode,
            "file_encoding": self.file_encoding,
            "console_output": self.console_output,
            "max_file_size": self.max_file_size,
            "backup_count": self.backup_count,
            "capture_exceptions": self.capture_exceptions,
            "colored_output": self.colored_output,
        }

    def __post_init__(self):
        """验证配置参数"""
        # 验证日志级别
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            raise ValueError(
                f"Invalid log level: {self.level}. Must be one of {valid_levels}"
            )

        # 标准化日志级别
        self.level = self.level.upper()

        # 验证文件模式
        valid_modes = ["a", "w"]
        if self.file_mode not in valid_modes:
            raise ValueError(
                f"Invalid file mode: {self.file_mode}. Must be one of {valid_modes}"
            )
