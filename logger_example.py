#!/usr/bin/env python3
"""
LoggerFactory使用示例

演示如何使用LoggerFactory创建和管理logger实例。
"""

import os
import sys

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyrocketmq.logging import LoggerFactory, LoggingConfig


def basic_usage_example():
    """基本使用示例"""
    print("=== 基本使用示例 ===")

    # 获取logger
    logger = LoggerFactory.get_logger("my_app")

    # 记录不同级别的日志
    logger.debug("这是一条调试信息")
    logger.info("这是一条普通信息")
    logger.warning("这是一条警告信息")
    logger.error("这是一条错误信息")
    logger.critical("这是一条严重错误信息")


def multiple_loggers_example():
    """多个logger示例"""
    print("\n=== 多个logger示例 ===")

    # 创建多个logger
    app_logger = LoggerFactory.get_logger("my_app.main")
    db_logger = LoggerFactory.get_logger("my_app.database")
    network_logger = LoggerFactory.get_logger("my_app.network")

    # 各个logger记录日志
    app_logger.info("应用程序启动")
    db_logger.debug("连接数据库")
    network_logger.warning("网络连接不稳定")
    db_logger.error("数据库查询失败")


def custom_config_example():
    """自定义配置示例"""
    print("\n=== 自定义配置示例 ===")

    # 创建自定义配置
    config = LoggingConfig(
        level="DEBUG",  # 设置为DEBUG级别
        console_output=True,
        colored_output=True,
        file_path="example.log",
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
    )

    # 应用配置（这会重置所有已配置的logger）
    LoggerFactory.setup_default_config(config)

    # 获取新的logger实例
    logger = LoggerFactory.get_logger("custom_app")
    logger.debug("调试信息 - 将显示在控制台和写入文件")
    logger.info("普通信息 - 将显示在控制台和写入文件")

    print("日志已写入 example.log 文件")


def get_config_example():
    """获取当前配置示例"""
    print("\n=== 获取当前配置示例 ===")

    # 获取当前配置
    config = LoggerFactory.get_current_config()
    print(f"当前日志级别: {config.level}")
    print(f"控制台输出: {config.console_output}")
    print(f"彩色输出: {config.colored_output}")
    print(f"文件路径: {config.file_path}")
    print(f"日志格式: {config.format}")


if __name__ == "__main__":
    # 运行所有示例
    basic_usage_example()
    multiple_loggers_example()
    custom_config_example()
    get_config_example()

    print("\n=== 示例完成 ===")
