#!/usr/bin/env python3
"""
配置文件读取工具模块

提供从配置文件读取Consumer配置的功能，支持JSON格式配置文件。
"""

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ConsumerConfig:
    """Consumer配置数据类"""

    topic: str
    group: str
    nameserver: str
    tag: Optional[str] = None
    message_model: str = "CLUSTERING"  # CLUSTERING 或 BROADCASTING
    consume_thread_min: int = 20
    consume_thread_max: int = 64
    pull_batch_size: int = 32

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConsumerConfig":
        """从字典创建配置对象"""
        return cls(
            topic=data.get("topic", ""),
            group=data.get("group", ""),
            nameserver=data.get("nameserver", ""),
            tag=data.get("tag"),
            message_model=data.get("message_model", "CLUSTERING"),
            consume_thread_min=data.get("consume_thread_min", 20),
            consume_thread_max=data.get("consume_thread_max", 64),
            pull_batch_size=data.get("pull_batch_size", 32),
        )


def load_config(config_path: str) -> ConsumerConfig:
    """
    从配置文件加载Consumer配置

    Args:
        config_path: 配置文件路径

    Returns:
        ConsumerConfig: 配置对象

    Raises:
        FileNotFoundError: 配置文件不存在
        json.JSONDecodeError: 配置文件格式错误
        ValueError: 必需的配置项缺失
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"配置文件格式错误: {e}", e.doc, e.pos)

    # 验证必需的配置项
    required_fields = ["topic", "group", "nameserver"]
    missing_fields = [field for field in required_fields if not config_data.get(field)]
    if missing_fields:
        raise ValueError(f"配置文件缺失必需字段: {', '.join(missing_fields)}")

    return ConsumerConfig.from_dict(config_data)


def get_config_file_path() -> str:
    """
    获取配置文件路径，默认为当前目录下的config.json

    Returns:
        str: 配置文件路径
    """
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "config.json")


def parse_config_file_path() -> str:
    """
    从命令行参数解析配置文件路径

    Returns:
        str: 配置文件路径

    Raises:
        SystemExit: 当参数解析失败时退出程序
    """
    parser = argparse.ArgumentParser(
        description="PyRocketMQ Consumer示例程序",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python cluster_concurrent_consumer.py                      # 使用默认配置文件
  python cluster_concurrent_consumer.py -c custom.json      # 使用自定义配置文件
  python cluster_concurrent_consumer.py --config config.json  # 使用完整参数名
        """,
    )

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=get_config_file_path(),
        help="配置文件路径 (默认: config.json)",
    )

    args, _ = parser.parse_known_args()
    return args.config
