"""
Producer工具函数模块 (MVP版本)

提供Producer常用的工具函数，包括客户端ID生成、消息验证等。
采用MVP设计理念，只包含最核心的功能。

MVP版本功能:
- 客户端ID生成
- 消息验证
- 消息大小计算

作者: pyrocketmq团队
版本: MVP 1.0
"""

import os
import socket

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message import Message

logger = get_logger(__name__)


def generate_client_id() -> str:
    """生成客户端唯一标识符

    使用主机名和进程ID组合生成唯一的客户端ID，
    格式为"hostname#pid"。这样可以确保在同一台机器上
    运行的多个Producer实例具有不同的client_id。

    Returns:
        str: 格式为"hostname#pid"的客户端ID
    """
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}#{pid}"


def validate_message(message: Message, max_size: int = 4 * 1024 * 1024) -> None:
    """验证消息的有效性

    检查消息是否符合RocketMQ的要求，包括主题、消息体等。
    如果验证失败，会抛出相应的异常。

    Args:
        message: 要验证的消息
        max_size: 允许的最大消息大小，默认4MB

    Raises:
        ValueError: 当消息验证失败时
    """
    if not message:
        raise ValueError("Message cannot be None")

    if not message.topic or not message.topic.strip():
        raise ValueError("Message topic cannot be empty")

    if len(message.topic) > 127:
        raise ValueError("Message topic length cannot exceed 127 characters")

    if not message.body:
        raise ValueError("Message body cannot be empty")

    if len(message.body) > max_size:
        raise ValueError(
            f"Message body size {len(message.body)} exceeds max size {max_size}"
        )

    # 验证主题格式（不能包含特殊字符）
    if any(
        char in message.topic
        for char in ["*", "/", ":", "|", "?", "<", ">", '"']
    ):
        raise ValueError("Message topic contains invalid characters")


def calculate_message_size(message: Message) -> int:
    """计算消息的总大小

    计算消息在序列化后的大致大小，用于压缩决策等。

    Args:
        message: 要计算大小的消息

    Returns:
        int: 消息的估计大小（字节）
    """
    size = 0

    # 主题大小
    size += len(message.topic.encode("utf-8"))

    # 消息体大小
    size += len(message.body)

    # 属性大小
    if message.properties:
        for key, value in message.properties.items():
            size += len(key.encode("utf-8"))
            size += len(value.encode("utf-8"))
            size += 2  # 分隔符和等号

    # 其他字段大小
    tags = message.get_tags()
    if tags:
        size += len(tags.encode("utf-8"))

    keys = message.get_keys()
    if keys:
        size += len(keys.encode("utf-8"))

    if message.flag is not None:
        size += 4  # int32

    if message.transaction_id:
        size += len(message.transaction_id.encode("utf-8"))

    # 布尔字段
    size += 1  # batch
    size += 1  # compress

    return size


def compress_message(message: Message) -> Message:
    """压缩消息体

    当消息体过大时，对消息体进行压缩以减少网络传输量。
    这是MVP版本的简单实现，后续可以扩展支持更多压缩算法。

    Args:
        message: 要压缩的消息

    Returns:
        Message: 压缩后的消息（新实例）
    """
    # TODO: 实现消息压缩
    # 这需要集成压缩库（如zlib）
    logger.debug("Message compression not implemented yet")
    return message


def decompress_message(message: Message) -> Message:
    """解压缩消息体

    解压缩被压缩的消息体。

    Args:
        message: 要解压缩的消息

    Returns:
        Message: 解压缩后的消息（新实例）
    """
    # TODO: 实现消息解压缩
    logger.debug("Message decompression not implemented yet")
    return message


def generate_message_id() -> str:
    """生成消息ID

    生成一个全局唯一的消息ID，用于标识消息。

    Returns:
        str: 消息ID
    """
    import time
    import uuid

    # 使用时间戳+UUID的组合确保唯一性
    timestamp = int(time.time() * 1000)
    unique_id = str(uuid.uuid4()).replace("-", "")[:16]
    return f"MSG{timestamp}{unique_id}"


def parse_broker_address(address: str) -> tuple[str, int]:
    """解析Broker地址

    从地址字符串中解析出主机名和端口号。

    Args:
        address: Broker地址，格式为"host:port"

    Returns:
        tuple[str, int]: 包含主机名和端口号的元组

    Raises:
        ValueError: 当地址格式无效时
    """
    if not address:
        raise ValueError("Broker address cannot be empty")

    if ":" in address:
        parts = address.split(":", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid broker address format: {address}")

        host, port_str = parts
        try:
            port = int(port_str)
            if port <= 0 or port > 65535:
                raise ValueError(f"Invalid port number: {port}")
            return host, port
        except ValueError as e:
            raise ValueError(
                f"Invalid port number in address {address}: {e}"
            ) from e
    else:
        # 默认端口9876
        return address, 9876
