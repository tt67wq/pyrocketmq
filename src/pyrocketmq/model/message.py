"""
消息数据结构
定义RocketMQ消息的核心数据结构，与Go语言实现保持兼容。
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .message_queue import MessageQueue


@dataclass
class Message:
    """RocketMQ消息数据结构

    与Go语言实现保持兼容的消息类，包含消息的基本信息和属性。
    """

    # 基本消息属性
    topic: str  # 主题名称
    body: bytes  # 消息体
    flag: int = 0  # 消息标志
    transaction_id: Optional[str] = None  # 事务ID
    batch: bool = False  # 是否为批量消息
    compress: bool = False  # 是否压缩

    # 消息队列（可选）
    queue: Optional[MessageQueue] = None  # 指定的消息队列

    # 消息属性
    properties: Dict[str, str] = field(default_factory=dict)  # 消息属性字典

    def __post_init__(self):
        """后处理，确保数据类型正确"""
        if not isinstance(self.body, bytes):
            if isinstance(self.body, str):
                self.body = self.body.encode("utf-8")
            else:
                self.body = str(self.body).encode("utf-8")

        # 确保properties是字典
        if self.properties is None:
            self.properties = {}

    # 属性管理便利方法
    def get_property(
        self, key: str, default: Optional[str] = None
    ) -> Optional[str]:
        """获取消息属性

        Args:
            key: 属性键
            default: 默认值

        Returns:
            属性值或默认值
        """
        return self.properties.get(key, default)

    def set_property(self, key: str, value: str) -> None:
        """设置消息属性

        Args:
            key: 属性键
            value: 属性值
        """
        self.properties[key] = value

    def remove_property(self, key: str) -> Optional[str]:
        """移除消息属性

        Args:
            key: 属性键

        Returns:
            被移除的属性值，如果不存在则返回None
        """
        return self.properties.pop(key, None)

    def has_property(self, key: str) -> bool:
        """检查是否包含指定属性

        Args:
            key: 属性键

        Returns:
            是否包含该属性
        """
        return key in self.properties

    def clear_properties(self) -> None:
        """清空所有消息属性"""
        self.properties.clear()

    def get_property_keys(self) -> List[str]:
        """获取所有属性键

        Returns:
            属性键列表
        """
        return list(self.properties.keys())

    # 标签和键的便利方法
    def get_tags(self) -> Optional[str]:
        """获取消息标签

        Returns:
            消息标签
        """
        return self.get_property("TAGS")

    def set_tags(self, tags: str) -> None:
        """设置消息标签

        Args:
            tags: 消息标签
        """
        self.set_property("TAGS", tags)

    def get_keys(self) -> Optional[str]:
        """获取消息键

        Returns:
            消息键（多个键用空格分隔）
        """
        return self.get_property("KEYS")

    def set_keys(self, keys: str) -> None:
        """设置消息键

        Args:
            keys: 消息键（多个键用空格分隔）
        """
        self.set_property("KEYS", keys)

    def add_key(self, key: str) -> None:
        """添加单个消息键

        Args:
            key: 消息键
        """
        current_keys = self.get_keys()
        if current_keys:
            new_keys = f"{current_keys} {key}"
        else:
            new_keys = key
        self.set_keys(new_keys)

    # 延时消息便利方法
    def get_delay_time_level(self) -> int:
        """获取延时级别

        Returns:
            延时级别
        """
        level_str = self.get_property("DELAY")
        return int(level_str) if level_str else 0

    def set_delay_time_level(self, level: int) -> None:
        """设置延时级别

        Args:
            level: 延时级别
        """
        self.set_property("DELAY", str(level))

    # 重试消息便利方法
    def get_retry_topic(self) -> Optional[str]:
        """获取重试主题

        Returns:
            重试主题
        """
        return self.get_property("RETRY_TOPIC")

    def set_retry_topic(self, topic: str) -> None:
        """设置重试主题

        Args:
            topic: 重试主题
        """
        self.set_property("RETRY_TOPIC", topic)

    def is_retry_message(self) -> bool:
        """判断是否为重试消息

        Returns:
            是否为重试消息
        """
        return self.has_property("RETRY_TOPIC")

    # 事务消息便利方法
    def is_transaction_message(self) -> bool:
        """判断是否为事务消息

        Returns:
            是否为事务消息
        """
        return self.has_property("TRAN_MSG") or self.transaction_id is not None

    def mark_as_prepared_transaction(self) -> None:
        """标记为预提交事务消息"""
        self.set_property("TRAN_MSG", "true")

    # 序列化方法
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式

        Returns:
            消息字典
        """
        result = {
            "topic": self.topic,
            "body": self.body.decode("utf-8", errors="ignore"),
            "flag": self.flag,
            "batch": self.batch,
            "compress": self.compress,
            "properties": dict(self.properties),
        }

        if self.transaction_id:
            result["transactionId"] = self.transaction_id

        if self.queue:
            result["queue"] = self.queue.to_dict()

        return result

    def to_json(self) -> str:
        """转换为JSON字符串

        Returns:
            JSON字符串
        """
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """从字典创建消息实例

        Args:
            data: 消息字典

        Returns:
            Message实例
        """
        # 处理body字段
        body = b""
        if "body" in data:
            body_data = data["body"]
            if isinstance(body_data, str):
                body = body_data.encode("utf-8")
            elif isinstance(body_data, bytes):
                body = body_data
            else:
                body = str(body_data).encode("utf-8")

        # 提取properties
        properties = data.get("properties", {})
        if not isinstance(properties, dict):
            properties = {}

        # 提取queue
        queue = None
        if "queue" in data:
            from .message_queue import MessageQueue

            queue_data = data["queue"]
            if isinstance(queue_data, dict):
                queue = MessageQueue.from_dict(queue_data)

        return cls(
            topic=data["topic"],
            body=body,
            flag=data.get("flag", 0),
            transaction_id=data.get("transactionId"),
            batch=data.get("batch", False),
            compress=data.get("compress", False),
            queue=queue,
            properties=properties,
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        """从JSON字符串创建消息实例

        Args:
            json_str: JSON字符串

        Returns:
            Message实例
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    # 复制方法
    def copy(self) -> "Message":
        """创建消息的副本

        Returns:
            消息副本
        """
        return Message(
            topic=self.topic,
            body=self.body,
            flag=self.flag,
            transaction_id=self.transaction_id,
            batch=self.batch,
            compress=self.compress,
            queue=self.queue,
            properties=dict(self.properties),
        )

    def __str__(self) -> str:
        """字符串表示"""
        body_preview = self.body[:50].decode("utf-8", errors="ignore")
        if len(self.body) > 50:
            body_preview += "..."

        return (
            f"Message[topic={self.topic}, "
            f"bodySize={len(self.body)}, "
            f"bodyPreview='{body_preview}', "
            f"flag={self.flag}, "
            f"propertiesCount={len(self.properties)}]"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (
            f"Message(topic='{self.topic}', "
            f"bodySize={len(self.body)}, "
            f"flag={self.flag}, "
            f"transactionId='{self.transaction_id}', "
            f"batch={self.batch}, "
            f"compress={self.compress}, "
            f"properties={self.properties})"
        )


# 便利工厂函数
def create_message(
    topic: str,
    body: Any,
    tags: Optional[str] = None,
    keys: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
) -> Message:
    """创建消息的便利函数

    Args:
        topic: 主题名称
        body: 消息体（任意类型，会被转换为bytes）
        tags: 消息标签
        keys: 消息键
        properties: 额外的消息属性

    Returns:
        Message实例
    """
    msg = Message(topic=topic, body=body)

    if tags:
        msg.set_tags(tags)
    if keys:
        msg.set_keys(keys)
    if properties:
        for key, value in properties.items():
            msg.set_property(key, value)

    return msg


def create_transaction_message(
    topic: str,
    body: Any,
    transaction_id: str,
    tags: Optional[str] = None,
    keys: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
) -> Message:
    """创建事务消息的便利函数

    Args:
        topic: 主题名称
        body: 消息体
        transaction_id: 事务ID
        tags: 消息标签
        keys: 消息键
        properties: 额外的消息属性

    Returns:
        事务消息Message实例
    """
    msg = create_message(topic, body, tags, keys, properties)
    msg.transaction_id = transaction_id
    msg.mark_as_prepared_transaction()
    return msg


def create_delay_message(
    topic: str,
    body: Any,
    delay_level: int,
    tags: Optional[str] = None,
    keys: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
) -> Message:
    """创建延时消息的便利函数

    Args:
        topic: 主题名称
        body: 消息体
        delay_level: 延时级别
        tags: 消息标签
        keys: 消息键
        properties: 额外的消息属性

    Returns:
        延时消息Message实例
    """
    msg = create_message(topic, body, tags, keys, properties)
    msg.set_delay_time_level(delay_level)
    return msg
