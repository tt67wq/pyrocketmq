"""
消息队列数据结构
定义RocketMQ消息队列的核心数据结构，与Go语言实现保持兼容。
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class MessageQueue:
    """消息队列信息

    表示一个主题下的具体队列，包含broker名称、队列ID等信息
    与Go语言实现保持兼容。
    """

    topic: str  # 主题名称
    broker_name: str  # broker名称
    queue_id: int  # 队列ID

    def __str__(self) -> str:
        """字符串表示"""
        return f"MessageQueue[topic={self.topic}, broker={self.broker_name}, queueId={self.queue_id}]"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return f"MessageQueue(topic='{self.topic}', broker_name='{self.broker_name}', queue_id={self.queue_id})"

    def __eq__(self, other: object) -> bool:
        """相等性比较"""
        if not isinstance(other, MessageQueue):
            return False
        return (
            self.topic == other.topic
            and self.broker_name == other.broker_name
            and self.queue_id == other.queue_id
        )

    def __hash__(self) -> int:
        """哈希值计算"""
        return hash((self.topic, self.broker_name, self.queue_id))

    # 序列化方法
    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            消息队列字典
        """
        return {
            "topic": self.topic,
            "brokerName": self.broker_name,
            "queueId": self.queue_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageQueue":
        """从字典创建消息队列实例

        Args:
            data: 消息队列字典

        Returns:
            MessageQueue实例
        """
        return cls(
            topic=data["topic"],
            broker_name=data["brokerName"],
            queue_id=data["queueId"],
        )

    # 复制方法
    def copy(self) -> "MessageQueue":
        """创建消息队列的副本

        Returns:
            消息队列副本
        """
        return MessageQueue(
            topic=self.topic,
            broker_name=self.broker_name,
            queue_id=self.queue_id,
        )

    # 验证方法
    def is_valid(self) -> bool:
        """验证消息队列是否有效

        Returns:
            是否有效
        """
        return bool(self.topic) and bool(self.broker_name) and self.queue_id >= 0

    @property
    def full_name(self) -> str:
        """获取完整名称

        Returns:
            完整名称格式：topic@brokerName:queueId
        """
        return f"{self.topic}@{self.broker_name}:{self.queue_id}"

    @classmethod
    def fake_message_queue(cls) -> "MessageQueue":
        """创建一个假的消息队列实例

        Returns:
            消息队列实例
        """
        return MessageQueue(
            topic="fake_topic",
            broker_name="fake_broker",
            queue_id=0,
        )
