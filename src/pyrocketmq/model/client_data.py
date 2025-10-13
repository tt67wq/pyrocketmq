"""
客户端数据结构
定义RocketMQ生产者和消费者的数据结构。
"""

import ast
from dataclasses import dataclass, field
from typing import Any, Dict

from .errors import DeserializationError


@dataclass
class ProducerData:
    """生产者信息

    用于向Broker注册生产者信息
    """

    group_name: str  # 生产者组名

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {"groupName": self.group_name}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProducerData":
        """从字典创建实例"""
        return cls(group_name=data["groupName"])

    @classmethod
    def from_bytes(cls, data: bytes) -> "ProducerData":
        """从字节数据创建实例

        Args:
            data: 原始响应体字节数据

        Returns:
            ProducerData: ProducerData实例

        Raises:
            DeserializationError: 数据格式无效时抛出异常
        """
        try:
            data_str = data.decode("utf-8")
            parsed_data = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(
                f"Failed to parse ProducerData from bytes: {e}"
            )
        except Exception as e:
            raise DeserializationError(f"Invalid ProducerData format: {e}")

    def __str__(self) -> str:
        """字符串表示"""
        return f"ProducerData[groupName={self.group_name}]"


@dataclass
class ConsumerData:
    """消费者信息

    用于向Broker注册消费者信息
    """

    group_name: str  # 消费者组名
    consume_type: str  # 消费类型 (PUSH/PULL)
    message_model: str  # 消费模式 (BROADCASTING/CLUSTERING)
    consume_from_where: (
        str  # 消费起始位置 (CONSUME_FROM_LAST_OFFSET/CONSUME_FROM_FIRST_OFFSET)
    )
    subscription_data: Dict[str, str] = field(
        default_factory=dict
    )  # 订阅关系 topic -> expression

    def __post_init__(self):
        """后处理，确保subscription_data不为None"""
        if self.subscription_data is None:
            self.subscription_data = {}

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "groupName": self.group_name,
            "consumeType": self.consume_type,
            "messageModel": self.message_model,
            "consumeFromWhere": self.consume_from_where,
            "subscriptionData": self.subscription_data,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConsumerData":
        """从字典创建实例"""
        return cls(
            group_name=data["groupName"],
            consume_type=data["consumeType"],
            message_model=data["messageModel"],
            consume_from_where=data["consumeFromWhere"],
            subscription_data=data.get("subscriptionData", {}),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "ConsumerData":
        """从字节数据创建实例

        Args:
            data: 原始响应体字节数据

        Returns:
            ConsumerData: ConsumerData实例

        Raises:
            DeserializationError: 数据格式无效时抛出异常
        """
        try:
            data_str = data.decode("utf-8")
            parsed_data = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(
                f"Failed to parse ConsumerData from bytes: {e}"
            )
        except Exception as e:
            raise DeserializationError(f"Invalid ConsumerData format: {e}")

    def add_subscription(self, topic: str, expression: str) -> None:
        """添加订阅关系

        Args:
            topic: 主题名称
            expression: 订阅表达式
        """
        self.subscription_data[topic] = expression

    def remove_subscription(self, topic: str) -> bool:
        """移除订阅关系

        Args:
            topic: 主题名称

        Returns:
            是否成功移除
        """
        return self.subscription_data.pop(topic, None) is not None

    def get_subscription(self, topic: str) -> str:
        """获取主题的订阅表达式

        Args:
            topic: 主题名称

        Returns:
            订阅表达式，如果不存在则返回None
        """
        return self.subscription_data.get(topic)

    def has_subscription(self, topic: str) -> bool:
        """检查是否订阅了指定主题

        Args:
            topic: 主题名称

        Returns:
            是否订阅了该主题
        """
        return topic in self.subscription_data

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ConsumerData[groupName={self.group_name}, "
            f"consumeType={self.consume_type}, "
            f"messageModel={self.message_model}, "
            f"subscriptions={len(self.subscription_data)}]"
        )
