"""
客户端数据结构
定义RocketMQ生产者和消费者的数据结构。
"""

import ast
from dataclasses import dataclass, field
from typing import Any, Dict, List

from .errors import DeserializationError


@dataclass
class SubscriptionData:
    """
    订阅数据
    包含消费者的订阅信息
    """

    topic: str
    sub_string: str = "*"
    sub_version: int = 0
    expression_type: str = "TAG"
    tags_set: List[str] = field(default_factory=list)
    code_set: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为字典格式"""
        return {
            "topic": self.topic,
            "subString": self.sub_string,
            "subVersion": self.sub_version,
            "expressionType": self.expression_type,
            "tagsSet": self.tags_set,
            "codeSet": self.code_set,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SubscriptionData":
        """从字典创建实例"""
        return cls(
            topic=data.get("topic", ""),
            sub_string=data.get("subString", "*"),
            sub_version=data.get("subVersion", 0),
            expression_type=data.get("expressionType", "TAG"),
            tags_set=data.get("tagsSet", []),
            code_set=data.get("codeSet", []),
        )

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"SubscriptionData[topic={self.topic}, subString={self.sub_string}]"
        )


@dataclass
class ProducerData:
    """生产者信息

    用于向Broker注册生产者信息
    """

    group_name: str  # 生产者组名

    def unique_id(self) -> str:
        """获取唯一标识符"""
        return self.group_name

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
    subscription_data: List[SubscriptionData] = field(
        default_factory=list
    )  # 订阅关系

    def unique_id(self) -> str:
        """获取唯一标识符，对应Go实现的UniqueID()方法"""
        return self.group_name

    def __post_init__(self):
        """后处理，确保subscription_data不为None"""
        if self.subscription_data is None:
            self.subscription_data = []

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

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ConsumerData[groupName={self.group_name}, "
            f"consumeType={self.consume_type}, "
            f"messageModel={self.message_model}, "
            f"subscriptions={len(self.subscription_data)}]"
        )
