"""
Broker 数据结构模型
定义与 RocketMQ Broker 交互所需的核心数据结构。
所有数据结构都与 RocketMQ Go 语言实现保持兼容。
"""

import ast
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..model.errors import DeserializationError


@dataclass
class MessageQueue:
    """消息队列信息

    表示一个主题下的具体队列，包含broker名称、队列ID等信息
    """

    topic: str  # 主题名称
    broker_name: str  # broker名称
    queue_id: int  # 队列ID

    def __str__(self) -> str:
        return f"MessageQueue[topic={self.topic}, broker={self.broker_name}, queueId={self.queue_id}]"

    def __eq__(self, other) -> bool:
        if not isinstance(other, MessageQueue):
            return False
        return (
            self.topic == other.topic
            and self.broker_name == other.broker_name
            and self.queue_id == other.queue_id
        )

    def __hash__(self) -> int:
        return hash((self.topic, self.broker_name, self.queue_id))

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "topic": self.topic,
            "brokerName": self.broker_name,
            "queueId": self.queue_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageQueue":
        """从字典创建实例"""
        return cls(
            topic=data["topic"],
            broker_name=data["brokerName"],
            queue_id=data["queueId"],
        )


@dataclass
class MessageExt:
    """扩展消息信息

    包含消息的完整信息，包括系统属性和用户属性
    """

    # 基础消息信息
    topic: str  # 主题名称
    body: bytes  # 消息体
    tags: Optional[str] = None  # 消息标签
    keys: Optional[List[str]] = None  # 消息key列表

    # 系统属性
    message_id: Optional[str] = None  # 消息ID
    queue_id: Optional[int] = None  # 队列ID
    queue_offset: Optional[int] = None  # 队列偏移量
    reconsume_times: int = 0  # 重新消费次数
    born_timestamp: Optional[int] = None  # 消息生产时间戳
    born_host: Optional[str] = None  # 消息生产主机
    store_timestamp: Optional[int] = None  # 消息存储时间戳
    store_host: Optional[str] = None  # 消息存储主机
    msg_flag: int = 0  # 消息标志
    delay_time_level: int = 0  # 延时级别
    wait_store_msg_ok: bool = True  # 是否等待存储完成

    # 事务相关
    transaction_id: Optional[str] = None  # 事务ID
    prepare_transaction_offset: Optional[int] = None  # 预提交事务偏移量

    # 用户属性
    properties: Dict[str, str] = field(default_factory=dict)  # 用户属性

    def __post_init__(self):
        """后处理，确保类型正确"""
        if self.keys is None:
            self.keys = []
        if self.born_timestamp is None:
            self.born_timestamp = int(time.time() * 1000)

    def get_property(
        self, key: str, default: Optional[str] = None
    ) -> Optional[str]:
        """获取用户属性"""
        return self.properties.get(key, default)

    def set_property(self, key: str, value: str) -> None:
        """设置用户属性"""
        self.properties[key] = value

    def has_property(self, key: str) -> bool:
        """检查是否包含指定属性"""
        return key in self.properties

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            "topic": self.topic,
            "body": self.body.decode("utf-8")
            if isinstance(self.body, bytes) and self.body
            else "",
            "properties": self.properties,
            "reconsumeTimes": self.reconsume_times,
            "msgFlag": self.msg_flag,
            "delayTimeLevel": self.delay_time_level,
            "waitStoreMsgOK": self.wait_store_msg_ok,
        }

        # 添加可选字段
        if self.tags:
            result["tags"] = self.tags
        if self.keys:
            result["keys"] = self.keys
        if self.message_id:
            result["msgId"] = self.message_id
        if self.queue_id is not None:
            result["queueId"] = self.queue_id
        if self.queue_offset is not None:
            result["queueOffset"] = self.queue_offset
        if self.born_timestamp:
            result["bornTimestamp"] = self.born_timestamp
        if self.born_host:
            result["bornHost"] = self.born_host
        if self.store_timestamp:
            result["storeTimestamp"] = self.store_timestamp
        if self.store_host:
            result["storeHost"] = self.store_host
        if self.transaction_id:
            result["transactionId"] = self.transaction_id
        if self.prepare_transaction_offset is not None:
            result["prepareTransactionOffset"] = self.prepare_transaction_offset

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageExt":
        """从字典创建实例"""
        # 处理body字段
        body = b""
        if "body" in data:
            body_str = data["body"]
            if isinstance(body_str, str):
                body = body_str.encode("utf-8")
            elif isinstance(body_str, bytes):
                body = body_str

        # 提取属性
        properties = data.get("properties", {})
        if isinstance(properties, str):
            try:
                properties = json.loads(properties)
            except json.JSONDecodeError:
                properties = {}

        return cls(
            topic=data["topic"],
            body=body,
            tags=data.get("tags"),
            keys=data.get("keys", []),
            message_id=data.get("msgId"),
            queue_id=data.get("queueId"),
            queue_offset=data.get("queueOffset"),
            reconsume_times=data.get("reconsumeTimes", 0),
            born_timestamp=data.get("bornTimestamp"),
            born_host=data.get("bornHost"),
            store_timestamp=data.get("storeTimestamp"),
            store_host=data.get("storeHost"),
            msg_flag=data.get("msgFlag", 0),
            delay_time_level=data.get("delayTimeLevel", 0),
            wait_store_msg_ok=data.get("waitStoreMsgOK", True),
            transaction_id=data.get("transactionId"),
            prepare_transaction_offset=data.get("prepareTransactionOffset"),
            properties=properties,
        )


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


@dataclass
class SendMessageResult:
    """发送消息结果

    包含消息发送后的返回信息
    """

    msg_id: str  # 消息ID
    queue_id: int  # 队列ID
    queue_offset: int  # 队列偏移量
    region_id: str = "DefaultRegion"  # 区域ID
    transaction_id: Optional[str] = None  # 事务ID
    offset_msg_id: Optional[str] = None  # 偏移量消息ID

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            "msgId": self.msg_id,
            "queueId": self.queue_id,
            "queueOffset": self.queue_offset,
            "regionId": self.region_id,
        }

        if self.transaction_id:
            result["transactionId"] = self.transaction_id
        if self.offset_msg_id:
            result["offsetMsgId"] = self.offset_msg_id

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SendMessageResult":
        """从字典创建实例"""
        return cls(
            msg_id=data["msgId"],
            queue_id=data["queueId"],
            queue_offset=data["queueOffset"],
            region_id=data.get("regionId", "DefaultRegion"),
            transaction_id=data.get("transactionId"),
            offset_msg_id=data.get("offsetMsgId"),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "SendMessageResult":
        """从字节数据创建实例

        Args:
            data: 原始响应体字节数据

        Returns:
            SendMessageResult: SendMessageResult实例

        Raises:
            DeserializationError: 数据格式无效时抛出异常
        """
        try:
            data_str = data.decode("utf-8")
            parsed_data = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(
                f"Failed to parse SendMessageResult from bytes: {e}"
            )
        except Exception as e:
            raise DeserializationError(f"Invalid SendMessageResult format: {e}")


@dataclass
class PullMessageResult:
    """拉取消息结果

    包含从Broker拉取的消息列表和相关信息
    """

    messages: List[MessageExt]  # 消息列表
    next_begin_offset: int  # 下次拉取的起始偏移量
    min_offset: int  # 最小偏移量
    max_offset: int  # 最大偏移量
    suggest_which_broker_id: Optional[int] = None  # 建议的broker ID
    pull_rt: Optional[float] = None  # 拉取耗时

    @property
    def is_found(self) -> bool:
        """是否拉取到消息"""
        return len(self.messages) > 0

    @property
    def message_count(self) -> int:
        """拉取到的消息数量"""
        return len(self.messages)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            "messages": [msg.to_dict() for msg in self.messages],
            "nextBeginOffset": self.next_begin_offset,
            "minOffset": self.min_offset,
            "maxOffset": self.max_offset,
            "isFound": self.is_found,
            "messageCount": self.message_count,
        }

        if self.suggest_which_broker_id is not None:
            result["suggestWhichBrokerId"] = self.suggest_which_broker_id
        if self.pull_rt is not None:
            result["pullRT"] = self.pull_rt

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PullMessageResult":
        """从字典创建实例"""
        messages = [
            MessageExt.from_dict(msg_data)
            for msg_data in data.get("messages", [])
        ]

        return cls(
            messages=messages,
            next_begin_offset=data["nextBeginOffset"],
            min_offset=data["minOffset"],
            max_offset=data["maxOffset"],
            suggest_which_broker_id=data.get("suggestWhichBrokerId"),
            pull_rt=data.get("pullRT"),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "PullMessageResult":
        """从字节数据创建实例

        Args:
            data: 原始响应体字节数据

        Returns:
            PullMessageResult: PullMessageResult实例

        Raises:
            DeserializationError: 数据格式无效时抛出异常
        """
        try:
            data_str = data.decode("utf-8")
            parsed_data = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(
                f"Failed to parse PullMessageResult from bytes: {e}"
            )
        except Exception as e:
            raise DeserializationError(f"Invalid PullMessageResult format: {e}")


@dataclass
class OffsetResult:
    """偏移量查询结果

    包含消费者偏移量查询的结果信息
    """

    offset: int  # 偏移量值
    retry_times: int = 0  # 重试次数

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {"offset": self.offset, "retryTimes": self.retry_times}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OffsetResult":
        """从字典创建实例"""
        return cls(offset=data["offset"], retry_times=data.get("retryTimes", 0))

    @classmethod
    def from_bytes(cls, data: bytes) -> "OffsetResult":
        """从字节数据创建实例

        Args:
            data: 原始响应体字节数据

        Returns:
            OffsetResult: OffsetResult实例

        Raises:
            DeserializationError: 数据格式无效时抛出异常
        """
        try:
            data_str = data.decode("utf-8")
            parsed_data = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(
                f"Failed to parse OffsetResult from bytes: {e}"
            )
        except Exception as e:
            raise DeserializationError(f"Invalid OffsetResult format: {e}")


# 消费类型枚举
class ConsumeType:
    """消费类型"""

    PUSH = "PUSH"  # 推式消费
    PULL = "PULL"  # 拉式消费


# 消息模式枚举
class MessageModel:
    """消息模式"""

    BROADCASTING = "BROADCASTING"  # 广播模式
    CLUSTERING = "CLUSTERING"  # 集群模式


# 消费起始位置枚举
class ConsumeFromWhere:
    """消费起始位置"""

    CONSUME_FROM_LAST_OFFSET = (
        "CONSUME_FROM_LAST_OFFSET"  # 从最后偏移量开始消费
    )
    CONSUME_FROM_FIRST_OFFSET = (
        "CONSUME_FROM_FIRST_OFFSET"  # 从第一个偏移量开始消费
    )
    CONSUME_FROM_TIMESTAMP = "CONSUME_FROM_TIMESTAMP"  # 从指定时间戳开始消费
