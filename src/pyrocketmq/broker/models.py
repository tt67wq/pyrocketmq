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

    包含消息发送后的返回信息，与Go语言实现保持兼容
    """

    status: int  # 发送状态 (SendStatus枚举值)
    msg_id: str  # 消息ID
    message_queue: MessageQueue  # 消息队列信息
    queue_offset: int  # 队列偏移量
    transaction_id: Optional[str] = None  # 事务ID
    offset_msg_id: Optional[str] = None  # 偏移量消息ID
    region_id: str = "DefaultRegion"  # 区域ID
    trace_on: bool = False  # 是否开启Trace

    @property
    def is_success(self) -> bool:
        """判断发送是否成功"""
        return SendStatus.is_success(self.status)

    @property
    def status_name(self) -> str:
        """获取状态名称"""
        return SendStatus.to_name(self.status)

    @property
    def queue_id(self) -> int:
        """获取队列ID (兼容性属性)"""
        return self.message_queue.queue_id

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            "status": self.status,
            "statusName": self.status_name,
            "msgId": self.msg_id,
            "messageQueue": self.message_queue.to_dict(),
            "queueOffset": self.queue_offset,
            "regionId": self.region_id,
            "traceOn": self.trace_on,
        }

        if self.transaction_id:
            result["transactionId"] = self.transaction_id
        if self.offset_msg_id:
            result["offsetMsgId"] = self.offset_msg_id

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SendMessageResult":
        """从字典创建实例"""
        # 解析MessageQueue
        mq_data = data.get("messageQueue", {})
        message_queue = MessageQueue(
            topic=mq_data.get("topic", ""),
            broker_name=mq_data.get("brokerName", ""),
            queue_id=mq_data.get("queueId", 0),
        )

        return cls(
            status=data.get("status", SendStatus.SEND_UNKNOWN_ERROR),
            msg_id=data["msgId"],
            message_queue=message_queue,
            queue_offset=data["queueOffset"],
            transaction_id=data.get("transactionId"),
            offset_msg_id=data.get("offsetMsgId"),
            region_id=data.get("regionId", "DefaultRegion"),
            trace_on=data.get("traceOn", False),
        )


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


# 消息属性常量
class MessageProperty:
    """消息属性常量定义"""

    KEY_SEPARATOR = " "
    KEYS = "KEYS"
    TAGS = "TAGS"
    WAIT_STORE_MSG_OK = "WAIT"
    DELAY_TIME_LEVEL = "DELAY"
    RETRY_TOPIC = "RETRY_TOPIC"
    REAL_TOPIC = "REAL_TOPIC"
    REAL_QUEUE_ID = "REAL_QID"
    TRANSACTION_PREPARED = "TRAN_MSG"
    PRODUCER_GROUP = "PGROUP"
    MIN_OFFSET = "MIN_OFFSET"
    MAX_OFFSET = "MAX_OFFSET"
    BUYER_ID = "BUYER_ID"
    ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID"
    TRANSFER_FLAG = "TRANSFER_FLAG"
    CORRECTION_FLAG = "CORRECTION_FLAG"
    MQ2_FLAG = "MQ2_FLAG"
    RECONSUME_TIME = "RECONSUME_TIME"
    MSG_REGION = "MSG_REGION"
    TRACE_SWITCH = "TRACE_ON"
    UNIQUE_CLIENT_MESSAGE_ID_KEYINDEX = "UNIQ_KEY"
    MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES"
    CONSUME_START_TIME = "CONSUME_START_TIME"
    TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET"
    TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES"
    CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS"
    SHARDING_KEY = "SHARDING_KEY"
    TRANSACTION_ID = "__transactionId__"
    START_DELIVER_TIME = "START_DELIVER_TIME"


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


# 发送状态枚举
class SendStatus:
    """发送状态枚举"""

    SEND_OK = 0  # 发送成功
    SEND_FLUSH_DISK_TIMEOUT = 1  # 刷盘超时
    SEND_FLUSH_SLAVE_TIMEOUT = 2  # 从节点刷盘超时
    SEND_SLAVE_NOT_AVAILABLE = 3  # 从节点不可用
    SEND_UNKNOWN_ERROR = 4  # 未知错误

    @classmethod
    def from_name(cls, name: str) -> int:
        """从状态名称获取状态值"""
        status_map = {
            "SEND_OK": cls.SEND_OK,
            "SEND_FLUSH_DISK_TIMEOUT": cls.SEND_FLUSH_DISK_TIMEOUT,
            "SEND_FLUSH_SLAVE_TIMEOUT": cls.SEND_FLUSH_SLAVE_TIMEOUT,
            "SEND_SLAVE_NOT_AVAILABLE": cls.SEND_SLAVE_NOT_AVAILABLE,
            "SEND_UNKNOWN_ERROR": cls.SEND_UNKNOWN_ERROR,
        }
        return status_map.get(name, cls.SEND_UNKNOWN_ERROR)

    @classmethod
    def to_name(cls, status: int) -> str:
        """从状态值获取状态名称"""
        name_map = {
            cls.SEND_OK: "SEND_OK",
            cls.SEND_FLUSH_DISK_TIMEOUT: "SEND_FLUSH_DISK_TIMEOUT",
            cls.SEND_FLUSH_SLAVE_TIMEOUT: "SEND_FLUSH_SLAVE_TIMEOUT",
            cls.SEND_SLAVE_NOT_AVAILABLE: "SEND_SLAVE_NOT_AVAILABLE",
            cls.SEND_UNKNOWN_ERROR: "SEND_UNKNOWN_ERROR",
        }
        return name_map.get(status, "SEND_UNKNOWN_ERROR")

    @classmethod
    def is_success(cls, status: int) -> bool:
        """判断是否为成功状态"""
        return status == cls.SEND_OK
