"""
消息结果数据结构
定义RocketMQ消息操作结果的数据结构。
"""

import ast
from dataclasses import dataclass
from typing import Any

from pyrocketmq.model.command import RemotingCommand

from .errors import DeserializationError
from .message_ext import MessageExt
from .message_queue import MessageQueue


# 消费类型枚举
class ConsumeType:
    """消费类型"""

    PUSH: str = "PUSH"  # 推式消费
    PULL: str = "PULL"  # 拉式消费


# 发送状态枚举
class SendStatus:
    """发送状态枚举"""

    SEND_OK: int = 0  # 发送成功
    SEND_FLUSH_DISK_TIMEOUT: int = 1  # 刷盘超时
    SEND_FLUSH_SLAVE_TIMEOUT: int = 2  # 从节点刷盘超时
    SEND_SLAVE_NOT_AVAILABLE: int = 3  # 从节点不可用
    SEND_UNKNOWN_ERROR: int = 4  # 未知错误

    @classmethod
    def from_name(cls, name: str) -> int:
        """从状态名称获取状态值"""
        status_map: dict[str, int] = {
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
        name_map: dict[int, str] = {
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


@dataclass
class SendMessageResult:
    """发送消息结果

    包含消息发送后的返回信息，与Go语言实现保持兼容
    """

    status: int  # 发送状态 (SendStatus枚举值)
    msg_id: str  # 消息ID
    message_queue: MessageQueue  # 消息队列信息
    queue_offset: int  # 队列偏移量
    transaction_id: str | None = None  # 事务ID
    offset_msg_id: str | None = None  # 偏移量消息ID
    region_id: str = "DefaultRegion"  # 区域ID
    trace_on: bool = False  # 是否开启Trace

    def __post_init__(self):
        """后处理，确保数据类型正确"""
        pass

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

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        result: dict[str, Any] = {
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
    def from_dict(cls, data: dict[str, Any]) -> "SendMessageResult":
        """从字典创建实例"""
        # 解析MessageQueue
        mq_data: dict[str, Any] = data.get("messageQueue", {})
        message_queue: MessageQueue = MessageQueue(
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
            data_str: str = data.decode("utf-8")
            parsed_data: dict[str, Any] = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(
                f"Failed to parse SendMessageResult from bytes: {e}"
            )
        except Exception as e:
            raise DeserializationError(f"Invalid SendMessageResult format: {e}")

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"SendMessageResult[msgId={self.msg_id}, "
            f"status={self.status_name}, "
            f"queue={self.message_queue}, "
            f"offset={self.queue_offset}]"
        )


@dataclass
class PullMessageResult:
    """拉取消息结果

    包含从Broker拉取的消息列表和相关信息
    """

    messages: list["MessageExt"]  # 消息列表
    next_begin_offset: int  # 下次拉取的起始偏移量
    min_offset: int  # 最小偏移量
    max_offset: int  # 最大偏移量
    suggest_which_broker_id: int | None = None  # 建议的broker ID
    pull_rt: float | None = None  # 拉取耗时
    body: bytes | None = None  # 原始响应体数据

    @property
    def is_found(self) -> bool:
        """是否拉取到消息"""
        return len(self.messages) > 0

    @property
    def message_count(self) -> int:
        """拉取到的消息数量"""
        return len(self.messages)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        result: dict[str, Any] = {
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
        if self.body is not None:
            result["body"] = self.body.hex()  # 将bytes转换为hex字符串以便序列化

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PullMessageResult":
        """从字典创建实例"""
        # 需要从message_ext导入MessageExt
        from .message_ext import MessageExt

        messages: list["MessageExt"] = [
            MessageExt.from_dict(msg_data) for msg_data in data.get("messages", [])
        ]

        # 处理body字段，如果存在则从hex字符串转换回bytes
        body: bytes | None = None
        if "body" in data and data["body"] is not None:
            if isinstance(data["body"], str):
                body = bytes.fromhex(data["body"])
            elif isinstance(data["body"], bytes):
                body = data["body"]

        return cls(
            messages=messages,
            next_begin_offset=data["nextBeginOffset"],
            min_offset=data["minOffset"],
            max_offset=data["maxOffset"],
            suggest_which_broker_id=data.get("suggestWhichBrokerId"),
            pull_rt=data.get("pullRT"),
            body=body,
        )

    @classmethod
    def decode_from_cmd(cls, cmd: RemotingCommand) -> "PullMessageResult":
        max_offset = int(cmd.ext_fields.get("maxOffset", "0"))
        min_offset = int(cmd.ext_fields.get("minOffset", "0"))
        next_begin_offset = int(cmd.ext_fields.get("nextBeginOffset", "0"))
        suggest_which_broker_id = int(cmd.ext_fields.get("suggestWhichBrokerId", "0"))
        body = cmd.body

        return cls(
            messages=MessageExt.decode_messages(body) if body else [],
            next_begin_offset=next_begin_offset,
            min_offset=min_offset,
            max_offset=max_offset,
            suggest_which_broker_id=suggest_which_broker_id,
            pull_rt=None,
            body=body,
        )

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"PullMessageResult[count={self.message_count}, "
            f"nextOffset={self.next_begin_offset}, "
            f"minOffset={self.min_offset}, "
            f"maxOffset={self.max_offset}]"
        )


@dataclass
class OffsetResult:
    """偏移量查询结果

    包含消费者偏移量查询的结果信息
    """

    offset: int  # 偏移量值
    retry_times: int = 0  # 重试次数

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {"offset": self.offset, "retryTimes": self.retry_times}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OffsetResult":
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
            data_str: str = data.decode("utf-8")
            parsed_data: dict[str, Any] = ast.literal_eval(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise DeserializationError(f"Failed to parse OffsetResult from bytes: {e}")
        except Exception as e:
            raise DeserializationError(f"Invalid OffsetResult format: {e}")

    def __str__(self) -> str:
        """字符串表示"""
        return f"OffsetResult[offset={self.offset}, retryTimes={self.retry_times}]"
