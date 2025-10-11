"""
扩展消息数据结构
定义RocketMQ扩展消息的核心数据结构，包含完整的消息信息。
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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

    def __str__(self) -> str:
        """字符串表示"""
        body_preview = self.body[:50].decode("utf-8", errors="ignore")
        if len(self.body) > 50:
            body_preview += "..."

        return (
            f"MessageExt[topic={self.topic}, "
            f"msgId={self.message_id}, "
            f"queueId={self.queue_id}, "
            f"offset={self.queue_offset}, "
            f"bodySize={len(self.body)}, "
            f"bodyPreview='{body_preview}']"
        )
