"""
客户端数据结构
定义RocketMQ生产者和消费者的数据结构。
"""

import ast
import json
from dataclasses import dataclass, field
from typing import Any

from .errors import DeserializationError
from .message_queue import MessageQueue


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
    tags_set: list[str] = field(default_factory=list)
    code_set: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
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
    def from_dict(cls, data: dict[str, Any]) -> "SubscriptionData":
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
        return f"SubscriptionData[topic={self.topic}, subString={self.sub_string}]"


@dataclass
class ProducerData:
    """生产者信息

    用于向Broker注册生产者信息
    """

    group_name: str  # 生产者组名

    def unique_id(self) -> str:
        """获取唯一标识符"""
        return self.group_name

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {"groupName": self.group_name}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProducerData":
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
            raise DeserializationError(f"Failed to parse ProducerData from bytes: {e}")
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
    subscription_data: list[SubscriptionData] = field(default_factory=list)  # 订阅关系

    def unique_id(self) -> str:
        """获取唯一标识符，对应Go实现的UniqueID()方法"""
        return self.group_name

    def __post_init__(self):
        """后处理，确保subscription_data不为None"""
        if self.subscription_data is None:
            self.subscription_data = []

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "groupName": self.group_name,
            "consumeType": self.consume_type,
            "messageModel": self.message_model,
            "consumeFromWhere": self.consume_from_where,
            "subscriptionData": self.subscription_data,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConsumerData":
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
            raise DeserializationError(f"Failed to parse ConsumerData from bytes: {e}")
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


@dataclass
class ProcessQueueInfo:
    """处理队列信息

    表示消息队列的处理状态信息，与Go语言实现保持兼容。
    """

    commit_offset: int = 0  # 提交偏移量
    cached_msg_min_offset: int = 0  # 缓存消息最小偏移量
    cached_msg_max_offset: int = 0  # 缓存消息最大偏移量
    cached_msg_count: int = 0  # 缓存消息数量
    cached_msg_size_in_mib: int = 0  # 缓存消息大小(MB)
    transaction_msg_min_offset: int = 0  # 事务消息最小偏移量
    transaction_msg_max_offset: int = 0  # 事务消息最大偏移量
    transaction_msg_count: int = 0  # 事务消息数量
    locked: bool = False  # 是否锁定
    try_unlock_times: int = 0  # 尝试解锁次数
    last_lock_timestamp: int = 0  # 最后锁定时间戳
    dropped: bool = False  # 是否已丢弃
    last_pull_timestamp: int = 0  # 最后拉取时间戳
    last_consume_timestamp: int = 0  # 最后消费时间戳

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            处理队列信息字典
        """
        return {
            "commitOffset": self.commit_offset,
            "cachedMsgMinOffset": self.cached_msg_min_offset,
            "cachedMsgMaxOffset": self.cached_msg_max_offset,
            "cachedMsgCount": self.cached_msg_count,
            "cachedMsgSizeInMiB": self.cached_msg_size_in_mib,
            "transactionMsgMinOffset": self.transaction_msg_min_offset,
            "transactionMsgMaxOffset": self.transaction_msg_max_offset,
            "transactionMsgCount": self.transaction_msg_count,
            "locked": self.locked,
            "tryUnlockTimes": self.try_unlock_times,
            "lastLockTimestamp": self.last_lock_timestamp,
            "dropped": self.dropped,
            "lastPullTimestamp": self.last_pull_timestamp,
            "lastConsumeTimestamp": self.last_consume_timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProcessQueueInfo":
        """从字典创建处理队列信息实例

        Args:
            data: 处理队列信息字典

        Returns:
            ProcessQueueInfo实例
        """
        return cls(
            commit_offset=data.get("commitOffset", 0),
            cached_msg_min_offset=data.get("cachedMsgMinOffset", 0),
            cached_msg_max_offset=data.get("cachedMsgMaxOffset", 0),
            cached_msg_count=data.get("cachedMsgCount", 0),
            cached_msg_size_in_mib=data.get("cachedMsgSizeInMiB", 0),
            transaction_msg_min_offset=data.get("transactionMsgMinOffset", 0),
            transaction_msg_max_offset=data.get("transactionMsgMaxOffset", 0),
            transaction_msg_count=data.get("transactionMsgCount", 0),
            locked=data.get("locked", False),
            try_unlock_times=data.get("tryUnlockTimes", 0),
            last_lock_timestamp=data.get("lastLockTimestamp", 0),
            dropped=data.get("dropped", False),
            last_pull_timestamp=data.get("lastPullTimestamp", 0),
            last_consume_timestamp=data.get("lastConsumeTimestamp", 0),
        )

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ProcessQueueInfo[commitOffset={self.commit_offset}, "
            f"cachedMsgCount={self.cached_msg_count}, "
            f"locked={self.locked}, dropped={self.dropped}]"
        )


@dataclass
class ConsumeStatus:
    """消费状态

    表示消费者的消费状态统计信息，与Go语言实现保持兼容。
    """

    pull_rt: float = 0.0  # 拉取响应时间(ms)
    pull_tps: float = 0.0  # 拉取TPS
    consume_rt: float = 0.0  # 消费响应时间(ms)
    consume_ok_tps: float = 0.0  # 消费成功TPS
    consume_failed_tps: float = 0.0  # 消费失败TPS
    consume_failed_msgs: int = 0  # 消费失败消息数量

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            消费状态字典
        """
        return {
            "pullRT": self.pull_rt,
            "pullTPS": self.pull_tps,
            "consumeRT": self.consume_rt,
            "consumeOKTPS": self.consume_ok_tps,
            "consumeFailedTPS": self.consume_failed_tps,
            "consumeFailedMsgs": self.consume_failed_msgs,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConsumeStatus":
        """从字典创建消费状态实例

        Args:
            data: 消费状态字典

        Returns:
            ConsumeStatus实例
        """
        return cls(
            pull_rt=data.get("pullRT", 0.0),
            pull_tps=data.get("pullTPS", 0.0),
            consume_rt=data.get("consumeRT", 0.0),
            consume_ok_tps=data.get("consumeOKTPS", 0.0),
            consume_failed_tps=data.get("consumeFailedTPS", 0.0),
            consume_failed_msgs=data.get("consumeFailedMsgs", 0),
        )

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ConsumeStatus[pullRT={self.pull_rt:.2f}ms, pullTPS={self.pull_tps:.2f}, "
            f"consumeRT={self.consume_rt:.2f}ms, consumeOKTPS={self.consume_ok_tps:.2f}, "
            f"consumeFailedTPS={self.consume_failed_tps:.2f}, consumeFailedMsgs={self.consume_failed_msgs}]"
        )


@dataclass
class ConsumerRunningInfo:
    """消费者运行信息

    表示消费者的运行状态信息，与Go语言实现保持兼容。
    """

    properties: dict[str, str] = field(default_factory=dict)  # 属性配置
    subscription_data: dict[SubscriptionData, bool] = field(
        default_factory=dict
    )  # 订阅数据映射
    mq_table: dict[MessageQueue, ProcessQueueInfo] = field(
        default_factory=dict
    )  # 消息队列处理信息表
    status_table: dict[str, ConsumeStatus] = field(default_factory=dict)  # 状态表

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            消费者运行信息字典
        """
        # 转换subscription_data - 将SubscriptionData对象作为key，bool作为value
        subscription_data_dict = {}
        for sub_data, is_active in self.subscription_data.items():
            sub_key = f"{sub_data.topic}:{sub_data.sub_string}"
            subscription_data_dict[sub_key] = {
                "subscriptionData": sub_data.to_dict(),
                "isActive": is_active,
            }

        # 转换mq_table - 将MessageQueue对象作为key
        mq_table_dict = {}
        for mq, process_info in self.mq_table.items():
            mq_key = f"{mq.topic}@{mq.broker_name}:{mq.queue_id}"
            mq_table_dict[mq_key] = process_info.to_dict()

        return {
            "properties": self.properties,
            "subscriptionData": subscription_data_dict,
            "mqTable": mq_table_dict,
            "statusTable": self.status_table,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConsumerRunningInfo":
        """从字典创建消费者运行信息实例

        Args:
            data: 消费者运行信息字典

        Returns:
            ConsumerRunningInfo实例
        """
        # 解析subscription_data
        subscription_data = {}
        sub_data_raw = data.get("subscriptionData", {})
        if isinstance(sub_data_raw, dict):
            for sub_key, sub_value in sub_data_raw.items():
                if isinstance(sub_value, dict) and "subscriptionData" in sub_value:
                    sub_info = sub_value["subscriptionData"]
                    is_active = sub_value.get("isActive", False)
                    subscription_data[SubscriptionData.from_dict(sub_info)] = is_active

        # 解析mq_table
        mq_table = {}
        mq_table_raw = data.get("mqTable", {})
        if isinstance(mq_table_raw, dict):
            for mq_key, process_info_data in mq_table_raw.items():
                # 解析MessageQueue key (格式: topic@brokerName:queueId)
                try:
                    topic_part, broker_queue_part = mq_key.split("@", 1)
                    broker_part, queue_part = broker_queue_part.split(":", 1)
                    queue_id = int(queue_part)

                    mq = MessageQueue(
                        topic=topic_part,
                        broker_name=broker_part,
                        queue_id=queue_id,
                    )
                    mq_table[mq] = ProcessQueueInfo.from_dict(process_info_data)
                except (ValueError, IndexError):
                    # 如果解析失败，跳过这个key
                    continue

        # 解析status_table
        status_table = {}
        status_table_raw = data.get("statusTable", {})
        if isinstance(status_table_raw, dict):
            for status_key, status_data in status_table_raw.items():
                status_table[status_key] = ConsumeStatus.from_dict(status_data)

        return cls(
            properties=data.get("properties", {}),
            subscription_data=subscription_data,
            mq_table=mq_table,
            status_table=status_table,
        )

    def add_subscription(
        self, subscription: SubscriptionData, is_active: bool = True
    ) -> None:
        """添加订阅信息

        Args:
            subscription: 订阅数据
            is_active: 是否活跃
        """
        self.subscription_data[subscription] = is_active

    def remove_subscription(self, subscription: SubscriptionData) -> bool:
        """移除订阅信息

        Args:
            subscription: 订阅数据

        Returns:
            是否成功移除
        """
        if subscription in self.subscription_data:
            del self.subscription_data[subscription]
            return True
        return False

    def add_queue_info(self, mq: MessageQueue, process_info: ProcessQueueInfo) -> None:
        """添加队列处理信息

        Args:
            mq: 消息队列
            process_info: 处理队列信息
        """
        self.mq_table[mq] = process_info

    def remove_queue_info(self, mq: MessageQueue) -> bool:
        """移除队列处理信息

        Args:
            mq: 消息队列

        Returns:
            是否成功移除
        """
        if mq in self.mq_table:
            del self.mq_table[mq]
            return True
        return False

    def add_status(self, key: str, status: ConsumeStatus) -> None:
        """添加状态信息

        Args:
            key: 状态键
            status: 消费状态
        """
        self.status_table[key] = status

    def remove_status(self, key: str) -> bool:
        """移除状态信息

        Args:
            key: 状态键

        Returns:
            是否成功移除
        """
        if key in self.status_table:
            del self.status_table[key]
            return True
        return False

    def set_property(self, key: str, value: str) -> None:
        """设置属性

        Args:
            key: 属性键
            value: 属性值
        """
        self.properties[key] = value

    def get_property(self, key: str, default: str = "") -> str:
        """获取属性

        Args:
            key: 属性键
            default: 默认值

        Returns:
            属性值
        """
        return self.properties.get(key, default)

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ConsumerRunningInfo[properties={len(self.properties)}, "
            f"subscriptions={len(self.subscription_data)}, "
            f"queues={len(self.mq_table)}, statuses={len(self.status_table)}]"
        )

    def encode(self) -> bytes:
        """序列化为JSON字节数据，参考Go实现的Encode方法

        Returns:
            序列化后的JSON字节数据

        Raises:
            TypeError: 当数据无法序列化时抛出异常
        """
        try:
            # 序列化properties
            properties_json = json.dumps(self.properties, ensure_ascii=False)

            # 序列化statusTable
            status_table_json = json.dumps(
                self.status_table,
                ensure_ascii=False,
                default=self._consume_status_to_dict,
            )

            # 序列化subscriptionSet - 按照Go实现的排序逻辑
            subscriptions = list(self.subscription_data.keys())
            subscriptions.sort(key=self._subscription_sort_key)

            subscription_set_json = json.dumps(
                [sub.to_dict() for sub in subscriptions], ensure_ascii=False
            )

            # 序列化mqTable - 按照Go实现的排序逻辑
            mq_keys = list(self.mq_table.keys())
            mq_keys.sort(key=self._message_queue_sort_key)

            mq_table_items = []
            for mq in mq_keys:
                mq_key = mq.to_dict()
                mq_value = self.mq_table[mq].to_dict()
                mq_table_items.append(
                    f"{json.dumps(mq_key, ensure_ascii=False)}:{json.dumps(mq_value, ensure_ascii=False)}"
                )

            mq_table_json = "{" + ",".join(mq_table_items) + "}"

            # 构建最终的JSON字符串
            json_data = (
                f'{{"properties":{properties_json},'
                f'"statusTable":{status_table_json},'
                f'"subscriptionSet":{subscription_set_json},'
                f'"mqTable":{mq_table_json}}}'
            )

            return json_data.encode("utf-8")

        except (TypeError, ValueError) as e:
            raise TypeError(f"Failed to encode ConsumerRunningInfo: {e}")

    def _subscription_sort_key(self, sub: "SubscriptionData") -> tuple:
        """订阅数据排序键，参考Go实现的排序逻辑

        Args:
            sub: 订阅数据

        Returns:
            排序键元组
        """
        # 这里简化了Go实现的复杂排序逻辑，主要按topic、sub_string排序
        # Go实现中还包含了classFilterMode、subVersion、expType、tags、codes的排序
        return (sub.topic, sub.sub_string, sub.sub_version, sub.expression_type)

    def _message_queue_sort_key(self, mq: MessageQueue) -> tuple:
        """消息队列排序键，参考Go实现的排序逻辑

        Args:
            mq: 消息队列

        Returns:
            排序键元组
        """
        return (mq.topic, mq.broker_name, mq.queue_id)

    def _consume_status_to_dict(self, obj: "ConsumeStatus") -> dict[str, Any]:
        """ConsumeStatus对象转换为字典，用于JSON序列化

        Args:
            obj: ConsumeStatus对象

        Returns:
            字典格式的消费状态
        """
        if isinstance(obj, ConsumeStatus):
            return obj.to_dict()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
