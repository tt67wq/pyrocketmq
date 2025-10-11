"""
RocketMQ请求Header数据结构定义
基于Go语言实现的Header结构
"""

import time
from dataclasses import dataclass
from typing import Dict


@dataclass
class SendMessageRequestHeader:
    """发送消息请求Header"""

    producer_group: str
    topic: str
    queue_id: int = 0
    sys_flag: int = 0
    born_timestamp: int = 0
    flag: int = 0
    properties: str = ""
    reconsume_times: int = 0
    unit_mode: bool = False
    max_reconsume_times: int = 16
    batch: bool = False
    default_topic: str = "TBW102"
    default_topic_queue_nums: int = 4

    def __post_init__(self):
        if self.born_timestamp == 0:
            self.born_timestamp = int(time.time() * 1000)

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "producerGroup": self.producer_group,
            "topic": self.topic,
            "queueId": str(self.queue_id),
            "sysFlag": str(self.sys_flag),
            "bornTimestamp": str(self.born_timestamp),
            "flag": str(self.flag),
            "reconsumeTimes": str(self.reconsume_times),
            "unitMode": str(self.unit_mode).lower(),
            "maxReconsumeTimes": str(self.max_reconsume_times),
            "defaultTopic": self.default_topic,
            "defaultTopicQueueNums": str(self.default_topic_queue_nums),
            "batch": str(self.batch).lower(),
            "properties": self.properties,
        }


@dataclass
class SendMessageRequestV2Header:
    """发送消息请求V2 Header - 使用单字母字段名"""

    producer_group: str
    topic: str
    queue_id: int = 0
    sys_flag: int = 0
    born_timestamp: int = 0
    flag: int = 0
    properties: str = ""
    reconsume_times: int = 0
    unit_mode: bool = False
    max_reconsume_times: int = 16
    batch: bool = False
    default_topic: str = "TBW102"
    default_topic_queue_nums: int = 4

    def __post_init__(self):
        if self.born_timestamp == 0:
            self.born_timestamp = int(time.time() * 1000)

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典 - 使用单字母字段名"""
        return {
            "a": self.producer_group,  # producerGroup
            "b": self.topic,  # topic
            "c": self.default_topic,  # defaultTopic
            "d": str(self.default_topic_queue_nums),  # defaultTopicQueueNums
            "e": str(self.queue_id),  # queueId
            "f": str(self.sys_flag),  # sysFlag
            "g": str(self.born_timestamp),  # bornTimestamp
            "h": str(self.flag),  # flag
            "i": self.properties,  # properties
            "j": str(self.reconsume_times),  # reconsumeTimes
            "k": str(self.unit_mode).lower(),  # unitMode
            "l": str(self.max_reconsume_times),  # maxReconsumeTimes
            "m": str(self.batch).lower(),  # batch
        }


@dataclass
class EndTransactionRequestHeader:
    """结束事务请求Header"""

    producer_group: str
    tran_state_table_offset: int
    commit_log_offset: int
    commit_or_rollback: int
    from_transaction_check: bool = False
    msg_id: str = ""
    transaction_id: str = ""

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "producerGroup": self.producer_group,
            "tranStateTableOffset": str(self.tran_state_table_offset),
            "commitLogOffset": str(self.commit_log_offset),
            "commitOrRollback": str(self.commit_or_rollback),
            "fromTransactionCheck": str(self.from_transaction_check).lower(),
            "msgId": self.msg_id,
            "transactionId": self.transaction_id,
        }


@dataclass
class CheckTransactionStateRequestHeader:
    """检查事务状态请求Header"""

    tran_state_table_offset: int
    commit_log_offset: int
    msg_id: str = ""
    transaction_id: str = ""
    offset_msg_id: str = ""

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "tranStateTableOffset": str(self.tran_state_table_offset),
            "commitLogOffset": str(self.commit_log_offset),
            "msgId": self.msg_id,
            "transactionId": self.transaction_id,
            "offsetMsgId": self.offset_msg_id,
        }

    @classmethod
    def decode(
        cls, properties: Dict[str, str]
    ) -> "CheckTransactionStateRequestHeader":
        """从属性字典解码"""
        return cls(
            tran_state_table_offset=int(
                properties.get("tranStateTableOffset", 0)
            ),
            commit_log_offset=int(properties.get("commitLogOffset", 0)),
            msg_id=properties.get("msgId", ""),
            transaction_id=properties.get("transactionId", ""),
            offset_msg_id=properties.get("offsetMsgId", ""),
        )


@dataclass
class ConsumerSendMsgBackRequestHeader:
    """消费者发送消息回退请求Header"""

    group: str
    offset: int
    delay_level: int
    origin_msg_id: str
    origin_topic: str
    unit_mode: bool = False
    max_reconsume_times: int = 16

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "group": self.group,
            "offset": str(self.offset),
            "delayLevel": str(self.delay_level),
            "originMsgId": self.origin_msg_id,
            "originTopic": self.origin_topic,
            "unitMode": str(self.unit_mode).lower(),
            "maxReconsumeTimes": str(self.max_reconsume_times),
        }


@dataclass
class PullMessageRequestHeader:
    """拉取消息请求Header"""

    consumer_group: str
    topic: str
    queue_id: int
    queue_offset: int
    max_msg_nums: int
    sys_flag: int = 0
    commit_offset: int = 0
    suspend_timeout_millis: int = 0
    sub_expression: str = "*"
    sub_version: int = 0
    expression_type: str = "TAG"
    support_compression_type: int = 0

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": str(self.queue_id),
            "queueOffset": str(self.queue_offset),
            "maxMsgNums": str(self.max_msg_nums),
            "sysFlag": str(self.sys_flag),
            "commitOffset": str(self.commit_offset),
            "suspendTimeoutMillis": str(self.suspend_timeout_millis),
            "subscription": self.sub_expression,
            "subVersion": str(self.sub_version),
            "expressionType": self.expression_type,
            "supportCompressionType": str(self.support_compression_type),
        }


@dataclass
class GetConsumerListRequestHeader:
    """获取消费者列表请求Header"""

    consumer_group: str

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "consumerGroup": self.consumer_group,
        }


@dataclass
class GetMaxOffsetRequestHeader:
    """获取最大偏移量请求Header"""

    topic: str
    queue_id: int

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
            "queueId": str(self.queue_id),
        }


@dataclass
class QueryConsumerOffsetRequestHeader:
    """查询消费者偏移量请求Header"""

    consumer_group: str
    topic: str
    queue_id: int

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": str(self.queue_id),
        }


@dataclass
class SearchOffsetRequestHeader:
    """搜索偏移量请求Header"""

    topic: str
    queue_id: int
    timestamp: int

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
            "queueId": str(self.queue_id),
            "timestamp": str(self.timestamp),
        }


@dataclass
class UpdateConsumerOffsetRequestHeader:
    """更新消费者偏移量请求Header"""

    consumer_group: str
    topic: str
    queue_id: int
    commit_offset: int

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": str(self.queue_id),
            "commitOffset": str(self.commit_offset),
        }


@dataclass
class GetRouteInfoRequestHeader:
    """获取路由信息请求Header"""

    topic: str

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
        }


@dataclass
class GetConsumerRunningInfoHeader:
    """获取消费者运行信息Header"""

    consumer_group: str
    client_id: str

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "consumerGroup": self.consumer_group,
            "clientId": self.client_id,
        }

    @classmethod
    def decode(
        cls, properties: Dict[str, str]
    ) -> "GetConsumerRunningInfoHeader":
        """从属性字典解码"""
        return cls(
            consumer_group=properties.get("consumerGroup", ""),
            client_id=properties.get("clientId", ""),
        )


@dataclass
class QueryMessageRequestHeader:
    """查询消息请求Header"""

    topic: str
    key: str
    max_num: int
    begin_timestamp: int
    end_timestamp: int

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
            "key": self.key,
            "maxNum": str(self.max_num),
            "beginTimestamp": str(self.begin_timestamp),
            "endTimestamp": str(self.end_timestamp),
        }


@dataclass
class ViewMessageRequestHeader:
    """查看消息请求Header"""

    offset: int

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "offset": str(self.offset),
        }


@dataclass
class CreateTopicRequestHeader:
    """创建主题请求Header"""

    topic: str
    default_topic: str = "TBW102"
    read_queue_nums: int = 8
    write_queue_nums: int = 8
    perm: int = 6
    topic_filter_type: str = "SINGLE_TAG"
    topic_sys_flag: int = 0
    order: bool = False

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
            "defaultTopic": self.default_topic,
            "readQueueNums": str(self.read_queue_nums),
            "writeQueueNums": str(self.write_queue_nums),
            "perm": str(self.perm),
            "topicFilterType": self.topic_filter_type,
            "topicSysFlag": str(self.topic_sys_flag),
            "order": str(self.order).lower(),
        }


@dataclass
class TopicListRequestHeader:
    """主题列表请求Header"""

    topic: str = ""

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
        }


@dataclass
class DeleteTopicRequestHeader:
    """删除主题请求Header"""

    topic: str

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
        }


@dataclass
class ResetOffsetHeader:
    """重置偏移量Header"""

    topic: str
    group: str
    timestamp: int
    is_force: bool = False

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "topic": self.topic,
            "group": self.group,
            "timestamp": str(self.timestamp),
            "isForce": str(self.is_force).lower(),
        }

    @classmethod
    def decode(cls, properties: Dict[str, str]) -> "ResetOffsetHeader":
        """从属性字典解码"""
        return cls(
            topic=properties.get("topic", ""),
            group=properties.get("group", ""),
            timestamp=int(properties.get("timestamp", 0)),
            is_force=properties.get("isForce", "false").lower() == "true",
        )


@dataclass
class ConsumeMessageDirectlyHeader:
    """直接消费消息Header"""

    consumer_group: str
    client_id: str
    msg_id: str
    broker_name: str

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "consumerGroup": self.consumer_group,
            "clientId": self.client_id,
            "msgId": self.msg_id,
            "brokerName": self.broker_name,
        }

    @classmethod
    def decode(
        cls, properties: Dict[str, str]
    ) -> "ConsumeMessageDirectlyHeader":
        """从属性字典解码"""
        return cls(
            consumer_group=properties.get("consumerGroup", ""),
            client_id=properties.get("clientId", ""),
            msg_id=properties.get("msgId", ""),
            broker_name=properties.get("brokerName", ""),
        )


@dataclass
class SaveOrGetMsgNoHeader:
    """保存或获取消息编号Header"""

    msg_no: str

    def encode(self) -> Dict[str, str]:
        """编码为字符串字典"""
        return {
            "msgNo": self.msg_no,
        }
