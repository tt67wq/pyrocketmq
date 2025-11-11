"""
RocketMQ远程命令工厂和构建器
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any

from pyrocketmq.model.headers import (
    CheckTransactionStateRequestHeader,
    ConsumeMessageDirectlyHeader,
    ConsumerSendMsgBackRequestHeader,
    CreateTopicRequestHeader,
    DeleteTopicRequestHeader,
    EndTransactionRequestHeader,
    GetConsumerListRequestHeader,
    GetConsumerRunningInfoHeader,
    GetMaxOffsetRequestHeader,
    GetRouteInfoRequestHeader,
    PullMessageRequestHeader,
    QueryConsumerOffsetRequestHeader,
    QueryMessageRequestHeader,
    ResetOffsetHeader,
    SaveOrGetMsgNoHeader,
    SearchOffsetRequestHeader,
    SendMessageRequestHeader,
    SendMessageRequestV2Header,
    UpdateConsumerOffsetRequestHeader,
    ViewMessageRequestHeader,
)
from pyrocketmq.model.message_queue import MessageQueue

from .command import RemotingCommand
from .enums import (
    TRANSACTION_PREPARED_TYPE,
    FlagType,
    LanguageCode,
    RequestCode,
)
from .heart_beat import HeartbeatData
from .message import Message, MessageProperty


@dataclass
class RemotingCommandBuilder:
    """远程命令构建器

    提供链式调用来构建RemotingCommand对象
    """

    code: int
    language: LanguageCode = LanguageCode.PYTHON
    version: int = 1
    opaque: int = 0
    flag: int = 0
    remark: str | None = None
    ext_fields: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None

    def build(self) -> RemotingCommand:
        """构建命令对象

        Returns:
            构造的RemotingCommand对象
        """
        return RemotingCommand(
            code=self.code,
            language=self.language,
            version=self.version,
            opaque=self.opaque,
            flag=self.flag,
            remark=self.remark,
            ext_fields=self.ext_fields.copy(),
            body=self.body,
        )

    def with_code(self, code: int) -> "RemotingCommandBuilder":
        """设置请求代码

        Args:
            code: 请求代码

        Returns:
            构建器实例
        """
        self.code = code
        return self

    def with_language(self, language: LanguageCode) -> "RemotingCommandBuilder":
        """设置语言代码

        Args:
            language: 语言代码

        Returns:
            构建器实例
        """
        self.language = language
        return self

    def with_version(self, version: int) -> "RemotingCommandBuilder":
        """设置版本号

        Args:
            version: 版本号

        Returns:
            构建器实例
        """
        self.version = version
        return self

    def with_opaque(self, opaque: int) -> "RemotingCommandBuilder":
        """设置消息ID

        Args:
            opaque: 消息自增ID

        Returns:
            构建器实例
        """
        self.opaque = opaque
        return self

    def with_flag(self, flag: int) -> "RemotingCommandBuilder":
        """设置标志位

        Args:
            flag: 标志位

        Returns:
            构建器实例
        """
        self.flag = flag
        return self

    def with_remark(self, remark: str) -> "RemotingCommandBuilder":
        """设置备注信息

        Args:
            remark: 备注信息

        Returns:
            构建器实例
        """
        self.remark = remark
        return self

    def with_ext_field(self, key: str, value: str) -> "RemotingCommandBuilder":
        """添加扩展字段

        Args:
            key: 字段名
            value: 字段值

        Returns:
            构建器实例
        """
        self.ext_fields[key] = value
        return self

    def with_ext_fields(self, ext_fields: dict[str, str]) -> "RemotingCommandBuilder":
        """批量添加扩展字段

        Args:
            ext_fields: 扩展字段字典

        Returns:
            构建器实例
        """
        self.ext_fields.update(ext_fields)
        return self

    def with_body(self, body: bytes) -> "RemotingCommandBuilder":
        """设置消息体

        Args:
            body: 消息体数据

        Returns:
            构建器实例
        """
        self.body = body
        return self

    def as_request(self) -> "RemotingCommandBuilder":
        """设置为请求类型

        Returns:
            构建器实例
        """
        self.flag = FlagType.RPC_TYPE
        return self

    def as_response(self) -> "RemotingCommandBuilder":
        """设置为响应类型

        Returns:
            构建器实例
        """
        self.flag = FlagType.RESPONSE_TYPE
        return self

    def as_oneway(self) -> "RemotingCommandBuilder":
        """设置为单向消息

        Returns:
            构建器实例
        """
        self.flag = FlagType.RPC_ONEWAY
        return self

    def with_topic(self, topic: str) -> "RemotingCommandBuilder":
        """设置主题

        Args:
            topic: 主题名称

        Returns:
            构建器实例
        """
        self.ext_fields["topic"] = topic
        return self

    def with_producer_group(self, group: str) -> "RemotingCommandBuilder":
        """设置生产者组

        Args:
            group: 生产者组名称

        Returns:
            构建器实例
        """
        self.ext_fields["producerGroup"] = group
        return self

    def with_consumer_group(self, group: str) -> "RemotingCommandBuilder":
        """设置消费者组

        Args:
            group: 消费者组名称

        Returns:
            构建器实例
        """
        self.ext_fields["consumerGroup"] = group
        return self

    def with_queue_id(self, queue_id: int) -> "RemotingCommandBuilder":
        """设置队列ID

        Args:
            queue_id: 队列ID

        Returns:
            构建器实例
        """
        self.ext_fields["queueId"] = str(queue_id)
        return self

    def with_offset(self, offset: int) -> "RemotingCommandBuilder":
        """设置偏移量

        Args:
            offset: 偏移量

        Returns:
            构建器实例
        """
        self.ext_fields["offset"] = str(offset)
        return self

    def with_tags(self, tags: str) -> "RemotingCommandBuilder":
        """设置消息标签

        Args:
            tags: 消息标签

        Returns:
            构建器实例
        """
        self.ext_fields["tags"] = tags
        return self

    def with_keys(self, keys: str) -> "RemotingCommandBuilder":
        """设置消息键

        Args:
            keys: 消息键

        Returns:
            构建器实例
        """
        self.ext_fields["keys"] = keys
        return self

    def with_wait_store_msg_ok(self, wait: bool) -> "RemotingCommandBuilder":
        """设置是否等待存储确认

        Args:
            wait: 是否等待

        Returns:
            构建器实例
        """
        self.ext_fields["waitStoreMsgOK"] = "true" if wait else "false"
        return self

    def with_batch(self, is_batch: bool) -> "RemotingCommandBuilder":
        """设置是否为批量消息

        Args:
            is_batch: 是否为批量消息

        Returns:
            构建器实例
        """
        self.ext_fields["batch"] = "true" if is_batch else "false"
        return self

    def clear_ext_fields(self) -> "RemotingCommandBuilder":
        """清空扩展字段

        Returns:
            构建器实例
        """
        self.ext_fields.clear()
        return self

    def copy(self) -> "RemotingCommandBuilder":
        """创建构建器的拷贝

        Returns:
            新的构建器实例
        """
        return RemotingCommandBuilder(
            code=self.code,
            language=self.language,
            version=self.version,
            opaque=self.opaque,
            flag=self.flag,
            remark=self.remark,
            ext_fields=self.ext_fields.copy(),
            body=self.body,
        )


class RemotingRequestFactory:
    """RocketMQ请求工厂类

    基于Go语言实现的方法，提供快速生成各类RocketMQ请求的工厂方法
    """

    @staticmethod
    def create_send_message_request(
        producer_group: str,
        topic: str,
        body: bytes,
        queue_id: int = 0,
        properties: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> RemotingCommand:
        """创建发送消息请求

        Args:
            producer_group: 生产者组
            topic: 主题
            body: 消息体
            queue_id: 队列ID
            properties: 消息属性字典（包含tags、keys等）
            **kwargs: 其他参数

        Returns:
            发送消息请求命令
        """
        # 创建临时Message对象来使用marshall_properties方法
        temp_message = Message(topic=topic, body=body)
        if properties:
            temp_message.properties = properties

        # 使用Message的marshall_properties方法序列化properties
        properties_str = temp_message.marshall_properties()

        # 处理事务消息的sys_flag
        sys_flag = 0
        if properties and MessageProperty.TRANSACTION_PREPARED in properties:
            # 检查事务准备标志
            tran_msg_value = properties.get(MessageProperty.TRANSACTION_PREPARED, "")
            if tran_msg_value.lower() == "true":
                sys_flag |= TRANSACTION_PREPARED_TYPE

        header = SendMessageRequestHeader(
            producer_group=producer_group,
            topic=topic,
            queue_id=queue_id,
            sys_flag=sys_flag,
            properties=properties_str,
            born_timestamp=int(time.time() * 1000),
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
            body=body,
        )

    @staticmethod
    def create_send_message_v2_request(
        producer_group: str,
        topic: str,
        body: bytes,
        queue_id: int = 0,
        properties: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> RemotingCommand:
        """创建发送消息V2请求（使用单字母字段名）

        Args:
            producer_group: 生产者组
            topic: 主题
            body: 消息体
            queue_id: 队列ID
            properties: 消息属性字典
            **kwargs: 其他参数

        Returns:
            发送消息V2请求命令
        """
        # 创建临时Message对象来使用marshall_properties方法
        temp_message = Message(topic=topic, body=body)
        if properties:
            temp_message.properties = properties

        # 使用Message的marshall_properties方法序列化properties
        properties_str = temp_message.marshall_properties()

        # 处理事务消息的sys_flag
        sys_flag = 0
        if properties and MessageProperty.TRANSACTION_PREPARED in properties:
            # 检查事务准备标志
            tran_msg_value = properties.get(MessageProperty.TRANSACTION_PREPARED, "")
            if tran_msg_value.lower() == "true":
                sys_flag |= TRANSACTION_PREPARED_TYPE

        header = SendMessageRequestV2Header(
            producer_group=producer_group,
            topic=topic,
            sys_flag=sys_flag,
            queue_id=queue_id,
            properties=properties_str,
            batch=True,  # V2请求中batch参数固定为true
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
            body=body,
        )

    @staticmethod
    def create_pull_message_request(
        consumer_group: str,
        topic: str,
        queue_id: int,
        queue_offset: int,
        max_msg_nums: int,
        **kwargs: Any,
    ) -> RemotingCommand:
        """创建拉取消息请求

        Args:
            consumer_group: 消费者组
            topic: 主题
            queue_id: 队列ID
            queue_offset: 队列偏移量
            max_msg_nums: 最大消息数量
            **kwargs: 其他参数

        Returns:
            拉取消息请求命令
        """
        header = PullMessageRequestHeader(
            consumer_group=consumer_group,
            topic=topic,
            queue_id=queue_id,
            queue_offset=queue_offset,
            max_msg_nums=max_msg_nums,
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.PULL_MESSAGE,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_get_consumer_list_request(
        consumer_group: str,
    ) -> RemotingCommand:
        """创建获取消费者列表请求

        Args:
            consumer_group: 消费者组

        Returns:
            获取消费者列表请求命令
        """
        header = GetConsumerListRequestHeader(consumer_group=consumer_group)

        return RemotingCommand(
            code=RequestCode.GET_CONSUMER_LIST_BY_GROUP,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_get_max_offset_request(topic: str, queue_id: int) -> RemotingCommand:
        """创建获取最大偏移量请求

        Args:
            topic: 主题
            queue_id: 队列ID

        Returns:
            获取最大偏移量请求命令
        """
        header = GetMaxOffsetRequestHeader(topic=topic, queue_id=queue_id)

        return RemotingCommand(
            code=RequestCode.GET_MAX_OFFSET,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_query_consumer_offset_request(
        consumer_group: str, topic: str, queue_id: int
    ) -> RemotingCommand:
        """创建查询消费者偏移量请求

        Args:
            consumer_group: 消费者组
            topic: 主题
            queue_id: 队列ID

        Returns:
            查询消费者偏移量请求命令
        """
        header = QueryConsumerOffsetRequestHeader(
            consumer_group=consumer_group, topic=topic, queue_id=queue_id
        )

        return RemotingCommand(
            code=RequestCode.QUERY_CONSUMER_OFFSET,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_update_consumer_offset_request(
        consumer_group: str, topic: str, queue_id: int, commit_offset: int
    ) -> RemotingCommand:
        """创建更新消费者偏移量请求

        Args:
            consumer_group: 消费者组
            topic: 主题
            queue_id: 队列ID
            commit_offset: 提交偏移量

        Returns:
            更新消费者偏移量请求命令
        """
        header = UpdateConsumerOffsetRequestHeader(
            consumer_group=consumer_group,
            topic=topic,
            queue_id=queue_id,
            commit_offset=commit_offset,
        )

        return RemotingCommand(
            code=RequestCode.UPDATE_CONSUMER_OFFSET,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_search_offset_request(
        topic: str, queue_id: int, timestamp: int
    ) -> RemotingCommand:
        """创建搜索偏移量请求

        Args:
            topic: 主题
            queue_id: 队列ID
            timestamp: 时间戳

        Returns:
            搜索偏移量请求命令
        """
        header = SearchOffsetRequestHeader(
            topic=topic, queue_id=queue_id, timestamp=timestamp
        )

        return RemotingCommand(
            code=RequestCode.SEARCH_OFFSET_BY_TIMESTAMP,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_get_route_info_request(topic: str) -> RemotingCommand:
        """创建获取路由信息请求

        Args:
            topic: 主题

        Returns:
            获取路由信息请求命令
        """
        header = GetRouteInfoRequestHeader(topic=topic)

        return RemotingCommand(
            code=RequestCode.GET_ROUTE_INFO_BY_TOPIC,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_heartbeat_request(
        heartbeat_data: HeartbeatData,
    ) -> RemotingCommand:
        """创建心跳请求

        Args:
            heartbeat_data: 心跳数据

        Returns:
            心跳请求命令
        """
        # 将HeartbeatData转换为JSON格式的body
        heartbeat_dict: dict[str, Any] = heartbeat_data.to_dict()
        body = json.dumps(heartbeat_dict).encode("utf-8")

        return RemotingCommand(
            code=RequestCode.HEART_BEAT,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            body=body,
        )

    @staticmethod
    def create_end_transaction_request(
        producer_group: str,
        tran_state_table_offset: int,
        commit_log_offset: int,
        commit_or_rollback: int,
        **kwargs: Any,
    ) -> RemotingCommand:
        """创建结束事务请求

        Args:
            producer_group: 生产者组
            tran_state_table_offset: 事务状态表偏移量
            commit_log_offset: 提交日志偏移量
            commit_or_rollback: 提交或回滚
            **kwargs: 其他参数

        Returns:
            结束事务请求命令
        """
        header = EndTransactionRequestHeader(
            producer_group=producer_group,
            tran_state_table_offset=tran_state_table_offset,
            commit_log_offset=commit_log_offset,
            commit_or_rollback=commit_or_rollback,
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.END_TRANSACTION,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_check_transaction_state_request(
        tran_state_table_offset: int, commit_log_offset: int, **kwargs: Any
    ) -> RemotingCommand:
        """创建检查事务状态请求

        Args:
            tran_state_table_offset: 事务状态表偏移量
            commit_log_offset: 提交日志偏移量
            **kwargs: 其他参数

        Returns:
            检查事务状态请求命令
        """
        header = CheckTransactionStateRequestHeader(
            tran_state_table_offset=tran_state_table_offset,
            commit_log_offset=commit_log_offset,
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.CHECK_TRANSACTION_STATE,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_consumer_send_msg_back_request(
        group: str,
        offset: int,
        delay_level: int,
        origin_msg_id: str,
        origin_topic: str,
        body: bytes,
        **kwargs: Any,
    ) -> RemotingCommand:
        """创建消费者发送消息回退请求

        Args:
            group: 消费者组
            offset: 偏移量
            delay_level: 延迟级别
            origin_msg_id: 原始消息ID
            origin_topic: 原始主题
            body: 消息体
            **kwargs: 其他参数

        Returns:
            消费者发送消息回退请求命令
        """
        header = ConsumerSendMsgBackRequestHeader(
            group=group,
            offset=offset,
            delay_level=delay_level,
            origin_msg_id=origin_msg_id,
            origin_topic=origin_topic,
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.CONSUMER_SEND_MSG_BACK,
            language=LanguageCode.PYTHON,
            body=body,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_get_consumer_running_info_request(
        consumer_group: str, client_id: str
    ) -> RemotingCommand:
        """创建获取消费者运行信息请求

        Args:
            consumer_group: 消费者组
            client_id: 客户端ID

        Returns:
            获取消费者运行信息请求命令
        """
        header = GetConsumerRunningInfoHeader(
            consumer_group=consumer_group, client_id=client_id
        )

        return RemotingCommand(
            code=RequestCode.GET_CONSUMER_RUNNING_INFO,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_consume_message_directly_request(
        consumer_group: str, client_id: str, msg_id: str, broker_name: str
    ) -> RemotingCommand:
        """创建直接消费消息请求

        Args:
            consumer_group: 消费者组
            client_id: 客户端ID
            msg_id: 消息ID
            broker_name: 代理名称

        Returns:
            直接消费消息请求命令
        """
        header = ConsumeMessageDirectlyHeader(
            consumer_group=consumer_group,
            client_id=client_id,
            msg_id=msg_id,
            broker_name=broker_name,
        )

        return RemotingCommand(
            code=RequestCode.CONSUME_MESSAGE_DIRECTLY,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_create_topic_request(topic: str, **kwargs: Any) -> RemotingCommand:
        """创建主题请求

        Args:
            topic: 主题名称
            **kwargs: 其他参数

        Returns:
            创建主题请求命令
        """
        header = CreateTopicRequestHeader(topic=topic, **kwargs)

        return RemotingCommand(
            code=RequestCode.CREATE_TOPIC,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_delete_topic_request(topic: str) -> RemotingCommand:
        """创建删除主题请求

        Args:
            topic: 主题名称

        Returns:
            删除主题请求命令
        """
        header = DeleteTopicRequestHeader(topic=topic)

        return RemotingCommand(
            code=RequestCode.DELETE_TOPIC_IN_BROKER,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_query_message_request(
        topic: str,
        key: str,
        max_num: int,
        begin_timestamp: int,
        end_timestamp: int,
    ) -> RemotingCommand:
        """创建查询消息请求

        Args:
            topic: 主题
            key: 消息键
            max_num: 最大数量
            begin_timestamp: 开始时间戳
            end_timestamp: 结束时间戳

        Returns:
            查询消息请求命令
        """
        header = QueryMessageRequestHeader(
            topic=topic,
            key=key,
            max_num=max_num,
            begin_timestamp=begin_timestamp,
            end_timestamp=end_timestamp,
        )

        return RemotingCommand(
            code=RequestCode.QUERY_MESSAGE,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_view_message_request(offset: int) -> RemotingCommand:
        """创建查看消息请求

        Args:
            offset: 偏移量

        Returns:
            查看消息请求命令
        """
        header = ViewMessageRequestHeader(offset=offset)

        return RemotingCommand(
            code=RequestCode.VIEW_MESSAGE_BY_ID,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_reset_offset_request(
        topic: str, group: str, timestamp: int, is_force: bool = False
    ) -> RemotingCommand:
        """创建重置偏移量请求

        Args:
            topic: 主题
            group: 消费者组
            timestamp: 时间戳
            is_force: 是否强制

        Returns:
            重置偏移量请求命令
        """
        header = ResetOffsetHeader(
            topic=topic, group=group, timestamp=timestamp, is_force=is_force
        )

        return RemotingCommand(
            code=RequestCode.RESET_CONSUMER_OFFSET,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_get_all_topic_list_request() -> RemotingCommand:
        """创建获取所有主题列表请求

        Returns:
            获取所有主题列表请求命令
        """
        return RemotingCommand(
            code=RequestCode.GET_ALL_TOPIC_LIST_FROM_NAME_SERVER,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
        )

    @staticmethod
    def create_send_batch_message_request(
        producer_group: str,
        topic: str,
        body: bytes,
        queue_id: int = 0,
        properties: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> RemotingCommand:
        """创建发送批量消息请求

        Args:
            producer_group: 生产者组
            topic: 主题
            body: 批量消息体
            queue_id: 队列ID，默认0
            properties: 消息属性字典（包含tags、keys等），默认为None
            **kwargs: 其他参数

        Returns:
            发送批量消息请求命令
        """
        # 创建临时Message对象来使用marshall_properties方法
        temp_message = Message(topic=topic, body=body)
        if properties:
            temp_message.properties = properties

        # 使用Message的marshall_properties方法序列化properties
        properties_str = temp_message.marshall_properties()

        # 处理事务消息的sys_flag
        sys_flag = 0
        if properties and MessageProperty.TRANSACTION_PREPARED in properties:
            # 检查事务准备标志
            tran_msg_value = properties.get(MessageProperty.TRANSACTION_PREPARED, "")
            if tran_msg_value.lower() == "true":
                sys_flag |= TRANSACTION_PREPARED_TYPE

        header = SendMessageRequestV2Header(
            producer_group=producer_group,
            topic=topic,
            queue_id=queue_id,
            sys_flag=sys_flag,
            properties=properties_str,
            batch=True,
            **kwargs,
        )

        return RemotingCommand(
            code=RequestCode.SEND_BATCH_MESSAGE,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
            body=body,
        )

    @staticmethod
    def create_save_msg_no_request(msg_no: str) -> RemotingCommand:
        """创建保存消息编号请求

        Args:
            msg_no: 消息编号

        Returns:
            保存消息编号请求命令
        """
        header = SaveOrGetMsgNoHeader(msg_no=msg_no)

        return RemotingCommand(
            code=RequestCode.PUT_MSG_NO,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_get_msg_no_request(msg_no: str) -> RemotingCommand:
        """创建获取消息编号请求

        Args:
            msg_no: 消息编号

        Returns:
            获取消息编号请求命令
        """
        header = SaveOrGetMsgNoHeader(msg_no=msg_no)

        return RemotingCommand(
            code=RequestCode.GET_MSG_NO,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            ext_fields=header.encode(),
        )

    @staticmethod
    def create_lock_batch_mq_request(
        consumer_group: str, client_id: str, mqs: list[MessageQueue]
    ) -> RemotingCommand:
        """创建批量锁定消息队列请求

        Args:
            consumer_group: 消费者组名称
            client_id: 客户端ID
            mqs: 消息队列列表

        Returns:
            批量锁定消息队列请求命令
        """
        # 构建请求数据结构
        request_data = {
            "consumerGroup": consumer_group,
            "clientId": client_id,
            "mqSet": [mq.to_dict() for mq in mqs],
        }

        # 将请求数据序列化为JSON并放入body
        body = json.dumps(request_data, ensure_ascii=False).encode("utf-8")

        return RemotingCommand(
            code=RequestCode.LOCK_BATCH_MQ,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            body=body,
        )

    @staticmethod
    def create_unlock_batch_mq_request(
        consumer_group: str, client_id: str, mqs: list[MessageQueue]
    ) -> RemotingCommand:
        """创建批量解锁消息队列请求

        Args:
            consumer_group: 消费者组名称
            client_id: 客户端ID
            mqs: 消息队列列表

        Returns:
            批量解锁消息队列请求命令
        """
        # 构建请求数据结构
        request_data = {
            "consumerGroup": consumer_group,
            "clientId": client_id,
            "mqSet": [mq.to_dict() for mq in mqs],
        }

        # 将请求数据序列化为JSON并放入body
        body = json.dumps(request_data, ensure_ascii=False).encode("utf-8")

        return RemotingCommand(
            code=RequestCode.UNLOCK_BATCH_MQ,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
            body=body,
        )
