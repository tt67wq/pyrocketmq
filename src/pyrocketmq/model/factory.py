"""
RocketMQ远程命令工厂和构建器
"""

from dataclasses import dataclass, field
from typing import Dict, Optional

from .command import RemotingCommand
from .enums import FlagType, LanguageCode, RequestCode


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
    remark: Optional[str] = None
    ext_fields: Dict[str, str] = field(default_factory=dict)
    body: Optional[bytes] = None

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

    def with_ext_fields(
        self, ext_fields: Dict[str, str]
    ) -> "RemotingCommandBuilder":
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


class RemotingCommandFactory:
    """远程命令工厂类

    提供便捷的方法创建各种类型的RemotingCommand对象
    """

    @staticmethod
    def create_request(
        code: int,
        opaque: int = 0,
        body: Optional[bytes] = None,
        language: LanguageCode = LanguageCode.PYTHON,
        version: int = 1,
    ) -> RemotingCommand:
        """创建请求命令

        Args:
            code: 请求代码
            opaque: 消息ID，默认为0
            body: 消息体，可选
            language: 语言代码，默认为PYTHON
            version: 版本号，默认为1

        Returns:
            构造的请求命令
        """
        return RemotingCommand(
            code=code,
            language=language,
            version=version,
            opaque=opaque,
            flag=FlagType.RPC_TYPE,
            body=body,
        )

    @staticmethod
    def create_response(
        code: int,
        opaque: int,
        body: Optional[bytes] = None,
        remark: Optional[str] = None,
        language: LanguageCode = LanguageCode.PYTHON,
        version: int = 1,
    ) -> RemotingCommand:
        """创建响应命令

        Args:
            code: 响应代码
            opaque: 消息ID，必须与请求对应
            body: 响应体，可选
            remark: 备注信息，可选
            language: 语言代码，默认为PYTHON
            version: 版本号，默认为1

        Returns:
            构造的响应命令
        """
        return RemotingCommand(
            code=code,
            language=language,
            version=version,
            opaque=opaque,
            flag=FlagType.RESPONSE_TYPE,
            remark=remark,
            body=body,
        )

    @staticmethod
    def create_oneway(
        code: int,
        opaque: int = 0,
        body: Optional[bytes] = None,
        language: LanguageCode = LanguageCode.PYTHON,
        version: int = 1,
    ) -> RemotingCommand:
        """创建单向命令

        Args:
            code: 请求代码
            opaque: 消息ID，默认为0
            body: 消息体，可选
            language: 语言代码，默认为PYTHON
            version: 版本号，默认为1

        Returns:
            构造的单向命令
        """
        return RemotingCommand(
            code=code,
            language=language,
            version=version,
            opaque=opaque,
            flag=FlagType.RPC_ONEWAY,
            body=body,
        )

    @staticmethod
    def create_heartbeat(opaque: int = 0) -> RemotingCommand:
        """创建心跳命令

        Args:
            opaque: 消息ID，默认为0

        Returns:
            构造的心跳命令
        """
        return RemotingCommandFactory.create_request(
            code=RequestCode.HEART_BEAT, opaque=opaque
        )

    @staticmethod
    def create_send_message_request(
        topic: str,
        body: bytes,
        producer_group: str,
        opaque: int = 0,
        queue_id: int = 0,
        tags: Optional[str] = None,
        keys: Optional[str] = None,
        wait_store_msg_ok: bool = True,
    ) -> RemotingCommand:
        """创建发送消息请求

        Args:
            topic: 主题名称
            body: 消息体
            producer_group: 生产者组
            opaque: 消息ID，默认为0
            queue_id: 队列ID，默认为0
            tags: 消息标签，可选
            keys: 消息键，可选
            wait_store_msg_ok: 是否等待存储确认，默认为True

        Returns:
            构造的发送消息请求
        """
        ext_fields = {
            "topic": topic,
            "producerGroup": producer_group,
            "queueId": str(queue_id),
            "waitStoreMsgOK": "true" if wait_store_msg_ok else "false",
        }

        if tags:
            ext_fields["tags"] = tags
        if keys:
            ext_fields["keys"] = keys

        return RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            language=LanguageCode.PYTHON,
            version=1,
            opaque=opaque,
            flag=FlagType.RPC_TYPE,
            ext_fields=ext_fields,
            body=body,
        )

    @staticmethod
    def create_pull_message_request(
        topic: str,
        consumer_group: str,
        queue_id: int,
        offset: int,
        max_num: int,
        opaque: int = 0,
    ) -> RemotingCommand:
        """创建拉取消息请求

        Args:
            topic: 主题名称
            consumer_group: 消费者组
            queue_id: 队列ID
            offset: 开始偏移量
            max_num: 最大拉取数量
            opaque: 消息ID，默认为0

        Returns:
            构造的拉取消息请求
        """
        return RemotingCommand(
            code=RequestCode.PULL_MESSAGE,
            language=LanguageCode.PYTHON,
            version=1,
            opaque=opaque,
            flag=FlagType.RPC_TYPE,
            ext_fields={
                "topic": topic,
                "consumerGroup": consumer_group,
                "queueId": str(queue_id),
                "offset": str(offset),
                "maxMsgNums": str(max_num),
            },
        )

    @staticmethod
    def create_consumer_offset_request(
        topic: str,
        consumer_group: str,
        queue_id: int,
        offset: int,
        opaque: int = 0,
    ) -> RemotingCommand:
        """创建消费者偏移量请求

        Args:
            topic: 主题名称
            consumer_group: 消费者组
            queue_id: 队列ID
            offset: 偏移量
            opaque: 消息ID，默认为0

        Returns:
            构造的消费者偏移量请求
        """
        return RemotingCommand(
            code=RequestCode.UPDATE_CONSUMER_OFFSET,
            language=LanguageCode.PYTHON,
            version=1,
            opaque=opaque,
            flag=FlagType.RPC_TYPE,
            ext_fields={
                "topic": topic,
                "consumerGroup": consumer_group,
                "queueId": str(queue_id),
                "offset": str(offset),
            },
        )

    @staticmethod
    def create_query_consumer_offset_request(
        topic: str, consumer_group: str, queue_id: int, opaque: int = 0
    ) -> RemotingCommand:
        """创建查询消费者偏移量请求

        Args:
            topic: 主题名称
            consumer_group: 消费者组
            queue_id: 队列ID
            opaque: 消息ID，默认为0

        Returns:
            构造的查询消费者偏移量请求
        """
        return RemotingCommand(
            code=RequestCode.QUERY_CONSUMER_OFFSET,
            language=LanguageCode.PYTHON,
            version=1,
            opaque=opaque,
            flag=FlagType.RPC_TYPE,
            ext_fields={
                "topic": topic,
                "consumerGroup": consumer_group,
                "queueId": str(queue_id),
            },
        )

    @staticmethod
    def create_success_response(
        opaque: int, body: Optional[bytes] = None
    ) -> RemotingCommand:
        """创建成功响应

        Args:
            opaque: 消息ID，必须与请求对应
            body: 响应体，可选

        Returns:
            构造的成功响应
        """
        from .enums import ResponseCode

        return RemotingCommandFactory.create_response(
            code=ResponseCode.SUCCESS,
            opaque=opaque,
            body=body,
        )

    @staticmethod
    def create_get_broker_info_request(opaque: int = 0) -> RemotingCommand:
        """创建获取Broker集群信息请求

        Args:
            opaque: 消息ID，默认为0

        Returns:
            构造的获取Broker集群信息请求
        """
        return RemotingCommandFactory.create_request(
            code=RequestCode.GET_BROKER_CLUSTER_INFO,
            language=LanguageCode.PYTHON,
            opaque=opaque,
        )

    @staticmethod
    def create_error_response(
        opaque: int, remark: str, code: Optional[int] = None
    ) -> RemotingCommand:
        """创建错误响应

        Args:
            opaque: 消息ID，必须与请求对应
            remark: 错误信息
            code: 错误代码，如果未指定则使用SYSTEM_ERROR

        Returns:
            构造的错误响应
        """
        from .enums import ResponseCode

        if code is None:
            code = ResponseCode.SYSTEM_ERROR

        return RemotingCommandFactory.create_response(
            code=code, opaque=opaque, remark=remark
        )

    @staticmethod
    def create_query_topic_route_info_request(
        topic: str, opaque: int = 0
    ) -> RemotingCommand:
        """创建查询Topic路由信息请求

        Args:
            topic: 主题名称
            opaque: 消息ID，默认为0

        Returns:
            构造的查询Topic路由信息请求
        """
        return RemotingCommand(
            code=RequestCode.GET_ROUTE_INFO_BY_TOPIC,
            language=LanguageCode.PYTHON,
            version=1,
            opaque=opaque,
            flag=FlagType.RPC_TYPE,
            ext_fields={
                "topic": topic,
            },
        )

    @staticmethod
    def create_get_broker_cluster_info_request(
        opaque: int = 0,
    ) -> RemotingCommand:
        """创建获取Broker集群信息请求

        Args:
            opaque: 消息ID，默认为0

        Returns:
            构造的获取Broker集群信息请求
        """
        return RemotingCommandFactory.create_request(
            code=RequestCode.GET_BROKER_CLUSTER_INFO,
            opaque=opaque,
        )

    @staticmethod
    def builder(code: int) -> RemotingCommandBuilder:
        """创建构建器

        Args:
            code: 请求代码

        Returns:
            构建器实例
        """
        return RemotingCommandBuilder(code=code)
