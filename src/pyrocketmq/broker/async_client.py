"""
异步 Broker 客户端实现
提供异步方式与 RocketMQ Broker 进行通信。
"""

import json
import time
import uuid
from typing import Any

from pyrocketmq.model.message import MessageProperty
from pyrocketmq.model.result_data import SendStatus

from ..logging import LoggerFactory
from ..model import (
    HeartbeatData,
    LocalTransactionState,
    MessageExt,
    MessageQueue,
    PullMessageResult,
    RemotingCommand,
    SendMessageResult,
)
from ..model.enums import ResponseCode
from ..model.factory import RemotingRequestFactory
from ..remote.async_remote import AsyncRemote
from .errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
    MessagePullError,
    OffsetError,
)

logger = LoggerFactory.get_logger(__name__)


class AsyncBrokerClient:
    """异步 Broker 客户端

    使用 AsyncRemote 类进行异步通信，提供与Broker交互的基础功能
    """

    remote: AsyncRemote
    timeout: float
    _client_id: str

    def __init__(self, remote: AsyncRemote, timeout: float = 5.0):
        """初始化异步客户端

        Args:
            remote: 异步远程通信实例
            timeout: 请求超时时间，默认5秒
        """
        self.remote = remote
        self.timeout = timeout
        self._client_id = f"client_{int(time.time() * 1000)}"

    async def connect(self) -> None:
        """建立连接"""
        try:
            logger.info(
                "Connecting to Broker",
                extra={
                    "client_id": self._client_id,
                    "broker_host": getattr(self.remote, "host", "unknown"),
                    "broker_port": getattr(self.remote, "port", "unknown"),
                    "operation_type": "connect",
                    "timestamp": time.time(),
                },
            )
            await self.remote.connect()
            logger.info(
                "Connected to Broker successfully",
                extra={
                    "client_id": self._client_id,
                    "broker_host": getattr(self.remote, "host", "unknown"),
                    "broker_port": getattr(self.remote, "port", "unknown"),
                    "operation_type": "connect",
                    "timestamp": time.time(),
                },
            )
        except Exception as e:
            logger.error(
                "Failed to connect to Broker",
                extra={
                    "client_id": self._client_id,
                    "broker_host": getattr(self.remote, "host", "unknown"),
                    "broker_port": getattr(self.remote, "port", "unknown"),
                    "operation_type": "connect",
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerConnectionError(f"Connection failed: {e}")

    async def disconnect(self) -> None:
        """断开连接"""
        try:
            logger.info(
                "Disconnecting from Broker",
                extra={
                    "client_id": self._client_id,
                    "broker_host": getattr(self.remote, "host", "unknown"),
                    "broker_port": getattr(self.remote, "port", "unknown"),
                    "operation_type": "disconnect",
                    "timestamp": time.time(),
                },
            )
            await self.remote.close()
            logger.info(
                "Disconnected from Broker successfully",
                extra={
                    "client_id": self._client_id,
                    "broker_host": getattr(self.remote, "host", "unknown"),
                    "broker_port": getattr(self.remote, "port", "unknown"),
                    "operation_type": "disconnect",
                    "timestamp": time.time(),
                },
            )
        except Exception as e:
            logger.error(
                "Failed to disconnect from Broker",
                extra={
                    "client_id": self._client_id,
                    "broker_host": getattr(self.remote, "host", "unknown"),
                    "broker_port": getattr(self.remote, "port", "unknown"),
                    "operation_type": "disconnect",
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            # 不抛出异常，因为断开连接失败不应该影响程序退出

    @property
    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.remote.is_connected

    @property
    def client_id(self) -> str:
        """获取客户端ID"""
        return self._client_id

    def _process_send_response(
        self,
        response: RemotingCommand,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
    ) -> SendMessageResult:
        """处理发送消息的响应结果（参考Go语言实现）

        Args:
            response: 远程命令响应
            mq: 消息队列
            properties: 消息属性

        Returns:
            SendMessageResult: 发送结果

        Raises:
            BrokerResponseError: 响应错误时抛出异常
        """
        # 根据响应代码确定发送状态（参考Go语言实现）
        if response.code == ResponseCode.SUCCESS:
            status = SendStatus.SEND_OK
        elif response.code == ResponseCode.FLUSH_DISK_TIMEOUT:
            status = SendStatus.SEND_FLUSH_DISK_TIMEOUT
        elif response.code == ResponseCode.FLUSH_SLAVE_TIMEOUT:
            status = SendStatus.SEND_FLUSH_SLAVE_TIMEOUT
        elif response.code == ResponseCode.SLAVE_NOT_AVAILABLE:
            status = SendStatus.SEND_SLAVE_NOT_AVAILABLE
        else:
            status = SendStatus.SEND_UNKNOWN_ERROR
            error_msg = response.ext_fields.get("remark", "Unknown error")
            logger.error(
                "Send message failed",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "process_send_response",
                    "response_code": response.code,
                    "error_message": error_msg,
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(error_msg)

        # 从响应扩展字段中提取信息
        ext_fields = response.ext_fields or {}

        # 获取消息ID（从UNIQ_KEY属性或msgId字段）
        msg_id = ""
        if (
            properties
            and MessageProperty.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX in properties
        ):
            msg_id = properties[MessageProperty.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX]

        # 获取区域ID和Trace开关
        region_id = ext_fields.get(MessageProperty.MSG_REGION, "DefaultRegion")
        trace_switch = ext_fields.get(MessageProperty.TRACE_SWITCH, "")
        trace_on = trace_switch != "" and trace_switch.lower() != "false"

        # 解析队列ID和偏移量
        queue_id = int(ext_fields.get("queueId", "0"))
        queue_offset = int(ext_fields.get("queueOffset", "0"))

        # 创建消息队列对象
        message_queue = MessageQueue(
            topic=mq.topic,
            broker_name=mq.broker_name,
            queue_id=queue_id,
        )

        # 创建发送结果对象
        result = SendMessageResult(
            status=status,
            msg_id=msg_id,
            message_queue=message_queue,
            queue_offset=queue_offset,
            transaction_id=ext_fields.get("transactionId"),
            offset_msg_id=ext_fields.get("msgId"),
            region_id=region_id,
            trace_on=trace_on,
        )

        logger.debug(
            "Process send response",
            extra={
                "client_id": self._client_id,
                "operation_type": "process_send_response",
                "message_id": result.msg_id,
                "status": result.status_name,
                "queue_offset": result.queue_offset,
                "timestamp": time.time(),
            },
        )

        return result

    async def async_send_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> SendMessageResult:
        """异步发送消息

        Args:
            producer_group: 生产者组名
            mq: 消息队列
            body: 消息体内容
            properties: 消息属性字典，默认为None
            timeout: 请求超时时间，如果为None则使用实例默认超时时间
            **kwargs: 其他参数

        Returns:
            SendMessageResult: 发送消息结果

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        async_operation_id: str = ""
        try:
            async_operation_id = str(uuid.uuid4())
            logger.debug(
                "Sending message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_send_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "body_length": len(body),
                    "timestamp": time.time(),
                },
            )

            # 创建发送消息请求
            request = RemotingRequestFactory.create_send_message_request(
                producer_group=producer_group,
                topic=mq.topic,
                body=body,
                queue_id=mq.queue_id,
                properties=properties,
                **kwargs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            send_msg_rt = time.time() - start_time

            # 检查响应状态
            if response.code != ResponseCode.SUCCESS:
                error_msg = f"Send message failed with code {response.code}"
                if response.language and response.body:
                    error_msg += f": {response.body.decode('utf-8', errors='ignore')}"
                logger.error(
                    "Send message failed with response error",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_send_message",
                        "producer_group": producer_group,
                        "topic": mq.topic,
                        "queue_id": mq.queue_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

            try:
                result = self._process_send_response(response, mq, properties)
            except Exception as e:
                logger.error(
                    "Failed to parse SendMessageResult",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_send_message",
                        "producer_group": producer_group,
                        "topic": mq.topic,
                        "queue_id": mq.queue_id,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Invalid response format: {e}")

            logger.info(
                "Successfully sent message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_send_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "message_id": result.msg_id,
                    "queue_offset": result.queue_offset,
                    "execution_time": send_msg_rt,
                    "timestamp": time.time(),
                },
            )

            return result

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during send_message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_send_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(f"Unexpected error during send_message: {e}")

    async def async_oneway_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """异步单向发送消息（不等待响应）

        Args:
            producer_group: 生产者组名
            mq: 消息队列
            body: 消息体内容
            properties: 消息属性字典，默认为None
            **kwargs: 其他参数

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        async_operation_id: str = ""
        try:
            async_operation_id = str(uuid.uuid4())
            logger.debug(
                "Async oneway sending message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_oneway_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "body_length": len(body),
                    "timestamp": time.time(),
                },
            )

            # 创建发送消息请求
            request = RemotingRequestFactory.create_send_message_request(
                producer_group=producer_group,
                topic=mq.topic,
                body=body,
                queue_id=mq.queue_id,
                properties=properties,
                **kwargs,
            )

            # 异步单向发送请求（不等待响应）
            start_time = time.time()
            await self.remote.oneway(request)
            send_msg_rt = time.time() - start_time

            logger.info(
                "Successfully async oneway sent message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_oneway_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "execution_time": send_msg_rt,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(e, (BrokerConnectionError, BrokerTimeoutError)):
                raise

            logger.error(
                "Unexpected error during async_oneway_message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_oneway_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(
                f"Unexpected error during async_oneway_message: {e}"
            )

    async def async_batch_send_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> SendMessageResult:
        """异步批量发送消息

        Args:
            producer_group: 生产者组名
            body: 批量消息体内容
            mq: 消息队列
            properties: 消息属性字典，默认为None
            timeout: 请求超时时间，如果为None则使用实例默认超时时间
            **kwargs: 其他参数

        Returns:
            SendMessageResult: 发送消息结果

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        async_operation_id: str = ""

        try:
            async_operation_id = str(uuid.uuid4())
            logger.debug(
                "Sending batch message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_batch_send_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "body_length": len(body),
                    "timestamp": time.time(),
                },
            )

            # 创建发送批量消息请求
            request = RemotingRequestFactory.create_send_batch_message_request(
                producer_group=producer_group,
                topic=mq.topic,
                body=body,
                queue_id=mq.queue_id,
                properties=properties,
                **kwargs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            send_msg_rt = time.time() - start_time

            # 检查响应状态
            if response.code != ResponseCode.SUCCESS:
                error_msg = f"Send batch message failed with code {response.code}"
                if response.language and response.body:
                    error_msg += f": {response.body.decode('utf-8', errors='ignore')}"
                logger.error(
                    "Send batch message failed with response error",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_batch_send_message",
                        "producer_group": producer_group,
                        "topic": mq.topic,
                        "queue_id": mq.queue_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

            # 解析响应体为SendMessageResult
            if not response.body:
                raise BrokerResponseError("Empty response body for send batch message")

            try:
                result = self._process_send_response(response, mq, properties)
            except Exception as e:
                logger.error(
                    "Failed to parse batch SendMessageResult",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_batch_send_message",
                        "producer_group": producer_group,
                        "topic": mq.topic,
                        "queue_id": mq.queue_id,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Invalid response format: {e}")

            logger.info(
                "Successfully sent batch message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_batch_send_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "message_id": result.msg_id,
                    "queue_offset": result.queue_offset,
                    "execution_time": send_msg_rt,
                    "timestamp": time.time(),
                },
            )

            return result

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during batch_send_message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_batch_send_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(
                f"Unexpected error during batch_send_message: {e}"
            )

    async def async_batch_oneway_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """异步单向批量发送消息（不等待响应）

        Args:
            producer_group: 生产者组名
            body: 批量消息体内容
            mq: 消息队列
            properties: 消息属性字典，默认为None
            **kwargs: 其他参数

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        async_operation_id: str = ""
        try:
            async_operation_id = str(uuid.uuid4())
            logger.debug(
                "Async oneway sending batch message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_batch_oneway_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "body_length": len(body),
                    "timestamp": time.time(),
                },
            )

            # 创建发送批量消息请求
            request = RemotingRequestFactory.create_send_batch_message_request(
                producer_group=producer_group,
                topic=mq.topic,
                body=body,
                queue_id=mq.queue_id,
                properties=properties,
                **kwargs,
            )

            # 异步单向发送请求（不等待响应）
            start_time = time.time()
            await self.remote.oneway(request)
            send_msg_rt = time.time() - start_time

            logger.info(
                "Successfully async oneway sent batch message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_batch_oneway_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "execution_time": send_msg_rt,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(e, (BrokerConnectionError, BrokerTimeoutError)):
                raise

            logger.error(
                "Unexpected error during async_batch_oneway_message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_batch_oneway_message",
                    "producer_group": producer_group,
                    "topic": mq.topic,
                    "queue_id": mq.queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(
                f"Unexpected error during async_batch_oneway_message: {e}"
            )

    async def pull_message(
        self,
        consumer_group: str,
        topic: str,
        queue_id: int,
        queue_offset: int,
        max_msg_nums: int,
        sys_flag: int = 0,
        commit_offset: int = 0,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> PullMessageResult:
        """异步拉取消息

        Args:
            consumer_group: 消费者组名
            topic: 主题名称
            queue_id: 队列ID
            queue_offset: 队列偏移量
            max_msg_nums: 最大拉取消息数量，默认32
            sys_flag: 系统标志位，默认0
            commit_offset: 提交偏移量，默认0
            timeout: 请求超时时间，如果为None则使用实例默认超时时间
            **kwargs: 其他参数（如sub_expression等）

        Returns:
            PullMessageResult: 拉取消息结果

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
            MessagePullError: 消息拉取错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        async_operation_id: str = ""
        try:
            async_operation_id = str(uuid.uuid4())
            logger.debug(
                "Pulling message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_pull_message",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "queue_offset": queue_offset,
                    "max_msg_nums": max_msg_nums,
                    "timestamp": time.time(),
                },
            )

            # 创建拉取消息请求
            request = RemotingRequestFactory.create_pull_message_request(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=queue_id,
                queue_offset=queue_offset,
                max_msg_nums=max_msg_nums,
                sys_flag=sys_flag,
                commit_offset=commit_offset,
                **kwargs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            pull_rt = time.time() - start_time

            logger.debug(
                "Pull response received",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_pull_message",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "response_code": response.code,
                    "execution_time": pull_rt,
                    "timestamp": time.time(),
                },
            )

            # 解码响应
            result: PullMessageResult = PullMessageResult.decode_from_cmd(response)

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功拉取到消息
                result.pull_rt = pull_rt
                logger.info(
                    "Successfully pulled messages",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_pull_message",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "message_count": result.message_count,
                        "next_offset": result.next_begin_offset,
                        "execution_time": pull_rt,
                        "status": "success",
                        "timestamp": time.time(),
                    },
                )
                return result

            elif response.code == ResponseCode.PULL_NOT_FOUND:
                # 没有找到消息
                logger.info(
                    "No messages found",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_pull_message",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": response.code,
                        "execution_time": pull_rt,
                        "status": "not_found",
                        "timestamp": time.time(),
                    },
                )
                return result

            elif response.code == ResponseCode.PULL_OFFSET_MOVED:
                # 偏移量已移动
                logger.warning(
                    "Pull offset moved",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_pull_message",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": response.code,
                        "error_message": response.remark,
                        "execution_time": pull_rt,
                        "status": "offset_moved",
                        "timestamp": time.time(),
                    },
                )
                raise MessagePullError(
                    f"Pull offset moved: {response.remark}",
                    topic=topic,
                    queue_id=queue_id,
                )

            elif response.code == ResponseCode.PULL_RETRY_IMMEDIATELY:
                # 需要立即重试
                logger.warning(
                    "Pull retry immediately",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_pull_message",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": response.code,
                        "error_message": response.remark,
                        "execution_time": pull_rt,
                        "status": "retry_immediately",
                        "timestamp": time.time(),
                    },
                )
                return result

            else:
                # 其他错误响应
                error_msg = response.remark or f"Unknown pull error: {response.code}"
                logger.error(
                    "Pull message failed",
                    extra={
                        "client_id": self._client_id,
                        "async_operation_id": async_operation_id,
                        "operation_type": "async_pull_message",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "execution_time": pull_rt,
                        "status": "failed",
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(
                    f"Pull message failed: {error_msg}",
                    response_code=response.code,
                )

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                    MessagePullError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during pull_message",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "async_pull_message",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise MessagePullError(f"Unexpected error during pull_message: {e}")

    async def query_consumer_offset(
        self,
        consumer_group: str,
        topic: str,
        queue_id: int,
        timeout: float | None = None,
    ) -> int:
        """异步查询消费者偏移量

        Args:
            consumer_group: 消费者组名
            topic: 主题名称
            queue_id: 队列ID
            timeout: 请求超时时间，如果为None则使用实例默认超时时间

        Returns:
            int: 消费者偏移量

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
            OffsetError: 偏移量查询错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            async_operation_id = str(uuid.uuid4())
            logger.debug(
                "Querying consumer offset",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "query_consumer_offset",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "timestamp": time.time(),
                },
            )

            # 创建查询消费者偏移量请求
            request = RemotingRequestFactory.create_query_consumer_offset_request(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=queue_id,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            query_rt = time.time() - start_time

            logger.debug(
                "Query offset response received",
                extra={
                    "client_id": self._client_id,
                    "async_operation_id": async_operation_id,
                    "operation_type": "query_consumer_offset",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "response_code": response.code,
                    "execution_time": query_rt,
                    "timestamp": time.time(),
                },
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功查询到偏移量，从 ext_fields 中获取 offset
                if response.ext_fields and "offset" in response.ext_fields:
                    try:
                        offset_str = response.ext_fields["offset"]
                        offset = int(offset_str)
                        logger.info(
                            "Successfully queried consumer offset",
                            extra={
                                "client_id": self._client_id,
                                "async_operation_id": async_operation_id,
                                "operation_type": "query_consumer_offset",
                                "consumer_group": consumer_group,
                                "topic": topic,
                                "queue_id": queue_id,
                                "offset": offset,
                                "execution_time": query_rt,
                                "timestamp": time.time(),
                            },
                        )
                        return offset
                    except (ValueError, TypeError) as e:
                        logger.error(
                            "Failed to parse offset from ext_fields",
                            extra={
                                "client_id": self._client_id,
                                "async_operation_id": async_operation_id,
                                "operation_type": "query_consumer_offset",
                                "consumer_group": consumer_group,
                                "topic": topic,
                                "queue_id": queue_id,
                                "error_message": str(e),
                                "timestamp": time.time(),
                            },
                        )
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}"
                        )
                else:
                    # 响应成功但没有offset字段，可能表示偏移量为0或未设置
                    logger.info(
                        "No offset field found for consumerGroup, topic, queueId, returning 0",
                        extra={
                            "client_id": self._client_id,
                            "operation_type": "query_consumer_offset",
                            "consumer_group": consumer_group,
                            "topic": topic,
                            "queue_id": queue_id,
                            "response_code": ResponseCode.SUCCESS,
                            "offset_returned": 0,
                            "reason": "no_offset_field",
                            "timestamp": time.time(),
                        },
                    )
                    return 0

            elif response.code == ResponseCode.QUERY_NOT_FOUND:
                # 没有找到偏移量，通常返回-1或0
                logger.info(
                    "Consumer offset not found for consumerGroup, topic, queueId",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "query_consumer_offset",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.QUERY_NOT_FOUND,
                        "reason": "offset_not_found",
                        "execution_time": query_rt,
                        "timestamp": time.time(),
                    },
                )
                raise OffsetError(
                    f"Consumer offset not found: consumerGroup={consumer_group}, "
                    f"topic={topic}, queueId={queue_id}"
                )

            elif response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                logger.error(
                    "Topic not exist",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "query_consumer_offset",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.TOPIC_NOT_EXIST,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Topic not exist: {topic}")

            elif response.code == ResponseCode.ERROR:
                # 通用错误，可能包括消费者组不存在、系统错误、权限错误等
                error_msg = response.remark or "General error"
                logger.error(
                    "Query consumer offset error",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "query_consumer_offset",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.ERROR,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Query consumer offset error: {error_msg}")

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = response.remark or "Service not available"
                logger.error(
                    "Service not available",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "query_consumer_offset",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.SERVICE_NOT_AVAILABLE,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Service not available: {error_msg}")

            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown query offset error: {response.code}"
                )
                logger.error(
                    "Query consumer offset failed",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "query_consumer_offset",
                        "consumer_group": consumer_group,
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Query consumer offset failed: {error_msg}")

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                    OffsetError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during query_consumer_offset",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "query_consumer_offset",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise OffsetError(f"Unexpected error during query_consumer_offset: {e}")

    async def update_consumer_offset(
        self,
        consumer_group: str,
        topic: str,
        queue_id: int,
        commit_offset: int,
    ) -> None:
        """异步更新消费者偏移量（使用oneway通信，无需等待响应）

        Args:
            consumer_group: 消费者组名
            topic: 主题名称
            queue_id: 队列ID
            commit_offset: 提交的偏移量

        Raises:
            BrokerConnectionError: 连接错误
            OffsetError: 偏移量更新错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Updating consumer offset (oneway)",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "update_consumer_offset",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "offset": commit_offset,
                    "timestamp": time.time(),
                },
            )

            # 创建更新消费者偏移量请求（使用oneway模式）
            request = RemotingRequestFactory.create_update_consumer_offset_request(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=queue_id,
                commit_offset=commit_offset,
            )

            # 发送oneway请求，不等待响应
            start_time = time.time()
            await self.remote.oneway(request)
            update_rt = time.time() - start_time

            logger.info(
                "Successfully sent consumer offset update (oneway)",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "update_consumer_offset",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "offset": commit_offset,
                    "execution_time": update_rt,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(e, BrokerConnectionError):
                raise

            logger.error(
                "Unexpected error during update_consumer_offset",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "update_consumer_offset",
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "queue_id": queue_id,
                    "offset": commit_offset,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise OffsetError(
                f"Unexpected error during update_consumer_offset: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    async def search_offset_by_timestamp(
        self,
        topic: str,
        queue_id: int,
        timestamp: int,
        timeout: float | None = None,
    ) -> int:
        """异步根据时间戳搜索偏移量

        Args:
            topic: 主题名称
            queue_id: 队列ID
            timestamp: 时间戳（毫秒）
            timeout: 请求超时时间，如果为None则使用实例默认超时时间

        Returns:
            int: 对应的偏移量

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
            OffsetError: 偏移量搜索错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Searching offset by timestamp",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "search_offset_by_timestamp",
                    "topic": topic,
                    "queue_id": queue_id,
                    "timestamp": timestamp,
                    "timestamp_actual": time.time(),
                },
            )

            # 创建搜索偏移量请求
            request = RemotingRequestFactory.create_search_offset_request(
                topic=topic,
                queue_id=queue_id,
                timestamp=timestamp,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            search_rt = time.time() - start_time

            logger.debug(
                "Search offset response received",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "search_offset_by_timestamp",
                    "topic": topic,
                    "queue_id": queue_id,
                    "timestamp": timestamp,
                    "response_code": response.code,
                    "execution_time": search_rt,
                    "timestamp_actual": time.time(),
                },
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功搜索到偏移量，从 ext_fields 中获取 offset
                if response.ext_fields and "offset" in response.ext_fields:
                    try:
                        offset_str = response.ext_fields["offset"]
                        offset = int(offset_str)
                        logger.info(
                            "Successfully searched offset by timestamp",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "search_offset_by_timestamp",
                                "topic": topic,
                                "queue_id": queue_id,
                                "timestamp": timestamp,
                                "offset": offset,
                                "execution_time": search_rt,
                                "timestamp_actual": time.time(),
                            },
                        )
                        return offset
                    except (ValueError, TypeError) as e:
                        logger.error(
                            "Failed to parse offset from ext_fields",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "search_offset_by_timestamp",
                                "topic": topic,
                                "queue_id": queue_id,
                                "timestamp": timestamp,
                                "error_message": str(e),
                                "timestamp_actual": time.time(),
                            },
                        )
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}",
                            topic=topic,
                            queue_id=queue_id,
                        )
                else:
                    # 响应成功但没有offset字段
                    logger.error(
                        "No offset field found in response",
                        extra={
                            "client_id": self._client_id,
                            "operation_type": "search_offset_by_timestamp",
                            "topic": topic,
                            "queue_id": queue_id,
                            "timestamp": timestamp,
                            "timestamp_actual": time.time(),
                        },
                    )
                    raise OffsetError(
                        f"No offset field found in response: topic={topic}, "
                        f"queueId={queue_id}, timestamp={timestamp}",
                        topic=topic,
                        queue_id=queue_id,
                    )

            elif response.code == ResponseCode.QUERY_NOT_FOUND:
                # 没有找到对应的偏移量
                logger.info(
                    "No offset found for timestamp",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "search_offset_by_timestamp",
                        "topic": topic,
                        "queue_id": queue_id,
                        "timestamp": timestamp,
                        "response_code": ResponseCode.QUERY_NOT_FOUND,
                        "execution_time": search_rt,
                        "timestamp_actual": time.time(),
                    },
                )
                return -1

            elif response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                logger.error(
                    "Topic not exist",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "search_offset_by_timestamp",
                        "topic": topic,
                        "queue_id": queue_id,
                        "timestamp": timestamp,
                        "response_code": ResponseCode.TOPIC_NOT_EXIST,
                        "timestamp_actual": time.time(),
                    },
                )
                raise BrokerResponseError(f"Topic not exist: {topic}")

            elif response.code == ResponseCode.ERROR:
                # 通用错误
                error_msg = response.remark or "General error"
                logger.error(
                    "Search offset by timestamp error",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "search_offset_by_timestamp",
                        "topic": topic,
                        "queue_id": queue_id,
                        "timestamp": timestamp,
                        "response_code": ResponseCode.ERROR,
                        "error_message": error_msg,
                        "timestamp_actual": time.time(),
                    },
                )
                raise BrokerResponseError(
                    f"Search offset by timestamp error: {error_msg}"
                )

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = response.remark or "Service not available"
                logger.error(
                    "Service not available",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "search_offset_by_timestamp",
                        "topic": topic,
                        "queue_id": queue_id,
                        "timestamp": timestamp,
                        "response_code": ResponseCode.SERVICE_NOT_AVAILABLE,
                        "error_message": error_msg,
                        "timestamp_actual": time.time(),
                    },
                )
                raise BrokerResponseError(f"Service not available: {error_msg}")
            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown search offset error: {response.code}"
                )
                logger.error(
                    "Search offset by timestamp failed",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "search_offset_by_timestamp",
                        "topic": topic,
                        "queue_id": queue_id,
                        "timestamp": timestamp,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp_actual": time.time(),
                    },
                )
                raise BrokerResponseError(
                    f"Search offset by timestamp failed: {error_msg}"
                )

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                    OffsetError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during search_offset_by_timestamp",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "search_offset_by_timestamp",
                    "topic": topic,
                    "queue_id": queue_id,
                    "timestamp": timestamp,
                    "error_message": str(e),
                    "timestamp_actual": time.time(),
                },
            )
            raise OffsetError(
                f"Unexpected error during search_offset_by_timestamp: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    async def get_max_offset(
        self,
        topic: str,
        queue_id: int,
        timeout: float | None = None,
    ) -> int:
        """异步获取队列的最大偏移量

        Args:
            topic: 主题名称
            queue_id: 队列ID
            timeout: 请求超时时间，如果为None则使用实例默认超时时间

        Returns:
            int: 最大偏移量

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
            OffsetError: 偏移量查询错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Getting max offset",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "get_max_offset",
                    "topic": topic,
                    "queue_id": queue_id,
                    "timestamp": time.time(),
                },
            )

            # 创建获取最大偏移量请求
            request = RemotingRequestFactory.create_get_max_offset_request(
                topic=topic,
                queue_id=queue_id,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            query_rt = time.time() - start_time

            logger.debug(
                "Get max offset response received",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "get_max_offset",
                    "topic": topic,
                    "queue_id": queue_id,
                    "response_code": response.code,
                    "execution_time": query_rt,
                    "timestamp": time.time(),
                },
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功获取到最大偏移量，从 ext_fields 中获取 offset
                if response.ext_fields and "offset" in response.ext_fields:
                    try:
                        offset_str = response.ext_fields["offset"]
                        offset = int(offset_str)
                        logger.info(
                            "Successfully got max offset",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "get_max_offset",
                                "topic": topic,
                                "queue_id": queue_id,
                                "max_offset": offset,
                                "execution_time": query_rt,
                                "timestamp": time.time(),
                            },
                        )
                        return offset
                    except (ValueError, TypeError) as e:
                        logger.error(
                            "Failed to parse offset from ext_fields",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "get_max_offset",
                                "topic": topic,
                                "queue_id": queue_id,
                                "error_message": str(e),
                                "timestamp": time.time(),
                            },
                        )
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}",
                            topic=topic,
                            queue_id=queue_id,
                        )
                else:
                    # 响应成功但没有offset字段
                    logger.error(
                        "No offset field found in response",
                        extra={
                            "client_id": self._client_id,
                            "operation_type": "get_max_offset",
                            "topic": topic,
                            "queue_id": queue_id,
                            "timestamp": time.time(),
                        },
                    )
                    raise OffsetError(
                        f"No offset field found in response: topic={topic}, "
                        f"queueId={queue_id}",
                        topic=topic,
                        queue_id=queue_id,
                    )

            elif response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                logger.error(
                    "Topic not exist",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "get_max_offset",
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.TOPIC_NOT_EXIST,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Topic not exist: {topic}")

            elif response.code == ResponseCode.ERROR:
                # 通用错误
                error_msg = response.remark or "General error"
                logger.error(
                    "Get max offset error",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "get_max_offset",
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.ERROR,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Get max offset error: {error_msg}")

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = response.remark or "Service not available"
                logger.error(
                    "Service not available",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "get_max_offset",
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": ResponseCode.SERVICE_NOT_AVAILABLE,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Service not available: {error_msg}")
            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown get max offset error: {response.code}"
                )
                logger.error(
                    "Get max offset failed",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "get_max_offset",
                        "topic": topic,
                        "queue_id": queue_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(f"Get max offset failed: {error_msg}")

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                    OffsetError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during get_max_offset",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "get_max_offset",
                    "topic": topic,
                    "queue_id": queue_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise OffsetError(
                f"Unexpected error during get_max_offset: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    async def send_heartbeat(
        self,
        heartbeat_data: HeartbeatData,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        """异步发送心跳

        Args:
            heartbeat_data: 心跳数据
            timeout: 请求超时时间，如果为None则使用实例默认超时时间
            **kwargs: 其他参数

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Sending heartbeat",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "send_heartbeat",
                    "heartbeat_client_id": heartbeat_data.client_id,
                    "timestamp": time.time(),
                },
            )

            # 创建心跳请求
            request = RemotingRequestFactory.create_heartbeat_request(
                heartbeat_data=heartbeat_data,
                **kwargs,
            )

            # 发送请求并获取响应
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)

            # 检查响应状态
            if response.code != ResponseCode.SUCCESS:
                error_msg = f"Send heartbeat failed with code {response.code}"
                if response.body:
                    error_msg += f": {response.body.decode('utf-8', errors='ignore')}"
                logger.error(
                    "Send heartbeat failed",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "send_heartbeat",
                        "heartbeat_client_id": heartbeat_data.client_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

            logger.debug(
                "Successfully sent heartbeat",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "send_heartbeat",
                    "heartbeat_client_id": heartbeat_data.client_id,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during send_heartbeat",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "send_heartbeat",
                    "heartbeat_client_id": heartbeat_data.client_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(f"Unexpected error during send_heartbeat: {e}")

    async def consumer_send_msg_back(
        self,
        message: MessageExt,
        consumer_group: str,
        delay_level: int = 0,
        max_consume_retry_times: int = 16,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        """异步消费者发送消息回退

        Args:
            message: 消息扩展对象
            consumer_group: 消费者组名
            delay_level: 延迟级别，默认0
            max_consume_retry_times: 最大消费重试次数，默认16
            **kwargs: 其他参数

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Consumer sending message back",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "consumer_send_msg_back",
                    "consumer_group": consumer_group,
                    "message_id": message.msg_id,
                    "delay_level": delay_level,
                    "max_consume_retry_times": max_consume_retry_times,
                    "timestamp": time.time(),
                },
            )

            # 创建消费者发送消息回退请求
            request = RemotingRequestFactory.create_consumer_send_msg_back_request(
                group=consumer_group,
                offset=message.commit_log_offset or 0,
                delay_level=delay_level,
                origin_msg_id=message.msg_id or "",
                origin_topic=message.topic,
                body=message.body,
                max_reconsume_times=max_consume_retry_times,
            )

            # 发送请求并获取响应
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)

            # 检查响应状态
            if response.code != ResponseCode.SUCCESS:
                error_msg = (
                    f"Consumer send message back failed with code {response.code}"
                )
                if response.body:
                    error_msg += f": {response.body.decode('utf-8', errors='ignore')}"
                logger.error(
                    "Consumer send message back failed",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "consumer_send_msg_back",
                        "consumer_group": consumer_group,
                        "message_id": message.msg_id,
                        "delay_level": delay_level,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

            logger.info(
                "Successfully sent message back",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "consumer_send_msg_back",
                    "consumer_group": consumer_group,
                    "message_id": message.msg_id,
                    "delay_level": delay_level,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during consumer_send_msg_back",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "consumer_send_msg_back",
                    "consumer_group": consumer_group,
                    "message_id": message.msg_id,
                    "delay_level": delay_level,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(
                f"Unexpected error during consumer_send_msg_back: {e}"
            )

    async def end_transaction(
        self,
        producer_group: str,
        tran_state_table_offset: int,
        commit_log_offset: int,
        local_transaction_state: LocalTransactionState,
        msg_id: str = "",
        transaction_id: str = "",
        from_transaction_check: bool = True,
    ) -> None:
        """异步结束事务请求（使用oneway通信，无需等待响应）

        Args:
            producer_group: 生产者组
            tran_state_table_offset: 事务状态表偏移量
            commit_log_offset: 提交日志偏移量
            local_transaction_state: 本地事务状态
            msg_id: 消息ID
            transaction_id: 事务ID
            from_transaction_check: 是否从事务检查而来

        Raises:
            BrokerConnectionError: 连接错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            # 将本地事务状态转换为事务类型
            from pyrocketmq.model.utils import transaction_state

            commit_or_rollback = transaction_state(local_transaction_state)

            action = (
                "commit"
                if local_transaction_state == LocalTransactionState.COMMIT_MESSAGE_STATE
                else "rollback"
                if local_transaction_state
                == LocalTransactionState.ROLLBACK_MESSAGE_STATE
                else "unknown"
            )

            logger.debug(
                "Sending end transaction",
                extra={
                    "client_id": self._client_id,
                    "action": action,
                    "producer_group": producer_group,
                    "tran_state_table_offset": tran_state_table_offset,
                    "commit_log_offset": commit_log_offset,
                    "local_transaction_state": local_transaction_state.name,
                    "commit_or_rollback": commit_or_rollback,
                    "msg_id": msg_id,
                    "transaction_id": transaction_id,
                    "operation_type": "end_transaction",
                    "timestamp": time.time(),
                },
            )

            # 创建结束事务请求
            request = RemotingRequestFactory.create_end_transaction_request(
                producer_group=producer_group,
                tran_state_table_offset=tran_state_table_offset,
                commit_log_offset=commit_log_offset,
                commit_or_rollback=commit_or_rollback,
                msg_id=msg_id,
                transaction_id=transaction_id,
                from_transaction_check=from_transaction_check,
            )

            # 使用oneway模式发送请求，不等待响应
            start_time = time.time()
            await self.remote.oneway(request)
            end_tx_rt = time.time() - start_time

            logger.info(
                "Successfully sent end transaction",
                extra={
                    "client_id": self._client_id,
                    "action": action,
                    "producer_group": producer_group,
                    "tran_state_table_offset": tran_state_table_offset,
                    "commit_log_offset": commit_log_offset,
                    "local_transaction_state": local_transaction_state.name,
                    "commit_or_rollback": commit_or_rollback,
                    "msg_id": msg_id,
                    "transaction_id": transaction_id,
                    "execution_time": end_tx_rt,
                    "operation_type": "end_transaction",
                    "status": "success",
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(e, BrokerConnectionError):
                raise

            logger.error(
                "Unexpected error during end_transaction",
                extra={
                    "client_id": self._client_id,
                    "producer_group": producer_group,
                    "tran_state_table_offset": tran_state_table_offset,
                    "commit_log_offset": commit_log_offset,
                    "local_transaction_state": local_transaction_state.name,
                    "msg_id": msg_id,
                    "transaction_id": transaction_id,
                    "error_message": str(e),
                    "operation_type": "end_transaction",
                    "status": "unexpected_error",
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(f"Unexpected error during end_transaction: {e}")

    async def get_consumers_by_group(
        self,
        consumer_group: str,
        timeout: float | None = None,
    ) -> list[Any]:
        """异步获取指定消费者组的消费者列表

        Args:
            consumer_group: 消费者组名称
            timeout: 请求超时时间，如果为None则使用实例默认超时时间

        Returns:
            list: 消费者ID列表

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Getting consumer list for group",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "get_consumers_by_group",
                    "consumer_group": consumer_group,
                    "timestamp": time.time(),
                },
            )

            # 创建获取消费者列表请求
            request = RemotingRequestFactory.create_get_consumer_list_request(
                consumer_group
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            get_consumers_rt = time.time() - start_time

            logger.debug(
                "Get consumer list response received",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "get_consumers_by_group",
                    "consumer_group": consumer_group,
                    "response_code": response.code,
                    "execution_time": get_consumers_rt,
                    "timestamp": time.time(),
                },
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 解析响应体中的消费者列表
                if response.body:
                    try:
                        # RocketMQ返回的消费者列表通常是JSON格式
                        consumer_data: dict[str, list[str]] = json.loads(
                            response.body.decode("utf-8")
                        )

                        # 根据RocketMQ协议，消费者列表通常在consumerIdList字段中
                        if (
                            isinstance(consumer_data, dict)
                            and "consumerIdList" in consumer_data
                        ):
                            consumer_list = consumer_data["consumerIdList"]
                        else:
                            logger.warning(
                                "Unexpected consumer data format",
                                extra={
                                    "client_id": self._client_id,
                                    "operation_type": "get_consumers_by_group",
                                    "consumer_group": consumer_group,
                                    "consumer_data": str(consumer_data),
                                    "timestamp": time.time(),
                                },
                            )
                            consumer_list = []

                        logger.info(
                            "Successfully got consumer list for group",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "get_consumers_by_group",
                                "consumer_group": consumer_group,
                                "consumer_count": len(consumer_list),
                                "execution_time": get_consumers_rt,
                                "timestamp": time.time(),
                            },
                        )
                        return consumer_list

                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(
                            "Failed to parse consumer list response",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "get_consumers_by_group",
                                "consumer_group": consumer_group,
                                "error_message": str(e),
                                "timestamp": time.time(),
                            },
                        )
                        # 如果解析失败，返回空列表而不是抛出异常
                        logger.warning(
                            "Returning empty consumer list due to parsing failure",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "get_consumers_by_group",
                                "consumer_group": consumer_group,
                                "timestamp": time.time(),
                            },
                        )
                        return []
                else:
                    logger.info(
                        "No consumer data returned for group",
                        extra={
                            "client_id": self._client_id,
                            "operation_type": "get_consumers_by_group",
                            "consumer_group": consumer_group,
                            "timestamp": time.time(),
                        },
                    )
                    return []
            else:
                error_msg = f"Failed to get consumer list for group '{consumer_group}': {response.code}-{response.remark}"
                logger.error(
                    "Failed to get consumer list for group",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "get_consumers_by_group",
                        "consumer_group": consumer_group,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during get_consumers_by_group",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "get_consumers_by_group",
                    "consumer_group": consumer_group,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(
                f"Unexpected error during get_consumers_by_group: {e}"
            )

    async def lock_batch_mq(
        self,
        consumer_group: str,
        client_id: str,
        mqs: list[MessageQueue],
        timeout: float | None = None,
    ) -> list[Any]:
        """异步批量锁定消息队列

        Args:
            consumer_group: 消费者组名称
            client_id: 客户端ID
            mqs: 消息队列列表
            timeout: 请求超时时间，如果为None则使用实例默认超时时间

        Returns:
            list: 锁定成功的消息队列列表

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Locking batch message queues",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "lock_batch_mq",
                    "consumer_group": consumer_group,
                    "client_id_lock": client_id,
                    "mq_count": len(mqs),
                    "timestamp": time.time(),
                },
            )

            # 创建批量锁定消息队列请求
            request = RemotingRequestFactory.create_lock_batch_mq_request(
                consumer_group=consumer_group,
                client_id=client_id,
                mqs=mqs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)
            lock_rt = time.time() - start_time

            logger.debug(
                "Lock batch MQ response received",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "lock_batch_mq",
                    "consumer_group": consumer_group,
                    "client_id_lock": client_id,
                    "mq_count": len(mqs),
                    "response_code": response.code,
                    "execution_time": lock_rt,
                    "timestamp": time.time(),
                },
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 解析响应体中的锁定结果
                if response.body:
                    try:
                        # RocketMQ返回的锁定结果通常是JSON格式
                        lock_result: dict[str, list[dict[str, Any]]] = json.loads(
                            response.body.decode("utf-8")
                        )

                        # 根据RocketMQ协议，锁定成功的队列列表通常在lockOKMQSet字段中
                        if (
                            isinstance(lock_result, dict)
                            and "lockOKMQSet" in lock_result
                        ):
                            locked_mqs = lock_result["lockOKMQSet"]
                        else:
                            logger.warning(
                                "Unexpected lock result format",
                                extra={
                                    "client_id": self._client_id,
                                    "operation_type": "lock_batch_mq",
                                    "consumer_group": consumer_group,
                                    "client_id_lock": client_id,
                                    "lock_result": str(lock_result),
                                    "timestamp": time.time(),
                                },
                            )
                            locked_mqs = []

                        # 将字典格式的消息队列转换为MessageQueue对象

                        locked_queue_list = [
                            MessageQueue.from_dict(mq_dict)
                            for mq_dict in locked_mqs
                            if isinstance(mq_dict, dict)
                        ]

                        logger.info(
                            "Successfully locked message queues",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "lock_batch_mq",
                                "consumer_group": consumer_group,
                                "client_id_lock": client_id,
                                "locked_count": len(locked_queue_list),
                                "execution_time": lock_rt,
                                "timestamp": time.time(),
                            },
                        )
                        return locked_queue_list

                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(
                            "Failed to parse lock batch response",
                            extra={
                                "client_id": self._client_id,
                                "operation_type": "lock_batch_mq",
                                "consumer_group": consumer_group,
                                "client_id_lock": client_id,
                                "error_message": str(e),
                                "timestamp": time.time(),
                            },
                        )
                        raise BrokerResponseError(
                            f"Failed to parse lock batch response: {e}"
                        )
                else:
                    logger.info(
                        "No locked queues returned",
                        extra={
                            "client_id": self._client_id,
                            "operation_type": "lock_batch_mq",
                            "consumer_group": consumer_group,
                            "client_id_lock": client_id,
                            "timestamp": time.time(),
                        },
                    )
                    return []
            else:
                error_msg = f"Failed to lock batch message queues: {response.code}-{response.remark}"
                logger.error(
                    "Failed to lock batch message queues",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "lock_batch_mq",
                        "consumer_group": consumer_group,
                        "client_id_lock": client_id,
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during lock_batch_mq",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "lock_batch_mq",
                    "consumer_group": consumer_group,
                    "client_id_lock": client_id,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(f"Unexpected error during lock_batch_mq: {e}")

    async def unlock_batch_mq(
        self,
        consumer_group: str,
        client_id: str,
        mqs: list[MessageQueue],
        timeout: float | None = None,
    ) -> None:
        """异步批量解锁消息队列

        Args:
            consumer_group: 消费者组名称
            client_id: 客户端ID
            mqs: 要解锁的消息队列列表
            timeout: 请求超时时间，如果为None则使用实例默认超时时间

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                "Unlocking batch message queues",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "unlock_batch_mq",
                    "consumer_group": consumer_group,
                    "client_id_unlock": client_id,
                    "mq_count": len(mqs),
                    "timestamp": time.time(),
                },
            )

            # 创建批量解锁消息队列请求
            request = RemotingRequestFactory.create_unlock_batch_mq_request(
                consumer_group=consumer_group,
                client_id=client_id,
                mqs=mqs,
            )

            # 发送请求并获取响应
            actual_timeout = timeout if timeout is not None else self.timeout
            response = await self.remote.rpc(request, timeout=actual_timeout)

            # 检查响应状态
            if response.code != ResponseCode.SUCCESS:
                error_msg = (
                    f"Unlock batch message queues failed with code {response.code}"
                )
                if response.body:
                    error_msg += f": {response.body.decode('utf-8', errors='ignore')}"
                logger.error(
                    "Unlock batch message queues failed",
                    extra={
                        "client_id": self._client_id,
                        "operation_type": "unlock_batch_mq",
                        "consumer_group": consumer_group,
                        "client_id_unlock": client_id,
                        "mq_count": len(mqs),
                        "response_code": response.code,
                        "error_message": error_msg,
                        "timestamp": time.time(),
                    },
                )
                raise BrokerResponseError(error_msg)

            logger.info(
                "Successfully unlocked message queues",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "unlock_batch_mq",
                    "consumer_group": consumer_group,
                    "client_id_unlock": client_id,
                    "mq_count": len(mqs),
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                ),
            ):
                raise

            logger.error(
                "Unexpected error during unlock_batch_mq",
                extra={
                    "client_id": self._client_id,
                    "operation_type": "unlock_batch_mq",
                    "consumer_group": consumer_group,
                    "client_id_unlock": client_id,
                    "mq_count": len(mqs),
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise BrokerResponseError(f"Unexpected error during unlock_batch_mq: {e}")


def create_async_broker_client(
    host: str,
    port: int,
    timeout: float = 30.0,
    **kwargs: Any,
) -> AsyncBrokerClient:
    """创建异步Broker客户端

    Args:
        host: Broker主机地址
        port: Broker端口
        timeout: 请求超时时间，默认30秒
        **kwargs: 其他配置参数

    Returns:
        AsyncBrokerClient: 异步Broker客户端实例
    """
    from ..remote.config import RemoteConfig
    from ..remote.factory import create_async_remote
    from ..transport.config import TransportConfig

    # 创建传输层配置
    transport_config = TransportConfig(host=host, port=port, **kwargs)

    # 创建远程通信配置
    remote_config = RemoteConfig(rpc_timeout=timeout)

    # 创建异步远程通信实例
    remote = create_async_remote(f"{host}:{port}", remote_config, transport_config)

    # 创建并返回异步Broker客户端
    return AsyncBrokerClient(remote=remote, timeout=timeout)
