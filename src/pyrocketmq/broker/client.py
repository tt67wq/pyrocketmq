"""
Broker 客户端实现
提供同步和异步两种方式与 RocketMQ Broker 进行通信。
"""

import time
from typing import Dict, Optional

from ..logging import LoggerFactory
from ..model.enums import ResponseCode
from ..model.factory import RemotingRequestFactory
from ..remote.sync_remote import Remote
from .errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
    MessagePullError,
    MessageSendError,
)
from .models import (
    MessageProperty,
    MessageQueue,
    PullMessageResult,
    SendMessageResult,
    SendStatus,
)

logger = LoggerFactory.get_logger(__name__)


class BrokerClient:
    """同步 Broker 客户端

    使用 Remote 类进行同步通信，提供与Broker交互的基础功能
    """

    def __init__(self, remote: Remote, timeout: float = 30.0):
        """初始化同步客户端

        Args:
            remote: 远程通信实例
            timeout: 请求超时时间，默认30秒
        """
        self.remote = remote
        self.timeout = timeout
        self._client_id = f"client_{int(time.time() * 1000)}"

    def connect(self) -> None:
        """建立连接"""
        try:
            logger.info(f"Connecting to Broker at {self.remote}")
            self.remote.connect()
            logger.info(
                f"Connected to Broker successfully, client_id: {self._client_id}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Broker: {e}")
            raise BrokerConnectionError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """断开连接"""
        try:
            logger.info(
                f"Disconnecting from Broker, client_id: {self._client_id}"
            )
            self.remote.close()
            logger.info("Disconnected from Broker successfully")
        except Exception as e:
            logger.error(f"Failed to disconnect from Broker: {e}")
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
        self, broker_name: str, response, topic: str, send_rt: float
    ) -> SendMessageResult:
        """处理发送消息响应，参考Go语言实现

        Args:
            broker_name: broker名称
            response: 响应对象
            topic: 主题名称
            send_rt: 发送耗时

        Returns:
            SendMessageResult: 发送结果

        Raises:
            MessageSendError: 发送失败时抛出异常
        """
        # 根据响应码确定状态
        status = SendStatus.SEND_UNKNOWN_ERROR
        if response.code == ResponseCode.SUCCESS:
            status = SendStatus.SEND_OK
        elif response.code == ResponseCode.FLUSH_DISK_TIMEOUT:
            status = SendStatus.SEND_FLUSH_DISK_TIMEOUT
        elif response.code == ResponseCode.FLUSH_SLAVE_TIMEOUT:
            status = SendStatus.SEND_FLUSH_SLAVE_TIMEOUT
        elif response.code == ResponseCode.SLAVE_NOT_AVAILABLE:
            status = SendStatus.SEND_SLAVE_NOT_AVAILABLE
        else:
            # 其他错误码视为未知错误
            raise MessageSendError(
                response.remark or f"Unknown send error: {response.code}",
                topic=topic,
            )

        # 从扩展字段中提取信息
        ext_fields = response.ext_fields or {}

        # 提取消息ID
        msg_id = ext_fields.get("msgId", "unknown")

        # 提取偏移量消息ID
        offset_msg_id = ext_fields.get("msgId")

        # 提取队列ID
        queue_id = 0
        try:
            queue_id = int(ext_fields.get("queueId", "0"))
        except (ValueError, TypeError):
            queue_id = 0

        # 提取队列偏移量
        queue_offset = 0
        try:
            queue_offset = int(ext_fields.get("queueOffset", "0"))
        except (ValueError, TypeError):
            queue_offset = 0

        # 提取区域ID
        region_id = ext_fields.get(MessageProperty.MSG_REGION, "DefaultRegion")

        # 提取事务ID
        transaction_id = ext_fields.get(MessageProperty.TRANSACTION_ID)

        # 提取Trace开关
        trace_on = (
            ext_fields.get(MessageProperty.TRACE_SWITCH) is not None
            and ext_fields.get(MessageProperty.TRACE_SWITCH) != "false"
        )

        # 创建MessageQueue对象
        message_queue = MessageQueue(
            topic=topic, broker_name=broker_name, queue_id=queue_id
        )

        # 创建发送结果
        result = SendMessageResult(
            status=status,
            msg_id=msg_id,
            message_queue=message_queue,
            queue_offset=queue_offset,
            transaction_id=transaction_id,
            offset_msg_id=offset_msg_id,
            region_id=region_id,
            trace_on=trace_on,
        )

        # 记录日志
        if status == SendStatus.SEND_OK:
            logger.info(
                f"Successfully sent message to topic={topic}, "
                f"queueId={queue_id}, offset={queue_offset}, "
                f"msgId={msg_id}, status={result.status_name}, sendRT={send_rt:.3f}s"
            )
        else:
            logger.warning(
                f"Message sent with warning status to topic={topic}, "
                f"queueId={queue_id}, status={result.status_name}, "
                f"msgId={msg_id}, sendRT={send_rt:.3f}s"
            )

        return result

    def pull_message(
        self,
        consumer_group: str,
        topic: str,
        queue_id: int,
        queue_offset: int,
        max_msg_nums: int = 32,
        **kwargs,
    ) -> PullMessageResult:
        """拉取消息

        Args:
            consumer_group: 消费者组名
            topic: 主题名称
            queue_id: 队列ID
            queue_offset: 队列偏移量
            max_msg_nums: 最大拉取消息数量，默认32
            **kwargs: 其他参数（如sub_expression、sys_flag等）

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

        try:
            logger.debug(
                f"Pulling message: consumerGroup={consumer_group}, "
                f"topic={topic}, queueId={queue_id}, offset={queue_offset}, "
                f"maxMsgNums={max_msg_nums}"
            )

            # 创建拉取消息请求
            request = RemotingRequestFactory.create_pull_message_request(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=queue_id,
                queue_offset=queue_offset,
                max_msg_nums=max_msg_nums,
                **kwargs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            pull_rt = time.time() - start_time

            logger.debug(
                f"Pull response received: code={response.code}, pullRT={pull_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功拉取到消息
                if response.body:
                    result = PullMessageResult.from_bytes(response.body)
                    result.pull_rt = pull_rt
                    logger.info(
                        f"Successfully pulled {result.message_count} messages from "
                        f"topic={topic}, queueId={queue_id}, nextOffset={result.next_begin_offset}"
                    )
                    return result
                else:
                    # 没有消息但响应成功
                    logger.info(
                        f"No messages found in topic={topic}, queueId={queue_id}"
                    )
                    return PullMessageResult(
                        messages=[],
                        next_begin_offset=queue_offset,
                        min_offset=queue_offset,
                        max_offset=queue_offset,
                        pull_rt=pull_rt,
                    )

            elif response.code == ResponseCode.PULL_NOT_FOUND:
                # 没有找到消息
                logger.info(
                    f"No messages found in topic={topic}, queueId={queue_id}"
                )
                return PullMessageResult(
                    messages=[],
                    next_begin_offset=queue_offset,
                    min_offset=queue_offset,
                    max_offset=queue_offset,
                    pull_rt=pull_rt,
                )

            elif response.code == ResponseCode.PULL_OFFSET_MOVED:
                # 偏移量已移动
                logger.warning(
                    f"Pull offset moved for topic={topic}, queueId={queue_id}"
                )
                raise MessagePullError(
                    f"Pull offset moved: {response.remark}",
                    topic=topic,
                    queue_id=queue_id,
                )

            elif response.code == ResponseCode.PULL_RETRY_IMMEDIATELY:
                # 需要立即重试
                logger.warning(
                    f"Pull retry immediately for topic={topic}, queueId={queue_id}"
                )
                raise MessagePullError(
                    f"Pull retry immediately: {response.remark}",
                    topic=topic,
                    queue_id=queue_id,
                )

            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown pull error: {response.code}"
                )
                logger.error(f"Pull message failed: {error_msg}")
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

            logger.error(f"Unexpected error during pull_message: {e}")
            raise MessagePullError(
                f"Unexpected error during pull_message: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    def send_message(
        self,
        producer_group: str,
        topic: str,
        body: bytes,
        queue_id: int = 0,
        tags: Optional[str] = None,
        keys: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> SendMessageResult:
        """发送消息

        Args:
            producer_group: 生产者组名
            topic: 主题名称
            body: 消息体内容
            queue_id: 队列ID，默认0
            tags: 消息标签，可选
            keys: 消息键，可选
            properties: 消息属性字典，可选
            **kwargs: 其他参数

        Returns:
            SendMessageResult: 发送消息结果

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
            MessageSendError: 消息发送错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                f"Sending message: producerGroup={producer_group}, "
                f"topic={topic}, queueId={queue_id}, bodySize={len(body)}"
            )

            # 准备消息属性
            properties_str = ""
            if properties:
                import json

                properties_str = json.dumps(properties)

            # 创建发送消息请求
            request = RemotingRequestFactory.create_send_message_request(
                producer_group=producer_group,
                topic=topic,
                body=body,
                queue_id=queue_id,
                properties=properties_str,
                tags=tags,
                keys=keys,
                **kwargs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            send_rt = time.time() - start_time

            logger.debug(
                f"Send response received: code={response.code}, sendRT={send_rt:.3f}s"
            )

            # 处理特殊错误响应
            if response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                error_msg = f"Topic not exist: {topic}"
                logger.error(error_msg)
                raise MessageSendError(error_msg, topic=topic)

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = f"Service not available: {response.remark}"
                logger.error(error_msg)
                raise MessageSendError(error_msg, topic=topic)

            elif response.code == ResponseCode.SYSTEM_BUSY:
                # 系统繁忙
                error_msg = f"System busy: {response.remark}"
                logger.error(error_msg)
                raise MessageSendError(error_msg, topic=topic)

            # 使用统一的响应处理逻辑
            broker_name = "unknown"  # TODO: 从连接信息中获取实际的broker名称
            return self._process_send_response(
                broker_name, response, topic, send_rt
            )

        except Exception as e:
            if isinstance(
                e,
                (
                    BrokerConnectionError,
                    BrokerTimeoutError,
                    BrokerResponseError,
                    MessageSendError,
                ),
            ):
                raise

            logger.error(f"Unexpected error during send_message: {e}")
            raise MessageSendError(
                f"Unexpected error during send_message: {e}",
                topic=topic,
            )


def create_broker_client(
    host: str, port: int, timeout: float = 30.0, **kwargs
) -> BrokerClient:
    """创建Broker客户端

    Args:
        host: Broker主机地址
        port: Broker端口
        timeout: 请求超时时间，默认30秒
        **kwargs: 其他配置参数

    Returns:
        BrokerClient: Broker客户端实例
    """
    from ..remote.config import RemoteConfig
    from ..remote.factory import create_sync_remote
    from ..transport.config import TransportConfig

    # 创建传输层配置
    transport_config = TransportConfig(host=host, port=port, **kwargs)

    # 创建远程通信配置
    remote_config = RemoteConfig(rpc_timeout=timeout)

    # 创建同步远程通信实例
    remote = create_sync_remote(
        f"{host}:{port}", remote_config, transport_config
    )

    # 创建并返回Broker客户端
    return BrokerClient(remote=remote, timeout=timeout)
