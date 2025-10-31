"""
Broker 客户端实现
提供同步和异步两种方式与 RocketMQ Broker 进行通信。
"""

import json
import time


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
    transaction_state,
)
from ..model.enums import ResponseCode
from ..model.factory import RemotingRequestFactory
from ..remote.sync_remote import Remote
from .errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
    MessagePullError,
    OffsetError,
)

logger = LoggerFactory.get_logger(__name__)


class BrokerClient:
    """同步 Broker 客户端

    使用 Remote 类进行同步通信，提供与Broker交互的基础功能
    """

    remote: Remote
    timeout: float
    _client_id: str

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
            logger.info(f"Disconnecting from Broker, client_id: {self._client_id}")
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
            logger.error(f"Send message failed: {response.code} - {error_msg}")
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
            f"Process send response: msgId={result.msg_id}, "
            f"status={result.status_name}, queueOffset={result.queue_offset}"
        )

        return result

    def sync_send_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        **kwargs,
    ) -> SendMessageResult:
        """发送消息

        Args:
            producer_group: 生产者组名
            mq: 消息队列
            body: 消息体内容
            properties: 消息属性字典，默认为None
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

        try:
            logger.debug(
                f"Sending message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, bodyLength={len(body)}, "
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
            response = self.remote.rpc(request, timeout=self.timeout)
            send_msg_rt = time.time() - start_time

            # 检查响应状态
            if response.code != ResponseCode.SUCCESS:
                error_msg = f"Send message failed with code {response.code}"
                if response.language and response.body:
                    error_msg += f": {response.body.decode('utf-8', errors='ignore')}"
                logger.error(error_msg)
                raise BrokerResponseError(error_msg)

            try:
                result = self._process_send_response(response, mq, properties)
            except Exception as e:
                logger.error(f"Failed to parse SendMessageResult: {e}")
                raise BrokerResponseError(f"Invalid response format: {e}")

            logger.info(
                f"Successfully sent message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, msgId={result.msg_id}, "
                f"queueOffset={result.queue_offset}, sendMsgRT={send_msg_rt:.3f}s"
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

            logger.error(f"Unexpected error during send_message: {e}")
            raise BrokerResponseError(f"Unexpected error during send_message: {e}")

    def oneway_send_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        """单向发送消息（不等待响应）

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

        try:
            logger.debug(
                f"Oneway sending message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, bodyLength={len(body)}"
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

            # 单向发送请求（不等待响应）
            start_time = time.time()
            self.remote.oneway(request)
            send_msg_rt = time.time() - start_time

            logger.info(
                f"Successfully oneway sent message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, sendMsgRT={send_msg_rt:.3f}s"
            )

        except Exception as e:
            if isinstance(e, (BrokerConnectionError, BrokerTimeoutError)):
                raise

            logger.error(f"Unexpected error during oneway_send_message: {e}")
            raise BrokerResponseError(
                f"Unexpected error during oneway_send_message: {e}"
            )

    def sync_batch_send_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        **kwargs,
    ) -> SendMessageResult:
        """批量发送消息

        Args:
            producer_group: 生产者组名
            body: 批量消息体内容
            mq: 消息队列
            properties: 消息属性字典，默认为None
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

        try:
            logger.debug(
                f"Sending batch message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, bodyLength={len(body)}, "
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

            # 发送请求
            response = self.remote.rpc(request)

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 创建发送结果
                result = self._process_send_response(response, mq, properties)

                logger.debug(
                    f"Batch message sent successfully: msgId={result.msg_id}, "
                    f"queueId={result.queue_id}, queueOffset={result.queue_offset}"
                )

                return result
            else:
                error_msg = response.ext_fields.get("remark", "Unknown error")
                raise BrokerResponseError(
                    f"Send batch message failed: {response.code} - {error_msg}"
                )

        except BrokerConnectionError:
            raise
        except BrokerTimeoutError:
            raise
        except BrokerResponseError:
            raise
        except Exception as e:
            # 检查是否是已知的网络异常
            if any(
                str(e).startswith(error_type)
                for error_type in [
                    "ConnectionError",
                    "TimeoutError",
                    "OSError",
                    "ConnectionAbortedError",
                    "ConnectionResetError",
                    "ConnectionRefusedError",
                ]
            ):
                logger.error(f"Network error during send_batch_message: {e}")
                raise BrokerConnectionError(f"Network error: {e}")

            logger.error(f"Unexpected error during send_batch_message: {e}")
            raise BrokerResponseError(
                f"Unexpected error during send_batch_message: {e}"
            )

    def oneway_batch_send_message(
        self,
        producer_group: str,
        body: bytes,
        mq: MessageQueue,
        properties: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        """单向批量发送消息（不等待响应）

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

        try:
            logger.debug(
                f"Oneway sending batch message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, bodyLength={len(body)}"
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

            # 单向发送请求（不等待响应）
            start_time = time.time()
            self.remote.oneway(request)
            send_msg_rt = time.time() - start_time

            logger.info(
                f"Successfully oneway sent batch message: producerGroup={producer_group}, "
                f"topic={mq.topic}, queueId={mq.queue_id}, sendMsgRT={send_msg_rt:.3f}s"
            )

        except Exception as e:
            if isinstance(e, (BrokerConnectionError, BrokerTimeoutError)):
                raise

            logger.error(f"Unexpected error during oneway_batch_send_message: {e}")
            raise BrokerResponseError(
                f"Unexpected error during oneway_batch_send_message: {e}"
            )

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
                logger.info(f"No messages found in topic={topic}, queueId={queue_id}")
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
                error_msg = response.remark or f"Unknown pull error: {response.code}"
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

    def query_consumer_offset(
        self,
        consumer_group: str,
        topic: str,
        queue_id: int,
    ) -> int:
        """查询消费者偏移量

        Args:
            consumer_group: 消费者组名
            topic: 主题名称
            queue_id: 队列ID

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
            logger.debug(
                f"Querying consumer offset: consumerGroup={consumer_group}, "
                f"topic={topic}, queueId={queue_id}"
            )

            # 创建查询消费者偏移量请求
            request = RemotingRequestFactory.create_query_consumer_offset_request(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=queue_id,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            query_rt = time.time() - start_time

            logger.debug(
                f"Query offset response received: code={response.code}, queryRT={query_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功查询到偏移量，从 ext_fields 中获取 offset
                if response.ext_fields and "offset" in response.ext_fields:
                    try:
                        offset_str = response.ext_fields["offset"]
                        offset = int(offset_str)
                        logger.info(
                            f"Successfully queried consumer offset: consumerGroup={consumer_group}, "
                            f"topic={topic}, queueId={queue_id}, offset={offset}"
                        )
                        return offset
                    except (ValueError, TypeError) as e:
                        logger.error(f"Failed to parse offset from ext_fields: {e}")
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}",
                            topic=topic,
                            queue_id=queue_id,
                        )
                else:
                    # 响应成功但没有offset字段，可能表示偏移量为0或未设置
                    logger.info(
                        f"No offset field found for consumerGroup={consumer_group}, "
                        f"topic={topic}, queueId={queue_id}, returning 0"
                    )
                    return 0

            elif response.code == ResponseCode.QUERY_NOT_FOUND:
                # 没有找到偏移量，通常返回-1或0
                logger.info(
                    f"Consumer offset not found for consumerGroup={consumer_group}, "
                    f"topic={topic}, queueId={queue_id}"
                )
                raise OffsetError(
                    f"Consumer offset not found: consumerGroup={consumer_group}, "
                    f"topic={topic}, queueId={queue_id}",
                    topic=topic,
                    queue_id=queue_id,
                )

            elif response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                logger.error(f"Topic not exist: {topic}")
                raise BrokerResponseError(
                    f"Topic not exist: {topic}",
                    response_code=response.code,
                )

            elif response.code == ResponseCode.ERROR:
                # 通用错误，可能包括消费者组不存在、系统错误、权限错误等
                error_msg = response.remark or "General error"
                logger.error(f"Query consumer offset error: {error_msg}")
                raise BrokerResponseError(
                    f"Query consumer offset error: {error_msg}",
                    response_code=response.code,
                )

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = response.remark or "Service not available"
                logger.error(f"Service not available: {error_msg}")
                raise BrokerResponseError(
                    f"Service not available: {error_msg}",
                    response_code=response.code,
                )

            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown query offset error: {response.code}"
                )
                logger.error(f"Query consumer offset failed: {error_msg}")
                raise BrokerResponseError(
                    f"Query consumer offset failed: {error_msg}",
                    response_code=response.code,
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

            logger.error(f"Unexpected error during query_consumer_offset: {e}")
            raise OffsetError(
                f"Unexpected error during query_consumer_offset: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    def update_consumer_offset(
        self,
        consumer_group: str,
        topic: str,
        queue_id: int,
        commit_offset: int,
    ) -> None:
        """更新消费者偏移量（使用oneway通信，无需等待响应）

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
                f"Updating consumer offset (oneway): consumerGroup={consumer_group}, "
                f"topic={topic}, queueId={queue_id}, offset={commit_offset}"
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
            self.remote.oneway(request)
            update_rt = time.time() - start_time

            logger.info(
                f"Successfully sent consumer offset update (oneway): consumerGroup={consumer_group}, "
                f"topic={topic}, queueId={queue_id}, offset={commit_offset}, updateRT={update_rt:.3f}s"
            )

        except Exception as e:
            if isinstance(e, BrokerConnectionError):
                raise

            logger.error(f"Unexpected error during update_consumer_offset: {e}")
            raise OffsetError(
                f"Unexpected error during update_consumer_offset: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    def search_offset_by_timestamp(
        self,
        topic: str,
        queue_id: int,
        timestamp: int,
    ) -> int:
        """根据时间戳搜索偏移量

        Args:
            topic: 主题名称
            queue_id: 队列ID
            timestamp: 时间戳（毫秒）

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
                f"Searching offset by timestamp: topic={topic}, "
                f"queueId={queue_id}, timestamp={timestamp}"
            )

            # 创建搜索偏移量请求
            request = RemotingRequestFactory.create_search_offset_request(
                topic=topic,
                queue_id=queue_id,
                timestamp=timestamp,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            search_rt = time.time() - start_time

            logger.debug(
                f"Search offset response received: code={response.code}, searchRT={search_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功搜索到偏移量，从 ext_fields 中获取 offset
                if response.ext_fields and "offset" in response.ext_fields:
                    try:
                        offset_str = response.ext_fields["offset"]
                        offset = int(offset_str)
                        logger.info(
                            f"Successfully searched offset by timestamp: topic={topic}, "
                            f"queueId={queue_id}, timestamp={timestamp}, offset={offset}"
                        )
                        return offset
                    except (ValueError, TypeError) as e:
                        logger.error(f"Failed to parse offset from ext_fields: {e}")
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}",
                            topic=topic,
                            queue_id=queue_id,
                        )
                else:
                    # 响应成功但没有offset字段
                    logger.error(
                        f"No offset field found for topic={topic}, "
                        f"queueId={queue_id}, timestamp={timestamp}"
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
                    f"No offset found for timestamp: topic={topic}, "
                    f"queueId={queue_id}, timestamp={timestamp}"
                )
                return -1

            elif response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                logger.error(f"Topic not exist: {topic}")
                raise BrokerResponseError(
                    f"Topic not exist: {topic}",
                    response_code=response.code,
                )

            elif response.code == ResponseCode.ERROR:
                # 通用错误
                error_msg = response.remark or "General error"
                logger.error(f"Search offset by timestamp error: {error_msg}")
                raise BrokerResponseError(
                    f"Search offset by timestamp error: {error_msg}",
                    response_code=response.code,
                )

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = response.remark or "Service not available"
                logger.error(f"Service not available: {error_msg}")
                raise BrokerResponseError(
                    f"Service not available: {error_msg}",
                    response_code=response.code,
                )
            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown search offset error: {response.code}"
                )
                logger.error(f"Search offset by timestamp failed: {error_msg}")
                raise BrokerResponseError(
                    f"Search offset by timestamp failed: {error_msg}",
                    response_code=response.code,
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

            logger.error(f"Unexpected error during search_offset_by_timestamp: {e}")
            raise OffsetError(
                f"Unexpected error during search_offset_by_timestamp: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    def get_max_offset(self, topic: str, queue_id: int) -> int:
        """获取队列的最大偏移量

        Args:
            topic: 主题名称
            queue_id: 队列ID

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
            logger.debug(f"Getting max offset: topic={topic}, queueId={queue_id}")

            # 创建获取最大偏移量请求
            request = RemotingRequestFactory.create_get_max_offset_request(
                topic=topic,
                queue_id=queue_id,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            query_rt = time.time() - start_time

            logger.debug(
                f"Get max offset response received: code={response.code}, queryRT={query_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 成功获取到最大偏移量，从 ext_fields 中获取 offset
                if response.ext_fields and "offset" in response.ext_fields:
                    try:
                        offset_str = response.ext_fields["offset"]
                        offset = int(offset_str)
                        logger.info(
                            f"Successfully got max offset: topic={topic}, "
                            f"queueId={queue_id}, maxOffset={offset}"
                        )
                        return offset
                    except (ValueError, TypeError) as e:
                        logger.error(f"Failed to parse offset from ext_fields: {e}")
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}",
                            topic=topic,
                            queue_id=queue_id,
                        )
                else:
                    # 响应成功但没有offset字段
                    logger.error(
                        f"No offset field found for topic={topic}, queueId={queue_id}"
                    )
                    raise OffsetError(
                        f"No offset field found in response: topic={topic}, "
                        f"queueId={queue_id}",
                        topic=topic,
                        queue_id=queue_id,
                    )

            elif response.code == ResponseCode.TOPIC_NOT_EXIST:
                # 主题不存在
                logger.error(f"Topic not exist: {topic}")
                raise BrokerResponseError(
                    f"Topic not exist: {topic}",
                    response_code=response.code,
                )

            elif response.code == ResponseCode.ERROR:
                # 通用错误
                error_msg = response.remark or "General error"
                logger.error(f"Get max offset error: {error_msg}")
                raise BrokerResponseError(
                    f"Get max offset error: {error_msg}",
                    response_code=response.code,
                )

            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                error_msg = response.remark or "Service not available"
                logger.error(f"Service not available: {error_msg}")
                raise BrokerResponseError(
                    f"Service not available: {error_msg}",
                    response_code=response.code,
                )
            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown get max offset error: {response.code}"
                )
                logger.error(f"Get max offset failed: {error_msg}")
                raise BrokerResponseError(
                    f"Get max offset failed: {error_msg}",
                    response_code=response.code,
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

            logger.error(f"Unexpected error during get_max_offset: {e}")
            raise OffsetError(
                f"Unexpected error during get_max_offset: {e}",
                topic=topic,
                queue_id=queue_id,
            )

    def send_heartbeat(self, heartbeat_data: HeartbeatData) -> None:
        """发送心跳数据

        Args:
            heartbeat_data: 心跳数据

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                f"Sending heartbeat: clientId={heartbeat_data.client_id}, "
                f"producers={len(heartbeat_data.producer_data_set)}, "
                f"consumers={len(heartbeat_data.consumer_data_set)}"
            )

            # 创建心跳请求
            request = RemotingRequestFactory.create_heartbeat_request(heartbeat_data)

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            heartbeat_rt = time.time() - start_time

            logger.debug(
                f"Heartbeat response received: code={response.code}, heartbeatRT={heartbeat_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 心跳成功
                logger.debug(
                    f"Successfully sent heartbeat: clientId={heartbeat_data.client_id}, "
                    f"producers={len(heartbeat_data.producer_data_set)}, "
                    f"consumers={len(heartbeat_data.consumer_data_set)}, heartbeatRT={heartbeat_rt:.3f}s"
                )
            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                logger.warning(
                    f"Service not available during heartbeat: clientId={heartbeat_data.client_id}, remark={response.remark}"
                )
                raise BrokerResponseError(
                    f"Service not available during heartbeat: {response.remark}",
                    response_code=response.code,
                )
            elif response.code == ResponseCode.ERROR:
                # 通用错误
                error_msg = response.remark or "Heartbeat error"
                logger.error(f"Heartbeat failed: {error_msg}")
                raise BrokerResponseError(
                    f"Heartbeat failed: {error_msg}",
                    response_code=response.code,
                )
            else:
                # 其他错误响应
                error_msg = (
                    response.remark or f"Unknown heartbeat error: {response.code}"
                )
                logger.error(f"Heartbeat failed: {error_msg}")
                raise BrokerResponseError(
                    f"Heartbeat failed: {error_msg}",
                    response_code=response.code,
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

            logger.error(f"Unexpected error during send_heartbeat: {e}")
            raise BrokerResponseError(f"Unexpected error during send_heartbeat: {e}")

    def consumer_send_msg_back(
        self,
        message_ext: MessageExt,
        delay_level: int,
        consumer_group: str,
        max_reconsume_times: int = 16,
    ) -> None:
        """消费者发送消息回退请求

        Args:
            message_ext: 扩展消息对象
            delay_level: 延迟级别
            consumer_group: 消费者组
            max_reconsume_times: 最大重新消费次数

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                f"Sending consumer send msg back: consumerGroup={consumer_group}, "
                f"originTopic={message_ext.topic}, originMsgId={message_ext.msg_id}, "
                f"delayLevel={delay_level}"
            )

            # 创建消费者发送消息回退请求
            request = RemotingRequestFactory.create_consumer_send_msg_back_request(
                group=consumer_group,
                offset=message_ext.commit_log_offset or 0,
                delay_level=delay_level,
                origin_msg_id=message_ext.msg_id or "",
                origin_topic=message_ext.topic,
                max_reconsume_times=max_reconsume_times,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            send_back_rt = time.time() - start_time

            logger.debug(
                f"Consumer send msg back response received: code={response.code}, sendBackRT={send_back_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 发送回退成功
                logger.info(
                    f"Successfully sent consumer send msg back: consumerGroup={consumer_group}, "
                    f"originTopic={message_ext.topic}, originMsgId={message_ext.msg_id}, "
                    f"delayLevel={delay_level}, sendBackRT={send_back_rt:.3f}s"
                )
            elif response.code == ResponseCode.SERVICE_NOT_AVAILABLE:
                # 服务不可用
                logger.warning(
                    f"Service not available during consumer send msg back: consumerGroup={consumer_group}, remark={response.remark}"
                )
                raise BrokerResponseError(
                    f"Service not available during consumer send msg back: {response.remark}",
                    response_code=response.code,
                )
            elif response.code == ResponseCode.ERROR:
                # 通用错误
                error_msg = response.remark or "Consumer send msg back error"
                logger.error(f"Consumer send msg back failed: {error_msg}")
                raise BrokerResponseError(
                    f"Consumer send msg back failed: {error_msg}",
                    response_code=response.code,
                )
            else:
                # 其他错误响应
                error_msg = (
                    response.remark
                    or f"Unknown consumer send msg back error: {response.code}"
                )
                logger.error(f"Consumer send msg back failed: {error_msg}")
                raise BrokerResponseError(
                    f"Consumer send msg back failed: {error_msg}",
                    response_code=response.code,
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

            logger.error(f"Unexpected error during consumer_send_msg_back: {e}")
            raise BrokerResponseError(
                f"Unexpected error during consumer_send_msg_back: {e}"
            )

    def end_transaction(
        self,
        producer_group: str,
        tran_state_table_offset: int,
        commit_log_offset: int,
        local_transaction_state: LocalTransactionState,
        msg_id: str = "",
        transaction_id: str = "",
        from_transaction_check: bool = True,
    ) -> None:
        """结束事务请求（使用oneway通信，无需等待响应）

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
                f"Sending end transaction {action}: producerGroup={producer_group}, "
                f"tranStateTableOffset={tran_state_table_offset}, "
                f"commitLogOffset={commit_log_offset}, "
                f"localTransactionState={local_transaction_state.name}, "
                f"commitOrRollback={commit_or_rollback}, "
                f"msgId={msg_id}, transactionId={transaction_id}"
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
            self.remote.oneway(request)
            end_tx_rt = time.time() - start_time

            logger.info(
                f"Successfully sent end transaction {action}: producerGroup={producer_group}, "
                f"tranStateTableOffset={tran_state_table_offset}, "
                f"commitLogOffset={commit_log_offset}, "
                f"localTransactionState={local_transaction_state.name}, "
                f"commitOrRollback={commit_or_rollback}, "
                f"msgId={msg_id}, transactionId={transaction_id}, endTxRT={end_tx_rt:.3f}s"
            )

        except Exception as e:
            if isinstance(e, BrokerConnectionError):
                raise

            logger.error(f"Unexpected error during end_transaction: {e}")
            raise BrokerResponseError(f"Unexpected error during end_transaction: {e}")

    def get_consumers_by_group(self, consumer_group: str) -> list[str]:
        """获取指定消费者组的消费者列表

        Args:
            consumer_group: 消费者组名称

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
            logger.debug(f"Getting consumer list for group: {consumer_group}")

            # 创建获取消费者列表请求
            request = RemotingRequestFactory.create_get_consumer_list_request(
                consumer_group
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            get_consumers_rt = time.time() - start_time

            logger.debug(
                f"Get consumer list response received: code={response.code}, "
                f"getConsumersRT={get_consumers_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 解析响应体中的消费者列表
                if response.body:
                    try:
                        # RocketMQ返回的消费者列表通常是JSON格式
                        consumer_data = json.loads(response.body.decode("utf-8"))

                        # 根据RocketMQ协议，消费者列表通常在consumerIdList字段中
                        if (
                            isinstance(consumer_data, dict)
                            and "consumerIdList" in consumer_data
                        ):
                            consumer_list = consumer_data["consumerIdList"]
                        elif isinstance(consumer_data, list):
                            # 如果直接返回列表
                            consumer_list = consumer_data
                        else:
                            logger.warning(
                                f"Unexpected consumer data format: {consumer_data}"
                            )
                            consumer_list = []

                        logger.info(
                            f"Successfully got consumer list for group '{consumer_group}': "
                            f"{len(consumer_list)} consumers"
                        )
                        return consumer_list

                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(f"Failed to parse consumer list response: {e}")
                        # 如果解析失败，返回空列表而不是抛出异常
                        logger.warning(
                            "Returning empty consumer list due to parsing failure"
                        )
                        return []
                else:
                    logger.info(
                        f"No consumer data returned for group '{consumer_group}'"
                    )
                    return []
            else:
                error_msg = f"Failed to get consumer list for group '{consumer_group}': {response.code}-{response.remark}"
                logger.error(error_msg)
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

            logger.error(f"Unexpected error during get_consumers_by_group: {e}")
            raise BrokerResponseError(
                f"Unexpected error during get_consumers_by_group: {e}"
            )

    def lock_batch_mq(
        self, consumer_group: str, client_id: str, mqs: list[MessageQueue]
    ) -> list[MessageQueue]:
        """批量锁定消息队列

        Args:
            consumer_group: 消费者组名称
            client_id: 客户端ID
            mqs: 消息队列列表

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
                f"Locking batch message queues: consumerGroup={consumer_group}, "
                f"clientId={client_id}, mqCount={len(mqs)}"
            )

            # 创建批量锁定消息队列请求
            request = RemotingRequestFactory.create_lock_batch_mq_request(
                consumer_group=consumer_group,
                client_id=client_id,
                mqs=mqs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            lock_rt = time.time() - start_time

            logger.debug(
                f"Lock batch MQ response received: code={response.code}, lockRT={lock_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 解析响应体中的锁定结果
                if response.body:
                    try:
                        # RocketMQ返回的锁定结果通常是JSON格式
                        lock_result = json.loads(response.body.decode("utf-8"))

                        # 根据RocketMQ协议，锁定成功的队列列表通常在lockOKMQSet字段中
                        if (
                            isinstance(lock_result, dict)
                            and "lockOKMQSet" in lock_result
                        ):
                            locked_mqs = lock_result["lockOKMQSet"]
                        elif isinstance(lock_result, list):
                            # 如果直接返回列表
                            locked_mqs = lock_result
                        else:
                            logger.warning(
                                f"Unexpected lock result format: {lock_result}"
                            )
                            locked_mqs = []

                        # 将字典格式的消息队列转换为MessageQueue对象

                        locked_queue_list = [
                            MessageQueue.from_dict(mq_dict)
                            for mq_dict in locked_mqs
                            if isinstance(mq_dict, dict)
                        ]

                        logger.info(
                            f"Successfully locked {len(locked_queue_list)} message queues "
                            f"for consumerGroup={consumer_group}, clientId={client_id}"
                        )
                        return locked_queue_list

                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.error(f"Failed to parse lock batch response: {e}")
                        raise BrokerResponseError(
                            f"Failed to parse lock batch response: {e}",
                            response_code=response.code,
                        )
                else:
                    logger.info(
                        f"No locked queues returned for consumerGroup={consumer_group}, "
                        f"clientId={client_id}"
                    )
                    return []
            else:
                error_msg = f"Failed to lock batch message queues: {response.code}-{response.remark}"
                logger.error(error_msg)
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

            logger.error(f"Unexpected error during lock_batch_mq: {e}")
            raise BrokerResponseError(f"Unexpected error during lock_batch_mq: {e}")

    def unlock_batch_mq(
        self, consumer_group: str, client_id: str, mqs: list[MessageQueue]
    ) -> None:
        """批量解锁消息队列

        Args:
            consumer_group: 消费者组名称
            client_id: 客户端ID
            mqs: 消息队列列表

        Raises:
            BrokerConnectionError: 连接错误
            BrokerTimeoutError: 请求超时
            BrokerResponseError: 响应错误
        """
        if not self.is_connected:
            raise BrokerConnectionError("Not connected to Broker")

        try:
            logger.debug(
                f"Unlocking batch message queues: consumerGroup={consumer_group}, "
                f"clientId={client_id}, mqCount={len(mqs)}"
            )

            # 创建批量解锁消息队列请求
            request = RemotingRequestFactory.create_unlock_batch_mq_request(
                consumer_group=consumer_group,
                client_id=client_id,
                mqs=mqs,
            )

            # 发送请求并获取响应
            start_time = time.time()
            response = self.remote.rpc(request, timeout=self.timeout)
            unlock_rt = time.time() - start_time

            logger.debug(
                f"Unlock batch MQ response received: code={response.code}, unlockRT={unlock_rt:.3f}s"
            )

            # 处理响应
            if response.code == ResponseCode.SUCCESS:
                # 解锁成功
                logger.info(
                    f"Successfully unlocked {len(mqs)} message queues "
                    f"for consumerGroup={consumer_group}, clientId={client_id}, unlockRT={unlock_rt:.3f}s"
                )
            else:
                error_msg = f"Failed to unlock batch message queues: {response.code}-{response.remark}"
                logger.error(error_msg)
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

            logger.error(f"Unexpected error during unlock_batch_mq: {e}")
            raise BrokerResponseError(f"Unexpected error during unlock_batch_mq: {e}")


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
    remote = create_sync_remote(f"{host}:{port}", remote_config, transport_config)

    # 创建并返回Broker客户端
    return BrokerClient(remote=remote, timeout=timeout)
