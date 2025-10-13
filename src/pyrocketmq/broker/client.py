"""
Broker 客户端实现
提供同步和异步两种方式与 RocketMQ Broker 进行通信。
"""

import time

from ..logging import LoggerFactory
from ..model import (
    PullMessageResult,
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
            request = (
                RemotingRequestFactory.create_query_consumer_offset_request(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id,
                )
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
                        logger.error(
                            f"Failed to parse offset from ext_fields: {e}"
                        )
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
                    response.remark
                    or f"Unknown query offset error: {response.code}"
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
            request = (
                RemotingRequestFactory.create_update_consumer_offset_request(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id,
                    commit_offset=commit_offset,
                )
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
                        logger.error(
                            f"Failed to parse offset from ext_fields: {e}"
                        )
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
                    response.remark
                    or f"Unknown search offset error: {response.code}"
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

            logger.error(
                f"Unexpected error during search_offset_by_timestamp: {e}"
            )
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
            logger.debug(
                f"Getting max offset: topic={topic}, queueId={queue_id}"
            )

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
                        logger.error(
                            f"Failed to parse offset from ext_fields: {e}"
                        )
                        raise OffsetError(
                            f"Failed to parse offset from ext_fields: {e}",
                            topic=topic,
                            queue_id=queue_id,
                        )
                else:
                    # 响应成功但没有offset字段
                    logger.error(
                        f"No offset field found for topic={topic}, "
                        f"queueId={queue_id}"
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
                    response.remark
                    or f"Unknown get max offset error: {response.code}"
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
