"""
RocketMQ异步Producer核心实现 (MVP版本)

该模块提供了一个简化但功能完整的RocketMQ异步Producer实现。
基于asyncio实现，提供非阻塞的消息发送功能。

MVP版本功能:
- 简单的布尔状态管理（移除复杂的状态机）
- 异步消息发送
- 异步单向消息发送
- 基础的队列选择和路由管理
- 基本的错误处理和重试机制

设计原则:
- 从最简单的实现开始
- 避免过度抽象
- 专注核心功能
- 易于理解和维护

作者: pyrocketmq团队
版本: MVP 1.0
"""

import asyncio
import time


# Local imports - broker
from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.broker.async_client import AsyncBrokerClient

# Local imports - model
from pyrocketmq.model import HeartbeatData, ProducerData, SendMessageResult
from pyrocketmq.model.message import Message, MessageProperty, encode_batch
from pyrocketmq.model.message_queue import MessageQueue

# Local imports - nameserver
from pyrocketmq.nameserver.client import AsyncNameServerClient

# Local imports - producer
from pyrocketmq.producer.config import ProducerConfig
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    MessageSendError,
    ProducerError,
    ProducerStartError,
    ProducerStateError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.producer.router import MessageRouter, RoutingStrategy
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping
from pyrocketmq.producer.utils import validate_message

# Local imports - remote
from pyrocketmq.remote.async_remote import AsyncRemote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig

# Local imports - utilities
from pyrocketmq.logging import get_logger

logger = get_logger(__name__)


class AsyncProducer:
    """
    RocketMQ异步Producer实现 (MVP版本)

    异步版本的Producer，提供非阻塞的消息发送功能。
    基于asyncio实现，适合高并发场景。

    核心功能:
    1. 异步生命周期管理 (start/shutdown)
    2. 异步消息发送
    3. 异步单向消息发送
    4. 基础的错误处理和重试机制

    使用示例:
        >>> import asyncio
        >>>
        >>> async def main():
        >>>     # 创建Producer实例
        >>>     producer = AsyncProducer()
        >>>     await producer.start()
        >>>
        >>>     # 发送消息
        >>>     message = Message(topic="test_topic", body=b"Hello RocketMQ")
        >>>     result = await producer.send_async(message)
        >>>     print(f"Send result: {result.success}")
        >>>
        >>>     # 关闭Producer
        >>>     await producer.shutdown()
        >>>
        >>> asyncio.run(main())
    """

    def __init__(self, config: ProducerConfig | None = None):
        """初始化AsyncProducer实例

        Args:
            config: Producer配置，如果为None则使用默认配置
        """
        # 配置管理
        self._config: ProducerConfig = config or ProducerConfig()

        # 简化的状态管理 (MVP设计)
        self._running: bool = False

        # 核心组件
        self._topic_mapping: TopicBrokerMapping = TopicBrokerMapping(
            route_timeout=self._config.update_topic_route_info_interval / 1000.0
        )

        # 消息路由器
        self._message_router: MessageRouter = MessageRouter(
            topic_mapping=self._topic_mapping,
            default_strategy=RoutingStrategy(self._config.routing_strategy),
        )

        # 基础状态统计
        self._total_sent: int = 0
        self._total_failed: int = 0

        # NameServer连接管理（仅用于路由查询）
        self._nameserver_connections: dict[str, AsyncRemote] = {}
        self._nameserver_addrs: dict[str, str] = self._parse_nameserver_addrs(
            self._config.namesrv_addr
        )

        # Broker管理器（使用异步连接池管理）
        transport_config = TransportConfig(
            host="localhost",  # 默认值，会被Broker覆盖
            port=10911,  # 默认值，会被Broker覆盖
        )
        remote_config = RemoteConfig(
            rpc_timeout=self._config.send_msg_timeout / 1000.0,
        )
        self._broker_manager: AsyncBrokerManager = AsyncBrokerManager(
            remote_config=remote_config,
            transport_config=transport_config,
        )

        # 后台任务管理
        self._background_task: asyncio.Task[None] | None = None
        self._shutdown_event: asyncio.Event = asyncio.Event()

        logger.info(
            "AsyncProducer initialized",
            extra={
                "producer_group": self._config.producer_group,
                "nameserver_addrs": list(self._nameserver_addrs.keys()),
            },
        )

    async def start(self) -> None:
        """启动AsyncProducer

        异步初始化内部组件并建立连接。这是一个幂等操作，
        多次调用不会产生副作用。

        Raises:
            ProducerStartError: 当启动失败时抛出异常
        """
        if self._running:
            logger.warning("AsyncProducer is already running")
            return

        try:
            logger.info("Starting async producer...")

            # 1. 初始化NameServer连接（仅用于路由查询）
            await self._init_nameserver_connections()

            # 2. 启动Broker管理器
            await self._broker_manager.start()

            # 3. 启动后台任务（路由更新等）
            self._start_background_tasks()

            # 设置运行状态
            self._running = True

            logger.info(
                "AsyncProducer started successfully",
                extra={
                    "producer_group": self._config.producer_group,
                    "client_id": self._config.client_id,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to start async producer",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )
            raise ProducerStartError(f"AsyncProducer start failed: {e}") from e

    async def shutdown(self) -> None:
        """关闭AsyncProducer

        异步清理资源并断开连接。这是一个幂等操作，
        多次调用不会产生副作用。
        """
        if not self._running:
            logger.warning("AsyncProducer is not running")
            return

        try:
            logger.info("Shutting down async producer...")

            # 设置停止状态
            self._running = False

            # 1. 停止后台任务
            await self._stop_background_tasks()

            # 2. 关闭Broker管理器
            await self._broker_manager.shutdown()

            # 3. 关闭NameServer连接
            await self._close_nameserver_connections()

            logger.info(
                "AsyncProducer shutdown completed",
                extra={
                    "total_sent": self._total_sent,
                    "total_failed": self._total_failed,
                },
            )

        except Exception as e:
            logger.error(
                "Error during async producer shutdown",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

    async def send(self, message: Message) -> SendMessageResult:
        """异步发送消息

        非阻塞发送消息，直到消息发送完成或失败。

        Args:
            message: 要发送的消息

        Returns:
            SendMessageResult: 发送结果，包含消息ID、队列信息、发送状态等

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
        """
        self._check_running()
        message.set_property(
            MessageProperty.PRODUCER_GROUP, self._config.producer_group
        )

        try:
            # 1. 验证消息
            validate_message(message, self._config.max_message_size)

            # 2. 更新路由信息
            if message.topic not in self._topic_mapping.get_all_topics():
                _ = await self.update_route_info(message.topic)

            # 3. 获取队列和Broker
            routing_result = self._message_router.route_message(message.topic, message)
            if not routing_result.success:
                raise RouteNotFoundError(f"Route not found for topic: {message.topic}")

            message_queue = routing_result.message_queue
            broker_data = routing_result.broker_data

            if not message_queue:
                raise QueueNotAvailableError(
                    f"No available queue for topic: {message.topic}"
                )
            if not broker_data:
                raise BrokerNotAvailableError(
                    f"No available broker data for topic: {message.topic}"
                )

            target_broker_addr = routing_result.broker_address
            if not target_broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for topic: {message.topic}"
                )
            logger.debug(
                "Sending message async to broker",
                extra={
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": message.topic,
                },
            )

            # 4. 发送消息到Broker
            send_result = await self._send_message_to_broker_async(
                message, target_broker_addr, message_queue
            )

            if send_result.is_success:
                self._total_sent += 1
                return send_result
            else:
                self._total_failed += 1
                return send_result

        except Exception as e:
            self._total_failed += 1
            logger.error(
                "Failed to send async message",
                extra={
                    "topic": message.topic,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Async message send failed: {e}") from e

    async def send_batch(self, *messages: Message) -> SendMessageResult:
        """异步批量发送消息

        将多个消息压缩为一个批量消息进行异步发送，提高发送效率。

        Args:
            *messages: 要发送的消息列表

        Returns:
            SendMessageResult: 发送结果，包含消息ID、队列信息、发送状态等

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
            ValueError: 当没有提供消息时

        Examples:
            >>> producer = await create_async_producer("group", "nameserver:9876")
            >>> await producer.start()
            >>> msg1 = Message(topic="test", body=b"message1")
            >>> msg2 = Message(topic="test", body=b"message2")
            >>> result = await producer.send_batch(msg1, msg2)
        """
        self._check_running()

        if not messages:
            raise ValueError("至少需要提供一个消息进行批量发送")

        try:
            # 1. 验证所有消息
            for i, message in enumerate(messages):
                validate_message(message, self._config.max_message_size)

                # 检查所有消息的主题是否相同
                if i > 0 and message.topic != messages[0].topic:
                    raise ValueError(
                        f"批量消息中的主题不一致: {messages[0].topic} vs {message.topic}"
                    )

            # 2. 将多个消息编码为批量消息
            batch_message = encode_batch(*messages)
            batch_message.set_property(
                MessageProperty.PRODUCER_GROUP, self._config.producer_group
            )
            logger.debug(
                "Encoded messages into async batch message",
                extra={
                    "message_count": len(messages),
                    "batch_size_bytes": len(batch_message.body),
                    "topic": batch_message.topic,
                },
            )

            # 3. 更新路由信息
            if batch_message.topic not in self._topic_mapping.get_all_topics():
                await self.update_route_info(batch_message.topic)

            # 4. 获取队列和Broker
            routing_result = self._message_router.route_message(
                batch_message.topic, batch_message
            )
            if not routing_result.success:
                raise RouteNotFoundError(
                    f"Route not found for topic: {batch_message.topic}"
                )

            message_queue = routing_result.message_queue
            broker_data = routing_result.broker_data

            if not message_queue:
                raise QueueNotAvailableError(
                    f"No available queue for topic: {batch_message.topic}"
                )
            if not broker_data:
                raise BrokerNotAvailableError(
                    f"No available broker data for topic: {batch_message.topic}"
                )

            target_broker_addr = routing_result.broker_address
            if not target_broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for topic: {batch_message.topic}"
                )
            logger.debug(
                "Sending async batch message to broker",
                extra={
                    "message_count": len(messages),
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": batch_message.topic,
                },
            )

            # 5. 发送批量消息到Broker
            send_result = await self._batch_send_message_to_broker_async(
                batch_message, target_broker_addr, message_queue
            )

            if send_result.is_success:
                self._total_sent += len(messages)
                logger.info(
                    "Async batch send success",
                    extra={
                        "message_count": len(messages),
                        "topic": batch_message.topic,
                        "queue_id": send_result.queue_id,
                        "msg_id": send_result.msg_id,
                    },
                )
                return send_result
            else:
                self._total_failed += len(messages)
                return send_result

        except Exception as e:
            self._total_failed += len(messages)
            logger.error(
                "Failed to send async batch messages",
                extra={
                    "message_count": len(messages),
                    "topic": messages[0].topic if messages else "unknown",
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Async batch message send failed: {e}") from e

    async def oneway(self, message: Message) -> None:
        """异步单向发送消息

        发送消息但不等待响应。适用于对可靠性要求不高的场景。

        Args:
            message: 要发送的消息

        Returns:
            None: 单向发送不返回任何结果

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
        """
        self._check_running()
        message.set_property(
            MessageProperty.PRODUCER_GROUP, self._config.producer_group
        )

        try:
            # 1. 验证消息
            validate_message(message, self._config.max_message_size)

            # 2. 更新路由信息
            if message.topic not in self._topic_mapping.get_all_topics():
                _ = await self.update_route_info(message.topic)

            # 3. 获取队列和Broker
            routing_result = self._message_router.route_message(message.topic, message)
            if not routing_result.success:
                raise RouteNotFoundError(f"Route not found for topic: {message.topic}")

            message_queue = routing_result.message_queue
            broker_data = routing_result.broker_data

            if not message_queue:
                raise BrokerNotAvailableError(
                    f"No available queue for topic: {message.topic}"
                )

            if not broker_data:
                raise BrokerNotAvailableError(
                    f"No available broker data for topic: {message.topic}"
                )

            # 4. 获取Broker地址
            target_broker_addr = routing_result.broker_address
            if not target_broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for: {broker_data.broker_name}"
                )

            logger.debug(
                "Sending async oneway message to broker",
                extra={
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": message.topic,
                },
            )

            # 5. 发送消息到Broker
            await self._send_message_to_broker_oneway_async(
                message, target_broker_addr, message_queue
            )

            # 更新统计（单向发送不计入成功/失败）
            logger.debug("Async oneway message sent successfully")

        except Exception as e:
            logger.error(
                "Failed to send async oneway message",
                extra={
                    "topic": message.topic,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Async oneway message send failed: {e}") from e

    async def oneway_batch(self, *messages: Message) -> None:
        """异步单向批量发送消息

        将多个消息压缩为一个批量消息进行异步单向发送，不等待响应。
        适用于对可靠性要求不高但追求高吞吐量的场景。

        Args:
            *messages: 要发送的消息列表

        Returns:
            None: 单向发送不返回任何结果

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
            ValueError: 当没有提供消息时

        Examples:
            >>> producer = await create_async_producer("group", "nameserver:9876")
            >>> await producer.start()
            >>> msg1 = Message(topic="test", body=b"message1")
            >>> msg2 = Message(topic="test", body=b"message2")
            >>> await producer.oneway_batch(msg1, msg2)  # 不等待响应
        """
        self._check_running()

        if not messages:
            raise ValueError("至少需要提供一个消息进行批量发送")

        try:
            # 1. 验证所有消息
            for i, message in enumerate(messages):
                validate_message(message, self._config.max_message_size)

                # 检查所有消息的主题是否相同
                if i > 0 and message.topic != messages[0].topic:
                    raise ValueError(
                        f"批量消息中的主题不一致: {messages[0].topic} vs {message.topic}"
                    )

            # 2. 将多个消息编码为批量消息
            batch_message = encode_batch(*messages)
            batch_message.set_property(
                MessageProperty.PRODUCER_GROUP, self._config.producer_group
            )
            logger.debug(
                "Encoded messages into batch message for async oneway",
                extra={
                    "message_count": len(messages),
                    "batch_size_bytes": len(batch_message.body),
                    "topic": batch_message.topic,
                },
            )

            # 3. 更新路由信息
            if batch_message.topic not in self._topic_mapping.get_all_topics():
                _ = await self.update_route_info(batch_message.topic)

            # 4. 获取队列和Broker
            routing_result = self._message_router.route_message(
                batch_message.topic, batch_message
            )
            if not routing_result.success:
                raise RouteNotFoundError(
                    f"Route not found for topic: {batch_message.topic}"
                )

            message_queue = routing_result.message_queue
            broker_data = routing_result.broker_data

            if not message_queue:
                raise QueueNotAvailableError(
                    f"No available queue for topic: {batch_message.topic}"
                )

            if not broker_data:
                raise BrokerNotAvailableError(
                    f"No available broker data for topic: {batch_message.topic}"
                )

            # 5. 获取Broker地址
            target_broker_addr = routing_result.broker_address
            if not target_broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for: {broker_data.broker_name}"
                )

            logger.debug(
                "Sending async oneway batch message to broker",
                extra={
                    "message_count": len(messages),
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": batch_message.topic,
                },
            )

            # 6. 异步单向发送批量消息到Broker
            await self._batch_send_message_to_broker_oneway_async(
                batch_message, target_broker_addr, message_queue
            )

            # 更新统计（单向发送不计入成功/失败）
            logger.debug(
                "Async oneway batch message sent successfully",
                extra={
                    "message_count": len(messages),
                    "topic": messages[0].topic if messages else "unknown",
                },
            )

        except Exception as e:
            logger.error(
                "Failed to send async oneway batch messages",
                extra={
                    "message_count": len(messages),
                    "topic": messages[0].topic if messages else "unknown",
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(
                f"Async oneway batch message send failed: {e}"
            ) from e

    def _parse_nameserver_addrs(self, namesrv_addr: str) -> dict[str, str]:
        """解析NameServer地址列表

        Args:
            namesrv_addr: NameServer地址，格式为"host1:port1;host2:port2"

        Returns:
            dict[str, str]: 地址字典 {addr: host:port}
        """
        addrs: dict[str, str] = {}
        for addr in namesrv_addr.split(";"):
            addr = addr.strip()
            if addr:
                addrs[addr] = addr
        return addrs

    async def _init_nameserver_connections(self) -> None:
        """异步初始化NameServer连接"""
        logger.info("Initializing async NameServer connections...")

        for addr in self._nameserver_addrs:
            try:
                host: str
                port: str
                host, port = addr.split(":")
                transport_config = TransportConfig(host=host, port=int(port))
                remote_config = RemoteConfig().with_rpc_timeout(
                    self._config.send_msg_timeout
                )
                remote = AsyncRemote(transport_config, remote_config)

                await remote.connect()
                self._nameserver_connections[addr] = remote
                logger.info(
                    "Connected to NameServer",
                    extra={
                        "nameserver_address": addr,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to connect to NameServer",
                    extra={
                        "nameserver_address": addr,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                    exc_info=True,
                )

        if not self._nameserver_connections:
            raise ProducerStartError("No NameServer connections available")

    def _start_background_tasks(self) -> None:
        """启动后台任务"""
        self._background_task = asyncio.create_task(
            self._background_task_loop(),
            name="AsyncProducerBackgroundTask",
        )
        logger.info("Async background tasks started")

    async def _background_task_loop(self) -> None:
        """异步后台任务循环"""
        logger.info("Async background task loop started")

        # 记录各任务的执行时间
        last_route_refresh_time: float = 0
        last_heartbeat_time: float = 0
        loop_count: int = 0

        while not self._shutdown_event.is_set():
            try:
                # 使用asyncio.wait_for来实现超时等待
                _ = await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                break  # 如果收到停止信号就退出
            except asyncio.TimeoutError:
                # 超时表示继续执行后台任务
                pass

            current_time: float = time.time()
            loop_count += 1

            try:
                # 检查是否需要刷新路由信息
                if (
                    current_time - last_route_refresh_time
                    >= self._config.update_topic_route_info_interval / 1000.0
                ):
                    logger.info(
                        "Async refreshing routes",
                        extra={
                            "loop_count": loop_count,
                        },
                    )
                    await self._refresh_all_routes()
                    _ = self._topic_mapping.clear_expired_routes()
                    last_route_refresh_time = current_time

                # 检查是否需要发送心跳
                if (
                    current_time - last_heartbeat_time
                    >= self._config.heartbeat_broker_interval / 1000.0
                ):
                    logger.info(
                        "Async sending heartbeat",
                        extra={
                            "loop_count": loop_count,
                            "time_since_last_heartbeat": round(
                                current_time - last_heartbeat_time, 1
                            ),
                        },
                    )
                    await self._send_heartbeat_to_all_broker_async()
                    last_heartbeat_time = current_time

                # 每10次循环记录一次状态
                if loop_count % 10 == 0:
                    topics: list[str] = list(self._topic_mapping.get_all_topics())
                    logger.debug(
                        "Async background task active",
                        extra={
                            "loop_count": loop_count,
                            "cached_topics_count": len(topics),
                        },
                    )

            except Exception as e:
                logger.error(
                    "Async background task error",
                    extra={
                        "loop_count": loop_count,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                    exc_info=True,
                )

        logger.info("Async background task loop stopped")

    async def _refresh_all_routes(self) -> None:
        """异步刷新所有Topic的路由信息"""
        topics = list(self._topic_mapping.get_all_topics())

        for topic in topics:
            try:
                if self._topic_mapping.get_route_info(topic) is None:
                    _ = await self.update_route_info(topic)
            except Exception as e:
                logger.debug(
                    "Failed to refresh route",
                    extra={
                        "topic": topic,
                        "error": str(e),
                    },
                )

    async def _send_message_to_broker_async(
        self, message: Message, broker_addr: str, message_queue: MessageQueue
    ) -> SendMessageResult:
        """异步发送消息到Broker

        Args:
            message: 消息对象
            broker_addr: Broker地址
            message_queue: 消息队列

        Returns:
            SendResult: 发送结果
        """

        async with self._broker_manager.connection(broker_addr) as broker_remote:
            return await AsyncBrokerClient(broker_remote).async_send_message(
                self._config.producer_group,
                message.body,
                message_queue,
                message.properties,
                flag=message.flag,
            )

    async def _send_message_to_broker_oneway_async(
        self, message: Message, broker_addr: str, message_queue: MessageQueue
    ) -> None:
        """异步单向发送消息到Broker

        发送消息但不等待响应，适用于对可靠性要求不高的场景。

        Args:
            message: 消息对象
            broker_addr: Broker地址
            message_queue: 消息队列
        """
        async with self._broker_manager.connection(broker_addr) as broker_remote:
            await AsyncBrokerClient(broker_remote).async_oneway_message(
                self._config.producer_group,
                message.body,
                message_queue,
                message.properties,
                flag=message.flag,
            )

    async def _batch_send_message_to_broker_async(
        self,
        batch_message: Message,
        broker_addr: str,
        message_queue: MessageQueue,
    ) -> SendMessageResult:
        """异步批量发送消息到Broker

        Args:
            batch_message: 批量消息对象（已编码）
            broker_addr: Broker地址
            message_queue: 消息队列

        Returns:
            SendMessageResult: 发送结果
        """
        async with self._broker_manager.connection(broker_addr) as broker_remote:
            return await AsyncBrokerClient(broker_remote).async_batch_send_message(
                self._config.producer_group,
                batch_message.body,
                message_queue,
                batch_message.properties,
                flag=batch_message.flag,
            )

    async def _batch_send_message_to_broker_oneway_async(
        self,
        batch_message: Message,
        broker_addr: str,
        message_queue: MessageQueue,
    ) -> None:
        """异步单向批量发送消息到Broker

        发送批量消息但不等待响应，适用于对可靠性要求不高但追求高吞吐量的场景。

        Args:
            batch_message: 批量消息对象（已编码）
            broker_addr: Broker地址
            message_queue: 消息队列
        """
        async with self._broker_manager.connection(broker_addr) as broker_remote:
            await AsyncBrokerClient(broker_remote).async_batch_oneway_message(
                self._config.producer_group,
                batch_message.body,
                message_queue,
                batch_message.properties,
                flag=batch_message.flag,
            )

    def _check_running(self) -> None:
        """检查Producer是否处于运行状态

        Raises:
            ProducerStateError: 当Producer未运行时
        """
        if not self._running:
            raise ProducerStateError(
                "AsyncProducer is not running. Call start() first."
            )

    async def _stop_background_tasks(self) -> None:
        """停止后台任务"""
        if self._background_task and not self._background_task.done():
            logger.info("Stopping async background tasks...")
            self._shutdown_event.set()

            try:
                await asyncio.wait_for(self._background_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Async background task did not stop gracefully")
                _ = self._background_task.cancel()
                try:
                    await self._background_task
                except asyncio.CancelledError:
                    pass

            logger.info("Async background tasks stopped")

    async def _send_heartbeat_to_all_broker_async(self) -> None:
        """异步向所有Broker发送心跳"""
        logger.debug("Sending async heartbeat to all brokers...")

        try:
            # 获取所有已知的Broker地址
            broker_addrs: set[str] = set()
            all_topics: set[str] = self._topic_mapping.get_all_topics()

            for topic in all_topics:
                route_info = self._topic_mapping.get_route_info(topic)
                if route_info:
                    for broker_data in route_info.broker_data_list:
                        # 获取主从地址
                        if broker_data.broker_addresses:
                            for addr in broker_data.broker_addresses.values():
                                if addr:  # 过滤空地址
                                    broker_addrs.add(addr)

            if not broker_addrs:
                logger.debug("No broker addresses found for heartbeat")
                return

            # 创建心跳数据
            if not self._config.client_id:
                logger.error("Client ID not set for heartbeat")
                return

            heartbeat_data = HeartbeatData(
                client_id=self._config.client_id,
                producer_data_set=[
                    ProducerData(group_name=self._config.producer_group)
                ],
            )

            # 统计结果
            success_count: int = 0
            failed_count: int = 0

            # 向每个Broker发送心跳
            for broker_addr in broker_addrs:
                try:
                    # 获取或创建Broker连接
                    async with self._broker_manager.connection(
                        broker_addr
                    ) as broker_remote:
                        await AsyncBrokerClient(broker_remote).send_heartbeat(
                            heartbeat_data
                        )
                        success_count += 1
                        logger.debug(
                            "Heartbeat sent to broker",
                            extra={
                                "broker_address": broker_addr,
                            },
                        )

                except Exception as e:
                    failed_count += 1
                    logger.debug(
                        "Failed to send heartbeat to broker",
                        extra={
                            "broker_address": broker_addr,
                            "error": str(e),
                        },
                    )

            if success_count > 0 or failed_count > 0:
                logger.info(
                    f"Async heartbeat sent: {success_count} succeeded, {failed_count} failed"
                )

        except Exception as e:
            logger.error(
                "Error sending async heartbeat to brokers",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

    async def _close_nameserver_connections(self) -> None:
        """异步关闭所有NameServer连接"""
        logger.info("Closing async NameServer connections...")

        if not self._nameserver_connections:
            logger.debug("No NameServer connections to close")
            return

        closed_count: int = 0
        failed_count: int = 0

        # 关闭所有连接
        for addr, remote in list(self._nameserver_connections.items()):
            try:
                if hasattr(remote, "close"):
                    await remote.close()
                    closed_count += 1
                    logger.debug(
                        "Closed NameServer connection",
                        extra={
                            "nameserver_address": addr,
                        },
                    )
                else:
                    logger.warning(
                        f"AsyncRemote object for {addr} does not have close() method"
                    )
                    failed_count += 1
            except Exception as e:
                logger.error(
                    "Failed to close NameServer connection",
                    extra={
                        "nameserver_address": addr,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                    exc_info=True,
                )
                failed_count += 1

        # 清空连接字典
        self._nameserver_connections.clear()

        logger.info(
            "NameServer connections closed",
            extra={
                "closed_count": closed_count,
                "failed_count": failed_count,
                "total_connections": closed_count + failed_count,
            },
        )

    def get_stats(self) -> dict[str, str | int | bool | None]:
        """获取Producer统计信息

        Returns:
            dict: 统计信息
        """
        success_rate: float = (
            self._total_sent / (self._total_sent + self._total_failed)
            if (self._total_sent + self._total_failed) > 0
            else 0.0
        )

        return {
            "running": self._running,
            "producer_group": self._config.producer_group,
            "client_id": self._config.client_id,
            "total_sent": self._total_sent,
            "total_failed": self._total_failed,
            "success_rate": f"{success_rate:.2%}",
            "cached_topics": len(self._topic_mapping.get_all_topics()),
        }

    async def update_route_info(self, topic: str) -> bool:
        """异步更新Topic路由信息

        手动触发Topic路由信息的更新。通常情况下，路由信息会自动更新，
        但在某些特殊场景下可能需要手动触发更新。

        Args:
            topic: 要更新的Topic名称

        Returns:
            bool: 更新是否成功
        """
        logger.info(
            "Updating async route info for topic",
            extra={
                "topic": topic,
            },
        )

        for addr, remote in self._nameserver_connections.items():
            try:
                # 使用异步NameServer客户端查询路由信息
                client = AsyncNameServerClient(
                    remote, self._config.send_msg_timeout / 1000.0
                )

                # 查询Topic路由信息
                topic_route_data = await client.query_topic_route_info(topic)

                # 维护broker连接
                for broker_data in topic_route_data.broker_data_list:
                    for idx, broker_addr in broker_data.broker_addresses.items():
                        logger.info(
                            "Adding broker",
                            extra={
                                "broker_id": idx,
                                "broker_address": broker_addr,
                                "broker_name": broker_data.broker_name,
                            },
                        )
                        await self._broker_manager.add_broker(
                            broker_addr,
                            broker_data.broker_name,
                        )

                # 更新本地缓存
                success: bool = self._topic_mapping.update_route_info(
                    topic, topic_route_data
                )

                if success:
                    logger.info(
                        "Route info updated for topic",
                        extra={
                            "topic": topic,
                            "nameserver_address": addr,
                            "brokers_count": len(topic_route_data.broker_data_list),
                        },
                    )
                    return True
                else:
                    logger.warning(
                        "Failed to update route cache for topic",
                        extra={
                            "topic": topic,
                            "nameserver_address": addr,
                        },
                    )

            except Exception as e:
                logger.error(
                    "Failed to get async route info from NameServer",
                    extra={
                        "nameserver_address": addr,
                        "topic": topic,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                    exc_info=True,
                )

        # 如果所有NameServer都失败，强制刷新缓存
        return self._topic_mapping.force_refresh(topic)

    def is_running(self) -> bool:
        """检查Producer是否正在运行

        Returns:
            bool: 运行状态
        """
        return self._running

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"AsyncProducer(group={self._config.producer_group}, "
            f"running={self._running}, "
            f"sent={self._total_sent}, "
            f"failed={self._total_failed})"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


# 便捷函数
def create_async_producer(
    producer_group: str = "DEFAULT_PRODUCER",
    namesrv_addr: str = "localhost:9876",
    **kwargs,
) -> AsyncProducer:
    """创建AsyncProducer实例的便捷函数

    提供简化的接口来创建异步Producer实例，特别适合高并发和异步编程场景。
    该函数封装了ProducerConfig的创建过程，让你能够用最少的代码启动一个功能完整的异步RocketMQ生产者。

    ========== 功能特性 ==========

    - 🚀 **异步高并发**: 基于asyncio实现，支持高并发消息发送
    - 📡 **自动连接**: 异步建立与NameServer的连接池
    - 🔄 **后台服务**: 自动启动异步路由刷新和心跳发送任务
    - ⚙️ **灵活配置**: 支持所有ProducerConfig的配置参数
    - 🛡️ **异常安全**: 异步操作中的完整异常处理机制
    - 🔧 **生产就绪**: 支持重试、超时、批量发送等生产环境特性
    - 📊 **性能监控**: 内置异步统计和监控功能

    ========== 与同步Producer的区别 ==========

    | 特性 | AsyncProducer | Producer |
    |------|-------------|----------|
    | 编程模型 | async/await | 同步阻塞 |
    | 并发性能 | 高并发 | 单线程 |
    | 资源占用 | 更低连接开销 | 更高连接开销 |
    | 适用场景 | 高并发、低延迟 | 简单场景、批处理 |
    | 学习成本 | 需要异步编程知识 | 简单易用 |

    ========== 参数详解 ==========

    Args:
        producer_group (str): 生产者组名
            - 同一组的生产者被视为同一应用的不同实例
            - 用于事务消息和消费端的消息去重
            - 默认值: "DEFAULT_PRODUCER"
            - 建议格式: "应用名_功能名_ASYNC_PRODUCER"，如 "ORDER_SERVICE_ASYNC_PRODUCER"

        namesrv_addr (str): NameServer服务器地址
            - RocketMQ的注册发现中心地址
            - 支持单个地址或多个地址（用分号分隔）
            - 格式: "host:port" 或 "host1:port1;host2:port2"
            - 默认值: "localhost:9876"
            - 生产环境建议配置多个NameServer实现高可用

        **kwargs: 其他ProducerConfig配置参数
            所有ProducerConfig支持的参数都可以通过kwargs传递，重要的异步相关参数包括：

            - send_message_timeout (int): 消息发送超时时间，默认3000ms
            - retry_times (int): 发送失败重试次数，默认2次
            - async_send_semaphore (int): 异步发送信号量大小，默认10000
            - routing_strategy (str): 路由策略，默认"round_robin"
            - compress_msg_body_over_howmuch (int): 消息体压缩阈值，默认4MB
            - max_message_size (int): 最大消息体大小，默认4MB

    ========== 返回值 ==========

    Returns:
        AsyncProducer: 配置完成的异步Producer实例
            - 实例已初始化但未启动，需要调用await producer.start()
            - 包含完整的异步消息发送功能（发送、批量发送、单向发送）
            - 内置异步连接池管理和路由缓存机制
            - 支持高并发消息发送和异步结果处理

    ========== 使用示例 ==========

    基础异步使用示例：

    >>> import asyncio
    >>> from pyrocketmq.producer import create_async_producer
    >>> from pyrocketmq.model.message import Message
    >>>
    >>> async def basic_example():
    >>>     # 1. 创建默认异步Producer
    >>>     producer = create_async_producer()
    >>>     await producer.start()
    >>>
    >>>     # 2. 异步发送消息
    >>>     message = Message(topic="test_topic", body=b"Hello, Async RocketMQ!")
    >>>     result = await producer.send(message)
    >>>     print(f"消息发送成功，ID: {result.msg_id}")
    >>>
    >>>     # 3. 关闭Producer
    >>>     await producer.shutdown()
    >>>
    >>> # 运行异步示例
    >>> asyncio.run(basic_example())

    高并发异步发送示例：

    >>> async def high_concurrency_example():
    >>>     # 创建高性能异步Producer
    >>>     producer = create_async_producer(
    >>>         producer_group="HIGH_CONCURRENCY_PRODUCER",
    >>>         namesrv_addr="192.168.1.100:9876;192.168.1.101:9876",
    >>>         async_send_semaphore=50000,  # 更大的并发信号量
    >>>         send_message_timeout=5000,   # 5秒超时
    >>>         retry_times=3,               # 更多重试
    >>>         routing_strategy="random",   # 随机路由策略
    >>>     )
    >>>     await producer.start()
    >>>
    >>>     # 创建大量异步发送任务
    >>>     tasks = []
    >>>     for i in range(1000):
    >>>         message = Message(
    >>>             topic="high_concurrency_topic",
    >>>             body=f"message_{i}".encode()
    >>>         )
    >>>         tasks.append(producer.send(message))
    >>>
    >>>     # 并发执行所有发送任务
    >>>     results = await asyncio.gather(*tasks)
    >>>     success_count = sum(1 for r in results if r.is_success)
    >>>     print(f"成功发送: {success_count}/1000")
    >>>
    >>>     await producer.shutdown()

    异步批量发送示例：

    >>> async def batch_send_example():
    >>>     producer = create_async_producer(
    >>>         producer_group="BATCH_ASYNC_PRODUCER",
    >>>         compress_msg_body_over_howmuch=1024,  # 1KB以上压缩
    >>>         batch_size=32,  # 批量大小
    >>>     )
    >>>     await producer.start()
    >>>
    >>>     # 创建多个消息
    >>>     messages = [
    >>>         Message(topic="batch_topic", body=f"data_{i}".encode())
    >>>         for i in range(100)
    >>>     ]
    >>>
    >>>     # 异步批量发送
    >>>     result = await producer.send_batch(*messages)
    >>>     print(f"批量发送完成，成功: {result.is_success}")
    >>>
    >>>     await producer.shutdown()

    异步上下文管理器使用：

    >>> async def context_manager_example():
    >>>     async with create_async_producer("SERVICE_PRODUCER") as producer:
    >>>         # 自动启动和关闭
    >>>         message = Message(topic="service_topic", body=b"service data")
    >>>         result = await producer.send(message)
    >>>         print(f"服务消息发送: {result.is_success}")

    ========== 最佳实践 ==========

    🎯 **异步编程最佳实践**:

    1. **合理设置并发数量**:
       >>> producer = create_async_producer(
       ...     async_send_semaphore=10000,  # 根据系统资源调整
       ... )

    2. **使用批量发送提升性能**:
       >>> # 批量发送减少网络调用
       >>> await producer.send_batch(*messages)

    3. **异常处理和重试**:
       >>> try:
       ...     result = await producer.send(message)
       ... except Exception as e:
       ...     logger.error(f"发送失败: {e}")

    4. **连接池管理**:
       >>> # 生产环境使用多个NameServer
       >>> producer = create_async_producer(
       ...     namesrv_addr="host1:9876;host2:9876;host3:9876"
       ... )

    🚀 **性能优化建议**:

    - 调整 `async_send_semaphore` 控制并发数量
    - 使用 `routing_strategy="random"` 提升性能
    - 启用消息压缩减少网络传输
    - 使用批量发送提高吞吐量

    ========== 常见问题 ==========

    Q: 什么时候应该使用AsyncProducer而不是Producer？
    A: 当需要处理大量并发消息发送，或者应用本身就是异步架构时，
       AsyncProducer能提供更好的性能和资源利用率。

    Q: 如何设置合适的async_send_semaphore？
    A: 根据系统资源和Broker的承受能力来设置，通常10000-50000比较合适，
       可以通过压测找到最佳值。

    Q: 异步发送失败如何处理？
    A: AsyncProducer内置重试机制，也可以在应用层实现重试逻辑，
       建议结合死信队列处理最终失败的消息。

    ========== 注意事项 ==========

    - 需要在asyncio事件循环中运行
    - Producer实例不是线程安全的，多线程使用时请确保每个线程使用独立的实例
    - 调用await producer.start()后才能发送消息
    - 使用完毕后记得调用await producer.shutdown()释放资源
    - 高并发时注意监控Broker的负载情况
    - 异步编程需要处理好异常和取消操作

    ========== 异常处理 ==========

    函数可能抛出的异常：
    - TypeError: 当producer_group或namesrv_addr参数类型错误时
    - ValueError: 当配置参数值不合法时
    - 所有ProducerConfig.__init__可能抛出的异常

    ========== 参见 ==========

    - AsyncProducer: 异步生产者核心实现类
    - ProducerConfig: 生产者配置类
    - Message: 消息数据结构
    - create_producer: 同步生产者创建函数
    """
    config = ProducerConfig(
        producer_group=producer_group,
        namesrv_addr=namesrv_addr,
        **kwargs,
    )
    return AsyncProducer(config)
