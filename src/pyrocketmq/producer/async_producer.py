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
from typing import Dict, Optional

from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.broker.async_client import AsyncBrokerClient
from pyrocketmq.logging import get_logger
from pyrocketmq.model import HeartbeatData, ProducerData, SendMessageResult
from pyrocketmq.model.enums import ResponseCode
from pyrocketmq.model.factory import RemotingRequestFactory
from pyrocketmq.model.message import Message, encode_batch
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import TopicRouteData
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
from pyrocketmq.producer.router import MessageRouter
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping
from pyrocketmq.producer.utils import validate_message
from pyrocketmq.remote.async_remote import AsyncRemote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig

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

    def __init__(self, config: Optional[ProducerConfig] = None):
        """初始化AsyncProducer实例

        Args:
            config: Producer配置，如果为None则使用默认配置
        """
        # 配置管理
        self._config = config or ProducerConfig()

        # 简化的状态管理 (MVP设计)
        self._running = False

        # 核心组件
        self._topic_mapping = TopicBrokerMapping(
            route_timeout=self._config.update_topic_route_info_interval / 1000.0
        )

        # 消息路由器
        self._message_router = MessageRouter(topic_mapping=self._topic_mapping)

        # 基础状态统计
        self._total_sent = 0
        self._total_failed = 0

        # NameServer连接管理（仅用于路由查询）
        self._nameserver_connections: Dict[str, AsyncRemote] = {}
        self._nameserver_addrs = self._parse_nameserver_addrs(self._config.namesrv_addr)

        # Broker管理器（使用异步连接池管理）
        transport_config = TransportConfig(
            host="localhost",  # 默认值，会被Broker覆盖
            port=10911,  # 默认值，会被Broker覆盖
        )
        remote_config = RemoteConfig(
            rpc_timeout=self._config.send_msg_timeout / 1000.0,
        )
        self._broker_manager = AsyncBrokerManager(
            remote_config=remote_config,
            transport_config=transport_config,
        )

        # 后台任务管理
        self._background_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        logger.info(
            f"AsyncProducer initialized with config: {self._config.producer_group}, "
            f"NameServer addrs: {self._nameserver_addrs}"
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
                f"AsyncProducer started successfully. Group: {self._config.producer_group}, "
                f"Client ID: {self._config.client_id}"
            )

        except Exception as e:
            logger.error(f"Failed to start async producer: {e}")
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
                f"AsyncProducer shutdown completed. Total sent: {self._total_sent}, "
                f"Total failed: {self._total_failed}"
            )

        except Exception as e:
            logger.error(f"Error during async producer shutdown: {e}")

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
                await self.update_route_info(message.topic)

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
                f"Sending message async to {target_broker_addr}, queue: {message_queue.full_name}"
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
            logger.error(f"Failed to send async message: {e}")

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
                f"Encoded {len(messages)} messages into async batch message, "
                f"batch size: {len(batch_message.body)} bytes"
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
                f"Sending async batch message ({len(messages)} messages) to {target_broker_addr}, "
                f"queue: {message_queue.full_name}"
            )

            # 5. 发送批量消息到Broker
            send_result = await self._batch_send_message_to_broker_async(
                batch_message, target_broker_addr, message_queue
            )

            if send_result.is_success:
                self._total_sent += len(messages)
                logger.info(
                    f"Async batch send success: {len(messages)} messages to topic {batch_message.topic}"
                )
                return send_result
            else:
                self._total_failed += len(messages)
                return send_result

        except Exception as e:
            self._total_failed += len(messages)
            logger.error(f"Failed to send async batch messages: {e}")

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
                await self.update_route_info(message.topic)

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
                f"Sending async oneway message to {target_broker_addr}, queue: {message_queue.full_name}"
            )

            # 5. 发送消息到Broker
            await self._send_message_to_broker_oneway_async(
                message, target_broker_addr, message_queue
            )

            # 更新统计（单向发送不计入成功/失败）
            logger.debug("Async oneway message sent successfully")

        except Exception as e:
            logger.error(f"Failed to send async oneway message: {e}")

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
                f"Encoded {len(messages)} messages into batch message, "
                f"batch size: {len(batch_message.body)} bytes"
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

            # 5. 获取Broker地址
            target_broker_addr = routing_result.broker_address
            if not target_broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for: {broker_data.broker_name}"
                )

            logger.debug(
                f"Sending async oneway batch message ({len(messages)} messages) to {target_broker_addr}, "
                f"queue: {message_queue.full_name}"
            )

            # 6. 异步单向发送批量消息到Broker
            await self._batch_send_message_to_broker_oneway_async(
                batch_message, target_broker_addr, message_queue
            )

            # 更新统计（单向发送不计入成功/失败）
            logger.debug(
                f"Async oneway batch message sent successfully: {len(messages)} messages"
            )

        except Exception as e:
            logger.error(f"Failed to send async oneway batch messages: {e}")

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(
                f"Async oneway batch message send failed: {e}"
            ) from e

    def _parse_nameserver_addrs(self, namesrv_addr: str) -> Dict[str, str]:
        """解析NameServer地址列表

        Args:
            namesrv_addr: NameServer地址，格式为"host1:port1;host2:port2"

        Returns:
            Dict[str, str]: 地址字典 {addr: host:port}
        """
        addrs = {}
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
                host, port = addr.split(":")
                transport_config = TransportConfig(host=host, port=int(port))
                remote_config = RemoteConfig().with_rpc_timeout(
                    self._config.send_msg_timeout
                )
                remote = AsyncRemote(transport_config, remote_config)

                await remote.connect()
                self._nameserver_connections[addr] = remote
                logger.info(f"Connected to NameServer: {addr}")

            except Exception as e:
                logger.error(f"Failed to connect to NameServer {addr}: {e}")

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
        last_route_refresh_time = 0
        last_heartbeat_time = 0

        while not self._shutdown_event.is_set():
            try:
                # 使用asyncio.wait_for来实现超时等待
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                break  # 如果收到停止信号就退出
            except asyncio.TimeoutError:
                # 超时表示继续执行后台任务
                pass

            current_time = time.time()

            try:
                # 检查是否需要刷新路由信息
                if (
                    current_time - last_route_refresh_time
                    >= self._config.update_topic_route_info_interval / 1000.0
                ):
                    await self._refresh_all_routes()
                    self._topic_mapping.clear_expired_routes()
                    last_route_refresh_time = current_time

                # 检查是否需要发送心跳
                if (
                    current_time - last_heartbeat_time
                    >= self._config.heartbeat_broker_interval / 1000.0
                ):
                    await self.send_heartbeat_to_all_broker_async()
                    last_heartbeat_time = current_time

            except Exception as e:
                logger.error(f"Async background task error: {e}")

        logger.info("Async background task loop stopped")

    async def _refresh_all_routes(self) -> None:
        """异步刷新所有Topic的路由信息"""
        topics = list(self._topic_mapping.get_all_topics())

        for topic in topics:
            try:
                if self._topic_mapping.get_route_info(topic) is None:
                    await self.update_route_info(topic)
            except Exception as e:
                logger.debug(f"Failed to refresh route for {topic}: {e}")

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
                self._config.producer_group, message.body, message_queue
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
                self._config.producer_group, message.body, message_queue
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
                self._config.producer_group, batch_message.body, message_queue
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
                self._config.producer_group, batch_message.body, message_queue
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
                self._background_task.cancel()
                try:
                    await self._background_task
                except asyncio.CancelledError:
                    pass

            logger.info("Async background tasks stopped")

    async def send_heartbeat_to_all_broker_async(self) -> None:
        """异步向所有Broker发送心跳"""
        logger.debug("Sending async heartbeat to all brokers...")

        try:
            # 获取所有已知的Broker地址
            broker_addrs = set()
            all_topics = self._topic_mapping.get_all_topics()

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

            # 创建心跳请求
            heartbeat_request = RemotingRequestFactory.create_heartbeat_request(
                heartbeat_data
            )

            # 统计结果
            success_count = 0
            failed_count = 0

            # 向每个Broker发送心跳
            for broker_addr in broker_addrs:
                try:
                    # 获取或创建Broker连接
                    async with self._broker_manager.connection(
                        broker_addr
                    ) as broker_remote:
                        # 发送单向心跳请求（不等待响应）
                        await broker_remote.oneway(heartbeat_request)
                        success_count += 1
                        logger.debug(f"Heartbeat sent to broker: {broker_addr}")

                except Exception as e:
                    failed_count += 1
                    logger.debug(f"Failed to send heartbeat to {broker_addr}: {e}")

            if success_count > 0 or failed_count > 0:
                logger.debug(
                    f"Heartbeat sent: {success_count} succeeded, {failed_count} failed"
                )

        except Exception as e:
            logger.error(f"Error sending async heartbeat to brokers: {e}")

    async def _close_nameserver_connections(self) -> None:
        """异步关闭所有NameServer连接"""
        logger.info("Closing async NameServer connections...")

        if not self._nameserver_connections:
            logger.debug("No NameServer connections to close")
            return

        closed_count = 0
        failed_count = 0

        # 关闭所有连接
        for addr, remote in list(self._nameserver_connections.items()):
            try:
                if hasattr(remote, "close"):
                    await remote.close()
                    closed_count += 1
                    logger.debug(f"Closed NameServer connection: {addr}")
                else:
                    logger.warning(
                        f"AsyncRemote object for {addr} does not have close() method"
                    )
                    failed_count += 1
            except Exception as e:
                logger.error(f"Failed to close NameServer connection {addr}: {e}")
                failed_count += 1

        # 清空连接字典
        self._nameserver_connections.clear()

        logger.info(
            f"NameServer connections closed: {closed_count} succeeded, "
            f"{failed_count} failed"
        )

    def get_stats(self) -> dict:
        """获取Producer统计信息

        Returns:
            dict: 统计信息
        """
        success_rate = (
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
        logger.info(f"Updating async route info for topic: {topic}")

        for addr, remote in self._nameserver_connections.items():
            try:
                # 创建获取路由信息的请求
                request = RemotingRequestFactory.create_get_route_info_request(topic)

                # 发送请求
                response = await remote.rpc(
                    request, self._config.send_msg_timeout / 1000.0
                )

                if response and response.code == ResponseCode.SUCCESS:
                    if not response.body:
                        logger.warning(f"Empty response body from {addr}")
                        return False

                    # 解析路由数据
                    topic_route_data = TopicRouteData.from_bytes(response.body)

                    # 维护broker连接
                    for broker_data in topic_route_data.broker_data_list:
                        for (
                            idx,
                            broker_addr,
                        ) in broker_data.broker_addresses.items():
                            logger.info(f"Adding broker {idx} {broker_addr}")
                            await self._broker_manager.add_broker(
                                broker_addr,
                                broker_data.broker_name,
                            )

                    # 更新本地缓存
                    success = self._topic_mapping.update_route_info(
                        topic, topic_route_data
                    )

                    if success:
                        logger.info(f"Route info updated for topic {topic} from {addr}")

                        return True
                    else:
                        logger.warning(
                            f"Failed to update route cache for topic {topic}"
                        )

                else:
                    logger.warning(
                        f"Get route info failed from {addr}, code: {response.code if response else 'None'}"
                    )

            except Exception as e:
                logger.error(f"Failed to get async route info from {addr}: {e}")

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

    提供简化的接口来创建AsyncProducer实例，特别适合快速开发。

    Args:
        producer_group: 生产者组名，默认为"DEFAULT_PRODUCER"
        namesrv_addr: NameServer地址，默认为"localhost:9876"
        **kwargs: 其他配置参数

    Returns:
        AsyncProducer: AsyncProducer实例

    Example:
        >>> import asyncio
        >>>
        >>> async def main():
        >>>     # 创建默认AsyncProducer
        >>>     producer = create_async_producer()
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
        >>> # 创建自定义AsyncProducer
        >>> producer = create_async_producer(
        >>>     producer_group="my_async_producer",
        >>>     namesrv_addr="192.168.1.100:9876",
        >>>     retry_times=3
        >>> )
    """
    config = ProducerConfig(
        producer_group=producer_group,
        namesrv_addr=namesrv_addr,
        **kwargs,
    )
    return AsyncProducer(config)
