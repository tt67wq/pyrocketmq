"""
RocketMQ Producer核心实现 (MVP版本)

该模块提供了一个简化但功能完整的RocketMQ Producer实现。
采用MVP设计理念，只包含最核心的功能，避免过度设计。

MVP版本功能:
- 简单的布尔状态管理（移除复杂的状态机）
- 同步消息发送
- 单向消息发送
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

import threading
import time
from typing import Any

from pyrocketmq.broker import TopicBrokerMapping

# Local imports - broker
from pyrocketmq.broker.broker_manager import BrokerManager
from pyrocketmq.broker.client import BrokerClient

# Local imports - utilities
from pyrocketmq.logging import get_logger

# Local imports - model
from pyrocketmq.model import HeartbeatData, ProducerData, SendMessageResult
from pyrocketmq.model.message import Message, MessageProperty, encode_batch
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.trace import MessageType, TraceBean, TraceContext, TraceType
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager

# Local imports - producer
from pyrocketmq.producer.config import ProducerConfig
from pyrocketmq.producer.errors import (
    MessageSendError,
    ProducerError,
    ProducerShutdownError,
    ProducerStartError,
    ProducerStateError,
)
from pyrocketmq.producer.utils import validate_message
from pyrocketmq.queue_helper import (
    BrokerNotAvailableError,
    MessageRouter,
    QueueNotAvailableError,
    RouteNotFoundError,
    RoutingStrategy,
)

# Local imports - remote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.pool import ConnectionPool
from pyrocketmq.trace import TraceConfig, TraceDispatcher
from pyrocketmq.transport.config import TransportConfig

logger = get_logger(__name__)


class Producer:
    """
    RocketMQ Producer实现 (MVP版本)

    简化版本的Producer，专注于核心的消息发送功能。
    采用简单的设计模式，避免过度复杂的抽象。

    核心功能:
    1. 生命周期管理 (start/shutdown)
    2. 同步消息发送
    3. 单向消息发送
    4. 基础的错误处理

    状态管理:
    - 使用简单的布尔状态 (_running: bool)
    - 移除复杂的状态机设计
    - 在关键操作前检查运行状态

    使用示例:
        >>> # 创建Producer实例
        >>> producer = Producer()
        >>> producer.start()
        >>>
        >>> # 发送消息
        >>> message = Message(topic="test_topic", body=b"Hello RocketMQ")
        >>> result = producer.send_sync(message)
        >>> print(f"Send result: {result.success}")
        >>>
        >>> # 关闭Producer
        >>> producer.shutdown()
    """

    def __init__(self, config: ProducerConfig | None = None):
        """初始化Producer实例

        Args:
            config: Producer配置，如果为None则使用默认配置
        """
        # 配置管理
        self._config: ProducerConfig = config or ProducerConfig()

        # 简化的状态管理 (MVP设计)
        self._running: bool = False

        # 核心组件
        self._topic_mapping: TopicBrokerMapping = TopicBrokerMapping(
            route_timeout=self._config.route_timeout_seconds
        )

        # 消息路由器
        self._message_router: MessageRouter = MessageRouter(
            topic_mapping=self._topic_mapping,
            default_strategy=RoutingStrategy(self._config.routing_strategy),
        )

        # 基础状态统计
        self._total_sent: int = 0
        self._total_failed: int = 0

        # NameServer连接管理）
        self._nameserver_manager: NameServerManager = create_nameserver_manager(
            self._config.namesrv_addr
        )

        # Broker管理器（使用现有的连接池管理）
        transport_config = TransportConfig(
            host="localhost",  # 默认值，会被Broker覆盖
            port=10911,  # 默认值，会被Broker覆盖
        )
        remote_config = RemoteConfig(
            rpc_timeout=self._config.send_msg_timeout / 1000.0,
        )
        self._broker_manager: BrokerManager = BrokerManager(
            remote_config=remote_config,
            transport_config=transport_config,
        )

        # 后台任务线程
        self._background_thread: threading.Thread | None = None
        self._shutdown_event: threading.Event = threading.Event()

        # Trace dispatcher for message tracing (optional)
        self._trace_dispatcher: TraceDispatcher | None = None
        if self._config.enable_trace:
            trace_config = TraceConfig.create_local_config(
                group_name=self._config.producer_group,
                namesrv_addr=self._config.namesrv_addr,
                trace_topic=self._config.trace_topic or "RMQ_SYS_TRACE_TOPIC",
                max_batch_size=self._config.trace_batch_size or 20,
                max_msg_size=self._config.trace_msg_size or 4 * 1024 * 1024,
            )
            self._trace_dispatcher = TraceDispatcher(trace_config)
            logger.info("Trace dispatcher initialized")

        logger.info("Producer initialized")

    def start(self) -> None:
        """启动Producer

        初始化内部组件并建立连接。这是一个幂等操作，
        多次调用不会产生副作用。

        Raises:
            ProducerStartError: 当启动失败时抛出异常
        """
        if self._running:
            return

        try:
            # 1. 初始化NameServer连接（仅用于路由查询）
            self._init_nameserver_connections()

            # 2. 启动Broker管理器
            self._broker_manager.start()

            # 3. 启动后台任务（路由更新等）
            self._start_background_tasks()

            # 4. 启动 Trace dispatcher（如果启用）
            if self._trace_dispatcher:
                self._trace_dispatcher.start()
                logger.info("Trace dispatcher started")

            # 设置运行状态
            self._running = True

        except Exception as e:
            raise ProducerStartError(f"Producer start failed: {e}") from e

    def shutdown(self) -> None:
        """关闭Producer

        清理资源并断开连接。这是一个幂等操作，
        多次调用不会产生副作用。
        """
        if not self._running:
            return

        try:
            # 设置停止状态
            self._running = False

            # 1. 停止后台任务
            self._stop_background_tasks()

            # 2. 停止 Trace dispatcher（如果启用）
            if self._trace_dispatcher:
                self._trace_dispatcher.stop()
                logger.info("Trace dispatcher stopped")

            # 3. 关闭Broker管理器
            self._broker_manager.shutdown()

            # 4. 关闭NameServer连接
            self._close_nameserver_connections()

        except Exception as e:
            raise ProducerShutdownError(f"Producer shutdown failed: {e}") from e

    def send(self, message: Message) -> SendMessageResult:
        """同步发送消息

        阻塞直到消息发送完成或失败。

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

        # Create trace context for tracking
        start_time = time.time()
        trace_context = None
        if self._trace_dispatcher:
            trace_context = TraceContext(
                trace_type=TraceType.PUB,
                timestamp=int(start_time * 1000),
                region_id="",
                region_name="",
                group_name=self._config.producer_group,
                cost_time=0,
                is_success=True,
                request_id="",
                context_code=0,
                trace_beans=[],
            )

        try:
            # 1. 验证消息
            validate_message(message, self._config.max_message_size)

            # 2. 更新路由信息
            if message.topic not in self._topic_mapping.get_all_topics():
                _ = self.update_route_info(message.topic)

            # 3. 获取队列和Broker
            routing_result = self._message_router.route_message(message.topic, message)
            if not routing_result.success:
                raise RouteNotFoundError(
                    f"Route not found for topic: {message.topic}, error: {routing_result.error}"
                )

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
                "Sending message to broker",
                extra={
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": message.topic,
                },
            )

            # 4. 发送消息到Broker
            send_result = self._send_message_to_broker(
                message, target_broker_addr, message_queue
            )

            if send_result.is_success:
                self._total_sent += 1

                # Add trace information if enabled
                if self._trace_dispatcher and trace_context:
                    end_time = time.time()
                    cost_time = int((end_time - start_time) * 1000)
                    trace_context.cost_time = cost_time
                    trace_context.is_success = True

                    # Create trace bean
                    trace_bean = TraceBean(
                        topic=message.topic,
                        msg_id=send_result.msg_id or "",
                        offset_msg_id=send_result.msg_id or "",
                        tags=message.get_tags() or "",
                        keys=message.get_keys() or "",
                        store_host=target_broker_addr or "",
                        client_host="",
                        store_time=int(end_time * 1000),
                        retry_times=0,
                        body_length=len(message.body),
                        msg_type=MessageType.NORMAL,
                    )
                    trace_context.trace_beans = [trace_bean]

                    # Dispatch trace context
                    self._trace_dispatcher.dispatch(trace_context)

                return send_result
            else:
                self._total_failed += 1

                # Add trace information for failed send
                if self._trace_dispatcher and trace_context:
                    end_time = time.time()
                    cost_time = int((end_time - start_time) * 1000)
                    trace_context.cost_time = cost_time
                    trace_context.is_success = False

                    # Create trace bean for failed send
                    trace_bean = TraceBean(
                        topic=message.topic,
                        msg_id="",
                        offset_msg_id="",
                        tags=message.get_tags() or "",
                        keys=message.get_keys() or "",
                        store_host=target_broker_addr or "",
                        client_host="",
                        store_time=int(end_time * 1000),
                        retry_times=0,
                        body_length=len(message.body),
                        msg_type=MessageType.NORMAL,
                    )
                    trace_context.trace_beans = [trace_bean]

                    # Dispatch trace context
                    self._trace_dispatcher.dispatch(trace_context)

                return send_result

        except Exception as e:
            self._total_failed += 1

            # Add trace information for exception
            if self._trace_dispatcher and trace_context:
                end_time = time.time()
                cost_time = int((end_time - start_time) * 1000)
                trace_context.cost_time = cost_time
                trace_context.is_success = False

                # Create trace bean for exception
                trace_bean = TraceBean(
                    topic=message.topic,
                    msg_id="",
                    offset_msg_id="",
                    tags=message.get_tags() or "",
                    keys=message.get_keys() or "",
                    store_host="",
                    client_host="",
                    store_time=int(end_time * 1000),
                    retry_times=0,
                    body_length=len(message.body),
                    msg_type=MessageType.NORMAL,
                )
                trace_context.trace_beans = [trace_bean]

                # Dispatch trace context
                self._trace_dispatcher.dispatch(trace_context)

            logger.error(
                "Failed to send message",
                extra={
                    "topic": message.topic,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Message send failed: {e}") from e

    def send_batch(self, *messages: Message) -> SendMessageResult:
        """批量发送消息

        将多个消息压缩为一个批量消息进行发送，提高发送效率。

        Args:
            *messages: 要发送的消息列表

        Returns:
            SendMessageResult: 发送结果，包含消息ID、队列信息、发送状态等

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
            ValueError: 当没有提供消息时

        Examples:
            >>> producer = create_producer("group", "nameserver:9876")
            >>> producer.start()
            >>> msg1 = Message(topic="test", body=b"message1")
            >>> msg2 = Message(topic="test", body=b"message2")
            >>> result = producer.send_batch(msg1, msg2)
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
                "Encoded messages into batch message",
                extra={
                    "message_count": len(messages),
                    "batch_size_bytes": len(batch_message.body),
                    "topic": batch_message.topic,
                },
            )

            # 3. 更新路由信息
            if batch_message.topic not in self._topic_mapping.get_all_topics():
                _ = self.update_route_info(batch_message.topic)

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
                "Sending batch message to broker",
                extra={
                    "message_count": len(messages),
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": batch_message.topic,
                },
            )

            # 5. 发送批量消息到Broker
            send_result = self._batch_send_message_to_broker(
                batch_message, target_broker_addr, message_queue
            )

            if send_result.is_success:
                self._total_sent += len(messages)
                logger.info(
                    "Batch send success",
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
                "Failed to send batch messages",
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

            raise MessageSendError(f"Batch message send failed: {e}") from e

    def oneway(self, message: Message) -> None:
        """单向发送消息

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
                _ = self.update_route_info(message.topic)

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

            # 4. 获取Broker地址
            target_broker_addr = routing_result.broker_address
            if not target_broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for: {broker_data.broker_name}"
                )

            logger.debug(
                "Sending oneway message to broker",
                extra={
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": message.topic,
                },
            )

            # 5. 发送消息到Broker
            self._send_message_to_broker_oneway(
                message, target_broker_addr, message_queue
            )

            # 更新统计（单向发送不计入成功/失败）
            logger.debug("Oneway message sent successfully")

        except Exception as e:
            logger.error(
                "Failed to send oneway message",
                extra={
                    "topic": message.topic,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Oneway message send failed: {e}") from e

    def oneway_batch(self, *messages: Message) -> None:
        """单向批量发送消息

        将多个消息压缩为一个批量消息进行单向发送，不等待响应。
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
            >>> producer = create_producer("group", "nameserver:9876")
            >>> producer.start()
            >>> msg1 = Message(topic="test", body=b"message1")
            >>> msg2 = Message(topic="test", body=b"message2")
            >>> producer.oneway_batch(msg1, msg2)  # 不等待响应
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
                "Encoded messages into batch message for oneway batch",
                extra={
                    "message_count": len(messages),
                    "batch_size_bytes": len(batch_message.body),
                    "topic": batch_message.topic,
                },
            )

            # 3. 更新路由信息
            if batch_message.topic not in self._topic_mapping.get_all_topics():
                _ = self.update_route_info(batch_message.topic)

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
                "Sending oneway batch message to broker",
                extra={
                    "message_count": len(messages),
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": batch_message.topic,
                },
            )

            # 6. 单向发送批量消息到Broker
            self._batch_send_message_to_broker_oneway(
                batch_message, target_broker_addr, message_queue
            )

            # 更新统计（单向发送不计入成功/失败）
            logger.debug(
                "Oneway batch message sent successfully",
                extra={
                    "message_count": len(messages),
                    "topic": messages[0].topic if messages else "unknown",
                },
            )

        except Exception as e:
            logger.error(
                "Failed to send oneway batch messages",
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

            raise MessageSendError(f"Oneway batch message send failed: {e}") from e

    def _init_nameserver_connections(self) -> None:
        """初始化NameServer连接"""
        logger.info("Initializing NameServer connections...")
        self._nameserver_manager.start()

    def _start_background_tasks(self) -> None:
        """启动后台任务"""
        self._background_thread = threading.Thread(
            target=self._background_task_loop,
            name="ProducerBackgroundTask",
            daemon=True,
        )
        self._background_thread.start()
        logger.info("Background tasks started")

    def _background_task_loop(self) -> None:
        """后台任务循环"""
        logger.info("Background task loop started")

        # 记录各任务的执行时间
        last_route_refresh_time = 0
        last_heartbeat_time = 0
        loop_count = 0

        while not self._shutdown_event.wait(1.0):  # 每秒检查一次
            current_time = time.time()
            loop_count += 1

            try:
                # 检查是否需要刷新路由信息
                if (
                    current_time - last_route_refresh_time
                    >= self._config.update_topic_route_info_interval / 1000.0
                ):
                    logger.info(
                        "Refreshing routes",
                        extra={
                            "loop_count": loop_count,
                        },
                    )
                    self._refresh_all_routes()
                    _ = self._topic_mapping.clear_expired_routes()
                    last_route_refresh_time = current_time

                # 检查是否需要发送心跳
                if (
                    current_time - last_heartbeat_time
                    >= self._config.heartbeat_broker_interval / 1000.0
                ):
                    logger.info(
                        "Sending heartbeat",
                        extra={
                            "loop_count": loop_count,
                            "time_since_last_heartbeat": round(
                                current_time - last_heartbeat_time, 1
                            ),
                        },
                    )
                    self._send_heartbeat_to_all_broker()
                    last_heartbeat_time = current_time

                # 每10次循环记录一次状态
                if loop_count % 10 == 0:
                    topics = list(self._topic_mapping.get_all_topics())
                    logger.debug(
                        "Background task active",
                        extra={
                            "loop_count": loop_count,
                            "cached_topics_count": len(topics),
                        },
                    )

            except Exception as e:
                logger.error(
                    "Background task error",
                    extra={
                        "loop_count": loop_count,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                    exc_info=True,
                )

        logger.info("Background task loop stopped")

    def _refresh_all_routes(self) -> None:
        """刷新所有Topic的路由信息"""
        topics = list(self._topic_mapping.get_all_topics())

        for topic in topics:
            try:
                _ = self.update_route_info(topic)
            except Exception as e:
                logger.debug(
                    "Failed to refresh route",
                    extra={
                        "topic": topic,
                        "error": str(e),
                    },
                )

    def _send_message_to_broker(
        self, message: Message, broker_addr: str, message_queue: MessageQueue
    ) -> SendMessageResult:
        """发送消息到Broker

        Args:
            message: 消息对象
            broker_addr: Broker地址
            message_queue: 消息队列

        Returns:
            SendResult: 发送结果
        """

        pool: ConnectionPool = self._broker_manager.must_connection_pool(broker_addr)
        with pool.get_connection() as broker_remote:
            return BrokerClient(broker_remote).sync_send_message(
                self._config.producer_group,
                message.body,
                message_queue,
                message.properties,
                flag=message.flag,
            )

    def _send_message_to_broker_oneway(
        self, message: Message, broker_addr: str, message_queue: MessageQueue
    ) -> None:
        """单向发送消息到Broker

        发送消息但不等待响应，适用于对可靠性要求不高的场景。

        Args:
            message: 消息对象
            broker_addr: Broker地址
            message_queue: 消息队列
        """

        pool: ConnectionPool = self._broker_manager.must_connection_pool(broker_addr)
        with pool.get_connection() as broker_remote:
            BrokerClient(broker_remote).oneway_send_message(
                self._config.producer_group,
                message.body,
                message_queue,
                message.properties,
                flag=message.flag,
            )

    def _batch_send_message_to_broker(
        self,
        batch_message: Message,
        broker_addr: str,
        message_queue: MessageQueue,
    ) -> SendMessageResult:
        """批量发送消息到Broker

        Args:
            batch_message: 批量消息对象（已编码）
            broker_addr: Broker地址
            message_queue: 消息队列

        Returns:
            SendMessageResult: 发送结果
        """
        pool: ConnectionPool = self._broker_manager.must_connection_pool(broker_addr)
        with pool.get_connection() as broker_remote:
            return BrokerClient(broker_remote).sync_batch_send_message(
                self._config.producer_group,
                batch_message.body,
                message_queue,
                batch_message.properties,
                flag=batch_message.flag,
            )

    def _batch_send_message_to_broker_oneway(
        self,
        batch_message: Message,
        broker_addr: str,
        message_queue: MessageQueue,
    ) -> None:
        """单向批量发送消息到Broker

        发送批量消息但不等待响应，适用于对可靠性要求不高但追求高吞吐量的场景。

        Args:
            batch_message: 批量消息对象（已编码）
            broker_addr: Broker地址
            message_queue: 消息队列
        """
        pool: ConnectionPool = self._broker_manager.must_connection_pool(broker_addr)
        with pool.get_connection() as broker_remote:
            BrokerClient(broker_remote).oneway_batch_send_message(
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
            raise ProducerStateError("Producer is not running. Call start() first.")

    def _stop_background_tasks(self) -> None:
        """停止后台任务"""
        if self._background_thread and self._background_thread.is_alive():
            logger.info("Stopping background tasks...")
            self._shutdown_event.set()
            self._background_thread.join(timeout=5.0)

            if self._background_thread.is_alive():
                logger.warning("Background task thread did not stop gracefully")
            else:
                logger.info("Background tasks stopped")

    def _send_heartbeat_to_all_broker(self) -> None:
        """向所有Broker发送心跳"""
        logger.debug("Sending heartbeat to all brokers...")

        try:
            # 获取所有已知的Broker地址
            broker_addrs: set[str] = set()
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

            # 统计结果
            success_count = 0
            failed_count = 0

            # 向每个Broker发送心跳
            for broker_addr in broker_addrs:
                try:
                    # 获取或创建Broker连接
                    pool: ConnectionPool = self._broker_manager.must_connection_pool(
                        broker_addr
                    )
                    with pool.get_connection() as broker_remote:
                        BrokerClient(broker_remote).send_heartbeat(heartbeat_data)
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
                    "Heartbeat sent",
                    extra={
                        "success_count": success_count,
                        "failed_count": failed_count,
                        "total_brokers": success_count + failed_count,
                    },
                )

        except Exception as e:
            logger.error(
                "Error sending heartbeat to brokers",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )

    def _close_nameserver_connections(self) -> None:
        """关闭所有NameServer连接"""
        logger.info("Closing NameServer connections...")
        self._nameserver_manager.stop()

    def get_stats(self) -> dict[str, str | int | bool | None]:
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

    def update_route_info(self, topic: str) -> bool:
        """更新Topic路由信息

        手动触发Topic路由信息的更新。通常情况下，路由信息会自动更新，
        但在某些特殊场景下可能需要手动触发更新。

        Args:
            topic: 要更新的Topic名称

        Returns:
            bool: 更新是否成功
        """
        logger.info(
            "Updating route info for topic",
            extra={
                "topic": topic,
            },
        )
        topic_route_data = self._nameserver_manager.get_topic_route(topic)
        if not topic_route_data:
            logger.error(
                "Failed to get topic route data",
                extra={
                    "topic": topic,
                },
            )
            return False

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
                self._broker_manager.add_broker(
                    broker_addr,
                    broker_data.broker_name,
                )

        # 更新本地缓存
        success = self._topic_mapping.update_route_info(topic, topic_route_data)
        if success:
            logger.info(
                "Route info updated for topic",
                extra={
                    "topic": topic,
                    "brokers_count": len(topic_route_data.broker_data_list),
                },
            )
            return True
        else:
            logger.warning(
                "Failed to update route cache for topic",
                extra={"topic": topic},
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
            f"Producer(group={self._config.producer_group}, "
            f"running={self._running}, "
            f"sent={self._total_sent}, "
            f"failed={self._total_failed})"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


# 便捷函数
def create_producer(
    producer_group: str = "DEFAULT_PRODUCER",
    namesrv_addr: str = "localhost:9876",
    **kwargs: Any,
) -> Producer:
    """创建Producer实例的便捷函数

    提供简化的接口来创建Producer实例，特别适合快速开发和原型验证。
    该函数封装了ProducerConfig的创建过程，让你能够用最少的代码启动一个功能完整的RocketMQ生产者。

    ========== 功能特性 ==========

    - 🚀 **快速启动**: 一行代码即可创建生产者实例
    - 📡 **自动连接**: 自动建立与NameServer的连接
    - 🔄 **后台服务**: 自动启动路由刷新和心跳发送等后台任务
    - ⚙️ **灵活配置**: 支持所有ProducerConfig的配置参数
    - 🛡️ **异常安全**: 配置错误时提供清晰的错误信息
    - 🔧 **生产就绪**: 支持重试、超时等生产环境必需的特性

    ========== 与AsyncProducer的对比 ==========

    | 特性 | Producer (同步) | AsyncProducer (异步) |
    |------|----------------|-------------------|
    | 编程模型 | 同步阻塞 | async/await |
    | 使用复杂度 | 简单易用 | 需要异步编程知识 |
    | 并发性能 | 单线程顺序处理 | 高并发处理 |
    | 资源占用 | 较高连接开销 | 更低连接开销 |
    | 适用场景 | 简单应用、批处理 | 高并发、低延迟应用 |
    | 学习成本 | 低 | 中等 |
    | 调试难度 | 容易 | 需要异步调试技巧 |

    **选择建议**:
    - 🟢 **选择Producer**: 如果你是RocketMQ新手、应用简单、或主要进行批处理操作
    - 🟡 **选择AsyncProducer**: 如果你的应用已经是异步架构、需要高并发消息发送、或对延迟有严格要求

    ========== 参数详解 ==========

    Args:
        producer_group (str): 生产者组名
            - 同一组的生产者被视为同一应用的不同实例
            - 用于事务消息和消费端的消息去重
            - 默认值: "DEFAULT_PRODUCER"
            - 建议格式: "应用名_功能名_PRODUCER"，如 "ORDER_SERVICE_PRODUCER"

        namesrv_addr (str): NameServer服务器地址
            - RocketMQ的注册发现中心地址
            - 支持单个地址或多个地址（用分号分隔）
            - 格式: "host:port" 或 "host1:port1;host2:port2"
            - 默认值: "localhost:9876"
            - 生产环境建议配置多个NameServer实现高可用

        **kwargs: 其他ProducerConfig配置参数
            所有ProducerConfig支持的参数都可以通过kwargs传递，包括但不限于：

            - send_message_timeout (int): 消息发送超时时间，默认3000ms
            - retry_times (int): 发送失败重试次数，默认2次
            - compress_msg_body_over_howmuch (int): 消息体压缩阈值，默认4096字节
            - compress_level (int): 压缩级别，默认5（0-9，越大压缩率越高但CPU消耗越多）
            - max_message_size (int): 最大消息体大小，默认4MB
            - retry_another_broker_when_not_store_ok (bool): 存储失败时是否重试其他broker，默认False

    ========== 返回值 ==========

    Returns:
        Producer: 配置完成的Producer实例
            - 实例已初始化但未启动，需要调用producer.start()启动
            - 包含完整的消息发送功能（同步、异步、单向）
            - 支持批量消息和事务消息（需要相应的Producer实例）
            - 内置连接池管理和路由缓存机制

    ========== 使用示例 ==========

    基础使用示例：

    >>> from pyrocketmq.producer import create_producer
    >>> from pyrocketmq.model.message import Message
    >>>
    >>> # 1. 创建默认配置的生产者
    >>> producer = create_producer()
    >>> producer.start()
    >>>
    >>> # 2. 发送简单消息
    >>> message = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    >>> result = producer.send(message)
    >>> print(f"消息发送成功，ID: {result.msg_id}")
    >>>
    >>> producer.shutdown()

    自定义配置示例：

    >>> # 3. 创建自定义配置的生产者
    >>> producer = create_producer(
    ...     producer_group="ORDER_SERVICE_PRODUCER",
    ...     namesrv_addr="192.168.1.100:9876;192.168.1.101:9876",
    ...     send_message_timeout=5000,
    ...     retry_times=3,
    ...     compress_msg_body_over_howmuch=1024,  # 1KB以上压缩
    ...     compress_level=6,
    ...     max_message_size=8 * 1024 * 1024,     # 8MB
    ... )
    >>> producer.start()
    >>>
    >>> # 4. 发送带属性的消息
    >>> message = Message(
    ...     topic="order_topic",
    ...     body=b'{"order_id": "12345", "amount": 99.99}',
    ...     flag=0
    ... )
    >>> message.set_property("order_type", "electronic")
    >>> message.set_property("priority", "high")
    >>>
    >>> result = producer.send(message)
    >>> print(f"订单消息发送成功，队列ID: {result.queue_id}")
    >>>
    >>> producer.shutdown()

    生产环境最佳实践：

    >>> # 5. 生产环境推荐配置
    >>> producer = create_producer(
    ...     producer_group=f"{APP_NAME}_PRODUCER",  # 使用环境变量
    ...     namesrv_addr=os.getenv("NAMESRV_ADDR", "localhost:9876"),
    ...     send_message_timeout=10000,            # 10秒超时
    ...     retry_times=5,                         # 更多重试
    ...     retry_another_broker_when_not_store_ok=True,  # 故障转移
    ...     compress_msg_body_over_howmuch=2048,   # 2KB压缩阈值
    ... )
    >>>
    >>> # 6. 使用上下文管理器（推荐）
    >>> try:
    ...     producer.start()
    ...     # 业务逻辑...
    ... finally:
    ...     producer.shutdown()

    ========== 常见问题 ==========

    Q: 如何选择合适的producer_group名称？
    A: 建议使用"应用名_功能名_PRODUCER"的格式，避免使用默认值，
       不同功能的生产者应该使用不同的组名。

    Q: namesrv_addr应该如何配置？
    A: 开发环境可以使用"localhost:9876"，生产环境建议配置多个
       NameServer地址，用分号分隔，实现高可用。

    Q: 消息发送失败怎么办？
    A: 调整retry_times参数增加重试次数，启用retry_another_broker_when_not_store_ok
       实现故障转移，同时检查网络连接和broker状态。

    Q: 什么时候需要调整超时时间？
    A: 网络延迟较高或消息体较大时，应该适当增加send_message_timeout。

    Q: 应该选择Producer还是AsyncProducer？
    A: 根据具体需求选择：
       - **新手用户**: 建议从Producer开始，学习曲线平缓
       - **简单应用**: Producer足够使用，代码更直观
       - **高并发需求**: 选择AsyncProducer，性能更优
       - **异步架构**: 如果整个应用都是异步的，使用AsyncProducer更一致
       - **批处理场景**: Producer更适合简单批量操作
       - **实时性要求高**: AsyncProducer提供更低的延迟

    ========== 注意事项 ==========

    - Producer实例不是线程安全的，多线程使用时请确保每个线程使用独立的Producer实例
    - 调用start()后才能发送消息，使用完毕后记得调用shutdown()释放资源
    - 消息体大小不要超过max_message_size限制，默认4MB
    - 生产环境中建议配置多个NameServer地址以确保高可用性
    - 发送重要消息时建议增加重试次数和超时时间

    ========== 异常处理 ==========

    函数可能抛出的异常：
    - TypeError: 当producer_group或namesrv_addr参数类型错误时
    - ValueError: 当配置参数值不合法时
    - 所有ProducerConfig.__init__可能抛出的异常

    ========== 参见 ==========

    - Producer: 生产者核心实现类
    - ProducerConfig: 生产者配置类
    - Message: 消息数据结构
    - create_async_producer: 异步生产者创建函数
    - create_transaction_producer: 事务生产者创建函数
    """
    config = ProducerConfig(
        producer_group=producer_group,
        namesrv_addr=namesrv_addr,
        **kwargs,
    )
    return Producer(config)
