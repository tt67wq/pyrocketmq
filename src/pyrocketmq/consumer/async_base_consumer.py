"""
AsyncBaseConsumer - 异步消费者抽象基类

AsyncBaseConsumer是pyrocketmq消费者模块的异步抽象基类，定义了所有异步消费者的
通用接口和基础功能。它提供统一的配置管理、消息监听器注册、订阅管理等
核心功能，为具体异步消费者实现（如AsyncConcurrentConsumer、AsyncOrderlyConsumer等）
提供坚实的基础。

所有网络操作、IO操作都采用asyncio异步编程模型，适用于高并发异步应用场景。

作者: pyrocketmq开发团队
"""

import asyncio
import time
from typing import Any

# pyrocketmq导入
from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.broker.async_client import AsyncBrokerClient
from pyrocketmq.consumer.async_listener import AsyncConsumeContext
from pyrocketmq.consumer.async_offset_store import AsyncOffsetStore
from pyrocketmq.consumer.async_offset_store_factory import AsyncOffsetStoreFactory
from pyrocketmq.consumer.topic_broker_mapping import ConsumerTopicBrokerMapping
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    BrokerData,
    ConsumerData,
    ConsumeResult,
    HeartbeatData,
    MessageExt,
    MessageModel,
    MessageProperty,
    MessageQueue,
    MessageSelector,
    TopicRouteData,
)
from pyrocketmq.nameserver.async_manager import (
    AsyncNameServerManager,
    create_async_nameserver_manager,
)
from pyrocketmq.remote import AsyncConnectionPool
from pyrocketmq.remote.config import RemoteConfig

from .async_consume_from_where_manager import AsyncConsumeFromWhereManager
from .async_listener import AsyncMessageListener

# 本地模块导入
from .config import ConsumerConfig
from .errors import ConsumerError, SubscribeError, UnsubscribeError
from .subscription_manager import SubscriptionManager

logger = get_logger(__name__)


class AsyncBaseConsumer:
    """
    异步消费者抽象基类

    定义了所有RocketMQ异步消费者的核心接口和基础功能。具体异步消费者实现
    （如异步并发消费者、异步顺序消费者等）需要继承这个类并实现其抽象方法。

    核心功能:
        - 异步配置管理: 统一的Consumer配置管理
        - 异步订阅管理: Topic订阅和消息选择器管理
        - 异步消息监听: 异步消息处理回调机制
        - 异步生命周期: 启动、停止等异步生命周期管理
        - 异步错误处理: 统一的异常处理和错误恢复

    设计原则:
        - 异步优先: 所有IO操作都采用async/await模式
        - 接口分离: 清晰定义各层职责
        - 可扩展性: 便于添加新的异步消费者类型
        - 线程安全: 所有公共操作保证线程安全
        - 资源管理: 完善的异步资源清理机制

    Attributes:
        _config: 消费者配置实例
        _subscription_manager: 订阅关系管理器
        _message_listeners: 异步消息监听器字典，按Topic存储监听器
        _is_running: 消费者运行状态标志
        _lock: 异步线程安全锁

    Example:
        >>> class MyAsyncConsumer(AsyncBaseConsumer):
        ...     def __init__(self, config):
        ...         super().__init__(config)
        ...         # 初始化特定资源
        ...
        ...     async def start(self):
        ...         # 实现具体的异步启动逻辑
        ...         await self._async_start()
        ...
        ...     async def shutdown(self):
        ...         # 实现具体的异步停止逻辑
        ...         await self._async_shutdown()
        ...
        ...     async def _consume_message(self, messages, context):
        ...         # 实现具体的异步消费逻辑
        ...         pass
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """
        初始化AsyncBaseConsumer实例

        创建异步消费者实例并初始化核心组件，包括订阅管理器、消息监听器存储等。
        验证配置的有效性并进行必要的初始化设置。

        Args:
            config: 消费者配置，包含所有必要的配置参数

        Raises:
            ValueError: 当config为None时抛出
        """

        self._config: ConsumerConfig = config
        self._subscription_manager: SubscriptionManager = SubscriptionManager()
        self._message_listeners: dict[str, AsyncMessageListener] = {}
        self._is_running: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

        # 异步管理器
        self._nameserver_manager: AsyncNameServerManager = (
            create_async_nameserver_manager(self._config.namesrv_addr)
        )

        remote_config: RemoteConfig = RemoteConfig(
            connection_pool_size=32, connection_max_lifetime=30
        )
        self._broker_manager: AsyncBrokerManager = AsyncBrokerManager(
            remote_config=remote_config
        )
        self._offset_store: AsyncOffsetStore = (
            AsyncOffsetStoreFactory.create_offset_store(
                consumer_group=self._config.consumer_group,
                message_model=self._config.message_model,
                namesrv_manager=self._nameserver_manager,
                broker_manager=self._broker_manager,
                store_path=self._config.offset_store_path,
                persist_interval=self._config.persist_interval,
            )
        )
        self._consume_from_where_manager: AsyncConsumeFromWhereManager = (
            AsyncConsumeFromWhereManager(
                consume_group=self._config.consumer_group,
                namesrv_manager=self._nameserver_manager,
                broker_manager=self._broker_manager,
            )
        )

        # 异步任务
        self._route_refresh_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

        # 路由刷新和心跳配置
        self._route_refresh_interval: float = 30.0  # 30秒
        self._heartbeat_interval: float = 30.0  # 30秒
        self._last_heartbeat_time: float = 0

        # 异步事件
        self._route_refresh_event: asyncio.Event = asyncio.Event()
        self._heartbeat_event: asyncio.Event = asyncio.Event()

        # 路由映射
        self._topic_broker_mapping: ConsumerTopicBrokerMapping = (
            ConsumerTopicBrokerMapping()
        )

        # 统计信息
        self._stats: dict[str, Any] = {
            "start_time": 0,
            "route_refresh_count": 0,
            "route_refresh_success_count": 0,
            "route_refresh_failure_count": 0,
            "heartbeat_count": 0,
            "heartbeat_success_count": 0,
            "heartbeat_failure_count": 0,
            "last_route_refresh_time": 0,
            "last_heartbeat_time": 0,
        }

        # 消费者状态
        self._start_time: float = 0
        self._shutdown_time: float = 0

        # 日志记录器
        self._logger = get_logger(f"{__name__}.{config.consumer_group}")

        self._logger.info(
            "异步消费者初始化完成",
            extra={
                "consumer_group": config.consumer_group,
                "namesrv_addr": config.namesrv_addr,
                "message_model": config.message_model,
                "client_id": config.client_id,
            },
        )

    async def start(self) -> None:
        """
        异步启动消费者

        异步启动消费者，开始消费消息。这是一个抽象方法，需要由具体的
        消费者实现类来提供具体的启动逻辑。

        Raises:
            ConsumerError: 启动过程中发生错误时抛出
        """
        await self._async_start()

    async def shutdown(self) -> None:
        """
        异步关闭消费者

        异步关闭消费者，停止消费消息并清理资源。这是一个抽象方法，
        需要由具体的消费者实现类来提供具体的关闭逻辑。

        Raises:
            ConsumerError: 关闭过程中发生错误时抛出
        """
        await self._async_shutdown()

    async def subscribe(
        self, topic: str, selector: MessageSelector, listener: AsyncMessageListener
    ) -> None:
        """
        异步订阅Topic并注册消息监听器

        异步订阅指定Topic并设置消息选择器和对应的监听器。支持基于TAG和SQL92的消息过滤。
        每个Topic只能注册一个监听器，如果重复订阅同一Topic，新的监听器会覆盖旧的监听器。

        Args:
            topic: 要订阅的Topic名称，不能为空
            selector: 消息选择器，用于过滤消息，不能为None
            listener: 消息监听器实例，用于处理该Topic的消息，不能为None

        Raises:
            SubscribeError: 订阅失败时抛出
            ValueError: 当参数无效时抛出
        """
        if not topic:
            raise ValueError("Topic cannot be empty")
        if not selector:
            raise ValueError("Message selector cannot be None")
        if not listener:
            raise ValueError("Message listener cannot be None")

        try:
            async with self._lock:
                # 验证订阅参数
                if not self._subscription_manager.validate_subscription(
                    topic, selector
                ):
                    raise SubscribeError(
                        topic, f"无效的订阅参数: topic={topic}, selector={selector}"
                    )

                # 订阅Topic
                success: bool = self._subscription_manager.subscribe(topic, selector)
                if not success:
                    raise SubscribeError(topic, f"订阅失败: topic={topic}")

                # 注册消息监听器
                old_listener = self._message_listeners.get(topic)
                self._message_listeners[topic] = listener

                self._logger.info(
                    "订阅Topic并注册监听器成功",
                    extra={
                        "topic": topic,
                        "selector": str(selector),
                        "listener_type": type(listener).__name__,
                        "consumer_group": self._config.consumer_group,
                        "listener_replaced": old_listener is not None,
                    },
                )

        except Exception as e:
            self._logger.error(
                "订阅Topic失败",
                extra={
                    "topic": topic,
                    "selector": str(selector),
                    "listener_type": type(listener).__name__ if listener else "None",
                    "error": str(e),
                },
                exc_info=True,
            )
            raise SubscribeError(topic, f"订阅失败: {e}", e) from e

    async def unsubscribe(self, topic: str) -> None:
        """
        异步取消订阅Topic

        异步取消指定Topic的订阅，并移除对应的消息监听器。

        Args:
            topic: 要取消订阅的Topic名称，不能为空

        Raises:
            SubscribeError: 取消订阅失败时抛出
            ValueError: 当topic为空时抛出
        """
        if not topic:
            raise ValueError("Topic cannot be empty")

        try:
            async with self._lock:
                # 取消订阅Topic
                success: bool = self._subscription_manager.unsubscribe(topic)

                # 同时移除对应的监听器
                removed_listener = self._message_listeners.pop(topic, None)

                if not success and removed_listener is None:
                    raise UnsubscribeError(
                        topic, f"Topic未订阅且无监听器: topic={topic}"
                    )

                self._logger.info(
                    "取消订阅Topic成功",
                    extra={
                        "topic": topic,
                        "consumer_group": self._config.consumer_group,
                        "subscription_removed": success,
                        "listener_removed": removed_listener is not None,
                    },
                )

        except Exception as e:
            self._logger.error(
                "取消订阅Topic失败",
                extra={
                    "topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise UnsubscribeError(topic, f"取消订阅失败: {e}", e) from e

    async def get_subscribed_topics(self) -> set[str]:
        """
        异步获取已订阅的Topic列表

        Returns:
            已订阅的Topic名称列表

        Raises:
            ConsumerError: 获取失败时抛出
        """
        return self._subscription_manager.get_topics()

    async def is_subscribed(self, topic: str) -> bool:
        """
        异步检查是否已订阅指定Topic

        Args:
            topic: 要检查的Topic名称

        Returns:
            如果已订阅返回True，否则返回False
        """
        return self._subscription_manager.is_subscribed(topic)

    async def is_running(self) -> bool:
        """
        异步检查消费者是否正在运行

        Returns:
            如果正在运行返回True，否则返回False
        """
        return self._is_running

    async def get_config(self) -> ConsumerConfig:
        """
        异步获取消费者配置

        Returns:
            消费者配置的副本
        """
        return self._config

    async def get_subscription_manager(self) -> SubscriptionManager:
        """
        异步获取订阅管理器

        Returns:
            订阅管理器实例
        """
        return self._subscription_manager

    async def get_message_listener(self, topic: str) -> AsyncMessageListener | None:
        """
        异步获取指定Topic的消息监听器

        Args:
            topic: Topic名称，不能为空

        Returns:
            指定Topic的消息监听器实例，如果未注册则返回None
        """
        if not topic:
            raise ValueError("Topic cannot be empty")
        return self._message_listeners.get(topic)

    async def get_all_listeners(self) -> dict[str, AsyncMessageListener]:
        """
        异步获取所有Topic的消息监听器

        Returns:
            包含所有Topic监听器的字典副本
        """
        return self._message_listeners.copy()

    async def _concurrent_consume_message(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> bool:
        """异步处理接收到的消息的内部方法。

        这是异步并发消费者的核心消息处理方法，负责根据消息的topic选择对应的监听器来处理消息。
        支持为不同topic配置不同的异步监听器，实现灵活的业务逻辑处理。同时支持重试主题的智能路由。

        Args:
            messages (list[MessageExt]): 要处理的消息列表，可以包含多条消息
            message_queue (MessageQueue): 消息来自的队列信息，包含topic、broker名称和队列ID

        Returns:
            bool: 消费处理是否成功
                - True: 消息处理成功（ConsumeResult.SUCCESS），可以更新偏移量
                - False: 消息处理失败或发生异常，消息将进入重试流程

        Raises:
            ConsumerError: 当消息处理过程中发生严重错误时抛出，例如：
                - 监听器调用失败且无法重试
                - 消息上下文创建失败
                - 其他无法恢复的系统错误
        """
        if not messages:
            logger.warning(
                "Empty message list received",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                },
            )
            return True  # 空消息列表视为处理成功

        # 获取对应Topic的监听器
        topic: str = message_queue.topic
        listener: AsyncMessageListener | None = self._message_listeners.get(topic)
        if not listener and self._is_retry_topic(topic):
            origin_topic: str | None = messages[0].get_property(
                MessageProperty.RETRY_TOPIC
            )
            listener = (
                self._message_listeners.get(origin_topic) if origin_topic else None
            )

        if not listener:
            logger.error(
                f"No message listener registered for topic: {topic}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": topic,
                    "queue_id": message_queue.queue_id,
                    "available_topics": list(self._message_listeners.keys()),
                },
            )
            return False

        # 创建异步消费上下文
        reconsume_times: int = messages[0].reconsume_times if messages else 0
        context: AsyncConsumeContext = AsyncConsumeContext(
            consumer_group=self._config.consumer_group,
            message_queue=message_queue,
            reconsume_times=reconsume_times,
        )

        try:
            logger.debug(
                "Processing messages",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "reconsume_times": reconsume_times,
                    "listener_type": type(listener).__name__,
                },
            )

            result: ConsumeResult = await listener.consume_message(messages, context)
            if result in [
                ConsumeResult.COMMIT,
                ConsumeResult.ROLLBACK,
                ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT,
            ]:
                logger.error(
                    "Invalid result for async concurrent consumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": context.topic,
                        "queue_id": context.queue_id,
                        "result": result.value,
                    },
                )
                return False

            logger.info(
                "Message processing completed",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "result": result.value,
                },
            )

            return result == ConsumeResult.SUCCESS

        except Exception as e:
            logger.error(
                f"Failed to process messages: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "error": str(e),
                },
                exc_info=True,
            )

            # 调用异常回调（如果监听器实现了）
            if hasattr(listener, "on_exception"):
                try:
                    await listener.on_exception(messages, context, e)
                except Exception as callback_error:
                    logger.error(
                        f"Exception callback failed: {callback_error}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "error": str(callback_error),
                        },
                        exc_info=True,
                    )

            return False

    async def _send_back_message(
        self, message_queue: MessageQueue, message: MessageExt
    ) -> bool:
        """将消费失败的消息异步发送回broker重新消费。

        当消息消费失败时，此方法负责将消息异步发送回原始broker，
        以便后续重新消费。这是RocketMQ异步消息重试机制的重要组成部分。

        Args:
            message_queue (MessageQueue): 消息来自的队列信息，包含broker名称和队列ID
            message (MessageExt): 需要发送回的消息对象，包含消息内容和属性

        Returns:
            bool: 发送操作是否成功
                - True: 消息成功发送回broker，将进入重试流程
                - False: 发送失败，消息可能丢失或需要其他处理

        Raises:
            该方法不抛出异常，所有错误都会被捕获并记录日志

        异步处理流程:
            1. 异步获取目标broker地址
            2. 验证broker地址有效性
            3. 异步建立与broker的连接池
            4. 使用异步上下文管理器获取连接
            5. 设置消息重试相关属性：
               - RETRY_TOPIC: 设置重试主题名
               - CONSUME_START_TIME: 记录消费开始时间
               - reconsume_times: 递增重试次数
            6. 异步调用broker的consumer_send_msg_back接口
            7. 记录处理结果和统计信息

        异步特性:
            - 所有IO操作都使用async/await模式
            - 使用AsyncConnectionPool进行异步连接管理
            - 使用AsyncBrokerClient进行异步通信
            - 异步上下文管理器确保资源正确释放
            - 不阻塞事件循环，支持高并发场景

        错误处理:
            - 如果无法获取broker地址，记录错误日志并返回False
            - 如果连接或发送失败，记录错误日志但不抛出异常
            - 确保异步消费循环的连续性，避免单个消息失败影响整体消费

        Examples:
            >>> # 在异步消费循环中处理失败消息
            >>> result = await self._consume_message(messages, context)
            >>> if result == ConsumeResult.RECONSUME_LATER:
            >>>     for msg in messages:
            >>>         success = await self._send_back_message(msg.queue, msg)
            >>>         if not success:
            >>>             logger.error(f"Failed to send back message: {msg.msg_id}")

        Note:
            - 该方法在消费失败时被调用，用于实现异步消息重试机制
            - 消息会被重新放入重试队列等待重新消费
            - 重试次数受max_reconsume_times配置限制，默认16次
            - 超过最大重试次数后，消息会进入死信队列(%DLQ%{consumer_group})
            - reconsume_times属性会递增，用于跟踪消息重试次数
            - 方法不会抛出异常，确保异步消费循环的稳定性
            - 使用异步方式提升并发性能，适合高吞吐量场景
        """
        broker_addr = await self._nameserver_manager.get_broker_address(
            message_queue.broker_name
        )
        if not broker_addr:
            logger.error(
                "Failed to get broker address for message send back",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "broker_name": message_queue.broker_name,
                    "message_id": message.msg_id,
                    "topic": message.topic,
                    "queue_id": message.queue.queue_id if message.queue else 0,
                },
            )
            return False

        try:
            pool: AsyncConnectionPool = await self._broker_manager.must_connection_pool(
                broker_addr
            )
            async with pool.get_connection(usage="发送消息回broker") as conn:
                self._reset_retry(message)
                message.reconsume_times += 1
                await AsyncBrokerClient(conn).consumer_send_msg_back(
                    message,
                    self._config.consumer_group,
                    message.reconsume_times,
                    self._config.max_reconsume_times,
                )

        except Exception as e:
            logger.error(
                f"Failed to send message back to broker: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_id": message.msg_id,
                    "topic": message.topic,
                    "queue_id": message.queue.queue_id if message.queue else 0,
                    "broker_name": message_queue.broker_name,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False
        else:
            logger.debug(
                "Message sent back to broker for reconsume",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_id": message.msg_id,
                    "topic": message.topic,
                    "queue_id": message.queue.queue_id if message.queue else 0,
                    "broker_name": message_queue.broker_name,
                    "reconsume_times": message.reconsume_times,
                    "max_reconsume_times": self._config.max_reconsume_times,
                },
            )
            return True

    def _reset_retry(self, msg: MessageExt) -> None:
        """
        重置消息的重试相关属性（同步方法）。

        当消息需要重新消费时，此方法负责重置或设置消息的重试相关属性，
        确保消息能够正确地参与重试机制。这通常在消息处理前或需要重新处理时调用。

        注意：该方法虽然是同步操作，但在异步环境中使用是安全的，
        因为它只涉及消息对象的内存属性设置，不涉及IO操作。

        Args:
            msg (MessageExt): 需要重置重试属性的消息对象

        设置的属性:
            - RETRY_TOPIC: 检查并设置重试主题名（如果存在）
            - CONSUME_START_TIME: 设置消费开始时间，使用当前时间戳

        属性说明:
            RETRY_TOPIC:
                - 指示消息在消费失败时应该发送到的重试主题
                - 由消费者组名唯一确定，确保重试消息的隔离性
                - RocketMQ会根据该属性将失败消息投递到正确的重试主题
                - 格式为：%RETRY%{consumer_group}

            CONSUME_START_TIME:
                - 记录消息开始消费的时间戳（毫秒）
                - 用于监控消费延迟和性能分析
                - 帮助判断消息处理的耗时情况
                - 格式为Unix时间戳的毫秒表示

        执行逻辑:
            1. 检查消息是否包含RETRY_TOPIC属性
            2. 如果存在，将该属性值设置为消息的topic字段
            3. 设置当前时间戳作为CONSUME_START_TIME属性

        使用场景:
            - 消息处理前的属性初始化
            - 消息重新消费前的属性重置
            - 重试机制中的属性设置
            - 异步消息处理过程中的属性维护
            - 从重试队列中消费的消息处理

        Examples:
            >>> # 在异步消息处理前调用
            >>> message = MessageExt()
            >>> message.set_property("RETRY_TOPIC", "%RETRY%my_group")
            >>> self._reset_retry(message)
            >>> # 现在消息topic已更新为重试主题，并具备消费时间戳
            >>> print(f"Topic: {message.topic}")  # %RETRY%my_group
            >>> print(f"Start time: {message.get_property('CONSUME_START_TIME')}")
            >>> await self._concurrent_consume_message([message], queue)

        重要注意事项:
            - 该方法只处理已有的RETRY_TOPIC属性，不会创建新的重试主题
            - 时间戳使用当前时刻，确保每次调用都更新为最新的消费开始时间
            - 与同步版本功能完全一致，确保异步环境下的行为一致性
            - 重试主题的切换是RocketMQ重试机制的关键环节
            - 在异步环境中安全使用，无IO阻塞风险

        RocketMQ重试流程:
            1. 消费失败的消息调用_send_back_message发送回broker
            2. Broker将消息投递到对应的重试主题
            3. 消费者从重试主题拉取消息
            4. 调用_reset_retry将消息topic重置为重试主题
            5. 设置消费开始时间戳
            6. 再次尝试消费处理
        """
        retry_topic: str | None = msg.get_property(MessageProperty.RETRY_TOPIC)
        if retry_topic:
            msg.topic = retry_topic
        msg.set_property(
            MessageProperty.CONSUME_START_TIME, str(int(time.time() * 1000))
        )

    async def get_status_summary(self) -> dict[str, Any]:
        """
        异步获取消费者状态摘要

        Returns:
            包含消费者状态信息的字典
        """
        subscriptions: dict[str, Any] = self._subscription_manager.get_status_summary()
        uptime: float = time.time() - self._start_time if self._is_running else 0

        return {
            "consumer_group": self._config.consumer_group,
            "namesrv_addr": self._config.namesrv_addr,
            "message_model": self._config.message_model,
            "client_id": self._config.client_id,
            "is_running": self._is_running,
            "start_time": self._start_time,
            "shutdown_time": self._shutdown_time,
            "uptime": uptime,
            "subscriptions": subscriptions,
            "has_message_listener": len(self._message_listeners) > 0,
            "listener_count": len(self._message_listeners),
            "topics_with_listeners": list(self._message_listeners.keys()),
        }

    async def _async_start(self) -> None:
        """异步启动异步消费者的基础组件。

        异步启动NameServer管理器、Broker管理器、偏移量存储等基础组件，
        这是所有异步消费者启动过程中的通用逻辑。

        Raises:
            ConsumerError: 当启动基础组件失败时抛出，例如：
                - NameServer管理器启动失败
                - Broker管理器启动失败
                - 偏移量存储启动失败
                - 路由刷新任务启动失败
                - 心跳任务启动失败
        """
        try:
            self._logger.info("开始启动异步消费者基础组件")

            await self._nameserver_manager.start()
            await self._broker_manager.start()
            await self._offset_store.start()

            # 启动路由刷新任务
            await self._start_route_refresh_task()

            # 启动心跳任务
            await self._start_heartbeat_task()

            self._start_time = time.time()
            self._is_running = True

            self._logger.info("异步消费者基础组件启动完成")

            # 订阅重试主题
            if self._config.message_model == MessageModel.CLUSTERING:
                self._subscribe_retry_topic()

        except Exception as e:
            self._logger.error(
                "启动异步消费者基础组件失败",
                extra={
                    "error": str(e),
                },
                exc_info=True,
            )
            await self._async_cleanup_resources()
            raise ConsumerError(f"启动异步消费者基础组件失败: {e}") from e

    async def _async_shutdown(self) -> None:
        """异步关闭异步消费者的基础组件。

        异步关闭NameServer管理器、Broker管理器、偏移量存储等基础组件，
        这是所有异步消费者关闭过程中的通用逻辑。

        Raises:
            ConsumerError: 当关闭基础组件失败时抛出，例如：
                - 异步任务关闭失败
                - 资源清理失败
                - 连接关闭失败
                - 偏移量持久化失败
        """
        try:
            self._logger.info("开始关闭异步消费者基础组件")

            self._is_running = False
            self._shutdown_time = time.time()

            # 通知路由刷新和心跳任务退出
            self._route_refresh_event.set()
            self._heartbeat_event.set()

            # 关闭异步任务
            await self._shutdown_async_tasks()

            # 清理资源
            await self._async_cleanup_resources()

            self._logger.info("异步消费者基础组件关闭完成")

        except Exception as e:
            self._logger.error(
                "关闭异步消费者基础组件失败",
                extra={
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ConsumerError(f"关闭异步消费者基础组件失败: {e}") from e

    def _get_retry_topic(self) -> str:
        """
        获取消费者组对应的重试主题名称。

        在RocketMQ中，当消息消费失败时，系统会按照重试主题将消息重新投递给消费者。
        重试主题的命名规则为：%RETRY%{consumer_group}。

        Returns:
            str: 重试主题名称，格式为 %RETRY%{consumer_group}

        Examples:
            >>> retry_topic = self._get_retry_topic()
            >>> print(retry_topic)
            '%RETRY%order_consumer_group'

        Note:
            - 重试主题名前缀是固定的 %RETRY%
            - 重试主题使用消费者组名而不是原始主题名
            - 重试机制的消息会根据重试次数延迟投递
            - 默认重试次数为16次，超过后消息会进入死信队列
            - 每个消费者组都有自己独立的重试主题
        """
        return f"%RETRY%{self._config.consumer_group}"

    def _is_retry_topic(self, topic: str) -> bool:
        """
        判断指定主题是否是重试主题

        检查给定的主题名是否符合重试主题的命名规范。
        重试主题的格式为：%RETRY%+consumer_group

        Args:
            topic (str): 要检查的主题名

        Returns:
            bool: 如果是重试主题返回True，否则返回False

        Examples:
            >>> consumer = AsyncBaseConsumer(config)
            >>> consumer.is_retry_topic("%RETRY%my_consumer_group")
            True
            >>> consumer.is_retry_topic("normal_topic")
            False
            >>> consumer.is_retry_topic("%DLQ%my_consumer_group")
            False

        Note:
            - 此方法与同步版本的功能完全一致
            - 使用精确字符串匹配确保准确性
            - 包含输入参数的安全检查
        """
        if not topic or not isinstance(topic, str):
            return False

        retry_topic_prefix = f"%RETRY%{self._config.consumer_group}"
        return topic == retry_topic_prefix

    def _subscribe_retry_topic(self) -> None:
        """
        异步订阅重试主题。

        自动订阅该消费者组的重试主题，格式为：%RETRY%+consumer_group。
        重试主题用于接收消费失败需要重试的消息。

        该方法使用异步方式执行订阅操作，避免阻塞其他异步任务的执行。

        异常处理:
            - 如果订阅失败，记录错误日志但不抛出异常
            - 重试主题订阅失败不应该影响消费者正常启动
            - 提供详细的错误上下文信息用于问题排查

        Note:
            - 重试主题使用TAG选择器订阅所有消息（"*"），因为重试消息不需要额外过滤
            - 使用异步方式避免阻塞消费者启动过程
            - 检查重复订阅，避免资源浪费
        """
        retry_topic = self._get_retry_topic()

        try:
            from pyrocketmq.model.client_data import create_tag_selector

            # 创建订阅所有消息的选择器
            retry_selector = create_tag_selector("*")

            # 检查是否已经订阅了重试主题，避免重复订阅
            is_subscribed: bool = self._subscription_manager.is_subscribed(retry_topic)
            if not is_subscribed:
                success = self._subscription_manager.subscribe(
                    retry_topic, retry_selector
                )

                if success:
                    logger.info(
                        f"Successfully subscribed to retry topic: {retry_topic}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "retry_topic": retry_topic,
                            "max_retry_times": self._config.max_retry_times,
                        },
                    )
                else:
                    logger.warning(
                        f"Failed to subscribe to retry topic: {retry_topic}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "retry_topic": retry_topic,
                        },
                    )
            else:
                logger.debug(
                    f"Retry topic already subscribed: {retry_topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "retry_topic": retry_topic,
                    },
                )

        except Exception as e:
            logger.error(
                f"Error subscribing to retry topic {retry_topic}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "retry_topic": retry_topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            # 不抛出异常，重试主题订阅失败不应该影响消费者正常启动

    async def _shutdown_async_tasks(self) -> None:
        """
        异步关闭异步任务

        异步关闭路由刷新和心跳等后台任务。
        """
        # 关闭路由刷新任务
        if self._route_refresh_task is not None:
            self._route_refresh_task.cancel()
            try:
                await self._route_refresh_task
            except asyncio.CancelledError:
                pass
            self._route_refresh_task = None

        # 关闭心跳任务
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        self._logger.info("异步任务关闭完成")

    async def _async_cleanup_resources(self) -> None:
        """
        异步清理资源

        异步清理所有资源，包括偏移量存储、管理器等。
        """
        try:
            # 关闭偏移量存储
            await self._offset_store.persist_all()
            await self._offset_store.stop()

            # 清理订阅管理器
            self._subscription_manager.cleanup_inactive_subscriptions()
            self._subscription_manager.clear_all()

            # 关闭Broker管理器
            await self._broker_manager.shutdown()

            # 关闭NameServer管理器
            await self._nameserver_manager.stop()

            self._logger.info("资源清理完成")

        except Exception as e:
            self._logger.error(
                "清理资源失败",
                extra={
                    "error": str(e),
                },
                exc_info=True,
            )

    def __str__(self) -> str:
        """字符串表示"""
        subscription_count: int = len(
            self._subscription_manager.get_all_subscriptions()
        )
        return (
            f"AsyncBaseConsumer["
            f"group={self._config.consumer_group}, "
            f"running={self._is_running}, "
            f"subscriptions={subscription_count}"
            f"]"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (
            f"AsyncBaseConsumer("
            f"consumer_group='{self._config.consumer_group}', "
            f"namesrv_addr='{self._config.namesrv_addr}', "
            f"message_model='{self._config.message_model}', "
            f"is_running={self._is_running}"
            f")"
        )

    # ==================== 异步路由刷新任务 ====================

    async def _start_route_refresh_task(self) -> None:
        """启动异步路由刷新任务。

        创建并启动一个异步任务来执行路由刷新循环，
        确保消费者能够及时感知到集群拓扑的变化。

        Returns:
            None: 无返回值

        Note:
            - 任务会在后台持续运行，直到消费者关闭
            - 使用asyncio.create_task创建异步任务
            - 任务会自动处理异常和退出
        """
        self._route_refresh_task = asyncio.create_task(self._route_refresh_loop())

    async def _route_refresh_loop(self) -> None:
        """异步路由刷新循环。

        定期刷新所有订阅Topic的路由信息，确保消费者能够感知到集群拓扑的变化。
        这是RocketMQ消费者高可用性的关键机制。

        Returns:
            None: 无返回值

        Note:
            - 启动时立即执行一次路由刷新
            - 按配置间隔定期执行刷新（默认30秒）
            - 支持通过事件信号优雅退出
            - 每次刷新都会更新TopicBrokerMapping缓存
            - 异常不会中断循环，会继续下次刷新
        """
        self._logger.info(
            "Async route refresh loop started",
            extra={
                "consumer_group": self._config.consumer_group,
                "refresh_interval": self._route_refresh_interval,
            },
        )

        await self._refresh_all_routes()

        while self._is_running:
            try:
                # 等待指定间隔或关闭事件
                try:
                    await asyncio.wait_for(
                        self._route_refresh_event.wait(),
                        timeout=self._route_refresh_interval,
                    )
                    # 收到事件信号，退出循环
                    break
                except asyncio.TimeoutError:
                    # 超时继续执行路由刷新
                    pass

                # 检查是否正在关闭
                if self._route_refresh_event.is_set():
                    break

                # 执行路由刷新
                await self._refresh_all_routes()
                self._topic_broker_mapping.clear_expired_routes()

                # 更新统计信息
                self._stats["route_refresh_count"] += 1
                self._stats["route_refresh_success_count"] += 1
                self._stats["last_route_refresh_time"] = time.time()

                self._logger.debug(
                    "Async route refresh completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "refresh_count": self._stats["route_refresh_count"],
                        "topics_count": (
                            len(self._topic_broker_mapping.get_all_topics())
                        ),
                    },
                )

            except Exception as e:
                self._stats["route_refresh_failure_count"] += 1
                self._logger.warning(
                    f"Error in async route refresh loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                        "refresh_count": self._stats["route_refresh_count"],
                        "failure_count": self._stats["route_refresh_failure_count"],
                    },
                    exc_info=True,
                )

                # 发生异常时，等待较短时间后重试
                try:
                    await asyncio.wait_for(
                        self._route_refresh_event.wait(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    pass

        self._logger.info(
            "Async route refresh loop stopped",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_refreshes": self._stats["route_refresh_count"],
                "success_count": self._stats["route_refresh_success_count"],
                "failure_count": self._stats["route_refresh_failure_count"],
            },
        )

    async def _refresh_all_routes(self) -> None:
        """异步刷新所有Topic的路由信息"""
        topics: set[str] = set()

        # 收集所有需要刷新路由的Topic
        topics.update(self._topic_broker_mapping.get_all_topics())
        topics.update(self._subscription_manager.get_topics())

        for topic in topics:
            try:
                if self._topic_broker_mapping.get_route_info(topic) is None:
                    _ = await self._update_route_info(topic)
            except Exception as e:
                self._logger.debug(
                    "Failed to refresh route",
                    extra={
                        "topic": topic,
                        "error": str(e),
                    },
                )

    async def _update_route_info(self, topic: str) -> bool:
        """异步更新Topic路由信息"""
        self._logger.info(
            "Updating route info for topic",
            extra={"topic": topic},
        )

        try:
            topic_route_data: (
                TopicRouteData | None
            ) = await self._nameserver_manager.get_topic_route(topic)
            if not topic_route_data:
                self._logger.error(
                    "Failed to get topic route data",
                    extra={
                        "topic": topic,
                    },
                )
                return False

            # 更新TopicBrokerMapping中的路由信息
            _ = self._topic_broker_mapping.update_route_info(topic, topic_route_data)

            return True

        except Exception as e:
            self._logger.error(
                f"Error updating route info for topic {topic}: {e}",
                extra={
                    "topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    # ==================== 异步心跳任务 ====================

    async def _start_heartbeat_task(self) -> None:
        """启动异步心跳任务。

        创建并启动一个异步任务来执行心跳发送循环，
        确保消费者与Broker保持活跃连接状态。

        Returns:
            None: 无返回值

        Note:
            - 任务会在后台持续运行，直到消费者关闭
            - 使用asyncio.create_task创建异步任务
            - 心跳机制是RocketMQ消费者存活状态的关键
        """
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _calculate_wait_time(self, current_time: float) -> float:
        """计算到下一次心跳的等待时间。

        Args:
            current_time (float): 当前时间戳

        Returns:
            float: 到下一次心跳的等待时间（秒）
        """
        return self._heartbeat_interval - (current_time - self._last_heartbeat_time)

    async def _wait_for_heartbeat_event_or_timeout(self, timeout: float) -> bool:
        """等待心跳事件或超时。

        Args:
            timeout (float): 超时时间（秒）

        Returns:
            bool: 如果事件被触发返回True，如果超时返回False
        """
        try:
            await asyncio.wait_for(self._heartbeat_event.wait(), timeout=timeout)
            # Event被触发，重置事件状态
            self._heartbeat_event.clear()
            return True
        except asyncio.TimeoutError:
            return False

    def _should_send_heartbeat(self, current_time: float) -> bool:
        """判断是否应该发送心跳。

        Args:
            current_time (float): 当前时间戳

        Returns:
            bool: 如果应该发送心跳返回True，否则返回False
        """
        return current_time - self._last_heartbeat_time >= self._heartbeat_interval

    def _handle_heartbeat_loop_error(self, error: Exception) -> None:
        """处理心跳循环中的异常。

        Args:
            error (Exception): 捕获的异常
        """
        self._logger.error(
            f"Error in async heartbeat loop: {error}",
            extra={
                "consumer_group": self._config.consumer_group,
                "error": str(error),
            },
            exc_info=True,
        )

    async def _perform_heartbeat_if_needed(self, current_time: float) -> float:
        """在需要时执行心跳并返回新的时间戳。

        Args:
            current_time (float): 当前时间戳

        Returns:
            float: 更新后的时间戳
        """
        if self._should_send_heartbeat(current_time):
            await self._send_heartbeat_to_all_brokers()
            self._last_heartbeat_time = time.time()
            return self._last_heartbeat_time
        return current_time

    async def _heartbeat_loop(self) -> None:
        """异步消费者心跳发送循环。

        定期向所有Broker发送心跳信息，维持消费者的存活状态。
        这是RocketMQ消费者高可用性和负载均衡的基础机制。

        Returns:
            None: 无返回值

        Note:
            - 按配置间隔定期发送心跳（默认30秒）
            - 支持通过事件信号优雅退出
            - 心跳失败不会中断循环，会继续下次心跳
            - 包含消费者组、订阅关系等关键信息
            - 统计心跳成功和失败次数用于监控
        """
        self._logger.info("Async heartbeat loop started")

        # 执行首次心跳
        await self._send_heartbeat_to_all_brokers()
        self._last_heartbeat_time = time.time()

        # 主心跳循环
        while self._is_running:
            try:
                current_time = time.time()

                # 计算等待时间
                wait_time = self._calculate_wait_time(current_time)

                # 如果需要等待，处理等待逻辑
                if wait_time > 0:
                    # 限制最大等待时间为1秒，以便及时响应退出信号
                    wait_timeout = min(wait_time, 1.0)
                    event_triggered = await self._wait_for_heartbeat_event_or_timeout(
                        wait_timeout
                    )

                    if event_triggered:
                        # 检查是否需要退出
                        if not self._is_running:
                            break
                        continue  # 重新计算等待时间

                # 重新获取当前时间并检查是否需要发送心跳
                current_time = time.time()
                await self._perform_heartbeat_if_needed(current_time)

            except Exception as e:
                self._handle_heartbeat_loop_error(e)
                # 等待一段时间再重试
                try:
                    await asyncio.wait_for(self._heartbeat_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        self._logger.info("Async heartbeat loop stopped")

    async def _send_heartbeat_to_all_brokers(self) -> None:
        """异步向所有Broker发送心跳"""
        self._logger.info("Sending heartbeat to all brokers...")

        try:
            # 收集所有Broker地址
            broker_addrs: set[str] = await self._collect_broker_addresses()
            if not broker_addrs:
                return

            # 构建心跳数据
            heartbeat_data: HeartbeatData = self._build_heartbeat_data()

            # 向每个Broker发送心跳
            heartbeat_success_count: int = 0
            heartbeat_failure_count: int = 0

            for broker_addr in broker_addrs:
                if await self._send_heartbeat_to_broker(broker_addr, heartbeat_data):
                    heartbeat_success_count += 1
                else:
                    heartbeat_failure_count += 1

            # 更新统计信息
            await self._update_heartbeat_statistics(
                heartbeat_success_count, heartbeat_failure_count, len(broker_addrs)
            )

        except Exception as e:
            self._logger.error(
                f"Error sending heartbeat to brokers: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _collect_broker_addresses(self) -> set[str]:
        """收集所有Broker地址

        Returns:
            Broker地址集合
        """
        broker_addrs: set[str] = set()

        # 获取所有Topic：缓存的Topic + 订阅的Topic
        all_topics: set[str] = set()
        all_topics = self._topic_broker_mapping.get_all_topics().union(
            self._subscription_manager.get_topics()
        )

        if not all_topics:
            self._logger.warning("No topics found for heartbeat")
            return broker_addrs

        for topic in all_topics:
            # 使用TopicBrokerMapping获取可用的Broker
            brokers: list[BrokerData] = (
                self._topic_broker_mapping.get_available_brokers(topic)
            )
            for broker_data in brokers:
                # 获取主从地址
                if broker_data.broker_addresses:
                    for addr in broker_data.broker_addresses.values():
                        if addr:  # 过滤空地址
                            broker_addrs.add(addr)

        if not broker_addrs:
            self._logger.warning("No broker addresses found for heartbeat")

        return broker_addrs

    def _build_heartbeat_data(self) -> HeartbeatData:
        """构建心跳数据

        Returns:
            心跳数据对象
        """
        return HeartbeatData(
            client_id=self._config.client_id,
            consumer_data_set=[
                ConsumerData(
                    group_name=self._config.consumer_group,
                    consume_type="CONSUME_PASSIVELY",
                    message_model=self._config.message_model,
                    consume_from_where=self._config.consume_from_where,
                    subscription_data=[
                        e.subscription_data
                        for e in self._subscription_manager.get_all_subscriptions()
                    ],
                )
            ],
        )

    async def _send_heartbeat_to_broker(
        self, broker_addr: str, heartbeat_data: HeartbeatData
    ) -> bool:
        """异步向指定Broker发送心跳"""
        try:
            pool: AsyncConnectionPool = await self._broker_manager.must_connection_pool(
                broker_addr
            )
            async with pool.get_connection(usage="heartbeat") as conn:
                from pyrocketmq.broker import AsyncBrokerClient

                broker_client: AsyncBrokerClient = AsyncBrokerClient(conn)

                # 发送心跳
                await broker_client.send_heartbeat(heartbeat_data)
                self._logger.debug(
                    f"Heartbeat sent successfully to {broker_addr}",
                    extra={"broker_addr": broker_addr},
                )
                return True

        except Exception as e:
            self._logger.warning(
                f"Error sending heartbeat to {broker_addr}: {e}",
                extra={
                    "broker_addr": broker_addr,
                    "error": str(e),
                },
            )
            return False

    async def _update_heartbeat_statistics(
        self, success_count: int, failure_count: int, total_count: int
    ) -> None:
        """更新心跳统计信息"""
        self._stats["heartbeat_count"] += 1
        self._stats["heartbeat_success_count"] += success_count
        self._stats["heartbeat_failure_count"] += failure_count
        self._stats["last_heartbeat_time"] = time.time()

        self._logger.debug(
            "Heartbeat statistics updated",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_heartbeats": self._stats["heartbeat_count"],
                "success_count": success_count,
                "failure_count": failure_count,
                "total_brokers": total_count,
            },
        )
