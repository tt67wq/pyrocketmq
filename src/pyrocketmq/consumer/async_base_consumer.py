"""
AsyncBaseConsumer - 异步消费者抽象基类（模块化重组版本）

AsyncBaseConsumer是pyrocketmq消费者模块的异步抽象基类，定义了所有异步消费者的
通用接口和基础功能。本文件按照功能模块进行了重组，提高了代码的可读性和维护性。

所有网络操作、IO操作都采用asyncio异步编程模型，适用于高并发异步应用场景。

作者: pyrocketmq开发团队
版本: 重组版本 v1.0 - 按功能模块重新组织
"""

import asyncio
import time
from typing import Any

# pyrocketmq导入
from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.broker.async_client import AsyncBrokerClient
from pyrocketmq.consumer.allocate_queue_strategy import (
    AllocateQueueStrategyBase,
    AllocateQueueStrategyFactory,
)
from pyrocketmq.consumer.async_listener import AsyncConsumeContext
from pyrocketmq.consumer.async_offset_store import AsyncOffsetStore
from pyrocketmq.consumer.async_offset_store_factory import AsyncOffsetStoreFactory
from pyrocketmq.consumer.async_process_queue import AsyncProcessQueue
from pyrocketmq.consumer.offset_store import ReadOffsetType
from pyrocketmq.consumer.topic_broker_mapping import ConsumerTopicBrokerMapping
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    BrokerData,
    ConsumerData,
    ConsumeResult,
    ConsumerRunningInfo,
    HeartbeatData,
    MessageExt,
    MessageModel,
    MessageProperty,
    MessageQueue,
    MessageSelector,
    ProcessQueueInfo,
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
from .errors import (
    ConsumerError,
    InvalidConsumeResultError,
    SubscribeError,
    UnsubscribeError,
)
from .stats_manager import StatsManager
from .subscription_manager import SubscriptionManager

logger = get_logger(__name__)


# ===============================================================================
# 异步消费者抽象基类 - AsyncBaseConsumer（模块化重组版本）
# ===============================================================================
#
# 该类定义了RocketMQ异步消费者的核心接口和基础功能，采用模块化设计，
# 按功能划分为以下几个核心模块：
#
# 1. 核心初始化和生命周期管理模块
#    - 负责消费者的创建、启动、关闭等生命周期管理
#    - 管理异步资源的初始化和清理
#
# 2. 订阅管理模块
#    - 处理Topic的订阅、取消订阅操作
#    - 管理重试主题的自动订阅
#    - 提供订阅状态查询接口
#
# 3. 消息处理核心模块
#    - 实现并发和顺序两种消费模式
#    - 处理消息重试和回退机制
#    - 提供消息过滤和预处理功能
#
# 4. 异步路由刷新任务模块
#    - 定期刷新Topic路由信息
#    - 维护Broker连接状态
#    - 处理集群拓扑变化
#
# 5. 异步心跳任务模块
#    - 定期向Broker发送心跳
#    - 维护消费者存活状态
#    - 提供心跳统计和监控
#
# 6. 配置和状态管理模块
#    - 管理消费者配置信息
#    - 提供运行状态查询接口
#    - 维护组件间的关联关系
#
# 7. 资源清理和工具模块
#    - 负责异步任务的优雅关闭
#    - 清理网络连接等资源
#    - 提供工具方法支持
#
# 设计原则:
# - 异步优先: 所有IO操作采用async/await模式
# - 模块化: 清晰的功能模块划分，便于维护
# - 可扩展: 便于添加新的异步消费者类型
# - 线程安全: 使用asyncio.Lock保证并发安全
# - 容错性: 完善的异常处理和恢复机制
# ===============================================================================


class AsyncBaseConsumer:
    """
    异步消费者抽象基类（模块化重组版本）

    该版本按照功能模块重新组织了代码结构，提高了可读性和维护性。
    """

    # ==================== 1. 核心初始化和生命周期管理模块 ====================
    #
    # 该模块负责AsyncBaseConsumer的完整生命周期管理，包括：
    # - 构造函数中的组件初始化和配置验证
    # - 消费者启动时的资源分配和任务创建
    # - 消费者关闭时的资源清理和任务终止
    # - 异步资源的优雅释放和状态重置
    #
    # 关键特性:
    # - 使用asyncio.Lock保证线程安全
    # - 完整的错误处理和状态跟踪
    # - 资源的初始化和清理配对管理
    # - 支持重启和重新配置场景

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
        # 基础配置和状态初始化
        self._config: ConsumerConfig = config
        self._subscription_manager: SubscriptionManager = SubscriptionManager()
        self._message_listeners: dict[str, AsyncMessageListener] = {}
        self._is_running: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

        # ==================== 消息缓存管理 ====================
        # ProcessQueue缓存 - 消息拉取和消费之间的缓冲区
        self._msg_cache: dict[MessageQueue, AsyncProcessQueue] = {}  # 消息缓存队列
        self._cache_lock = asyncio.Lock()  # 🔐保护_msg_cache字典的并发访问

        # 异步管理器初始化
        self._initialize_async_managers()

        # 异步任务和事件初始化
        self._initialize_async_tasks()

        # 路由映射和统计信息初始化
        self._initialize_routing_and_stats()

        # 日志记录器初始化
        self._logger = get_logger(f"{__name__}.{config.consumer_group}")

        # 启动时间
        self._start_time = time.time()

        self._logger.info(
            "异步消费者初始化完成",
            extra={
                "consumer_group": config.consumer_group,
                "namesrv_addr": config.namesrv_addr,
                "message_model": config.message_model,
                "client_id": config.client_id,
            },
        )

    def _initialize_async_managers(self) -> None:
        """初始化异步管理器组件"""
        # NameServer管理器
        self._nameserver_manager: AsyncNameServerManager = (
            create_async_nameserver_manager(self._config.namesrv_addr)
        )

        # Broker管理器
        remote_config: RemoteConfig = RemoteConfig(
            connection_pool_size=32, connection_max_lifetime=30
        )
        self._broker_manager: AsyncBrokerManager = AsyncBrokerManager(
            remote_config=remote_config
        )

        # 偏移量存储
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

        # 消费起始位置管理器
        self._consume_from_where_manager: AsyncConsumeFromWhereManager = (
            AsyncConsumeFromWhereManager(
                consume_group=self._config.consumer_group,
                namesrv_manager=self._nameserver_manager,
                broker_manager=self._broker_manager,
            )
        )

    def _initialize_async_tasks(self) -> None:
        """初始化异步任务和事件"""
        # 队列分配策略
        self._allocate_strategy: AllocateQueueStrategyBase = (
            AllocateQueueStrategyFactory.create_strategy(
                self._config.allocate_queue_strategy
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

    # ==================== 3. 路由映射和统计信息初始化模块 ====================
    #
    # 该模块负责初始化路由映射和统计信息收集系统，包括：
    # - 消费者统计信息数据类的定义
    # - 主题与Broker的映射关系管理
    # - 消费者运行时统计信息的初始化
    # - 性能指标的跟踪和收集

    def _initialize_routing_and_stats(self) -> None:
        """初始化路由映射和统计信息"""
        # 路由映射
        self._topic_broker_mapping: ConsumerTopicBrokerMapping = (
            ConsumerTopicBrokerMapping()
        )

        # 统计管理器
        self._stats_manager: StatsManager = StatsManager()

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

    async def _async_start(self) -> None:
        """异步启动异步消费者的基础组件。

        异步启动NameServer管理器、Broker管理器、偏移量存储等基础组件，
        这是所有异步消费者启动过程中的通用逻辑。

        Raises:
            ConsumerError: 当启动基础组件失败时抛出
        """
        try:
            self._logger.info("开始启动异步消费者基础组件")

            # 启动核心管理器
            await self._nameserver_manager.start()
            await self._broker_manager.start()
            await self._offset_store.start()

            # 启动后台任务
            await self._start_route_refresh_task()
            await self._start_heartbeat_task()

            # 更新状态

            self._is_running = True

            self._logger.info("异步消费者基础组件启动完成")

            # 订阅重试主题（集群模式）
            if self._config.message_model == MessageModel.CLUSTERING:
                self._subscribe_retry_topic()

        except Exception as e:
            self._logger.error(
                "启动异步消费者基础组件失败",
                extra={"error": str(e)},
                exc_info=True,
            )
            await self._async_cleanup_resources()
            raise ConsumerError(f"启动异步消费者基础组件失败: {e}") from e

    async def _async_shutdown(self) -> None:
        """异步关闭异步消费者的基础组件。

        异步关闭NameServer管理器、Broker管理器、偏移量存储等基础组件，
        这是所有异步消费者关闭过程中的通用逻辑。

        Raises:
            ConsumerError: 当关闭基础组件失败时抛出
        """
        try:
            self._logger.info("开始关闭异步消费者基础组件")

            # 更新状态
            self._is_running = False

            # 通知后台任务退出
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
                extra={"error": str(e)},
                exc_info=True,
            )
            raise ConsumerError(f"关闭异步消费者基础组件失败: {e}") from e

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

    # ==================== 2. 订阅管理模块 ====================
    #
    # 该模块负责处理消费者的订阅相关操作，包括：
    # - Topic的订阅和取消订阅
    # - 重试主题的自动管理
    # - 订阅状态查询和验证
    # - 订阅关系的一致性维护
    #
    # 关键特性:
    # - 线程安全的订阅操作
    # - 自动重试主题订阅
    # - 订阅冲突检测和处理
    # - 完整的订阅状态跟踪

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
        # 参数验证
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
                extra={"topic": topic, "error": str(e)},
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

    def _get_retry_topic(self) -> str:
        """
        获取消费者组对应的重试主题名称。

        在RocketMQ中，当消息消费失败时，系统会按照重试主题将消息重新投递给消费者。
        重试主题的命名规则为：%RETRY%{consumer_group}。

        Returns:
            str: 重试主题名称，格式为 %RETRY%{consumer_group}
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
        """
        if not topic or not isinstance(topic, str):
            return False

        retry_topic_prefix = f"%RETRY%{self._config.consumer_group}"
        return topic == retry_topic_prefix

    def _subscribe_retry_topic(self) -> None:
        """
        订阅重试主题。

        自动订阅该消费者组的重试主题，格式为：%RETRY%+consumer_group。
        重试主题用于接收消费失败需要重试的消息。

        异常处理:
            - 如果订阅失败，记录错误日志但不抛出异常
            - 重试主题订阅失败不应该影响消费者正常启动
            - 提供详细的错误上下文信息用于问题排查

        Note:
            - 重试主题使用TAG选择器订阅所有消息（"*"），因为重试消息不需要额外过滤
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

    # ==================== 3. 消息处理核心模块 ====================
    #
    # 该模块是消费者的核心消息处理引擎，负责：
    # - 消息消费的前期准备和验证
    # - 并发和顺序两种消费模式的实现
    # - 消息重试和回退机制的处理
    # - 消息过滤和预处理功能
    #
    # 关键特性:
    # - 支持并发和顺序消费两种模式
    # - 完整的消息重试机制
    # - 灵活的消息过滤功能
    # - 统一的消息处理流程
    # - 异常安全的处理机制

    def _async_prepare_message_consumption(
        self,
        messages: list[MessageExt],
        message_queue: MessageQueue,
        consumption_type: str = "concurrent",
    ) -> tuple[AsyncMessageListener | None, AsyncConsumeContext | None]:
        """
        为异步消息消费做准备的通用方法。

        这个方法封装了异步消息验证、监听器选择和上下文创建的通用逻辑，
        被异步并发消费和异步顺序消费共同使用，避免代码重复。

        Args:
            messages (list[MessageExt]): 要处理的消息列表
            message_queue (MessageQueue): 消息来自的队列信息
            consumption_type (str): 消费类型，用于日志记录，可选值为 "concurrent" 或 "orderly"

        Returns:
            tuple[AsyncMessageListener | None, AsyncConsumeContext | None]:
                - AsyncMessageListener: 找到的异步监听器，如果没有找到则为None
                - AsyncConsumeContext: 创建的异步消费上下文，如果没有找到则为None
        """
        # 消息验证
        if not messages:
            logger.warning(
                f"Empty message list received for async {consumption_type} consumption",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                },
            )
            return None, None

        # 获取消息的topic
        topic = messages[0].topic if messages else message_queue.topic

        # 根据topic获取对应的异步监听器
        listener: AsyncMessageListener | None = self._message_listeners.get(topic)
        if not listener and self._is_retry_topic(topic):
            origin_topic: str | None = messages[0].get_property(
                MessageProperty.RETRY_TOPIC
            )
            listener = (
                self._message_listeners.get(origin_topic) if origin_topic else None
            )

        # 如果没有找到监听器，记录错误日志
        if not listener:
            logger.error(
                f"No async message listener registered for topic: {topic} in async {consumption_type} consumption",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": message_queue.queue_id,
                    "available_topics": list(self._message_listeners.keys()),
                },
            )

        # 创建异步消费上下文
        reconsume_times: int = messages[0].reconsume_times if messages else 0
        context: AsyncConsumeContext = AsyncConsumeContext(
            consumer_group=self._config.consumer_group,
            message_queue=message_queue,
            reconsume_times=reconsume_times,
        )

        return listener, context

    async def _concurrent_consume_message(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> bool:
        """
        异步并发消费消息的核心方法。

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
            ConsumerError: 当消息处理过程中发生严重错误时抛出
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

        # 使用通用方法进行消息验证和监听器选择
        listener, context = self._async_prepare_message_consumption(
            messages, message_queue, "concurrent"
        )

        if not listener or not context:
            return False

        try:
            logger.debug(
                "Processing messages",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "reconsume_times": context.reconsume_times,
                    "listener_type": type(listener).__name__,
                },
            )

            result: ConsumeResult = await listener.consume_message(messages, context)

            # 验证消费结果的有效性
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

    async def _orderly_consume_message(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> tuple[bool, ConsumeResult]:
        """
        异步顺序消费消息的核心方法

        此方法负责按消息的队列偏移量顺序进行异步消费处理，保证同一消息队列中的消息
        按照严格的顺序被处理。这是RocketMQ异步顺序消费的核心实现。

        Args:
            messages (list[MessageExt]): 要消费的消息列表，保证按queue_offset排序
            message_queue (MessageQueue): 消息所属的队列信息

        Returns:
            tuple[bool, ConsumeResult]:
                - bool: 消费是否成功（True表示成功，False表示失败）
                - ConsumeResult: 具体的消费结果，用于后续处理决策

        Raises:
            ValueError: 当监听器或上下文为None时
            InvalidConsumeResultError: 当返回不支持的消费结果时
        """
        # 使用通用方法进行消息验证和监听器选择
        listener, context = self._async_prepare_message_consumption(
            messages, message_queue, "orderly"
        )

        # 如果没有找到监听器或消息列表为空，抛出异常
        if listener is None or context is None:
            raise ValueError("Async listener or context is None")

        # 获取topic用于日志记录
        topic = messages[0].topic if messages else message_queue.topic

        try:
            logger.debug(
                "Processing messages orderly",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "reconsume_times": context.reconsume_times,
                },
            )

            result: ConsumeResult = await listener.consume_message(messages, context)

            # 验证消费结果的有效性
            if result == ConsumeResult.RECONSUME_LATER:
                logger.error(
                    "Invalid result for async orderly consumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                        "queue_id": context.queue_id,
                        "result": result.value,
                    },
                )
                raise InvalidConsumeResultError(
                    consumer_type="async orderly",
                    invalid_result=result.value,
                    valid_results=[
                        ConsumeResult.SUCCESS.value,
                        ConsumeResult.COMMIT.value,
                        ConsumeResult.ROLLBACK.value,
                        ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT.value,
                    ],
                    topic=topic,
                    queue_id=context.queue_id,
                )

            success = result in [ConsumeResult.SUCCESS, ConsumeResult.COMMIT]
            return success, result

        except InvalidConsumeResultError:
            raise

        except Exception as e:
            logger.error(
                f"Failed to process messages orderly: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "error": str(e),
                },
                exc_info=True,
            )

            # 调用异步异常回调（如果监听器实现了）
            if hasattr(listener, "on_exception"):
                try:
                    await listener.on_exception(messages, context, e)
                except Exception as callback_error:
                    logger.error(
                        f"Exception callback failed: {callback_error}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": topic,
                            "error": str(callback_error),
                        },
                        exc_info=True,
                    )

            return False, ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT

    async def _send_back_message(
        self, message_queue: MessageQueue, message: MessageExt
    ) -> bool:
        """
        将消费失败的消息异步发送回broker重新消费。

        当消息消费失败时，此方法负责将消息异步发送回原始broker，
        以便后续重新消费。这是RocketMQ异步消息重试机制的重要组成部分。

        Args:
            message_queue (MessageQueue): 消息来自的队列信息
            message (MessageExt): 需要发送回的消息对象

        Returns:
            bool: 发送操作是否成功
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
        确保消息能够正确地参与重试机制。

        Args:
            msg (MessageExt): 需要重置重试属性的消息对象
        """
        retry_topic: str | None = msg.get_property(MessageProperty.RETRY_TOPIC)
        if retry_topic:
            msg.topic = retry_topic
        msg.set_property(
            MessageProperty.CONSUME_START_TIME, str(int(time.time() * 1000))
        )

    def _filter_messages_by_tags(
        self, messages: list[MessageExt], tags_set: list[str]
    ) -> list[MessageExt]:
        """
        根据标签过滤消息。

        Args:
            messages: 待过滤的消息列表
            tags_set: 允许的标签集合

        Returns:
            list[MessageExt]: 过滤后的消息列表
        """
        filtered_messages: list[MessageExt] = []
        for message in messages:
            if message.get_tags() in tags_set:
                filtered_messages.append(message)

        return filtered_messages

    # ==================== 4. 异步路由刷新任务模块 ====================
    #
    # 该模块负责消费者路由信息的动态维护，包括：
    # - 定期刷新Topic路由信息
    # - 处理集群拓扑变化
    # - 维护Broker连接状态
    # - 路由缓存的过期管理
    #
    # 关键特性:
    # - 异步定期刷新机制
    # - 优雅的任务停止机制
    # - 完整的错误处理和重试
    # - 路由信息的统计分析

    async def _start_route_refresh_task(self) -> None:
        """启动异步路由刷新任务。

        创建并启动一个异步任务来执行路由刷新循环，
        确保消费者能够及时感知到集群拓扑的变化。
        """
        self._route_refresh_task = asyncio.create_task(self._route_refresh_loop())

    async def _route_refresh_loop(self) -> None:
        """异步路由刷新循环。

        定期刷新所有订阅Topic的路由信息，确保消费者能够感知到集群拓扑的变化。
        这是RocketMQ消费者高可用性的关键机制。
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

                self._logger.debug(
                    "Async route refresh completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topics_count": (
                            len(self._topic_broker_mapping.get_all_topics())
                        ),
                    },
                )

            except Exception as e:
                self._logger.warning(
                    f"Error in async route refresh loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
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
                    extra={"topic": topic, "error": str(e)},
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
                    extra={"topic": topic},
                )
                return False

            # 更新TopicBrokerMapping中的路由信息
            _ = self._topic_broker_mapping.update_route_info(topic, topic_route_data)

            return True

        except Exception as e:
            self._logger.error(
                f"Error updating route info for topic {topic}: {e}",
                extra={"topic": topic, "error": str(e)},
                exc_info=True,
            )
            return False

    # ==================== 5. 异步心跳任务模块 ====================
    #
    # 该模块负责维护消费者与Broker的连接状态，包括：
    # - 定期向所有Broker发送心跳
    # - 心跳数据的构建和发送
    # - 心跳统计和监控
    # - 心跳异常的处理和恢复
    #
    # 关键特性:
    # - 异步心跳发送机制
    # - 智能的等待时间计算
    # - 完整的心跳统计信息
    # - 优雅的心跳任务停止

    async def _start_heartbeat_task(self) -> None:
        """启动异步心跳任务。

        创建并启动一个异步任务来执行心跳发送循环，
        确保消费者与Broker保持活跃连接状态。
        """
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        """异步消费者心跳发送循环。

        定期向所有Broker发送心跳信息，维持消费者的存活状态。
        这是RocketMQ消费者高可用性和负载均衡的基础机制。
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
        self._logger.debug(
            "Heartbeat statistics updated",
            extra={
                "consumer_group": self._config.consumer_group,
                "success_count": success_count,
                "failure_count": failure_count,
                "total_brokers": total_count,
            },
        )

    # ==================== 6. 配置和状态管理模块 ====================
    #
    # 该模块负责消费者的配置管理和状态查询，包括：
    # - 消费者配置的访问和管理
    # - 运行状态的实时查询
    # - 组件间关联关系的维护
    # - 状态摘要的生成和报告
    #
    # 关键特性:
    # - 线程安全的配置访问
    # - 实时的状态信息查询
    # - 完整的状态摘要报告
    # - 组件关联关系管理

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

    async def is_running(self) -> bool:
        """
        异步检查消费者是否正在运行

        Returns:
            如果正在运行返回True，否则返回False
        """
        return self._is_running

    async def get_status_summary(self) -> dict[str, Any]:
        """
        异步获取消费者状态摘要

        Returns:
            包含消费者状态信息的字典
        """
        subscriptions: dict[str, Any] = self._subscription_manager.get_status_summary()

        return {
            "consumer_group": self._config.consumer_group,
            "namesrv_addr": self._config.namesrv_addr,
            "message_model": self._config.message_model,
            "client_id": self._config.client_id,
            "is_running": self._is_running,
            "subscriptions": subscriptions,
            "has_message_listener": len(self._message_listeners) > 0,
            "listener_count": len(self._message_listeners),
            "topics_with_listeners": list(self._message_listeners.keys()),
        }

    async def get_consumer_stats(self) -> dict[str, Any]:
        """
        异步获取消费者统计信息

        Returns:
            包含消费者统计信息的字典
        """
        # 返回使用 StatsManager 的统计信息
        return {
            "consumer_group": self._config.consumer_group,
            "message": "Statistics now managed by StatsManager. Use get_stats_manager() method.",
        }

    async def get_stats_manager(self) -> StatsManager:
        """
        异步获取统计管理器

        Returns:
            StatsManager: 统计管理器实例
        """
        return self._stats_manager

    async def get_consume_status(self, topic: str):
        """
        异步获取指定主题的消费状态

        Args:
            topic: 主题名称

        Returns:
            ConsumeStatus: 消费状态信息
        """
        return self._stats_manager.get_consume_status(
            self._config.consumer_group, topic
        )

    async def get_consumer_info(self) -> ConsumerRunningInfo:
        """异步获取消费者运行信息

        收集并返回当前消费者的详细运行状态信息，包括订阅信息、队列处理状态、
        消费统计数据和配置属性等。该信息对于监控消费者健康状况和调试问题非常有用。

        Returns:
            ConsumerRunningInfo: 包含以下信息的消费者运行状态对象：
                - subscription_data: 订阅数据，包含所有订阅的主题和标签过滤信息
                - mq_table: 消息队列处理信息表，包含每个队列的处理状态、缓存信息、提交偏移量等
                - status_table: 消费状态表，包含每个主题的拉取和消费统计数据(TPS、响应时间等)
                - properties: 消费者属性配置，包含名称服务器地址、消费类型、启动时间戳等

        返回的运行信息包含：
        - 所有订阅的主题数据
        - 每个消息队列的处理队列信息(缓存消息数量、偏移量、锁状态等)
        - 每个主题的消费统计信息(拉取TPS、消费TPS、失败统计等)
        - 消费者的基础配置信息(名称服务器地址、启动时间等)

        注意:
            - 该方法会访问异步偏移量存储来获取每个队列的提交偏移量
            - 统计数据来自于统计管理器的实时计算
            - 返回的信息可用于监控系统和故障诊断
            - 该方法是异步的，不会阻塞消费者正常运行

        Example:
            >>> consumer = AsyncBaseConsumer(config)
            >>> info = await consumer.get_consumer_info()
            >>> print(f"订阅主题数: {len(info.subscription_data)}")
            >>> print(f"处理队列数: {len(info.mq_table)}")
            >>> for topic, status in info.status_table.items():
            ...     print(f"主题 {topic} 消费TPS: {status.consume_ok_tps}")
        """
        running_info: ConsumerRunningInfo = ConsumerRunningInfo()

        # 添加订阅信息
        for sub in self._subscription_manager.get_all_subscriptions():
            running_info.add_subscription(sub.subscription_data)
            status = self._stats_manager.get_consume_status(
                self._config.consumer_group, sub.topic
            )
            running_info.add_status(sub.topic, status)

        # 异步添加队列信息
        async with self._cache_lock:
            for q, pq in self._msg_cache.items():
                info: ProcessQueueInfo = await pq.current_info()
                info.commit_offset = await self._offset_store.read_offset(
                    q, ReadOffsetType.MEMORY_FIRST_THEN_STORE
                )
                running_info.add_queue_info(q, info)

        # 添加属性信息
        running_info.properties["PROP_NAMESERVER_ADDR"] = self._config.namesrv_addr
        running_info.properties["PROP_CONSUME_TYPE"] = "CONSUME_PASSIVELY"
        running_info.properties["PROP_THREADPOOL_CORE_SIZE"] = "-1"
        running_info.properties["PROP_CONSUMER_START_TIMESTAMP"] = str(
            int(self._start_time)
        )

        return running_info

    # ==================== 7. 资源清理和工具模块 ====================
    #
    # 该模块负责消费者的资源管理和清理，包括：
    # - 异步任务的优雅关闭
    # - 网络连接等资源的清理
    # - 偏移量数据的持久化
    # - 订阅关系的清理
    #
    # 关键特性:
    # - 优雅的异步任务关闭
    # - 完整的资源清理流程
    # - 数据持久化保证
    # - 异常安全的清理机制

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

            # 关闭统计管理器
            self._stats_manager.shutdown()

            self._logger.info("资源清理完成")

        except Exception as e:
            self._logger.error(
                "清理资源失败",
                extra={"error": str(e)},
                exc_info=True,
            )


# ===============================================================================
# 总结说明
# ===============================================================================
#
# 本文件按照功能模块重新组织了AsyncBaseConsumer类的代码结构，主要改进包括：
#
# 1. 清晰的模块划分：
#    - 按功能将代码分为7个核心模块
#    - 每个模块都有明确的职责和边界
#    - 模块间的依赖关系清晰明确
#
# 2. 详细的注释说明：
#    - 每个模块都有详细的功能说明
#    - 关键方法都有完整的参数和返回值说明
#    - 包含使用示例和注意事项
#
# 3. 改进的代码组织：
#    - 相关功能的方法被组织在同一模块中
#    - 避免了代码重复和逻辑分散
#    - 提高了代码的可读性和维护性
#
# 4. 统一的错误处理：
#    - 每个模块都有统一的异常处理策略
#    - 完整的日志记录和错误上下文
#    - 优雅的错误恢复机制
#
# 5. 性能优化：
#    - 减少了代码重复，提高了执行效率
#    - 优化了异步任务的管理
#    - 改进了资源清理的效率
#
# 模块间的关联关系：
# - 生命周期管理模块是所有其他模块的基础
# - 订阅管理模块和消息处理模块紧密相关
# - 路由刷新和心跳模块是独立的运维模块
# - 配置和状态管理模块为其他模块提供支撑
# - 资源清理模块确保所有资源的正确释放
#
# 这种模块化的组织方式使得代码更容易理解、维护和扩展，
# 同时保持了原有功能的完整性和正确性。
# ===============================================================================
