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
from abc import ABC, abstractmethod
from typing import Any

# pyrocketmq导入
from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.consumer.async_offset_store import AsyncOffsetStore
from pyrocketmq.consumer.async_offset_store_factory import AsyncOffsetStoreFactory
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    ConsumeResult,
    MessageExt,
    MessageSelector,
)
from pyrocketmq.nameserver.async_manager import (
    AsyncNameServerManager,
    create_async_nameserver_manager,
)
from pyrocketmq.remote.config import RemoteConfig

from .async_consume_from_where_manager import AsyncConsumeFromWhereManager
from .async_listener import AsyncConsumeContext, AsyncMessageListener

# 本地模块导入
from .config import ConsumerConfig
from .errors import ConsumerError, SubscribeError, UnsubscribeError
from .subscription_manager import SubscriptionManager

logger = get_logger(__name__)


class AsyncBaseConsumer(ABC):
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
        _message_listener: 异步消息监听器实例
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
        self._message_listener: AsyncMessageListener | None = None
        self._is_running: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

        # 异步管理器
        self._nameserver_manager: AsyncNameServerManager | None = None
        self._broker_manager: AsyncBrokerManager | None = None
        self._offset_store: AsyncOffsetStore | None = None
        self._consume_from_where_manager: AsyncConsumeFromWhereManager | None = None

        # 异步任务
        self._route_refresh_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

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

    @abstractmethod
    async def start(self) -> None:
        """
        异步启动消费者

        异步启动消费者，开始消费消息。这是一个抽象方法，需要由具体的
        消费者实现类来提供具体的启动逻辑。

        Raises:
            ConsumerError: 启动过程中发生错误时抛出
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """
        异步关闭消费者

        异步关闭消费者，停止消费消息并清理资源。这是一个抽象方法，
        需要由具体的消费者实现类来提供具体的关闭逻辑。

        Raises:
            ConsumerError: 关闭过程中发生错误时抛出
        """
        pass

    @abstractmethod
    async def _consume_message(
        self, messages: list[MessageExt], context: AsyncConsumeContext
    ) -> ConsumeResult:
        """
        异步消费消息

        异步消费一批消息，需要由具体实现类来提供具体的消费逻辑。
        这是一个抽象方法，定义了消费消息的核心接口。

        Args:
            messages: 需要消费的消息列表
            context: 异步消费上下文

        Returns:
            消费结果，决定消息如何被确认或重试

        Raises:
            ConsumerError: 消费过程中发生错误时抛出
        """
        pass

    async def subscribe(self, topic: str, selector: MessageSelector) -> None:
        """
        异步订阅Topic

        异步订阅指定Topic并设置消息选择器。支持基于TAG和SQL92的消息过滤。

        Args:
            topic: 要订阅的Topic名称
            selector: 消息选择器，用于过滤消息

        Raises:
            SubscribeError: 订阅失败时抛出
            ValueError: 参数无效时抛出
        """
        try:
            async with self._lock:
                if not self._subscription_manager.validate_subscription(
                    topic, selector
                ):
                    raise SubscribeError(
                        topic, f"无效的订阅参数: topic={topic}, selector={selector}"
                    )

                success = self._subscription_manager.subscribe(topic, selector)
                if not success:
                    raise SubscribeError(topic, f"订阅失败: topic={topic}")

                self._logger.info(
                    "订阅Topic成功",
                    extra={
                        "topic": topic,
                        "selector": str(selector),
                        "consumer_group": self._config.consumer_group,
                    },
                )

        except Exception as e:
            self._logger.error(
                "订阅Topic失败",
                extra={
                    "topic": topic,
                    "selector": str(selector),
                    "error": str(e),
                },
                exc_info=True,
            )
            raise SubscribeError(topic, f"订阅失败: {e}", e) from e

    async def unsubscribe(self, topic: str) -> None:
        """
        异步取消订阅Topic

        异步取消指定Topic的订阅。

        Args:
            topic: 要取消订阅的Topic名称

        Raises:
            SubscribeError: 取消订阅失败时抛出
        """
        try:
            async with self._lock:
                success = self._subscription_manager.unsubscribe(topic)
                if not success:
                    raise UnsubscribeError(topic, f"取消订阅失败: topic={topic}")

                self._logger.info(
                    "取消订阅Topic成功",
                    extra={
                        "topic": topic,
                        "consumer_group": self._config.consumer_group,
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

    async def register_message_listener(self, listener: AsyncMessageListener) -> None:
        """
        异步注册消息监听器

        异步注册消息监听器，用于处理消费到的消息。

        Args:
            listener: 消息监听器实例

        Raises:
            ValueError: 当listener为None时抛出
        """
        async with self._lock:
            self._message_listener = listener
            self._logger.info(
                "注册消息监听器成功",
                extra={
                    "listener_type": type(listener).__name__,
                    "consumer_group": self._config.consumer_group,
                },
            )

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

    async def get_message_listener(self) -> AsyncMessageListener | None:
        """
        异步获取消息监听器

        Returns:
            消息监听器实例，如果未注册则返回None
        """
        return self._message_listener

    async def get_status_summary(self) -> dict[str, Any]:
        """
        异步获取消费者状态摘要

        Returns:
            包含消费者状态信息的字典
        """
        subscriptions = self._subscription_manager.get_status_summary()

        return {
            "consumer_group": self._config.consumer_group,
            "namesrv_addr": self._config.namesrv_addr,
            "message_model": self._config.message_model,
            "client_id": self._config.client_id,
            "is_running": self._is_running,
            "start_time": self._start_time,
            "shutdown_time": self._shutdown_time,
            "uptime": time.time() - self._start_time if self._is_running else 0,
            "subscriptions": subscriptions,
            "has_message_listener": self._message_listener is not None,
        }

    async def _async_start(self) -> None:
        """
        异步启动基础组件

        异步启动NameServer管理器、Broker管理器、偏移量存储等基础组件。
        这是所有异步消费者启动过程中的通用逻辑。

        Raises:
            ConsumerError: 启动基础组件失败时抛出
        """
        try:
            self._logger.info("开始启动异步消费者基础组件")

            # 启动NameServer管理器
            self._nameserver_manager = create_async_nameserver_manager(
                self._config.namesrv_addr
            )
            await self._nameserver_manager.start()
            self._logger.info("NameServer管理器启动成功")

            # 启动Broker管理器
            remote_config = RemoteConfig(
                connection_pool_size=16, connection_max_lifetime=60
            )
            self._broker_manager = AsyncBrokerManager(remote_config=remote_config)
            await self._broker_manager.start()
            self._logger.info("Broker管理器启动成功")

            # 创建偏移量存储
            self._offset_store = await AsyncOffsetStoreFactory.create_offset_store(
                consumer_group=self._config.consumer_group,
                message_model=self._config.message_model,
                namesrv_manager=self._nameserver_manager,
                broker_manager=self._broker_manager,
                store_path=self._config.offset_store_path,
                persist_interval=self._config.persist_interval,
                auto_start=True,
            )
            self._logger.info("偏移量存储创建成功")

            # 创建消费起始位置管理器
            self._consume_from_where_manager = AsyncConsumeFromWhereManager(
                consume_group=self._config.consumer_group,
                namesrv_manager=self._nameserver_manager,
                broker_manager=self._broker_manager,
            )
            self._logger.info("消费起始位置管理器创建成功")

            self._start_time = time.time()
            self._is_running = True

            self._logger.info("异步消费者基础组件启动完成")

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
        """
        异步关闭基础组件

        异步关闭NameServer管理器、Broker管理器、偏移量存储等基础组件。
        这是所有异步消费者关闭过程中的通用逻辑。

        Raises:
            ConsumerError: 关闭基础组件失败时抛出
        """
        try:
            self._logger.info("开始关闭异步消费者基础组件")

            self._is_running = False
            self._shutdown_time = time.time()

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
            if self._offset_store is not None:
                await self._offset_store.stop()
                self._offset_store = None

            # 关闭Broker管理器
            if self._broker_manager is not None:
                # 等待连接池关闭
                await self._broker_manager.shutdown()
                self._broker_manager = None

            # 关闭NameServer管理器
            if self._nameserver_manager is not None:
                await self._nameserver_manager.stop()
                self._nameserver_manager = None

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
        return (
            f"AsyncBaseConsumer["
            f"group={self._config.consumer_group}, "
            f"running={self._is_running}, "
            f"subscriptions={len(self._subscription_manager.get_all_subscriptions())}"
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
