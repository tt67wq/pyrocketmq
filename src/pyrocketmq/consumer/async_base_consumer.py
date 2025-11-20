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
from pyrocketmq.consumer.async_offset_store import AsyncOffsetStore
from pyrocketmq.consumer.async_offset_store_factory import AsyncOffsetStoreFactory
from pyrocketmq.consumer.topic_broker_mapping import ConsumerTopicBrokerMapping
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    BrokerData,
    ConsumerData,
    HeartbeatData,
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
        self._nameserver_manager: AsyncNameServerManager = (
            create_async_nameserver_manager(self._config.namesrv_addr)
        )

        remote_config: RemoteConfig = RemoteConfig(
            connection_pool_size=16, connection_max_lifetime=60
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
        self._route_refresh_interval: float = 30000.0  # 30秒
        self._heartbeat_interval: float = 30000.0  # 30秒
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

                success: bool = self._subscription_manager.subscribe(topic, selector)
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
                success: bool = self._subscription_manager.unsubscribe(topic)
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
        """启动异步路由刷新任务"""
        self._route_refresh_task = asyncio.create_task(self._route_refresh_loop())

    async def _route_refresh_loop(self) -> None:
        """异步路由刷新循环

        定期刷新所有订阅Topic的路由信息，确保消费者能够感知到集群拓扑的变化。
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
                        timeout=self._route_refresh_interval / 1000,
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
            extra={
                "topic": topic,
            },
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
        """启动异步心跳任务"""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        """异步消费者心跳发送循环"""
        self._logger.info("Async heartbeat loop started")

        await self._send_heartbeat_to_all_brokers()
        self._last_heartbeat_time = time.time()

        while self._is_running:
            try:
                current_time = time.time()

                # 计算到下一次心跳的等待时间
                time_until_next_heartbeat = self._heartbeat_interval - (
                    current_time - self._last_heartbeat_time
                )

                # 如果还没到心跳时间，等待一小段时间或直到被唤醒
                if time_until_next_heartbeat > 0:
                    # 使用Event.wait()替代asyncio.sleep()
                    wait_timeout = min(time_until_next_heartbeat, 1.0)  # 最多等待1秒
                    try:
                        await asyncio.wait_for(
                            self._heartbeat_event.wait(), timeout=wait_timeout
                        )
                        # Event被触发，检查是否需要退出
                        if not self._is_running:
                            break
                        # 重置事件状态
                        self._heartbeat_event.clear()
                        continue  # 重新计算等待时间
                    except asyncio.TimeoutError:
                        pass  # 超时继续执行心跳逻辑

                # 检查是否需要发送心跳
                current_time = time.time()  # 重新获取当前时间
                if current_time - self._last_heartbeat_time >= self._heartbeat_interval:
                    await self._send_heartbeat_to_all_brokers()
                    self._last_heartbeat_time = current_time

            except Exception as e:
                self._logger.error(
                    f"Error in async heartbeat loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # 等待一段时间再重试
                try:
                    await asyncio.wait_for(self._heartbeat_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        self._logger.info("Async heartbeat loop stopped")

    async def _send_heartbeat_to_all_brokers(self) -> None:
        """异步向所有Broker发送心跳"""
        self._logger.debug("Sending heartbeat to all brokers...")

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
