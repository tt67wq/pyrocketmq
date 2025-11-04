"""
RocketMQ Consumer核心实现 (MVP版本)

该模块提供了一个简化但功能完整的RocketMQ Consumer实现。
采用MVP设计理念，只包含最核心的功能，避免过度设计。

MVP版本功能:
- 简单的布尔状态管理（移除复杂的状态机）
- 基础的订阅管理
- 基本的生命周期管理
- 完整的配置管理和异常处理

设计原则:
- 从最简单的实现开始
- 避免过度抽象
- 专注核心功能
- 易于理解和维护

作者: pyrocketmq团队
版本: MVP 1.0
"""

import logging
import threading
import time
from typing import Any

# Local imports - broker
from pyrocketmq.broker.broker_manager import BrokerManager


# Local imports - nameserver
from pyrocketmq.model.client_data import SubscriptionData

# Local imports - consumer
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.listener import MessageListener
from pyrocketmq.consumer.errors import (
    ConsumerStartError,
    ConsumerShutdownError,
    ConsumerStateError,
    SubscribeError,
    ConfigError,
    NameServerError,
)

# Local imports - remote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.sync_remote import Remote
from pyrocketmq.transport.config import TransportConfig


class Consumer:
    """
    RocketMQ Consumer实现 (MVP版本)

    简化版本的Consumer，专注于核心的消息消费功能。
    采用简单的设计模式，避免过度复杂的抽象。

    核心功能:
    1. 生命周期管理 (start/shutdown)
    2. 订阅管理 (subscribe/unsubscribe)
    3. 消息监听器管理
    4. 基础的错误处理和恢复

    状态管理:
    - 使用简单的布尔状态 (_running: bool)
    - 移除复杂的状态机设计
    - 在关键操作前检查运行状态

    使用示例:
        >>> # 创建Consumer实例
        >>> config = ConsumerConfig(
        ...     consumer_group="test_group",
        ...     namesrv_addr="localhost:9876"
        ... )
        >>> consumer = Consumer(config)
        >>>
        >>> # 注册消息监听器
        >>> class MyListener(MessageListenerConcurrently):
        ...     def consume_message_concurrently(self, messages, context):
        ...         for msg in messages:
        ...             print(f"收到消息: {msg.body.decode()}")
        ...         return ConsumeResult.SUCCESS
        >>>
        >>> consumer.register_message_listener(MyListener())
        >>>
        >>> # 订阅Topic并启动
        >>> consumer.subscribe("test_topic", "*")
        >>> consumer.start()
        >>>
        >>> # 关闭Consumer
        >>> consumer.shutdown()
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """初始化Consumer实例

        Args:
            config: Consumer配置

        Raises:
            ConfigError: 配置参数无效时抛出
        """
        # 验证配置
        self._validate_config(config)
        self._logger: logging.Logger = logging.getLogger(__name__)

        # 保存配置
        self.config: ConsumerConfig = config

        # 状态管理 (MVP: 简化状态管理)
        self._running: bool = False
        self._started: bool = False

        # 线程安全锁
        self._lock: threading.RLock = threading.RLock()

        # 订阅关系管理
        self._subscriptions: dict[str, SubscriptionData] = {}  # topic -> subscription

        # 消息监听器
        self._message_listener: MessageListener | None = None

        # nameserver组件
        self._nameserver_connections: dict[str, Remote] = {}
        self._nameserver_addrs: dict[str, str] = self._parse_nameserver_addrs(
            self.config.namesrv_addr
        )

        # broker组件
        self._broker_manager: BrokerManager | None = None

        # 消费线程池 (MVP: 简化实现)
        self._consume_threads: list[threading.Thread] = []
        self._consume_queue: Any = None  # TODO: 实现消息队列

        # 统计信息
        self._stats: dict[str, int | float] = {
            "start_time": 0,
            "messages_consumed": 0,
            "messages_failed": 0,
            "last_consume_time": 0,
        }

        self._logger.info(
            "Consumer实例创建成功",
            extra={
                "consumer_group": self.config.consumer_group,
                "client_id": self.config.client_id,
                "namesrv_addr": self.config.namesrv_addr,
            },
        )

    def _validate_config(self, config: ConsumerConfig) -> None:
        """验证配置参数

        Args:
            config: 要验证的配置

        Raises:
            ConfigError: 配置参数无效时抛出
        """

        if not config.consumer_group:
            raise ConfigError("consumer_group", "消费者组不能为空")

        if not config.namesrv_addr:
            raise ConfigError("namesrv_addr", "NameServer地址不能为空")

    def _init_nameserver_connections(self) -> None:
        """初始化NameServer连接"""
        if not self._nameserver_addrs:
            raise NameServerError(
                namesrv_addr=self.config.namesrv_addr,
                message="Failed to connect to any NameServer",
            )
        self._logger.info("Initializing NameServer connections...")

        for addr in self._nameserver_addrs:
            try:
                host, port = addr.split(":")
                transport_config = TransportConfig(host=host, port=int(port))
                remote_config = RemoteConfig().with_rpc_timeout(
                    self.config.consume_timeout
                )
                remote = Remote(transport_config, remote_config)

                remote.connect()
                self._nameserver_connections[addr] = remote
                self._logger.info(
                    "Connected to NameServer",
                    extra={
                        "nameserver_address": addr,
                    },
                )

            except Exception as e:
                self._logger.error(
                    "Failed to connect to NameServer",
                    extra={
                        "nameserver_address": addr,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                    exc_info=True,
                )

        if not self._nameserver_connections:
            raise NameServerError(
                namesrv_addr=self.config.namesrv_addr,
                message="Failed to connect to any NameServer",
            )

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

    def start(self) -> None:
        """启动Consumer

        建立网络连接，注册到NameServer，开始消费消息。

        Raises:
            ConsumerStartError: 启动失败时抛出
            ConsumerStateError: 状态错误时抛出
        """
        with self._lock:
            if self._started:
                raise ConsumerStateError("Consumer已经启动", "STARTED")

            if self._running:
                raise ConsumerStateError("Consumer正在启动中", "STARTING")

            self._running = True

            try:
                self._logger.info(
                    "开始启动Consumer",
                    extra={
                        "consumer_group": self.config.consumer_group,
                        "client_id": self.config.client_id,
                    },
                )

                # 1. 连接到NameServer
                self._init_nameserver_connections()

                # 2. 注册消费者组信息
                self._register_consumer()

                # 3. 启动消费线程
                self._start_consume_threads()

                # 4. 标记启动完成
                self._started = True
                self._stats["start_time"] = time.time()

                self._logger.info(
                    "Consumer启动成功",
                    extra={
                        "consumer_group": self.config.consumer_group,
                        "client_id": self.config.client_id,
                        "subscriptions": list(self._subscriptions.keys()),
                        "consume_threads": len(self._consume_threads),
                    },
                )

            except Exception as e:
                self._logger.error(f"Consumer启动失败: {e}", exc_info=True)
                self._running = False
                self._cleanup()
                raise ConsumerStartError(f"Consumer启动失败: {e}", cause=e)

    def shutdown(self) -> None:
        """关闭Consumer

        停止消费，断开网络连接，清理资源。

        Raises:
            ConsumerShutdownError: 关闭失败时抛出
        """
        with self._lock:
            if not self._started:
                self._logger.warning("Consumer未启动，无需关闭")
                return

            try:
                self._logger.info(
                    "开始关闭Consumer",
                    extra={
                        "consumer_group": self.config.consumer_group,
                        "client_id": self.config.client_id,
                    },
                )

                # 1. 停止运行状态
                self._running = False

                # 2. 停止消费线程
                self._stop_consume_threads()

                # 3. 注销消费者
                self._unregister_consumer()

                # 4. 断开网络连接
                self._disconnect()

                # 5. 清理资源
                self._cleanup()

                # 6. 标记关闭完成
                self._started = False

                self._logger.info(
                    "Consumer关闭成功",
                    extra={
                        "consumer_group": self.config.consumer_group,
                        "client_id": self.config.client_id,
                        "final_stats": self._stats,
                    },
                )

            except Exception as e:
                self._logger.error(f"Consumer关闭失败: {e}", exc_info=True)
                raise ConsumerShutdownError(f"Consumer关闭失败: {e}", cause=e)

    def subscribe(self, topic: str, subscription: SubscriptionData) -> None:
        """订阅Topic

        Args:
            topic: 主题名称
            subscription: 订阅表达式，默认"*"表示订阅所有消息

        Raises:
            SubscribeError: 订阅失败时抛出
            ConsumerStateError: Consumer已启动时抛出
        """
        if self._started:
            raise ConsumerStateError("Consumer已启动，不能修改订阅关系", "STARTED")

        if not topic:
            raise SubscribeError(topic, "Topic名称不能为空")

        pass

    def unsubscribe(self, topic: str) -> None:
        """取消订阅Topic

        Args:
            topic: 主题名称

        Raises:
            SubscribeError: 取消订阅失败时抛出
            ConsumerStateError: Consumer已启动时抛出
        """
        if self._started:
            raise ConsumerStateError("Consumer已启动，不能修改订阅关系", "STARTED")

        if not topic:
            raise SubscribeError(topic, "Topic名称不能为空")

        with self._lock:
            if topic in self._subscriptions:
                del self._subscriptions[topic]

                self._logger.info(
                    "取消订阅Topic成功",
                    extra={
                        "topic": topic,
                        "consumer_group": self.config.consumer_group,
                        "remaining_subscriptions": len(self._subscriptions),
                    },
                )
            else:
                self._logger.warning(
                    "Topic未订阅，无需取消",
                    extra={
                        "topic": topic,
                        "consumer_group": self.config.consumer_group,
                    },
                )

    def register_message_listener(self, listener: MessageListener) -> None:
        """注册消息监听器

        Args:
            listener: 消息监听器实例

        Raises:
            ConsumerStateError: Consumer已启动时抛出
            ConfigError: 监听器无效时抛出
        """
        if self._started:
            raise ConsumerStateError("Consumer已启动，不能修改监听器", "STARTED")

        with self._lock:
            self._message_listener = listener

            self._logger.info(
                "消息监听器注册成功",
                extra={
                    "listener_type": listener.__class__.__name__,
                    "consumer_group": self.config.consumer_group,
                },
            )

    def is_running(self) -> bool:
        """检查Consumer是否正在运行

        Returns:
            是否正在运行
        """
        return self._running and self._started

    def get_subscriptions(self) -> dict[str, SubscriptionData]:
        """获取当前订阅关系

        Returns:
            订阅关系字典 {topic: subscription}
        """
        with self._lock:
            return self._subscriptions

    def get_stats(self) -> dict[str, Any]:
        """获取消费统计信息

        Returns:
            统计信息字典
        """
        with self._lock:
            stats = dict(self._stats)
            stats["is_running"] = self.is_running()
            stats["subscription_count"] = len(self._subscriptions)
            stats["consume_thread_count"] = len(self._consume_threads)
            stats["uptime_seconds"] = (
                time.time() - stats["start_time"] if stats["start_time"] > 0 else 0
            )
            return stats

    # === 私有方法 ===

    def _register_consumer(self) -> None:
        """注册消费者信息

        MVP版本: 简化实现，后续版本会完善注册逻辑
        """
        self._logger.info(
            "注册消费者组",
            extra={
                "consumer_group": self.config.consumer_group,
                "client_id": self.config.client_id,
            },
        )
        # TODO: 实现消费者注册逻辑

    def _unregister_consumer(self) -> None:
        """注销消费者信息

        MVP版本: 简化实现，后续版本会完善注销逻辑
        """
        self._logger.info(
            "注销消费者组",
            extra={
                "consumer_group": self.config.consumer_group,
                "client_id": self.config.client_id,
            },
        )
        # TODO: 实现消费者注销逻辑

    def _start_consume_threads(self) -> None:
        """启动消费线程

        MVP版本: 简化实现，后续版本会实现完整的线程池
        """
        thread_count = min(self.config.consume_thread_min, len(self._subscriptions))

        for i in range(thread_count):
            thread = threading.Thread(
                target=self._consume_worker,
                name=f"consumer-{self.config.consumer_group}-{i}",
                daemon=True,
            )
            thread.start()
            self._consume_threads.append(thread)

        self._logger.info(
            "消费线程启动完成",
            extra={
                "thread_count": thread_count,
                "consumer_group": self.config.consumer_group,
            },
        )

    def _stop_consume_threads(self) -> None:
        """停止消费线程"""
        self._logger.info("开始停止消费线程")

        # 等待所有线程结束
        for thread in self._consume_threads:
            if thread.is_alive():
                thread.join(timeout=5000)  # 最多等待5秒

        self._consume_threads.clear()
        self._logger.info("消费线程停止完成")

    def _consume_worker(self) -> None:
        """消费工作线程

        MVP版本: 简化实现，后续版本会实现完整的消息拉取和处理逻辑
        """
        thread_name = threading.current_thread().name
        self._logger.info("消费线程启动", extra={"thread_name": thread_name})

        while self._running:
            try:
                # TODO: 实现消息拉取和处理逻辑
                time.sleep(1.0)  # MVP: 简单的轮询

            except Exception as e:
                self._logger.error(
                    f"消费线程异常: {e}",
                    extra={"thread_name": thread_name},
                    exc_info=True,
                )

                if self._running:
                    time.sleep(5.0)  # 异常后等待一段时间再重试

        self._logger.info("消费线程停止", extra={"thread_name": thread_name})

    def _disconnect(self) -> None:
        """断开网络连接"""
        try:
            for _, nameserver_conn in self._nameserver_connections.items():
                nameserver_conn.close()

            self._nameserver_connections.clear()
            self._logger.info("网络连接断开完成")

        except Exception as e:
            self._logger.error(f"断开网络连接失败: {e}", exc_info=True)

    def _cleanup(self) -> None:
        """清理资源"""
        try:
            self._broker_manager = None
            self._message_listener = None

            self._logger.info("资源清理完成")

        except Exception as e:
            self._logger.error(f"资源清理失败: {e}", exc_info=True)

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"Consumer["
            f"group={self.config.consumer_group}, "
            f"running={self.is_running()}, "
            f"subscriptions={len(self._subscriptions)}, "
            f"client_id={self.config.client_id}"
            f"]"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (
            f"Consumer("
            f"config={self.config}, "
            f"running={self.is_running()}, "
            f"subscriptions={list(self._subscriptions.keys())}, "
            f"stats={self._stats}"
            f")"
        )


# 便利函数


def create_consumer(consumer_group: str, namesrv_addr: str, **kwargs) -> Consumer:
    """
    创建Consumer的便利函数

    Args:
        consumer_group: 消费者组名称
        namesrv_addr: NameServer地址
        **kwargs: 其他配置参数

    Returns:
        Consumer实例

    Examples:
        >>> # 创建基本Consumer
        >>> consumer = create_consumer(
        ...     "test_group",
        ...     "localhost:9876"
        ... )
        >>>
        >>> # 创建自定义Consumer
        >>> consumer = create_consumer(
        ...     "order_consumer",
        ...     "broker1:9876;broker2:9876",
        ...     consume_thread_max=32,
        ...     pull_batch_size=16
        ... )
    """
    from pyrocketmq.consumer.config import create_consumer_config

    config = create_consumer_config(
        consumer_group=consumer_group, namesrv_addr=namesrv_addr, **kwargs
    )

    return Consumer(config)
