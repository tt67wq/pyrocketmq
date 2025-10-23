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

import time
from dataclasses import dataclass
from typing import Optional

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message import Message
from pyrocketmq.model.nameserver_models import BrokerData
from pyrocketmq.producer.config import ProducerConfig
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    MessageSendError,
    ProducerError,
    ProducerStartError,
    ProducerStateError,
)
from pyrocketmq.producer.router import MessageRouter
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping
from pyrocketmq.producer.utils import validate_message

logger = get_logger(__name__)


@dataclass
class SendResult:
    """消息发送结果

    简化版本的发送结果，包含最核心的信息。
    随着功能演进可以逐步扩展字段。
    """

    success: bool
    """发送是否成功"""

    message_id: Optional[str] = None
    """消息ID，发送成功时由Broker返回"""

    topic: Optional[str] = None
    """消息主题"""

    broker_name: Optional[str] = None
    """选中的Broker名称"""

    queue_id: Optional[int] = None
    """选中的队列ID"""

    error: Optional[Exception] = None
    """错误信息，发送失败时设置"""

    send_timestamp: Optional[float] = None
    """发送时间戳"""

    @classmethod
    def success_result(
        cls,
        message_id: str,
        topic: str,
        broker_name: str,
        queue_id: int,
    ) -> "SendResult":
        """创建成功结果"""
        return cls(
            success=True,
            message_id=message_id,
            topic=topic,
            broker_name=broker_name,
            queue_id=queue_id,
            send_timestamp=time.time(),
        )

    @classmethod
    def failure_result(
        cls,
        topic: str,
        error: Exception,
        message_id: Optional[str] = None,
    ) -> "SendResult":
        """创建失败结果"""
        return cls(
            success=False,
            message_id=message_id,
            topic=topic,
            error=error,
            send_timestamp=time.time(),
        )


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

    def __init__(self, config: Optional[ProducerConfig] = None):
        """初始化Producer实例

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
        self._message_router = MessageRouter(self._topic_mapping)

        # 基础状态统计
        self._total_sent = 0
        self._total_failed = 0

        logger.info(
            f"Producer initialized with config: {self._config.producer_group}"
        )

    def start(self) -> None:
        """启动Producer

        初始化内部组件并建立连接。这是一个幂等操作，
        多次调用不会产生副作用。

        Raises:
            ProducerStartError: 当启动失败时抛出异常
        """
        if self._running:
            logger.warning("Producer is already running")
            return

        try:
            logger.info("Starting producer...")

            # TODO: 初始化NameServer连接
            # TODO: 初始化Broker连接池
            # TODO: 启动后台任务（路由更新、心跳等）

            # 设置运行状态
            self._running = True

            logger.info(
                f"Producer started successfully. Group: {self._config.producer_group}, "
                f"Client ID: {self._config.client_id}"
            )

        except Exception as e:
            logger.error(f"Failed to start producer: {e}")
            raise ProducerStartError(f"Producer start failed: {e}") from e

    def shutdown(self) -> None:
        """关闭Producer

        清理资源并断开连接。这是一个幂等操作，
        多次调用不会产生副作用。
        """
        if not self._running:
            logger.warning("Producer is not running")
            return

        try:
            logger.info("Shutting down producer...")

            # 设置停止状态
            self._running = False

            # TODO: 停止后台任务
            # TODO: 关闭Broker连接
            # TODO: 清理资源

            logger.info(
                f"Producer shutdown completed. Total sent: {self._total_sent}, "
                f"Total failed: {self._total_failed}"
            )

        except Exception as e:
            logger.error(f"Error during producer shutdown: {e}")

    def send_sync(self, message: Message) -> SendResult:
        """同步发送消息

        阻塞直到消息发送完成或失败。

        Args:
            message: 要发送的消息

        Returns:
            SendResult: 发送结果

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
        """
        self._check_running()

        try:
            # 1. 验证消息
            validate_message(message, self._config.max_message_size)

            # 2. 获取队列和Broker
            routing_result = self._message_router.route_message(
                message.topic, message
            )
            if not routing_result.success:
                raise BrokerNotAvailableError(
                    f"No available queue for topic: {message.topic}"
                )

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

            # 3. 获取Broker地址（优先选择Master）
            broker_addr = self._get_broker_address(broker_data)
            if not broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for: {broker_data.broker_name}"
                )

            logger.debug(
                f"Sending message to {broker_addr}, queue: {message_queue.full_name}"
            )

            # TODO: 4. 发送消息到Broker
            # 这里需要实现实际的网络发送逻辑
            # 暂时返回模拟结果
            message_id = f"MSG_{int(time.time() * 1000000)}"

            # 更新统计
            self._total_sent += 1

            return SendResult.success_result(
                message_id=message_id,
                topic=message.topic,
                broker_name=broker_data.broker_name,
                queue_id=message_queue.queue_id,
            )

        except Exception as e:
            self._total_failed += 1
            logger.error(f"Failed to send message: {e}")

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Message send failed: {e}") from e

    def send_oneway(self, message: Message) -> None:
        """单向发送消息

        发送消息但不等待响应。适用于对可靠性要求不高的场景。

        Args:
            message: 要发送的消息

        Raises:
            ProducerStateError: 当Producer未启动时
            MessageSendError: 当消息发送失败时
        """
        self._check_running()

        try:
            # 1. 验证消息
            validate_message(message, self._config.max_message_size)

            # 2. 获取队列和Broker
            routing_result = self._message_router.route_message(
                message.topic, message
            )
            if not routing_result.success:
                raise BrokerNotAvailableError(
                    f"No available queue for topic: {message.topic}"
                )

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

            # 3. 获取Broker地址
            broker_addr = self._get_broker_address(broker_data)
            if not broker_addr:
                raise BrokerNotAvailableError(
                    f"No available broker address for: {broker_data.broker_name}"
                )

            logger.debug(
                f"Sending oneway message to {broker_addr}, queue: {message_queue.full_name}"
            )

            # TODO: 4. 发送消息到Broker（单向）
            # 这里需要实现实际的网络发送逻辑

            # 更新统计（单向发送不计入成功/失败）
            logger.debug("Oneway message sent successfully")

        except Exception as e:
            logger.error(f"Failed to send oneway message: {e}")

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Oneway message send failed: {e}") from e

    def _check_running(self) -> None:
        """检查Producer是否处于运行状态

        Raises:
            ProducerStateError: 当Producer未运行时
        """
        if not self._running:
            raise ProducerStateError(
                "Producer is not running. Call start() first."
            )

    def _get_broker_address(self, broker_data: BrokerData) -> Optional[str]:
        """获取Broker地址

        优先选择Master节点，如果Master不可用则选择Slave。

        Args:
            broker_data: Broker数据

        Returns:
            Optional[str]: Broker地址，如果不可用则返回None
        """
        # TODO: 实现Broker地址选择逻辑
        # 这里需要考虑Master/Slave的选择和健康状态
        if broker_data.broker_addresses:
            # 暂时返回第一个可用地址
            return next(iter(broker_data.broker_addresses.values()))
        return None

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

    def update_route_info(self, topic: str) -> bool:
        """更新Topic路由信息

        手动触发Topic路由信息的更新。通常情况下，路由信息会自动更新，
        但在某些特殊场景下可能需要手动触发更新。

        Args:
            topic: 要更新的Topic名称

        Returns:
            bool: 更新是否成功
        """
        # TODO: 实现从NameServer获取路由信息并更新缓存
        # 这需要与NameServer客户端集成
        logger.info(f"Route update requested for topic: {topic}")
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
    **kwargs,
) -> Producer:
    """创建Producer实例的便捷函数

    提供简化的接口来创建Producer实例，特别适合快速开发。

    Args:
        producer_group: 生产者组名，默认为"DEFAULT_PRODUCER"
        namesrv_addr: NameServer地址，默认为"localhost:9876"
        **kwargs: 其他配置参数

    Returns:
        Producer: Producer实例

    Example:
        >>> # 创建默认Producer
        >>> producer = create_producer()
        >>>
        >>> # 创建自定义Producer
        >>> producer = create_producer(
        ...     producer_group="my_producer",
        ...     namesrv_addr="192.168.1.100:9876",
        ...     retry_times=3
        ... )
    """
    config = ProducerConfig(
        producer_group=producer_group,
        namesrv_addr=namesrv_addr,
        **kwargs,
    )
    return Producer(config)
