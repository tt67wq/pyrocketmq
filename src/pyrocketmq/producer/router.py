"""
消息路由器模块

负责高级的消息路由决策，包括路由策略选择、队列选择算法、故障规避机制和路由信息更新。
这是Producer模块的核心组件，为消息发送提供智能路由功能。

MVP版本功能:
- 路由策略选择和执行
- 智能故障规避机制
- 路由信息更新和状态管理
- 与TopicBrokerMapping的深度集成

作者: pyrocketmq团队
版本: MVP 1.0
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message import Message
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import BrokerData
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.producer.queue_selectors import (
    QueueSelector,
)
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping

logger = get_logger(__name__)


# ============================================================================
# Broker ID 常量
# ============================================================================

# RocketMQ 中 Master Broker 的 ID，按照惯例 Master 的 ID 为 0
MASTER_BROKER_ID = 0


class RoutingStrategy(Enum):
    """路由策略枚举"""

    ROUND_ROBIN = "round_robin"  # 轮询策略
    RANDOM = "random"  # 随机策略
    MESSAGE_HASH = "message_hash"  # 消息哈希策略


@dataclass
class RoutingResult:
    """路由结果"""

    success: bool
    message_queue: MessageQueue | None = None
    broker_data: BrokerData | None = None
    broker_address: str | None = None
    error: Exception | None = None
    retry_count: int = 0
    routing_strategy: RoutingStrategy | None = None

    def __post_init__(self) -> None:
        if self.success and (not self.message_queue or not self.broker_data):
            raise ValueError(
                "Successful routing must have message_queue and broker_data"
            )

    def get_broker_name(self) -> str | None:
        """获取Broker名称"""
        return self.broker_data.broker_name if self.broker_data else None


class MessageRouter:
    """
    消息路由器

    提供高级的消息路由决策功能，包括:
    1. 多种路由策略支持
    2. 故障感知和规避
    3. 延迟感知优化
    4. 自动路由更新
    """

    def __init__(
        self,
        topic_mapping: TopicBrokerMapping,
        default_strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN,
    ):
        # Topic-Broker映射
        self.topic_mapping: TopicBrokerMapping = topic_mapping

        # 路由策略配置
        self.default_strategy: RoutingStrategy = default_strategy

        self._logger: logging.Logger = get_logger(__name__)

        # 预创建队列选择器（性能优化）
        self._selectors: dict[RoutingStrategy, QueueSelector] = {}
        self._create_selectors()

        self._logger.info(
            "MessageRouter initialized",
            extra={
                "strategy": default_strategy.value,
                "pre_created_selectors": list(self._selectors.keys()),
            },
        )

    def route_message(
        self,
        topic: str,
        message: Message | None = None,
        strategy: RoutingStrategy | None = None,
    ) -> RoutingResult:
        """
        为消息进行路由决策

        Args:
            topic: 主题名称
            message: 消息对象（某些策略可能会使用）
            strategy: 路由策略，如果为None则使用默认策略

        Returns:
            RoutingResult: 路由结果
        """
        routing_strategy = strategy or self.default_strategy
        start_time = time.time()

        # Debug: 记录路由请求开始
        self._logger.debug(
            "Starting message routing",
            extra={
                "operation": "route_message",
                "topic": topic,
                "strategy": routing_strategy.value,
                "message_size": len(message.body) if message else 0,
                "message_properties": len(message.properties)
                if message and message.properties
                else 0,
                "timestamp": time.time(),
            },
        )

        try:
            # Debug: 记录即将获取可用队列
            self._logger.debug(
                "Fetching available queues for topic",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "routing_strategy": routing_strategy.value,
                    "timestamp": time.time(),
                },
            )
            # 获取可用队列
            available_queues: list[tuple[MessageQueue, BrokerData]] = (
                self._get_available_queues(topic)
            )

            # Debug: 记录可用队列信息
            self._logger.debug(
                "Retrieved available queues",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "available_queues_count": len(available_queues),
                    "timestamp": time.time(),
                },
            )

            if not available_queues:
                error = RouteNotFoundError(
                    f"No available queues found for topic: {topic}"
                )

                # Debug: 记录队列为空的情况
                self._logger.debug(
                    "No available queues found",
                    extra={
                        "operation": "route_message",
                        "topic": topic,
                        "error_type": "RouteNotFoundError",
                        "error_message": str(error),
                        "routing_strategy": routing_strategy.value,
                        "timestamp": time.time(),
                    },
                )

                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            # 获取预创建的选择器
            selector = self._selectors.get(routing_strategy)
            if selector is None:
                # 这种情况不应该发生，因为我们预创建了所有策略
                error = ValueError(
                    f"Selector not found for strategy: {routing_strategy.value}"
                )
                self._logger.error(
                    "Unexpected error: selector not found",
                    extra={
                        "operation": "route_message",
                        "topic": topic,
                        "routing_strategy": routing_strategy.value,
                        "available_strategies": list(self._selectors.keys()),
                        "timestamp": time.time(),
                    },
                )

                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            # Debug: 记录选择器使用
            self._logger.debug(
                "Using pre-created queue selector",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "selector_type": type(selector).__name__,
                    "routing_strategy": routing_strategy.value,
                    "timestamp": time.time(),
                },
            )

            selected_result = selector.select(topic, available_queues, message)

            # Debug: 记录队列选择结果
            self._logger.debug(
                "Selected queue from available options",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "selection_success": selected_result is not None,
                    "timestamp": time.time(),
                },
            )

            if not selected_result:
                error = QueueNotAvailableError(f"No queue selected for topic: {topic}")

                # Debug: 记录队列选择失败
                self._logger.debug(
                    "Queue selection failed",
                    extra={
                        "operation": "route_message",
                        "topic": topic,
                        "error_type": "QueueNotAvailableError",
                        "error_message": str(error),
                        "available_queues_count": len(available_queues),
                        "routing_strategy": routing_strategy.value,
                        "timestamp": time.time(),
                    },
                )

                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            message_queue, broker_data = selected_result

            # Debug: 记录选择的队列和broker信息
            self._logger.debug(
                "Queue and broker selected",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "selected_queue": f"{message_queue.broker_name}:{message_queue.queue_id}",
                    "selected_broker": broker_data.broker_name,
                    "broker_addresses": list(broker_data.broker_addresses.keys())
                    if broker_data.broker_addresses
                    else [],
                    "timestamp": time.time(),
                },
            )

            # 选择Broker地址
            broker_address: str | None = self._select_broker_address(broker_data)

            # Debug: 记录broker地址选择结果
            self._logger.debug(
                "Selected broker address",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "broker_name": broker_data.broker_name,
                    "selected_address": broker_address,
                    "available_addresses": list(broker_data.broker_addresses.keys())
                    if broker_data.broker_addresses
                    else [],
                    "address_selection_success": broker_address is not None,
                    "timestamp": time.time(),
                },
            )

            if not broker_address:
                error = BrokerNotAvailableError(
                    f"No available address for broker: {broker_data.broker_name}"
                )

                # Debug: 记录broker地址选择失败
                self._logger.debug(
                    "Broker address selection failed",
                    extra={
                        "operation": "route_message",
                        "topic": topic,
                        "error_type": "BrokerNotAvailableError",
                        "error_message": str(error),
                        "broker_name": broker_data.broker_name,
                        "available_addresses": list(broker_data.broker_addresses.keys())
                        if broker_data.broker_addresses
                        else [],
                        "routing_strategy": routing_strategy.value,
                        "timestamp": time.time(),
                    },
                )

                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            routing_time = (time.time() - start_time) * 1000  # ms

            # Debug: 记录成功路由的结构化日志
            self._logger.debug(
                "Message routed successfully",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "queue_name": message_queue.full_name,
                    "broker_name": broker_data.broker_name,
                    "broker_address": broker_address,
                    "routing_strategy": routing_strategy.value,
                    "routing_time_ms": round(routing_time, 2),
                    "timestamp": time.time(),
                },
            )

            return RoutingResult(
                success=True,
                message_queue=message_queue,
                broker_data=broker_data,
                broker_address=broker_address,
                routing_strategy=routing_strategy,
            )

        except Exception as e:
            # Error: 记录路由失败的结构化日志
            self._logger.error(
                "Routing failed with exception",
                extra={
                    "operation": "route_message",
                    "topic": topic,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "routing_strategy": routing_strategy.value,
                    "timestamp": time.time(),
                },
            )

            return RoutingResult(
                success=False, error=e, routing_strategy=routing_strategy
            )

    def _get_available_queues(
        self, topic: str
    ) -> list[tuple[MessageQueue, BrokerData]]:
        """获取Topic的可用队列列表"""
        return self.topic_mapping.get_available_queues(topic)

    def _create_selectors(self) -> None:
        """预创建所有队列选择器（性能优化）"""
        from pyrocketmq.producer.queue_selectors import (
            MessageHashSelector,
            RandomSelector,
            RoundRobinSelector,
        )

        # 创建所有可能用到的选择器
        self._selectors = {
            RoutingStrategy.ROUND_ROBIN: RoundRobinSelector(),
            RoutingStrategy.RANDOM: RandomSelector(),
            RoutingStrategy.MESSAGE_HASH: MessageHashSelector(),
        }

        self._logger.debug(
            "Pre-created queue selectors",
            extra={
                "selectors_count": len(self._selectors),
                "selector_types": [
                    type(selector).__name__ for selector in self._selectors.values()
                ],
                "timestamp": time.time(),
            },
        )

    def _select_broker_address(self, broker_data: BrokerData) -> str | None:
        """选择Broker地址"""
        # 简单策略：优先选择master地址
        if broker_data.broker_addresses.get(
            MASTER_BROKER_ID
        ):  # MASTER_BROKER_ID 表示master
            return broker_data.broker_addresses[MASTER_BROKER_ID]

        # 如果没有master，选择第一个可用的地址
        for _, address in broker_data.broker_addresses.items():
            if address:
                return address

        return None

    def select_broker_address(self, broker_data: BrokerData) -> str | None:
        """选择Broker地址"""
        return self._select_broker_address(broker_data)
