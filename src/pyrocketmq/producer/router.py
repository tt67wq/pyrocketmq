"""
消息路由器模块

负责高级的消息路由决策，包括路由策略选择、队列选择算法、故障规避机制和路由信息更新。
这是Producer模块的核心组件，为消息发送提供智能路由功能。

MVP版本功能:
- 路由策略选择和执行
- 智能故障规避机制
- 路由信息更新和状态管理
- 与TopicBrokerMapping的深度集成
- 路由性能监控和统计

作者: pyrocketmq团队
版本: MVP 1.0
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message import Message
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import BrokerData
from pyrocketmq.producer.broker_manager import BrokerState
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.producer.queue_selectors import (
    QueueSelector,
)
from pyrocketmq.producer.topic_broker_mapping import (
    AsyncTopicBrokerMapping,
    TopicBrokerMapping,
)

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
class BrokerHealthInfo:
    """Broker健康信息"""

    broker_data: BrokerData

    # 健康状态
    status: BrokerState = BrokerState.HEALTHY

    # 统计信息
    success_count: int = 0
    failure_count: int = 0
    total_requests: int = 0
    last_success_time: float = 0
    last_failure_time: float = 0

    # 性能指标
    avg_latency: float = 0.0  # 平均延迟 (ms)
    max_latency: float = 0.0  # 最大延迟 (ms)
    recent_latencies: List[float] = field(default_factory=list)  # 最近延迟记录

    # 故障恢复
    failure_start_time: float = 0
    recovery_attempt_time: float = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    def __post_init__(self):
        pass

    def update_success(self, latency_ms: float) -> None:
        """更新成功请求统计"""
        self.success_count += 1
        self.total_requests += 1
        self.last_success_time = time.time()
        self.consecutive_successes += 1
        self.consecutive_failures = 0

        # 更新延迟统计
        self.recent_latencies.append(latency_ms)
        if len(self.recent_latencies) > 100:  # 只保留最近100次
            self.recent_latencies.pop(0)

        self.avg_latency = sum(self.recent_latencies) / len(
            self.recent_latencies
        )
        self.max_latency = max(self.max_latency, latency_ms)

        # 故障恢复检查
        if (
            self.status != BrokerState.HEALTHY
            and self.consecutive_successes >= 5
        ):
            self.status = BrokerState.HEALTHY
            logger.info(
                f"Broker {self.broker_data.broker_name} recovered to healthy status"
            )

    def update_failure(self) -> None:
        """更新失败请求统计"""
        self.failure_count += 1
        self.total_requests += 1
        self.last_failure_time = time.time()
        self.consecutive_failures += 1
        self.consecutive_successes = 0

        # 故障检测
        if (
            self.consecutive_failures >= 3
            and self.status == BrokerState.HEALTHY
        ):
            self.status = BrokerState.DEGRADED
            self.failure_start_time = time.time()
            logger.warning(
                f"Broker {self.broker_data.broker_name} degraded due to consecutive failures"
            )
        elif (
            self.consecutive_failures >= 5
            and self.status == BrokerState.DEGRADED
        ):
            self.status = BrokerState.UNHEALTHY
            logger.error(
                f"Broker {self.broker_data.broker_name} marked as unhealthy"
            )

    def get_success_rate(self) -> float:
        """获取成功率"""
        if self.total_requests == 0:
            return 1.0
        return self.success_count / self.total_requests

    def is_available(self) -> bool:
        """判断Broker是否可用"""
        return (
            self.status != BrokerState.SUSPENDED
            and self.status != BrokerState.UNHEALTHY
        )

    def should_use_for_routing(self) -> bool:
        """判断是否应该用于路由（考虑健康状态和性能）"""
        if not self.is_available():
            return False

        # 如果性能下降但成功率较高，仍然可以使用
        if self.status == BrokerState.DEGRADED:
            return self.get_success_rate() > 0.8

        return True


@dataclass
class RoutingResult:
    """路由结果"""

    success: bool
    message_queue: Optional[MessageQueue] = None
    broker_data: Optional[BrokerData] = None
    broker_address: Optional[str] = None
    error: Optional[Exception] = None
    retry_count: int = 0
    routing_strategy: Optional[RoutingStrategy] = None

    def __post_init__(self):
        if self.success and (not self.message_queue or not self.broker_data):
            raise ValueError(
                "Successful routing must have message_queue and broker_data"
            )

    def get_broker_name(self) -> Optional[str]:
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
    5. 性能监控和统计
    """

    def __init__(
        self,
        topic_mapping: TopicBrokerMapping,
        default_strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN,
        health_check_interval: float = 30.0,
    ):
        # Topic-Broker映射
        self.topic_mapping = topic_mapping

        # 路由策略配置
        self.default_strategy = default_strategy

        # Broker健康信息
        self.broker_health: Dict[str, BrokerHealthInfo] = {}
        self._health_lock = threading.RLock()

        # 健康检查配置
        self.health_check_interval = health_check_interval

        # 统计信息
        self._routing_stats = {
            "total_routing": 0,
            "successful_routing": 0,
            "failed_routing": 0,
            "strategy_usage": {
                strategy.value: 0 for strategy in RoutingStrategy
            },
            "broker_usage": {},
        }
        self._stats_lock = threading.RLock()

        logger.info(
            f"MessageRouter initialized with strategy={default_strategy.value}"
        )

    def route_message(
        self,
        topic: str,
        message: Optional[Message] = None,
        strategy: Optional[RoutingStrategy] = None,
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

        try:
            with self._stats_lock:
                self._routing_stats["total_routing"] += 1
                self._routing_stats["strategy_usage"][
                    routing_strategy.value
                ] += 1

            # 获取可用队列
            available_queues = self._get_available_queues(topic)
            if not available_queues:
                error = RouteNotFoundError(
                    f"No available queues found for topic: {topic}"
                )
                with self._stats_lock:
                    self._routing_stats["failed_routing"] += 1
                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            # 根据策略选择队列
            selector = self._create_selector(routing_strategy)
            selected_result = selector.select(topic, available_queues, message)

            if not selected_result:
                error = QueueNotAvailableError(
                    f"No queue selected for topic: {topic}"
                )
                with self._stats_lock:
                    self._routing_stats["failed_routing"] += 1
                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            message_queue, broker_data = selected_result

            # 选择Broker地址
            broker_address = self._select_broker_address(broker_data)
            if not broker_address:
                error = BrokerNotAvailableError(
                    f"No available address for broker: {broker_data.broker_name}"
                )
                with self._stats_lock:
                    self._routing_stats["failed_routing"] += 1
                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            # 更新统计信息
            with self._stats_lock:
                self._routing_stats["successful_routing"] += 1
                broker_name = broker_data.broker_name
                self._routing_stats["broker_usage"][broker_name] = (
                    self._routing_stats["broker_usage"].get(broker_name, 0) + 1
                )

            routing_time = (time.time() - start_time) * 1000  # ms

            logger.debug(
                f"Message routed successfully for topic {topic}: "
                f"queue={message_queue.full_name}, broker={broker_data.broker_name}, "
                f"address={broker_address}, strategy={routing_strategy.value}, time={routing_time:.2f}ms"
            )

            return RoutingResult(
                success=True,
                message_queue=message_queue,
                broker_data=broker_data,
                broker_address=broker_address,
                routing_strategy=routing_strategy,
            )

        except Exception as e:
            with self._stats_lock:
                self._routing_stats["failed_routing"] += 1

            logger.error(f"Routing failed for topic {topic}: {e}")
            return RoutingResult(
                success=False, error=e, routing_strategy=routing_strategy
            )

    def report_routing_result(
        self,
        result: RoutingResult,
        latency_ms: Optional[float] = None,
    ) -> None:
        """
        报告路由结果（用于性能监控和故障检测）

        Args:
            result: 路由结果
            latency_ms: 请求延迟（毫秒）
        """
        if not result.success or not result.get_broker_name():
            return

        broker_name = result.get_broker_name()
        if not broker_name:
            logger.warning("Cannot report routing result: no broker name")
            return

        with self._health_lock:
            health_info = self.broker_health.get(broker_name)
            if not health_info:
                # 创建新的健康信息
                if result.broker_data:
                    health_info = BrokerHealthInfo(
                        broker_data=result.broker_data
                    )
                    self.broker_health[broker_name] = health_info
                else:
                    logger.warning(
                        f"Cannot create health info for broker {broker_name}: no broker data"
                    )
                    return

            # 更新统计信息
            if latency_ms is not None:
                health_info.update_success(latency_ms)
            else:
                health_info.update_success(0.0)  # 默认延迟

    def report_routing_failure(
        self,
        broker_name: str,
        error: Exception,
        broker_data: Optional[BrokerData] = None,
    ) -> None:
        """
        报告路由失败（用于故障检测）

        Args:
            broker_name: Broker名称
            error: 错误信息
            broker_data: Broker数据（可选）
        """
        with self._health_lock:
            health_info = self.broker_health.get(broker_name)
            if not health_info:
                # 创建新的健康信息
                if broker_data:
                    health_info = BrokerHealthInfo(broker_data=broker_data)
                    self.broker_health[broker_name] = health_info
                    health_info.update_failure()
            elif health_info:
                health_info.update_failure()

    def get_routing_stats(self) -> Dict[str, Any]:
        """
        获取路由统计信息

        Returns:
            Dict[str, any]: 统计信息
        """
        with self._stats_lock:
            stats = self._routing_stats.copy()

        # 添加健康信息统计
        with self._health_lock:
            health_summary = {}
            for broker_name, health_info in self.broker_health.items():
                health_summary[broker_name] = {
                    "status": health_info.status.value,
                    "success_rate": health_info.get_success_rate(),
                    "avg_latency": health_info.avg_latency,
                    "total_requests": health_info.total_requests,
                    "is_available": health_info.is_available(),
                }

        stats["broker_health"] = health_summary
        return stats

    def reset_stats(self) -> None:
        """重置统计信息"""
        with self._stats_lock:
            self._routing_stats = {
                "total_routing": 0,
                "successful_routing": 0,
                "failed_routing": 0,
                "strategy_usage": {
                    strategy.value: 0 for strategy in RoutingStrategy
                },
                "broker_usage": {},
            }

        logger.info("Routing stats reset")

    def update_broker_health_info(
        self,
        broker_name: str,
        broker_data: BrokerData,
    ) -> None:
        """
        更新Broker健康信息

        Args:
            broker_name: Broker名称
            broker_data: Broker数据
        """
        with self._health_lock:
            health_info = self.broker_health.get(broker_name)
            if not health_info:
                health_info = BrokerHealthInfo(
                    broker_data=broker_data,
                )
                self.broker_health[broker_name] = health_info
            else:
                # 更新地址信息
                health_info.broker_data = broker_data

    def force_broker_recovery(self, broker_name: str) -> bool:
        """
        强制恢复Broker状态

        Args:
            broker_name: Broker名称

        Returns:
            bool: 是否成功恢复
        """
        with self._health_lock:
            health_info = self.broker_health.get(broker_name)
            if health_info:
                health_info.status = BrokerState.HEALTHY
                health_info.consecutive_failures = 0
                health_info.consecutive_successes = 0
                logger.info(
                    f"Force recovered broker {broker_name} to healthy status"
                )
                return True
            return False

    def get_available_brokers(self) -> List[str]:
        """
        获取可用的Broker列表

        Returns:
            List[str]: 可用的Broker名称列表
        """
        available_brokers = []
        with self._health_lock:
            for broker_name, health_info in self.broker_health.items():
                if health_info.is_available():
                    available_brokers.append(broker_name)
        return available_brokers

    def _get_available_queues(
        self, topic: str
    ) -> List[Tuple[MessageQueue, BrokerData]]:
        """获取Topic的可用队列列表"""
        return self.topic_mapping.get_available_queues(topic)

    def _create_selector(self, strategy: RoutingStrategy) -> QueueSelector:
        """根据策略创建队列选择器"""
        if strategy == RoutingStrategy.ROUND_ROBIN:
            from pyrocketmq.producer.queue_selectors import RoundRobinSelector

            return RoundRobinSelector()
        elif strategy == RoutingStrategy.RANDOM:
            from pyrocketmq.producer.queue_selectors import RandomSelector

            return RandomSelector()
        elif strategy == RoutingStrategy.MESSAGE_HASH:
            from pyrocketmq.producer.queue_selectors import MessageHashSelector

            return MessageHashSelector()

        else:
            # 回退到默认策略
            from pyrocketmq.producer.queue_selectors import RoundRobinSelector

            return RoundRobinSelector()

    def _select_broker_address(self, broker_data: BrokerData) -> Optional[str]:
        """选择Broker地址"""
        # 简单策略：优先选择master地址
        if broker_data.broker_addresses.get(
            MASTER_BROKER_ID
        ):  # MASTER_BROKER_ID 表示master
            return broker_data.broker_addresses[MASTER_BROKER_ID]

        # 如果没有master，选择第一个可用的地址
        for broker_id, address in broker_data.broker_addresses.items():
            if address:
                return address

        return None


class AsyncMessageRouter:
    """
    异步版本的MessageRouter

    提供异步的消息路由决策功能。
    """

    def __init__(
        self,
        topic_mapping: AsyncTopicBrokerMapping,
        default_strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN,
        health_check_interval: float = 30.0,
    ):
        # Topic-Broker映射
        self.topic_mapping = topic_mapping

        # 路由策略配置
        self.default_strategy = default_strategy

        # Broker健康信息
        self.broker_health: Dict[str, BrokerHealthInfo] = {}
        self._health_lock = asyncio.Lock()

        # 健康检查配置
        self.health_check_interval = health_check_interval

        # 统计信息
        self._routing_stats = {
            "total_routing": 0,
            "successful_routing": 0,
            "failed_routing": 0,
            "strategy_usage": {
                strategy.value: 0 for strategy in RoutingStrategy
            },
            "broker_usage": {},
        }
        self._stats_lock = asyncio.Lock()

        logger.info(
            f"AsyncMessageRouter initialized with strategy={default_strategy.value}"
        )

    async def route_message(
        self,
        topic: str,
        message: Optional[Message] = None,
        strategy: Optional[RoutingStrategy] = None,
    ) -> RoutingResult:
        """
        为消息进行异步路由决策

        Args:
            topic: 主题名称
            message: 消息对象（某些策略可能会使用）
            strategy: 路由策略，如果为None则使用默认策略

        Returns:
            RoutingResult: 路由结果
        """
        routing_strategy = strategy or self.default_strategy
        start_time = time.time()

        try:
            async with self._stats_lock:
                self._routing_stats["total_routing"] += 1
                self._routing_stats["strategy_usage"][
                    routing_strategy.value
                ] += 1

            # 获取可用队列
            available_queues = await self._get_available_queues(topic)
            if not available_queues:
                error = RouteNotFoundError(
                    f"No available queues found for topic: {topic}"
                )
                async with self._stats_lock:
                    self._routing_stats["failed_routing"] += 1
                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            # 根据策略选择队列
            selector = await self._create_selector(routing_strategy)
            selected_result = await selector.select(
                topic, available_queues, message
            )

            if not selected_result:
                error = QueueNotAvailableError(
                    f"No queue selected for topic: {topic}"
                )
                async with self._stats_lock:
                    self._routing_stats["failed_routing"] += 1
                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            message_queue, broker_data = selected_result

            # 选择Broker地址
            broker_address = self._select_broker_address(broker_data)
            if not broker_address:
                error = BrokerNotAvailableError(
                    f"No available address for broker: {broker_data.broker_name}"
                )
                async with self._stats_lock:
                    self._routing_stats["failed_routing"] += 1
                return RoutingResult(
                    success=False,
                    error=error,
                    routing_strategy=routing_strategy,
                )

            # 更新统计信息
            async with self._stats_lock:
                self._routing_stats["successful_routing"] += 1
                broker_name = broker_data.broker_name
                self._routing_stats["broker_usage"][broker_name] = (
                    self._routing_stats["broker_usage"].get(broker_name, 0) + 1
                )

            routing_time = (time.time() - start_time) * 1000  # ms

            logger.debug(
                f"Async message routed successfully for topic {topic}: "
                f"queue={message_queue.full_name}, broker={broker_data.broker_name}, "
                f"address={broker_address}, strategy={routing_strategy.value}, time={routing_time:.2f}ms"
            )

            return RoutingResult(
                success=True,
                message_queue=message_queue,
                broker_data=broker_data,
                broker_address=broker_address,
                routing_strategy=routing_strategy,
            )

        except Exception as e:
            async with self._stats_lock:
                self._routing_stats["failed_routing"] += 1

            logger.error(f"Async routing failed for topic {topic}: {e}")
            return RoutingResult(
                success=False, error=e, routing_strategy=routing_strategy
            )

    async def report_routing_result(
        self,
        result: RoutingResult,
        latency_ms: Optional[float] = None,
    ) -> None:
        """
        报告路由结果（异步版本）

        Args:
            result: 路由结果
            latency_ms: 请求延迟（毫秒）
        """
        if not result.success or not result.get_broker_name():
            return

        broker_name = result.get_broker_name()
        if not broker_name:
            logger.warning("Cannot report routing result for empty broker name")
            return

        async with self._health_lock:
            health_info = self.broker_health.get(broker_name)
            if not health_info:
                # 创建新的健康信息
                if result.broker_data:
                    health_info = BrokerHealthInfo(
                        broker_data=result.broker_data
                    )
                    self.broker_health[broker_name] = health_info
                else:
                    logger.warning(
                        f"Cannot create health info for broker {broker_name}: no broker data"
                    )
                    return

            # 更新统计信息
            if latency_ms is not None:
                health_info.update_success(latency_ms)
            else:
                health_info.update_success(0.0)  # 默认延迟

    async def report_routing_failure(
        self,
        broker_name: str,
        error: Exception,
        broker_data: Optional[BrokerData] = None,
    ) -> None:
        """
        报告路由失败（异步版本）

        Args:
            broker_name: Broker名称
            error: 错误信息
            broker_data: Broker数据（可选）
        """
        async with self._health_lock:
            health_info = self.broker_health.get(broker_name)
            if not health_info:
                if broker_data:
                    # 创建新的健康信息
                    health_info = BrokerHealthInfo(broker_data=broker_data)
                    self.broker_health[broker_name] = health_info
                    health_info.update_failure()
            else:
                health_info.update_failure()

    async def get_routing_stats(self) -> Dict[str, Any]:
        """
        获取路由统计信息（异步版本）

        Returns:
            Dict[str, any]: 统计信息
        """
        async with self._stats_lock:
            stats = self._routing_stats.copy()

        # 添加健康信息统计
        async with self._health_lock:
            health_summary = {}
            for broker_name, health_info in self.broker_health.items():
                health_summary[broker_name] = {
                    "status": health_info.status.value,
                    "success_rate": health_info.get_success_rate(),
                    "avg_latency": health_info.avg_latency,
                    "total_requests": health_info.total_requests,
                    "is_available": health_info.is_available(),
                }

        stats["broker_health"] = health_summary
        return stats

    async def _get_available_queues(
        self, topic: str
    ) -> List[Tuple[MessageQueue, BrokerData]]:
        """获取Topic的可用队列列表（异步版本）"""
        return await self.topic_mapping.get_available_queues(topic)

    async def _create_selector(self, strategy: RoutingStrategy):
        """根据策略创建队列选择器（异步版本）"""
        if strategy == RoutingStrategy.ROUND_ROBIN:
            from pyrocketmq.producer.queue_selectors import (
                AsyncRoundRobinSelector,
            )

            return AsyncRoundRobinSelector()
        elif strategy == RoutingStrategy.RANDOM:
            from pyrocketmq.producer.queue_selectors import AsyncRandomSelector

            return AsyncRandomSelector()
        elif strategy == RoutingStrategy.MESSAGE_HASH:
            from pyrocketmq.producer.queue_selectors import (
                AsyncMessageHashSelector,
            )

            return AsyncMessageHashSelector()
        else:
            # 回退到默认策略
            from pyrocketmq.producer.queue_selectors import (
                AsyncRoundRobinSelector,
            )

            return AsyncRoundRobinSelector()

    def _select_broker_address(self, broker_data: BrokerData) -> Optional[str]:
        """选择Broker地址"""
        # 简单策略：优先选择master地址
        if broker_data.broker_addresses.get(
            MASTER_BROKER_ID
        ):  # MASTER_BROKER_ID 表示master
            return broker_data.broker_addresses[MASTER_BROKER_ID]

        # 如果没有master，选择第一个可用的地址
        for broker_id, address in broker_data.broker_addresses.items():
            if address:
                return address

        return None
