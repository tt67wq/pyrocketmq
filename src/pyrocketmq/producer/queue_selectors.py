"""
队列选择器模块

提供各种队列选择策略，用于MessageRouter中的路由决策。
所有队列选择器都是纯算法实现，不涉及IO操作，因此无需区分异步和同步版本。

作者: pyrocketmq团队
版本: MVP 1.0
"""

import abc
import random
import threading
from typing import override

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message import Message
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import BrokerData

logger = get_logger(__name__)


class QueueSelector(abc.ABC):
    """队列选择器抽象基类"""

    @abc.abstractmethod
    def select(
        self,
        topic: str,
        available_queues: list[tuple[MessageQueue, BrokerData]],
        message: Message | None = None,
    ) -> tuple[MessageQueue, BrokerData] | None:
        """从可用队列中选择一个队列"""
        pass


class RoundRobinSelector(QueueSelector):
    """轮询队列选择器

    按顺序轮流选择队列，确保消息均匀分布到所有可用队列中。
    使用线程安全的计数器保证并发环境下的正确性。

    特点：
    - 负载均衡：消息均匀分布到所有队列
    - 线程安全：支持多线程并发调用
    - 状态管理：每个主题维护独立的计数器
    """

    def __init__(self):
        self._counters: dict[str, int] = {}
        self._lock: threading.RLock = threading.RLock()

    @override
    def select(
        self,
        topic: str,
        available_queues: list[tuple[MessageQueue, BrokerData]],
        message: Message | None = None,
    ) -> tuple[MessageQueue, BrokerData] | None:
        if not available_queues:
            return None

        with self._lock:
            counter = self._counters.get(topic, 0)
            queue_index = counter % len(available_queues)
            selected_queue, selected_broker = available_queues[queue_index]
            self._counters[topic] = counter + 1

            logger.debug(
                "RoundRobin selected queue for topic",
                extra={
                    "topic": topic,
                    "queue": selected_queue.full_name,
                    "broker_name": selected_broker.broker_name,
                    "queue_index": queue_index,
                },
            )

            return selected_queue, selected_broker

    def reset_counter(self, topic: str) -> None:
        """重置指定主题的计数器"""
        with self._lock:
            if topic in self._counters:
                del self._counters[topic]

    def reset_all_counters(self) -> None:
        """重置所有计数器"""
        with self._lock:
            self._counters.clear()


class RandomSelector(QueueSelector):
    """随机队列选择器

    随机选择一个可用队列，适用于无需考虑顺序性的场景。
    实现简单，性能开销最小。

    特点：
    - 完全随机：每个队列被选中的概率相等
    - 无状态：不需要维护计数器
    - 高性能：计算复杂度O(1)
    """

    @override
    def select(
        self,
        topic: str,
        available_queues: list[tuple[MessageQueue, BrokerData]],
        message: Message | None = None,
    ) -> tuple[MessageQueue, BrokerData] | None:
        if not available_queues:
            return None

        selected_queue, selected_broker = random.choice(available_queues)
        logger.debug(
            "Random selected queue for topic",
            extra={
                "topic": topic,
                "queue": selected_queue.full_name,
                "broker_name": selected_broker.broker_name,
            },
        )
        return selected_queue, selected_broker


class MessageHashSelector(QueueSelector):
    """基于消息哈希的队列选择器

    根据消息的分片键或消息键计算哈希值，确保相同分片键的消息
    始终路由到同一个队列，保证消息的顺序性。

    优先级：
    1. 分片键（SHARDING_KEY属性）
    2. 第一个消息键（KEYS属性）
    3. 随机选择（无分片键时）

    特点：
    - 顺序保证：相同分片键的消息顺序处理
    - 负载分散：通过哈希算法分散到不同队列
    - 容错处理：无分片键时回退到随机选择
    """

    @override
    def select(
        self,
        topic: str,
        available_queues: list[tuple[MessageQueue, BrokerData]],
        message: Message | None = None,
    ) -> tuple[MessageQueue, BrokerData] | None:
        if not available_queues:
            return None

        if not message:
            return RandomSelector().select(topic, available_queues, message)

        # 获取分片键
        sharding_key = message.get_sharding_key()
        if not sharding_key:
            logger.debug("No sharding key found for message, using random selection")
            return RandomSelector().select(topic, available_queues, message)

        # 计算哈希值并选择队列
        hash_value = hash(sharding_key)
        queue_index = abs(hash_value) % len(available_queues)
        selected_queue, selected_broker = available_queues[queue_index]

        logger.debug(
            "MessageHash selected queue for topic",
            extra={
                "topic": topic,
                "queue": selected_queue.full_name,
                "broker_name": selected_broker.broker_name,
                "sharding_key": sharding_key,
                "hash_value": hash_value,
            },
        )

        return selected_queue, selected_broker


# 便利函数
def create_round_robin_selector() -> RoundRobinSelector:
    """创建轮询队列选择器"""
    return RoundRobinSelector()


def create_random_selector() -> RandomSelector:
    """创建随机队列选择器"""
    return RandomSelector()


def create_message_hash_selector() -> MessageHashSelector:
    """创建消息哈希队列选择器"""
    return MessageHashSelector()
