"""
队列选择器模块

提供各种队列选择策略，用于MessageRouter中的路由决策。
支持同步和异步两种模式。

作者: pyrocketmq团队
版本: MVP 1.0
"""

import abc
import asyncio
import random
import threading
from typing import List, Optional, Tuple

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message import Message, MessageProperty
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import BrokerData

logger = get_logger(__name__)


# ============================================================================
# 同步队列选择器
# ============================================================================


class QueueSelector(abc.ABC):
    """队列选择器抽象基类"""

    @abc.abstractmethod
    def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        """从可用队列中选择一个队列"""
        pass


class RoundRobinSelector(QueueSelector):
    """轮询队列选择器"""

    def __init__(self):
        self._counters: dict[str, int] = {}
        self._lock = threading.RLock()

    def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        if not available_queues:
            return None

        with self._lock:
            counter = self._counters.get(topic, 0)
            queue_index = counter % len(available_queues)
            selected_queue, selected_broker = available_queues[queue_index]
            self._counters[topic] = counter + 1

            logger.debug(
                f"RoundRobin selected queue for topic {topic}: {selected_queue.full_name} "
                f"(broker: {selected_broker.broker_name}, index: {queue_index})"
            )

            return selected_queue, selected_broker

    def reset_counter(self, topic: str):
        with self._lock:
            if topic in self._counters:
                del self._counters[topic]

    def reset_all_counters(self):
        with self._lock:
            self._counters.clear()


class RandomSelector(QueueSelector):
    """随机队列选择器"""

    def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        if not available_queues:
            return None

        selected_queue, selected_broker = random.choice(available_queues)
        logger.debug(
            f"Random selected queue for topic {topic}: {selected_queue.full_name} "
            f"(broker: {selected_broker.broker_name})"
        )
        return selected_queue, selected_broker


class MessageHashSelector(QueueSelector):
    """基于消息哈希的队列选择器"""

    def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        if not available_queues:
            return None

        if not message:
            return RandomSelector().select(topic, available_queues, message)

        # 优先使用分片键，其次使用消息键
        sharding_key = message.get_property(MessageProperty.SHARDING_KEY)
        if not sharding_key:
            keys = message.get_keys()
            if keys:
                sharding_key = keys.split()[0]

        if not sharding_key:
            logger.debug(
                "No sharding key found for message, using random selection"
            )
            return RandomSelector().select(topic, available_queues, message)

        hash_value = hash(sharding_key)
        queue_index = abs(hash_value) % len(available_queues)
        selected_queue, selected_broker = available_queues[queue_index]

        logger.debug(
            f"MessageHash selected queue for topic {topic}: {selected_queue.full_name} "
            f"(broker: {selected_broker.broker_name}, sharding_key: {sharding_key}, hash: {hash_value})"
        )

        return selected_queue, selected_broker


# ============================================================================
# 异步队列选择器
# ============================================================================


class AsyncQueueSelector(abc.ABC):
    """异步队列选择器抽象基类"""

    @abc.abstractmethod
    async def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        """从可用队列中选择一个队列（异步版本）"""
        pass


class AsyncRoundRobinSelector(AsyncQueueSelector):
    """异步轮询队列选择器"""

    def __init__(self):
        self._counters: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        if not available_queues:
            return None

        async with self._lock:
            counter = self._counters.get(topic, 0)
            queue_index = counter % len(available_queues)
            selected_queue, selected_broker = available_queues[queue_index]
            self._counters[topic] = counter + 1

            logger.debug(
                f"AsyncRoundRobin selected queue for topic {topic}: {selected_queue.full_name} "
                f"(broker: {selected_broker.broker_name}, index: {queue_index})"
            )

            return selected_queue, selected_broker

    async def reset_counter(self, topic: str):
        async with self._lock:
            if topic in self._counters:
                del self._counters[topic]

    async def reset_all_counters(self):
        async with self._lock:
            self._counters.clear()


class AsyncRandomSelector(AsyncQueueSelector):
    """异步随机队列选择器"""

    async def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        if not available_queues:
            return None

        loop = asyncio.get_event_loop()
        selected_queue, selected_broker = await loop.run_in_executor(
            None, random.choice, available_queues
        )

        logger.debug(
            f"AsyncRandom selected queue for topic {topic}: {selected_queue.full_name} "
            f"(broker: {selected_broker.broker_name})"
        )

        return selected_queue, selected_broker


class AsyncMessageHashSelector(AsyncQueueSelector):
    """异步基于消息哈希的队列选择器"""

    async def select(
        self,
        topic: str,
        available_queues: List[Tuple[MessageQueue, BrokerData]],
        message: Optional[Message] = None,
    ) -> Optional[Tuple[MessageQueue, BrokerData]]:
        if not available_queues:
            return None

        if not message:
            return await AsyncRandomSelector().select(
                topic, available_queues, message
            )

        # 优先使用分片键，其次使用消息键
        sharding_key = message.get_property(MessageProperty.SHARDING_KEY)
        if not sharding_key:
            keys = message.get_keys()
            if keys:
                sharding_key = keys.split()[0]

        if not sharding_key:
            logger.debug(
                "No sharding key found for message, using random selection"
            )
            return await AsyncRandomSelector().select(
                topic, available_queues, message
            )

        loop = asyncio.get_event_loop()
        hash_value = await loop.run_in_executor(None, hash, sharding_key)
        queue_index = abs(hash_value) % len(available_queues)
        selected_queue, selected_broker = available_queues[queue_index]

        logger.debug(
            f"AsyncMessageHash selected queue for topic {topic}: {selected_queue.full_name} "
            f"(broker: {selected_broker.broker_name}, sharding_key: {sharding_key}, hash: {hash_value})"
        )

        return selected_queue, selected_broker
