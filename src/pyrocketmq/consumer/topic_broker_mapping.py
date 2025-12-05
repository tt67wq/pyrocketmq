#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consumer模块 - 主题-Broker映射管理模块

复用Producer模块的RouteInfo和TopicBrokerMapping实现，为Consumer提供路由信息管理功能。

该模块提供适配器和扩展方法，专门为Consumer场景优化的路由管理功能。

作者: pyrocketmq团队
版本: MVP 1.0
"""

from typing import TYPE_CHECKING

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import BrokerData
from pyrocketmq.producer.topic_broker_mapping import (
    AsyncTopicBrokerMapping as ProducerAsyncTopicBrokerMapping,
)
from pyrocketmq.producer.topic_broker_mapping import RouteInfo
from pyrocketmq.producer.topic_broker_mapping import (
    TopicBrokerMapping as ProducerTopicBrokerMapping,
)

if TYPE_CHECKING:
    from pyrocketmq.model.nameserver_models import TopicRouteData

logger = get_logger(__name__)


class ConsumerTopicBrokerMapping(ProducerTopicBrokerMapping):
    """
    Consumer专用的Topic-Broker映射管理器

    继承Producer的TopicBrokerMapping，添加Consumer特定的功能：
    - 专注于读队列的消费场景
    - 提供Consumer专用的队列获取方法
    - Consumer特定的日志和统计信息
    """

    def __init__(self, route_timeout: float = 30.0):
        super().__init__(route_timeout)
        logger.info(
            "ConsumerTopicBrokerMapping initialized",
            extra={"route_timeout": route_timeout},
        )

    def get_consume_queues(self, topic: str) -> list[tuple[MessageQueue, BrokerData]]:
        """
        获取Topic的所有可消费队列和对应的Broker

        Consumer关注读队列，使用subscribe_message_queues

        Args:
            topic: 主题名称

        Returns:
            list[tuple[MessageQueue, BrokerData]]: 队列和Broker对列表
        """
        return self.get_subscribe_queues(topic)

    def get_subscribe_queues(self, topic: str) -> list[tuple[MessageQueue, BrokerData]]:
        """
        获取Topic的所有可订阅队列和对应的Broker

        使用父类的get_subscribe_queues方法获取读队列

        Args:
            topic: 主题名称

        Returns:
            list[tuple[MessageQueue, BrokerData]]: 队列和Broker对列表
        """
        return super().get_subscribe_queues(topic)

    def update_route_info(self, topic: str, topic_route_data: "TopicRouteData") -> bool:
        """
        更新Topic的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            topic: 主题名称
            topic_route_data: 新的路由信息

        Returns:
            bool: 更新是否成功
        """
        return super().update_route_info(topic, topic_route_data)

    def remove_route_info(self, topic: str) -> bool:
        """
        移除Topic的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            topic: 主题名称

        Returns:
            bool: 移除是否成功
        """
        return super().remove_route_info(topic)

    def get_cache_stats(self) -> dict[str, int | list[str] | float]:
        """
        获取缓存统计信息

        重写父类方法，提供Consumer特定的统计信息

        Returns:
            dict[str, any]: 统计信息
        """
        stats = super().get_cache_stats()

        # 为Consumer场景调整统计信息名称
        consumer_stats = {
            "total_topics": stats["total_topics"],
            "total_brokers": stats["total_brokers"],
            "total_queue_data": stats["total_queue_data"],
            "total_subscribe_queues": stats[
                "total_subscribe_queues"
            ],  # Consumer关注订阅队列
            "topics": stats["topics"],
        }

        return consumer_stats

    def clear_expired_routes(self, timeout: float | None = None) -> int:
        """
        清理过期的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            timeout: 过期时间，如果为None则使用默认值

        Returns:
            int: 清理的Topic数量
        """
        return super().clear_expired_routes(timeout)

    def __str__(self) -> str:
        """字符串表示"""
        stats = self.get_cache_stats()
        return (
            f"ConsumerTopicBrokerMapping(topics={stats['total_topics']}, "
            f"brokers={stats['total_brokers']}, "
            f"queue_data={stats['total_queue_data']}, "
            f"subscribe_queues={stats['total_subscribe_queues']})"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


class AsyncConsumerTopicBrokerMapping(ProducerAsyncTopicBrokerMapping):
    """
    异步Consumer专用的Topic-Broker映射管理器

    继承异步Producer的AsyncTopicBrokerMapping，添加Consumer特定的功能：
    - 专注于读队列的消费场景
    - 提供异步Consumer专用的队列获取方法
    - Consumer特定的日志和统计信息
    """

    def __init__(self, route_timeout: float = 30.0):
        super().__init__(route_timeout)
        logger.info(
            "AsyncConsumerTopicBrokerMapping initialized",
            extra={"route_timeout": route_timeout},
        )

    async def aget_consume_queues(
        self, topic: str
    ) -> list[tuple[MessageQueue, BrokerData]]:
        """
        异步获取Topic的所有可消费队列和对应的Broker

        Consumer关注读队列，使用subscribe_message_queues

        Args:
            topic: 主题名称

        Returns:
            list[tuple[MessageQueue, BrokerData]]: 队列和Broker对列表
        """
        return await self.aget_subscribe_queues(topic)

    async def aget_subscribe_queues(
        self, topic: str
    ) -> list[tuple[MessageQueue, BrokerData]]:
        """
        异步获取Topic的所有可订阅队列和对应的Broker

        使用父类的aget_subscribe_queues方法获取读队列

        Args:
            topic: 主题名称

        Returns:
            list[tuple[MessageQueue, BrokerData]]: 队列和Broker对列表
        """
        return await super().aget_subscribe_queues(topic)

    async def aupdate_route_info(
        self, topic: str, topic_route_data: "TopicRouteData"
    ) -> bool:
        """
        异步更新Topic的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            topic: 主题名称
            topic_route_data: 新的路由信息

        Returns:
            bool: 更新是否成功
        """
        return await super().aupdate_route_info(topic, topic_route_data)

    async def aremove_route_info(self, topic: str) -> bool:
        """
        异步移除Topic的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            topic: 主题名称

        Returns:
            bool: 移除是否成功
        """
        return await super().aremove_route_info(topic)

    async def aget_cache_stats(self) -> dict[str, int | list[str] | float]:
        """
        异步获取缓存统计信息

        重写父类方法，提供Consumer特定的统计信息

        Returns:
            dict[str, any]: 统计信息
        """
        stats = await super().aget_cache_stats()

        # 为Consumer场景调整统计信息名称
        consumer_stats = {
            "total_topics": stats["total_topics"],
            "total_brokers": stats["total_brokers"],
            "total_queue_data": stats["total_queue_data"],
            "total_subscribe_queues": stats[
                "total_subscribe_queues"
            ],  # Consumer关注订阅队列
            "topics": stats["topics"],
        }

        return consumer_stats

    async def aclear_expired_routes(self, timeout: float | None = None) -> int:
        """
        异步清理过期的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            timeout: 过期时间，如果为None则使用默认值

        Returns:
            int: 清理的Topic数量
        """
        return await super().aclear_expired_routes(timeout)

    async def aforce_refresh(self, topic: str) -> bool:
        """
        异步强制刷新Topic的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            topic: 主题名称

        Returns:
            bool: 刷新是否成功
        """
        return await super().aforce_refresh(topic)

    def __str__(self) -> str:
        """字符串表示"""
        # 由于这个方法可能被异步上下文调用，我们使用同步方式获取基本统计信息
        total_topics = len(self._route_cache)
        return (
            f"AsyncConsumerTopicBrokerMapping(topics={total_topics}, "
            f"route_timeout={self._default_route_timeout})"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


# 为了向后兼容，提供一个别名
TopicBrokerMapping = ConsumerTopicBrokerMapping

__all__ = [
    "RouteInfo",  # 从Producer导入
    "ConsumerTopicBrokerMapping",
    "TopicBrokerMapping",  # 别名
    "AsyncConsumerTopicBrokerMapping",
]
