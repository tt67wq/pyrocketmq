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
        queues = super().get_subscribe_queues(topic)
        logger.debug(f"Consumer got {len(queues)} subscribe queues for topic: {topic}")
        return queues

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
        result = super().update_route_info(topic, topic_route_data)

        if result:
            logger.info(
                "Consumer route info updated successfully",
                extra={
                    "topic": topic,
                    "brokers_count": len(topic_route_data.broker_data_list),
                    "queue_data_count": len(topic_route_data.queue_data_list),
                },
            )

        return result

    def remove_route_info(self, topic: str) -> bool:
        """
        移除Topic的路由信息

        重写父类方法，添加Consumer特定的日志

        Args:
            topic: 主题名称

        Returns:
            bool: 移除是否成功
        """
        result = super().remove_route_info(topic)

        if result:
            logger.info(f"Consumer route info removed for topic: {topic}")

        return result

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
        cleared_count = super().clear_expired_routes(timeout)

        if cleared_count > 0:
            logger.info(f"Cleared {cleared_count} expired consumer routes")

        return cleared_count

    async def start_background_cleanup(self, interval: float = 60.0):
        """
        启动后台清理任务

        重写父类方法，添加Consumer特定的日志

        Args:
            interval: 清理间隔（秒）
        """
        logger.info(
            f"Starting consumer background cleanup task with interval {interval}s"
        )

        # 使用父类的后台清理方法
        await super().start_background_cleanup(interval)

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


# 为了向后兼容，提供一个别名
TopicBrokerMapping = ConsumerTopicBrokerMapping

__all__ = [
    "RouteInfo",  # 从Producer导入
    "ConsumerTopicBrokerMapping",
    "TopicBrokerMapping",  # 别名
]
