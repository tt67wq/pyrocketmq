"""
主题-Broker映射管理模块

负责管理Topic到Broker的路由信息缓存，为MessageRouter提供基础路由数据支持。
这是Producer的基础组件，专注于路由信息的存储和管理，不涉及路由决策逻辑。

MVP版本功能:
- 基础的路由信息缓存
- 路由更新机制
- 线程安全保证
- 路由过期管理

作者: pyrocketmq团队
版本: MVP 1.0
"""

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from pyrocketmq.logging import get_logger
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import (
    BrokerData,
    QueueData,
    TopicRouteData,
)

logger = get_logger(__name__)


@dataclass
class RouteInfo:
    """路由信息包装类"""

    topic_route_data: TopicRouteData
    last_update_time: float = field(default_factory=time.time)

    # 预构建的队列列表，提升性能
    available_queues: List[Tuple[MessageQueue, BrokerData]] = field(
        default_factory=list
    )

    def is_expired(self, default_timeout: float = 30.0) -> bool:
        """检查路由信息是否过期"""
        return time.time() - self.last_update_time > default_timeout

    def refresh_update_time(self):
        """刷新更新时间"""
        self.last_update_time = time.time()

    @classmethod
    def create_with_queues(
        cls, topic_route_data: TopicRouteData, topic: str
    ) -> "RouteInfo":
        """
        创建RouteInfo并预先构建队列列表

        Args:
            topic_route_data: Topic路由数据
            topic: Topic名称

        Returns:
            RouteInfo: 包含预构建队列列表的RouteInfo实例
        """
        route_info = cls(topic_route_data=topic_route_data)

        # 预构建所有可用的队列列表
        available_queues: List[Tuple[MessageQueue, BrokerData]] = []

        # 构建broker名称到broker_data的映射，提升查找性能
        broker_map = {
            broker.broker_name: broker
            for broker in topic_route_data.broker_data_list
        }

        for queue_data in topic_route_data.queue_data_list:
            broker_data = broker_map.get(queue_data.broker_name)
            if broker_data is None:
                logger.warning(
                    f"Broker {queue_data.broker_name} not found in broker list"
                )
                continue

            # 为每个写队列创建MessageQueue
            for queue_id in range(queue_data.write_queue_nums):
                message_queue = MessageQueue(
                    topic=topic,
                    broker_name=queue_data.broker_name,
                    queue_id=queue_id,
                )
                available_queues.append((message_queue, broker_data))

        route_info.available_queues = available_queues
        return route_info


class TopicBrokerMapping:
    """
    Topic-Broker映射管理器

    专注于路由信息的缓存和管理，为MessageRouter提供基础数据支持。

    功能:
    1. 缓存Topic路由信息
    2. 管理路由信息更新
    3. 路由过期管理
    4. 提供可用队列列表（不涉及选择逻辑）
    """

    def __init__(self, route_timeout: float = 30.0):
        # 路由信息缓存: topic -> RouteInfo
        self._route_cache: Dict[str, RouteInfo] = {}

        # 线程安全锁
        self._lock = threading.RLock()

        # 默认路由过期时间 (秒)
        self._default_route_timeout = route_timeout

        logger.info(
            f"TopicBrokerMapping initialized with timeout={route_timeout}s"
        )

    def get_route_info(self, topic: str) -> Optional[TopicRouteData]:
        """
        获取Topic的路由信息

        Args:
            topic: 主题名称

        Returns:
            TopicRouteData: 路由信息，如果不存在则返回None
        """
        with self._lock:
            route_info = self._route_cache.get(topic)

            if route_info is None:
                logger.debug(f"No route info found for topic: {topic}")
                return None

            # 检查路由信息是否过期
            if route_info.is_expired(self._default_route_timeout):
                logger.debug(f"Route info expired for topic: {topic}")
                return None

            logger.debug(f"Found route info for topic: {topic}")
            return route_info.topic_route_data

    def update_route_info(
        self, topic: str, topic_route_data: TopicRouteData
    ) -> bool:
        """
        更新Topic的路由信息

        Args:
            topic: 主题名称
            topic_route_data: 新的路由信息

        Returns:
            bool: 更新是否成功
        """
        if not topic_route_data:
            logger.warning(f"Empty route data provided for topic: {topic}")
            return False

        with self._lock:
            try:
                # 创建新的路由信息并预先构建队列列表
                new_route_info = RouteInfo.create_with_queues(
                    topic_route_data, topic
                )

                # 更新缓存
                self._route_cache[topic] = new_route_info

                logger.info(
                    f"Route info updated for topic: {topic}, "
                    f"brokers: {len(topic_route_data.broker_data_list)}, "
                    f"queue_data: {len(topic_route_data.queue_data_list)}, "
                    f"available_queues: {len(new_route_info.available_queues)}"
                )
                return True

            except Exception as e:
                logger.error(
                    f"Failed to update route info for topic {topic}: {e}"
                )
                return False

    def remove_route_info(self, topic: str) -> bool:
        """
        移除Topic的路由信息

        Args:
            topic: 主题名称

        Returns:
            bool: 移除是否成功
        """
        with self._lock:
            removed = False

            if topic in self._route_cache:
                del self._route_cache[topic]
                removed = True

            if removed:
                logger.info(f"Route info removed for topic: {topic}")

            return removed

    def get_available_queues(
        self, topic: str
    ) -> List[Tuple[MessageQueue, BrokerData]]:
        """
        获取Topic的所有可用队列和对应的Broker

        Args:
            topic: 主题名称

        Returns:
            List[Tuple[MessageQueue, BrokerData]]: 队列和Broker对列表
        """
        with self._lock:
            route_info = self._route_cache.get(topic)
            if route_info is None:
                return []

            # 检查路由信息是否过期
            if route_info.is_expired(self._default_route_timeout):
                return []

            # 直接返回预构建队列列表的副本
            return route_info.available_queues.copy()

    def get_available_brokers(self, topic: str) -> List[BrokerData]:
        """
        获取Topic的所有可用Broker

        Args:
            topic: 主题名称

        Returns:
            List[BrokerData]: Broker列表
        """
        route_info = self.get_route_info(topic)
        if not route_info:
            return []

        return route_info.broker_data_list.copy()

    def get_queue_data(self, topic: str) -> List[QueueData]:
        """
        获取Topic的队列数据

        Args:
            topic: 主题名称

        Returns:
            List[QueueData]: 队列数据列表
        """
        route_info = self.get_route_info(topic)
        if not route_info:
            return []

        return route_info.queue_data_list.copy()

    def get_all_topics(self) -> Set[str]:
        """
        获取所有已缓存的Topic

        Returns:
            Set[str]: Topic集合
        """
        with self._lock:
            return set(self._route_cache.keys())

    def clear_expired_routes(self, timeout: Optional[float] = None) -> int:
        """
        清理过期的路由信息

        Args:
            timeout: 过期时间，如果为None则使用默认值

        Returns:
            int: 清理的Topic数量
        """
        if timeout is None:
            timeout = self._default_route_timeout

        current_time = time.time()
        expired_topics = []

        with self._lock:
            for topic, route_info in self._route_cache.items():
                if current_time - route_info.last_update_time > timeout:
                    expired_topics.append(topic)

            # 移除过期的路由信息
            for topic in expired_topics:
                del self._route_cache[topic]

        if expired_topics:
            logger.info(
                f"Cleared {len(expired_topics)} expired routes: {expired_topics}"
            )

        return len(expired_topics)

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            Dict[str, any]: 统计信息
        """
        with self._lock:
            total_topics = len(self._route_cache)
            total_brokers = sum(
                len(route.topic_route_data.broker_data_list)
                for route in self._route_cache.values()
            )
            total_queue_data = sum(
                len(route.topic_route_data.queue_data_list)
                for route in self._route_cache.values()
            )

            # 使用预构建的队列列表统计实际可用的写队列总数
            total_available_queues = sum(
                len(route.available_queues)
                for route in self._route_cache.values()
            )

            return {
                "total_topics": total_topics,
                "total_brokers": total_brokers,
                "total_queue_data": total_queue_data,
                "total_available_queues": total_available_queues,
                "topics": list(self._route_cache.keys()),
            }

    def set_route_timeout(self, timeout: float):
        """
        设置路由过期时间

        Args:
            timeout: 过期时间（秒）
        """
        if timeout <= 0:
            raise ValueError("Route timeout must be positive")

        self._default_route_timeout = timeout
        logger.info(f"Route timeout updated to {timeout}s")

    def force_refresh(self, topic: str) -> bool:
        """
        强制刷新Topic的路由信息

        实际上是移除缓存，下次访问时会触发重新获取

        Args:
            topic: 主题名称

        Returns:
            bool: 刷新是否成功
        """
        return self.remove_route_info(topic)

    async def start_background_cleanup(self, interval: float = 60.0):
        """
        启动后台清理任务

        Args:
            interval: 清理间隔（秒）
        """
        logger.info(
            f"Starting background cleanup task with interval {interval}s"
        )

        while True:
            try:
                await asyncio.sleep(interval)
                cleared_count = self.clear_expired_routes()
                if cleared_count > 0:
                    logger.debug(
                        f"Background cleanup cleared {cleared_count} expired routes"
                    )
            except asyncio.CancelledError:
                logger.info("Background cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Background cleanup error: {e}")

    def __str__(self) -> str:
        """字符串表示"""
        stats = self.get_cache_stats()
        return (
            f"TopicBrokerMapping(topics={stats['total_topics']}, "
            f"brokers={stats['total_brokers']}, "
            f"queue_data={stats['total_queue_data']}, "
            f"available_queues={stats['total_available_queues']})"
        )

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


# ============================================================================
# 异步版本的TopicBrokerMapping
# ============================================================================


class AsyncTopicBrokerMapping:
    """
    异步版本的Topic-Broker映射管理器

    专注于路由信息的异步缓存和管理，为AsyncMessageRouter提供基础数据支持。

    功能:
    1. 异步缓存Topic路由信息
    2. 异步管理路由信息更新
    3. 路由过期管理
    4. 提供可用队列列表（不涉及选择逻辑）
    5. 异步安全的并发访问
    """

    def __init__(self, route_timeout: float = 30.0):
        # 路由信息缓存: topic -> RouteInfo
        self._route_cache: Dict[str, RouteInfo] = {}

        # 异步锁
        self._lock = asyncio.Lock()

        # 默认路由过期时间 (秒)
        self._default_route_timeout = route_timeout

        logger.info(
            f"AsyncTopicBrokerMapping initialized with timeout={route_timeout}s"
        )

    async def get_route_info(self, topic: str) -> Optional[TopicRouteData]:
        """
        获取Topic的路由信息（异步版本）

        Args:
            topic: 主题名称

        Returns:
            TopicRouteData: 路由信息，如果不存在则返回None
        """
        async with self._lock:
            route_info = self._route_cache.get(topic)

            if route_info is None:
                logger.debug(f"No route info found for topic: {topic}")
                return None

            # 检查路由信息是否过期
            if route_info.is_expired(self._default_route_timeout):
                logger.debug(f"Route info expired for topic: {topic}")
                return None

            logger.debug(f"Found route info for topic: {topic}")
            return route_info.topic_route_data

    async def update_route_info(
        self, topic: str, topic_route_data: TopicRouteData
    ) -> bool:
        """
        更新Topic的路由信息（异步版本）

        Args:
            topic: 主题名称
            topic_route_data: 新的路由信息

        Returns:
            bool: 更新是否成功
        """
        if not topic_route_data:
            logger.warning(f"Empty route data provided for topic: {topic}")
            return False

        async with self._lock:
            try:
                # 创建新的路由信息并预先构建队列列表
                new_route_info = RouteInfo.create_with_queues(
                    topic_route_data, topic
                )

                # 更新缓存
                self._route_cache[topic] = new_route_info

                logger.info(
                    f"Route info updated for topic: {topic}, "
                    f"brokers: {len(topic_route_data.broker_data_list)}, "
                    f"queue_data: {len(topic_route_data.queue_data_list)}, "
                    f"available_queues: {len(new_route_info.available_queues)}"
                )
                return True

            except Exception as e:
                logger.error(
                    f"Failed to update route info for topic {topic}: {e}"
                )
                return False

    async def remove_route_info(self, topic: str) -> bool:
        """
        移除Topic的路由信息（异步版本）

        Args:
            topic: 主题名称

        Returns:
            bool: 移除是否成功
        """
        async with self._lock:
            removed = False

            if topic in self._route_cache:
                del self._route_cache[topic]
                removed = True

            if removed:
                logger.info(f"Route info removed for topic: {topic}")

            return removed

    async def get_available_queues(
        self, topic: str
    ) -> List[Tuple[MessageQueue, BrokerData]]:
        """
        获取Topic的所有可用队列和对应的Broker（异步版本）

        Args:
            topic: 主题名称

        Returns:
            List[Tuple[MessageQueue, BrokerData]]: 队列和Broker对列表
        """
        async with self._lock:
            route_info = self._route_cache.get(topic)
            if route_info is None:
                return []

            # 检查路由信息是否过期
            if route_info.is_expired(self._default_route_timeout):
                return []

            # 直接返回预构建队列列表的副本
            return route_info.available_queues.copy()

    async def get_available_brokers(self, topic: str) -> List[BrokerData]:
        """
        获取Topic的所有可用Broker（异步版本）

        Args:
            topic: 主题名称

        Returns:
            List[BrokerData]: Broker列表
        """
        route_info = await self.get_route_info(topic)
        if not route_info:
            return []

        return route_info.broker_data_list.copy()

    async def get_queue_data(self, topic: str) -> List[QueueData]:
        """
        获取Topic的队列数据（异步版本）

        Args:
            topic: 主题名称

        Returns:
            List[QueueData]: 队列数据列表
        """
        route_info = await self.get_route_info(topic)
        if not route_info:
            return []

        return route_info.queue_data_list.copy()

    async def get_all_topics(self) -> Set[str]:
        """
        获取所有已缓存的Topic（异步版本）

        Returns:
            Set[str]: Topic集合
        """
        async with self._lock:
            return set(self._route_cache.keys())

    async def clear_expired_routes(
        self, timeout: Optional[float] = None
    ) -> int:
        """
        清理过期的路由信息（异步版本）

        Args:
            timeout: 过期时间，如果为None则使用默认值

        Returns:
            int: 清理的Topic数量
        """
        if timeout is None:
            timeout = self._default_route_timeout

        current_time = time.time()
        expired_topics = []

        async with self._lock:
            for topic, route_info in self._route_cache.items():
                if current_time - route_info.last_update_time > timeout:
                    expired_topics.append(topic)

            # 移除过期的路由信息
            for topic in expired_topics:
                del self._route_cache[topic]

        if expired_topics:
            logger.info(
                f"Cleared {len(expired_topics)} expired routes: {expired_topics}"
            )

        return len(expired_topics)

    async def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息（异步版本）

        Returns:
            Dict[str, any]: 统计信息
        """
        async with self._lock:
            total_topics = len(self._route_cache)
            total_brokers = sum(
                len(route.topic_route_data.broker_data_list)
                for route in self._route_cache.values()
            )
            total_queue_data = sum(
                len(route.topic_route_data.queue_data_list)
                for route in self._route_cache.values()
            )

            # 使用预构建的队列列表统计实际可用的写队列总数
            total_available_queues = sum(
                len(route.available_queues)
                for route in self._route_cache.values()
            )

            return {
                "total_topics": total_topics,
                "total_brokers": total_brokers,
                "total_queue_data": total_queue_data,
                "total_available_queues": total_available_queues,
                "topics": list(self._route_cache.keys()),
            }

    def set_route_timeout(self, timeout: float):
        """
        设置路由过期时间

        Args:
            timeout: 过期时间（秒）
        """
        if timeout <= 0:
            raise ValueError("Route timeout must be positive")

        self._default_route_timeout = timeout
        logger.info(f"Route timeout updated to {timeout}s")

    async def force_refresh(self, topic: str) -> bool:
        """
        强制刷新Topic的路由信息（异步版本）

        实际上是移除缓存，下次访问时会触发重新获取

        Args:
            topic: 主题名称

        Returns:
            bool: 刷新是否成功
        """
        return await self.remove_route_info(topic)

    async def start_background_cleanup(self, interval: float = 60.0):
        """
        启动后台清理任务（异步版本）

        Args:
            interval: 清理间隔（秒）
        """
        logger.info(
            f"Starting async background cleanup task with interval {interval}s"
        )

        while True:
            try:
                await asyncio.sleep(interval)
                cleared_count = await self.clear_expired_routes()
                if cleared_count > 0:
                    logger.debug(
                        f"Async background cleanup cleared {cleared_count} expired routes"
                    )
            except asyncio.CancelledError:
                logger.info("Async background cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Async background cleanup error: {e}")

    def __str__(self) -> str:
        """字符串表示"""
        # 注意：这里使用同步方法获取基本信息，避免在__str__中使用await
        return f"AsyncTopicBrokerMapping(topics={len(self._route_cache)})"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()
