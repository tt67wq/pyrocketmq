"""
异步NameServer管理模块

提供异步的NameServer路由查询和缓存管理功能，适用于异步应用场景。
与同步版本共享相同的配置和缓存策略，但提供异步API接口。
"""

import asyncio
import time
from typing import Any

from pyrocketmq.logging import get_logger
from pyrocketmq.nameserver.client import AsyncNameServerClient
from pyrocketmq.nameserver.errors import NameServerError
from pyrocketmq.nameserver.models import BrokerData, TopicRouteData
from pyrocketmq.remote.async_remote import AsyncRemote
from pyrocketmq.remote.factory import create_async_remote

from .manager import CacheEntry, NameServerConfig


class AsyncNameServerManager:
    """
    异步NameServer管理器

    提供异步的NameServer查询和缓存管理功能，支持broker地址查询、
    路由信息缓存等常用操作。采用线程安全设计，支持高并发访问。
    与同步版本使用相同的配置和缓存策略。
    """

    def __init__(self, config: NameServerConfig) -> None:
        """初始化异步NameServer管理器.

        Args:
            config: NameServer配置对象，包含连接和缓存相关配置
        """
        self.config: NameServerConfig = config
        self._logger = get_logger(f"{__name__}.AsyncNameServerManager")

        # 解析NameServer地址
        self._nameserver_addrs: list[str] = self._parse_nameserver_addrs(
            config.nameserver_addrs
        )

        # 缓存存储
        self._broker_addr_cache: dict[str, CacheEntry] = {}
        self._route_cache: dict[str, CacheEntry] = {}

        # 异步锁
        self._cache_lock: asyncio.Lock = asyncio.Lock()

        # NameServer异步连接池
        self._async_connections: dict[str, AsyncRemote] = {}

        self._logger.info(
            "异步NameServer管理器初始化完成",
            extra={
                "nameserver_addrs": self._nameserver_addrs,
                "broker_cache_ttl": config.broker_cache_ttl,
                "route_cache_ttl": config.route_cache_ttl,
            },
        )

    async def start(self) -> None:
        """启动异步NameServer管理器，建立连接.

        Raises:
            NameServerError: 当无法建立任何NameServer连接时抛出

        Note:
            此方法会尝试连接配置中的所有NameServer地址，
            至少需要一个连接成功才能启动成功。
        """
        self._logger.info("启动异步NameServer管理器")

        # 建立异步连接
        for addr in self._nameserver_addrs:
            try:
                remote: AsyncRemote = create_async_remote(
                    address=addr,
                    config=self.config.cast_remote_config(),
                    transport_config=self.config.cast_transport_config(),
                )
                await remote.connect()
                self._async_connections[addr] = remote
                self._logger.info("建立NameServer异步连接", extra={"addr": addr})
            except Exception as e:
                self._logger.warning(
                    "建立NameServer异步连接失败", extra={"addr": addr, "error": str(e)}
                )

        if not self._async_connections:
            raise NameServerError("", "无法建立任何NameServer异步连接")

        self._logger.info("异步NameServer管理器启动完成")

    async def stop(self) -> None:
        """停止异步NameServer管理器，清理资源.

        Note:
            此方法会关闭所有NameServer连接并清空缓存。
            调用后需要重新调用start()才能再次使用管理器。
        """
        self._logger.info("停止异步NameServer管理器")

        # 关闭异步连接
        for addr, remote in self._async_connections.items():
            try:
                await remote.close()
                self._logger.info("关闭NameServer异步连接", extra={"addr": addr})
            except Exception as e:
                self._logger.warning(
                    "关闭NameServer异步连接失败", extra={"addr": addr, "error": str(e)}
                )

        self._async_connections.clear()

        # 清理缓存
        async with self._cache_lock:
            self._broker_addr_cache.clear()
            self._route_cache.clear()

        self._logger.info("异步NameServer管理器停止完成")

    async def get_broker_address(
        self, broker_name: str, topic: str | None = None
    ) -> str | None:
        """异步获取broker地址.

        Args:
            broker_name: broker名称
            topic: 可选的topic名称，用于通过topic路由查询broker地址。
                     如果为None，则通过集群信息查询

        Returns:
            str | None: broker地址，格式为"host:port"，未找到则返回None

        Note:
            此方法会优先检查缓存，如果缓存未命中或已过期，
            则从NameServer查询最新信息并更新缓存。
        """
        self._logger.debug("异步查询broker地址", extra={"broker_name": broker_name})

        # 检查缓存
        async with self._cache_lock:
            if broker_name in self._broker_addr_cache:
                cache_entry = self._broker_addr_cache[broker_name]
                if not cache_entry.is_expired:
                    self._logger.debug(
                        "使用缓存的broker地址",
                        extra={"broker_name": broker_name, "address": cache_entry.data},
                    )
                    return cache_entry.data
                else:
                    # 缓存过期，删除
                    del self._broker_addr_cache[broker_name]
                    self._logger.debug(
                        "broker地址缓存过期", extra={"broker_name": broker_name}
                    )

        # 从NameServer查询
        address: str | None = await self._query_broker_address_from_nameserver(
            broker_name, topic
        )

        if address:
            # 更新缓存
            async with self._cache_lock:
                self._broker_addr_cache[broker_name] = CacheEntry(
                    data=address,
                    timestamp=time.time(),
                    ttl=self.config.broker_cache_ttl,
                )

        return address

    async def get_topic_route(self, topic: str) -> TopicRouteData | None:
        """异步获取Topic路由信息.

        Args:
            topic: topic名称

        Returns:
            TopicRouteData | None: Topic路由数据，包含broker列表和队列信息，
                                  未找到则返回None

        Note:
            此方法会优先检查缓存，如果缓存未命中或已过期，
            则从NameServer查询最新路由信息并更新缓存。
        """
        self._logger.debug("异步查询Topic路由", extra={"topic": topic})

        # 检查缓存
        async with self._cache_lock:
            if topic in self._route_cache:
                cache_entry = self._route_cache[topic]
                if not cache_entry.is_expired:
                    self._logger.debug("使用缓存的Topic路由", extra={"topic": topic})
                    return cache_entry.data
                else:
                    # 缓存过期，删除
                    del self._route_cache[topic]
                    self._logger.debug("Topic路由缓存过期", extra={"topic": topic})

        # 从NameServer查询
        route_data: (
            TopicRouteData | None
        ) = await self._query_topic_route_from_nameserver(topic)

        if route_data:
            # 更新缓存
            async with self._cache_lock:
                self._route_cache[topic] = CacheEntry(
                    data=route_data,
                    timestamp=time.time(),
                    ttl=self.config.route_cache_ttl,
                )

        return route_data

    async def get_all_broker_addresses(self, topic: str) -> list[str]:
        """异步获取Topic下的所有broker地址.

        Args:
            topic: topic名称

        Returns:
            list[str]: broker地址列表，格式为["host1:port1", "host2:port2"]
                      如果Topic不存在或无可用broker则返回空列表

        Note:
            此方法会从Topic路由信息中提取所有master broker地址。
            只返回broker id为0的master节点地址。
        """
        route_data = await self.get_topic_route(topic)
        if not route_data:
            return []

        addresses: list[str] = []
        for broker_data in route_data.broker_data_list:
            if broker_data.broker_addresses:
                # 选择主broker地址
                master_addr: str | None = broker_data.broker_addresses.get(
                    0
                )  # 0表示master
                if master_addr:
                    addresses.append(master_addr)

        return addresses

    async def get_cache_info(self) -> dict[str, Any]:
        """异步获取缓存信息.

        Returns:
            dict[str, Any]: 包含以下键的字典：
                - cached_broker_addresses: 缓存的broker地址数量
                - cached_routes: 缓存的路由信息数量
                - connected_nameservers: 已连接的NameServer数量
        """
        async with self._cache_lock:
            return {
                "cached_broker_addresses": len(self._broker_addr_cache),
                "cached_routes": len(self._route_cache),
                "connected_nameservers": len(self._async_connections),
            }

    async def clear_cache(self) -> None:
        """异步清理所有缓存.

        Note:
            此方法会清空broker地址缓存和Topic路由缓存。
            清空后下次查询会重新从NameServer获取数据。
        """
        async with self._cache_lock:
            self._broker_addr_cache.clear()
            self._route_cache.clear()

        self._logger.info("已清理所有缓存")

    def _parse_nameserver_addrs(self, nameserver_addrs: str) -> list[str]:
        """解析NameServer地址列表.

        Args:
            nameserver_addrs: NameServer地址字符串，多个地址用分号分隔

        Returns:
            list[str]: 解析后的NameServer地址列表，已去除前后空格
                      并过滤掉空字符串
        """
        return [addr.strip() for addr in nameserver_addrs.split(";") if addr.strip()]

    async def _query_broker_address_from_nameserver(
        self, broker_name: str, topic: str | None = None
    ) -> str | None:
        """异步从NameServer查询broker地址.

        Args:
            broker_name: broker名称
            topic: 可选的topic名称，如果提供则通过topic路由查询，
                   否则通过集群信息查询

        Returns:
            str | None: broker地址，格式为"host:port"，未找到则返回None

        Note:
            此方法会按照配置的重试次数和间隔进行重试。
            优先选择master broker(id为0)的地址。
        """
        last_error: Exception | None = None

        for attempt in range(self.config.max_retry_times):
            for addr, remote in self._async_connections.items():
                try:
                    client: AsyncNameServerClient = AsyncNameServerClient(
                        remote, self.config.timeout
                    )

                    if topic:
                        # 通过topic路由查询
                        route_data: (
                            TopicRouteData | None
                        ) = await client.query_topic_route_info(topic)
                        if route_data:
                            for broker_data in route_data.broker_data_list:
                                if broker_data.broker_name == broker_name:
                                    address: str | None = self._select_broker_address(
                                        broker_data
                                    )
                                    if address:
                                        self._logger.debug(
                                            "异步通过topic路由查询到broker地址",
                                            extra={
                                                "broker_name": broker_name,
                                                "address": address,
                                            },
                                        )
                                        return address
                    else:
                        # 通过集群信息查询
                        cluster_info = await client.get_broker_cluster_info()
                        if (
                            cluster_info
                            and broker_name in cluster_info.broker_addr_table
                        ):
                            broker_data = cluster_info.broker_addr_table[broker_name]
                            address = self._select_broker_address(broker_data)
                            if address:
                                self._logger.debug(
                                    "异步通过集群信息查询到broker地址",
                                    extra={
                                        "broker_name": broker_name,
                                        "address": address,
                                    },
                                )
                                return address

                except Exception as e:
                    last_error = e
                    self._logger.warning(
                        "异步从NameServer查询broker地址失败",
                        extra={"addr": addr, "attempt": attempt + 1, "error": str(e)},
                    )
                    continue

            if attempt < self.config.max_retry_times - 1:
                await asyncio.sleep(self.config.retry_interval)

        self._logger.error(
            f"异步查询broker地址失败: {broker_name}",
            extra={
                "broker_name": broker_name,
                "topic": topic,
                "last_error": str(last_error),
            },
        )
        return None

    async def _query_topic_route_from_nameserver(
        self, topic: str
    ) -> TopicRouteData | None:
        """异步从NameServer查询Topic路由信息.

        Args:
            topic: topic名称

        Returns:
            TopicRouteData | None: Topic路由数据，包含broker列表和队列信息，
                                  未找到则返回None

        Note:
            此方法会按照配置的重试次数和间隔进行重试。
            如果所有NameServer都查询失败，返回None。
        """
        last_error: Exception | None = None

        for attempt in range(self.config.max_retry_times):
            for addr, remote in self._async_connections.items():
                try:
                    client: AsyncNameServerClient = AsyncNameServerClient(
                        remote, self.config.timeout
                    )
                    route_data: (
                        TopicRouteData | None
                    ) = await client.query_topic_route_info(topic)

                    if route_data:
                        self._logger.debug(
                            "异步查询到Topic路由",
                            extra={
                                "topic": topic,
                                "broker_count": len(route_data.broker_data_list),
                            },
                        )
                        return route_data

                except Exception as e:
                    last_error = e
                    self._logger.warning(
                        "异步从NameServer查询Topic路由失败",
                        extra={"addr": addr, "attempt": attempt + 1, "error": str(e)},
                    )
                    continue

            if attempt < self.config.max_retry_times - 1:
                await asyncio.sleep(self.config.retry_interval)

        self._logger.error(
            f"异步查询Topic路由失败: {topic}",
            extra={"topic": topic, "last_error": str(last_error)},
        )
        return None

    def _select_broker_address(self, broker_data: BrokerData) -> str | None:
        """从broker数据中选择地址.

        优先选择master broker（id为0），如果没有则选择第一个可用地址。

        Args:
            broker_data: broker数据，包含broker地址映射信息

        Returns:
            str | None: 选择的broker地址，格式为"host:port"，
                       如果broker_data中没有可用地址则返回None

        Note:
            broker_addrs字典的键为broker_id字符串类型，
            其中"0"表示master broker。
        """
        if not broker_data.broker_addresses:
            return None

        # 优先选择master (id为0)
        master_addr: str | None = broker_data.broker_addresses.get(0)
        if master_addr:
            return master_addr

        # 选择第一个可用地址
        for _broker_id, address in broker_data.broker_addresses.items():
            if address:
                return address

        return None


# 便利函数
def create_async_nameserver_manager(
    nameserver_addrs: str, **kwargs: Any
) -> AsyncNameServerManager:
    """创建异步NameServer管理器.

    Args:
        nameserver_addrs: NameServer地址，多个地址用分号分隔
        **kwargs: 其他配置参数，支持NameServerConfig的所有字段

    Returns:
        AsyncNameServerManager: 异步NameServer管理器实例

    Example:
        >>> async def example():
        ...     manager = create_async_nameserver_manager(
        ...         "localhost:9876;localhost:9877",
        ...         timeout=10.0,
        ...         broker_cache_ttl=600
        ...     )
        ...     await manager.start()
        ...     address = await manager.get_broker_address("broker-a")
        ...     await manager.stop()

    Note:
        返回的管理器需要调用await start()方法才能使用。
        使用完毕后应调用await stop()方法清理资源。
    """
    config = NameServerConfig(nameserver_addrs=nameserver_addrs, **kwargs)
    return AsyncNameServerManager(config)
