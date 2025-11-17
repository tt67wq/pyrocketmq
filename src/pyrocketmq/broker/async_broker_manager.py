"""
异步Broker管理器模块

提供异步版本的Broker连接管理功能，适用于高并发异步应用场景。
"""

import logging
import time

from pyrocketmq.logging import get_logger
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.pool import AsyncConnectionPool
from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.utils import AsyncReadWriteContext, AsyncReadWriteLock


class AsyncBrokerManager:
    """异步版本的Broker连接管理器

    管理多个Broker的异步连接，提供统一的服务接口。包括：
    - Broker连接池的创建和管理
    - 负载均衡和连接选择

    使用asyncio和异步原语实现，适用于高并发异步应用场景。
    """

    remote_config: RemoteConfig
    transport_config: TransportConfig | None
    connection_pool_size: int
    _logger: logging.Logger
    _broker_pools: dict[str, AsyncConnectionPool]
    _rwlock: AsyncReadWriteLock

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: TransportConfig | None = None,
        connection_pool_size: int = 5,
    ):
        """初始化异步Broker管理器

        Args:
            remote_config: 远程通信配置
            transport_config: 传输层配置
            connection_pool_size: 每个Broker的连接池大小
        """
        self.remote_config = remote_config
        self.transport_config = transport_config
        self.connection_pool_size = connection_pool_size

        self._logger = get_logger("broker.manager.async")

        # Broker连接池映射
        self._broker_pools = {}
        self._rwlock = AsyncReadWriteLock()

        self._logger.info(
            "异步Broker管理器初始化完成",
            extra={
                "connection_pool_size": self.connection_pool_size,
                "timestamp": time.time(),
            },
        )

    async def start(self) -> None:
        """启动Broker管理器"""
        self._logger.info(
            "异步Broker管理器已启动",
            extra={"timestamp": time.time()},
        )

    def _validate_and_parse_address(self, broker_addr: str) -> tuple[str, int]:
        """验证和解析Broker地址

        Args:
            broker_addr: Broker地址，格式为"host:port"

        Returns:
            tuple[str, int]: (host, port) 元组

        Raises:
            ValueError: 地址格式无效
        """
        # 验证broker_addr格式
        if not broker_addr or ":" not in broker_addr:
            self._logger.error(
                "无效的Broker地址格式",
                extra={
                    "broker_addr": broker_addr,
                    "error_reason": "missing_colon_or_empty",
                    "timestamp": time.time(),
                },
            )
            raise ValueError(f"无效的Broker地址格式: {broker_addr}")

        # 解析主机和端口
        try:
            host, port_str = broker_addr.split(":")
            port = int(port_str)
            if not host or port <= 0 or port > 65535:
                raise ValueError("无效的主机或端口")

            self._logger.debug(
                "异步Broker地址解析成功",
                extra={
                    "broker_addr": broker_addr,
                    "host": host,
                    "port": port,
                    "timestamp": time.time(),
                },
            )
            return host, port

        except ValueError as e:
            self._logger.error(
                "异步Broker地址解析失败",
                extra={
                    "broker_addr": broker_addr,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise ValueError(f"无效的Broker地址格式: {broker_addr}") from e

    def _create_transport_config_for_broker(
        self, host: str, port: int
    ) -> TransportConfig:
        """为Broker创建传输配置

        Args:
            host: 主机地址
            port: 端口号

        Returns:
            TransportConfig: 传输配置对象
        """
        self._logger.debug(
            "异步传输配置创建中",
            extra={
                "host": host,
                "port": port,
                "timestamp": time.time(),
            },
        )

        if self.transport_config:
            # 复制现有配置，更新主机和端口
            transport_config_dict = {
                k: v
                for k, v in self.transport_config.__dict__.items()
                if k not in ("host", "port")
            }
            transport_config = TransportConfig(
                host=host,
                port=port,
                **transport_config_dict,
            )
        else:
            # 创建默认配置
            transport_config = TransportConfig(host=host, port=port)

        self._logger.debug(
            "异步传输配置创建成功",
            extra={
                "transport_host": transport_config.host,
                "transport_port": transport_config.port,
                "timeout": transport_config.timeout,
                "timestamp": time.time(),
            },
        )

        return transport_config

    async def _create_broker_pool(
        self,
        broker_addr: str,
        broker_name: str,
        transport_config: TransportConfig,
    ) -> AsyncConnectionPool:
        """创建Broker连接池

        Args:
            broker_addr: Broker地址
            broker_name: Broker名称
            transport_config: 传输配置

        Returns:
            AsyncConnectionPool: 连接池实例
        """
        self._logger.debug(
            "创建异步连接池",
            extra={
                "broker_addr": broker_addr,
                "broker_name": broker_name,
                "max_connections": self.connection_pool_size,
                "timestamp": time.time(),
            },
        )

        pool = AsyncConnectionPool(
            address=broker_addr,
            pool_size=self.connection_pool_size,
            remote_config=self.remote_config,
            transport_config=transport_config,
        )

        self._logger.debug(
            "异步连接池创建成功",
            extra={
                "broker_addr": broker_addr,
                "broker_name": broker_name,
                "timestamp": time.time(),
            },
        )

        return pool

    async def shutdown(self) -> None:
        """关闭Broker管理器

        关闭所有连接池。
        """
        # 关闭所有连接池 - 使用写锁
        async with AsyncReadWriteContext(self._rwlock, write=True):
            # 关闭所有连接池
            broker_pools = list(self._broker_pools.values())
            self._broker_pools.clear()

        # 关闭连接池（在锁外执行）
        for pool in broker_pools:
            await pool.close()

        self._logger.info(
            "异步Broker管理器已关闭",
            extra={
                "closed_brokers_count": len(broker_pools),
                "timestamp": time.time(),
            },
        )

    async def add_broker(
        self, broker_addr: str, broker_name: str | None = None
    ) -> None:
        """添加Broker

        Args:
            broker_addr: Broker地址，格式为"host:port"
            broker_name: Broker名称，为None时从地址提取
        """
        # 步骤1: 验证和解析地址
        host, port = self._validate_and_parse_address(broker_addr)

        # 步骤2: 处理broker_name
        if not broker_name:
            broker_name = host
            self._logger.debug(
                "从地址提取异步Broker名称",
                extra={
                    "broker_addr": broker_addr,
                    "extracted_broker_name": broker_name,
                    "timestamp": time.time(),
                },
            )

        # 步骤3: 创建传输配置
        transport_config = self._create_transport_config_for_broker(host, port)

        # 步骤4: 创建连接池
        async with AsyncReadWriteContext(self._rwlock, write=True):
            try:
                # 检查_broker_pools中是否已经存在的pool，如果有就跳过当前的添加逻辑
                if broker_addr in self._broker_pools:
                    self._logger.debug(
                        "异步Broker已存在，跳过添加",
                        extra={
                            "broker_addr": broker_addr,
                            "broker_name": broker_name,
                            "timestamp": time.time(),
                        },
                    )
                    return

                pool = await self._create_broker_pool(
                    broker_addr, broker_name, transport_config
                )
                self._broker_pools[broker_addr] = pool

                self._logger.info(
                    "异步Broker添加成功",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "timestamp": time.time(),
                    },
                )

            except Exception as e:
                # 添加失败时清理
                self._logger.error(
                    "添加异步Broker失败",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
                )
                if broker_addr in self._broker_pools:
                    await self._broker_pools[broker_addr].close()
                    del self._broker_pools[broker_addr]
                raise

    async def remove_broker(self, broker_addr: str) -> None:
        """移除Broker

        Args:
            broker_addr: Broker地址
        """
        async with AsyncReadWriteContext(self._rwlock, write=True):
            # 关闭连接池
            if broker_addr in self._broker_pools:
                pool = self._broker_pools.pop(broker_addr)
                await pool.close()

            self._logger.info(
                "已移除异步Broker",
                extra={
                    "broker_addr": broker_addr,
                    "timestamp": time.time(),
                },
            )

    async def connection_pool(self, broker_addr: str) -> AsyncConnectionPool | None:
        """获取Broker连接池

        Args:
            broker_addr: Broker地址

        Returns:
            AsyncConnectionPool | None: 连接池实例，如果不存在则返回None
        """
        # 使用读锁进行读取操作
        async with AsyncReadWriteContext(self._rwlock, write=False):
            if self._broker_pools.get(broker_addr):
                return self._broker_pools[broker_addr]
            return None

    async def must_connection_pool(self, broker_addr: str) -> AsyncConnectionPool:
        """获取Broker连接池，如果不存在则创建

        Args:
            broker_addr: Broker地址

        Returns:
            AsyncConnectionPool: 连接池实例
        """
        # 先使用读锁检查是否存在
        async with AsyncReadWriteContext(self._rwlock, write=False):
            if self._broker_pools.get(broker_addr):
                return self._broker_pools[broker_addr]

        # 不存在时调用add_broker（内部使用写锁）
        await self.add_broker(broker_addr)

        # 再次使用读锁获取
        async with AsyncReadWriteContext(self._rwlock, write=False):
            return self._broker_pools[broker_addr]
