"""
异步Broker管理器模块

提供异步版本的Broker连接管理功能，适用于高并发异步应用场景。
"""

import logging
import time

from pyrocketmq.logging import get_logger
from pyrocketmq.remote.async_remote import AsyncClientRequestFunc
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
    _pool_processors: dict[int, AsyncClientRequestFunc]

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: TransportConfig | None = None,
    ):
        """初始化异步Broker管理器

        Args:
            remote_config: 远程通信配置
            transport_config: 传输层配置
        """
        self.remote_config = remote_config
        self.transport_config = transport_config
        self.connection_pool_size = remote_config.connection_pool_size

        self._logger = get_logger("broker.manager.async")

        # Broker连接池映射
        self._broker_pools = {}
        self._rwlock = AsyncReadWriteLock()

        # 存储需要为所有连接池注册的处理器
        self._pool_processors = {}

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

        # 为新创建的连接池注册所有已存储的处理器
        for request_code, processor_func in self._pool_processors.items():
            await pool.register_request_processor(request_code, processor_func)

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

    async def register_pool_processor(
        self, request_code: int, processor_func: AsyncClientRequestFunc
    ) -> None:
        """注册连接池处理器

        注册一个异步处理器函数，该函数会自动应用到当前和所有未来创建的连接池。
        这对于需要在所有Broker连接上统一处理特定请求代码的场景非常有用。

        注册流程:
        -----------
        1. 将处理器函数存储到内部注册表中
        2. 为所有现有连接池注册该处理器
        3. 后续创建的新连接池会自动应用已注册的处理器

        应用场景:
        -----------
        - 需要为所有Broker连接注册相同的异步请求处理器
        - 统一处理特定的响应代码
        - 实现全局的异步请求拦截或处理逻辑
        - 避免在每个连接池创建后重复注册处理器

        线程安全:
        -----------
        使用异步读写锁确保注册过程的线程安全，防止并发修改导致的竞争条件。

        Args:
            request_code (int): 请求代码
                - 用于标识特定类型的请求
                - 必须是整数类型
                - 相同的request_code重复注册会覆盖之前的处理器
            processor_func (AsyncClientRequestFunc): 异步处理器函数
                - 类型为 Callable[[RemotingCommand, tuple[str, int]], Awaitable[RemotingCommand | None]]
                - 接收RemotingCommand和连接地址，返回处理后的RemotingCommand或None
                - 用于异步处理服务器主动发起的请求或特定类型的响应

        Example:
            >>> async def custom_processor(request, addr):
            ...     # 异步处理自定义请求
            ...     print(f"异步处理请求 {request.code} 来自 {addr}")
            ...     await asyncio.sleep(0.1)  # 模拟异步操作
            ...     return None  # 返回None表示不处理
            ...
            >>> manager = AsyncBrokerManager(remote_config)
            >>> # 注册处理器，会应用到所有连接池
            >>> await manager.register_pool_processor(1001, custom_processor)
            >>>
            >>> # 添加Broker，新连接池会自动注册处理器
            >>> await manager.add_broker("localhost:10911")
            >>>
            >>> # 为已存在的连接池注册处理器
            >>> await manager.add_broker("broker1:10911")
            >>> await manager.register_pool_processor(1002, another_processor)
            >>> # 1002处理器会立即应用到broker1的连接池

        Note:
            - 处理器注册是全局的，会影响到所有连接池
            - 重复注册相同的request_code会覆盖之前的处理器
            - 注册过程是异步且线程安全的
            - 处理器函数应该是协程函数，可以被多个连接并发调用
        """
        # 存储处理器到注册表
        self._pool_processors[request_code] = processor_func

        # 为所有现有的连接池注册处理器
        async with AsyncReadWriteContext(self._rwlock, write=False):
            for pool in self._broker_pools.values():
                await pool.register_request_processor(request_code, processor_func)

        self._logger.info(
            "已注册异步连接池处理器",
            extra={
                "request_code": request_code,
                "existing_pools_count": len(self._broker_pools),
                "timestamp": time.time(),
            },
        )
