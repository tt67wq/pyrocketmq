"""
连接池实现
"""

import asyncio
import logging
import threading
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Generator

from pyrocketmq.logging import get_logger
from pyrocketmq.transport.config import TransportConfig

from .async_remote import AsyncRemote
from .config import RemoteConfig
from .errors import ConnectionError
from .sync_remote import Remote


class ConnectionPool:
    """同步连接池"""

    def __init__(
        self,
        address: str | tuple[str, int],
        pool_size: int = 1,
        remote_config: RemoteConfig | None = None,
        transport_config: TransportConfig | None = None,
    ) -> None:
        self.address: str | tuple[str, int] = address
        self.pool_size: int = pool_size
        self.remote_config: RemoteConfig = remote_config or RemoteConfig()
        self.transport_config: TransportConfig = transport_config or TransportConfig()

        # 更新配置中的连接池大小
        self.remote_config = self.remote_config.with_connection_pool_size(pool_size)

        self._logger: logging.Logger = get_logger("remote.pool.sync")

        # 连接池
        self._pool: list[Remote] = []
        self._pool_lock: threading.Lock = threading.Lock()

        # 创建连接池
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """初始化连接池"""
        with self._pool_lock:
            for i in range(self.pool_size):
                try:
                    # 这里需要导入工厂函数，避免循环导入
                    from .factory import RemoteFactory

                    remote = RemoteFactory.create_sync_remote(
                        self.address, self.remote_config, self.transport_config
                    )
                    remote.connect()
                    self._pool.append(remote)
                    self._logger.debug(
                        "连接池初始化连接",
                        extra={
                            "current": i + 1,
                            "total": self.pool_size,
                        },
                    )
                except Exception as e:
                    self._logger.error(
                        "初始化连接池连接失败",
                        extra={"error": str(e)},
                    )
                    raise ConnectionError(f"初始化连接池失败: {e}") from e

            self._logger.info(
                "连接池初始化完成",
                extra={"pool_size": self.pool_size},
            )

    @contextmanager
    def get_connection(
        self, timeout: float | None = None
    ) -> Generator[Any, None, None]:
        """获取连接

        Args:
            timeout: 获取连接超时时间

        Yields:
            远程通信实例

        Raises:
            ResourceExhaustedError: 连接池耗尽
            TimeoutError: 获取连接超时
        """
        connection: Remote | None = None
        start_time = time.time()

        try:
            # 获取连接超时时间
            get_timeout: float = timeout or self.remote_config.connection_pool_timeout

            while True:
                with self._pool_lock:
                    if self._pool:
                        connection = self._pool.pop(0)
                        break

                # 检查是否超时
                if time.time() - start_time > get_timeout:
                    raise TimeoutError("获取连接超时")

                # 短暂等待后重试
                time.sleep(0.1)

            self._logger.debug(
                "获取连接成功",
                extra={"remaining_connections": len(self._pool)},
            )
            yield connection

        except Exception:
            # 如果连接已建立但发生错误，尝试放回池中
            if connection and connection.is_connected:
                self._return_connection(connection)
            raise
        finally:
            # 正常完成时归还连接
            if connection and connection.is_connected:
                self._return_connection(connection)

    def _return_connection(self, connection: Remote) -> None:
        """归还连接到池中"""
        if connection.is_connected:
            with self._pool_lock:
                if len(self._pool) < self.pool_size:
                    self._pool.append(connection)
                    self._logger.debug(
                        "归还连接到池中",
                        extra={"current_connections": len(self._pool)},
                    )
                    return

        # 连接已断开或池已满，关闭连接
        try:
            connection.close()
        except Exception as e:
            self._logger.warning(
                "关闭连接时发生错误",
                extra={"error": str(e)},
            )

    def close(self) -> None:
        """关闭连接池"""
        with self._pool_lock:
            for connection in self._pool:
                try:
                    connection.close()
                except Exception as e:
                    self._logger.warning(
                        "关闭连接时发生错误",
                        extra={"error": str(e)},
                    )
            self._pool.clear()

        self._logger.info("连接池已关闭")

    @property
    def size(self) -> int:
        """连接池大小"""
        return self.pool_size

    @property
    def active_connections(self) -> int:
        """活跃连接数"""
        with self._pool_lock:
            return len(self._pool)

    @property
    def available_connections(self) -> int:
        """可用连接数"""
        with self._pool_lock:
            return len(self._pool)

    def __enter__(self) -> "ConnectionPool":
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """上下文管理器出口"""
        self.close()


class AsyncConnectionPool:
    """异步连接池"""

    def __init__(
        self,
        address: str | tuple[str, int],
        pool_size: int = 1,
        remote_config: RemoteConfig | None = None,
        transport_config: TransportConfig | None = None,
    ) -> None:
        self.address: str | tuple[str, int] = address
        self.pool_size: int = pool_size
        self.remote_config: RemoteConfig = remote_config or RemoteConfig()
        self.transport_config: TransportConfig = transport_config or TransportConfig()

        # 更新配置中的连接池大小
        self.remote_config = self.remote_config.with_connection_pool_size(pool_size)

        self._logger: logging.Logger = get_logger("remote.pool.async")

        # 连接池
        self._pool: list[AsyncRemote] = []
        self._pool_lock: asyncio.Lock = asyncio.Lock()

        # 创建连接池
        self._initialize_task: asyncio.Task[None] | None = None
        self._is_initialized: bool = False

    async def _initialize_pool(self) -> None:
        """初始化连接池"""
        # 防止重复初始化
        if self._is_initialized:
            return

        async with self._pool_lock:
            # 双重检查，防止在等待锁的过程中已经被其他任务初始化
            if self._is_initialized:
                return

            for i in range(self.pool_size):
                try:
                    # 这里需要导入工厂函数，避免循环导入
                    from .factory import RemoteFactory

                    remote = RemoteFactory.create_async_remote(
                        self.address, self.remote_config, self.transport_config
                    )
                    await remote.connect()
                    self._pool.append(remote)
                    self._logger.debug(
                        "异步连接池初始化连接",
                        extra={
                            "current": i + 1,
                            "total": self.pool_size,
                        },
                    )
                except Exception as e:
                    self._logger.error(
                        "初始化异步连接池连接失败",
                        extra={"error": str(e)},
                    )
                    raise ConnectionError(f"初始化连接池失败: {e}") from e

            # 标记为已初始化
            self._is_initialized = True
            self._logger.info(
                "异步连接池初始化完成",
                extra={"pool_size": self.pool_size},
            )

    async def initialize(self) -> None:
        """初始化连接池（异步）"""
        if self._initialize_task is None or self._initialize_task.done():
            self._initialize_task = asyncio.create_task(
                self._initialize_pool(), name="pool-initialize"
            )

        await self._initialize_task

    @asynccontextmanager
    async def get_connection(
        self, timeout: float | None = None
    ) -> AsyncGenerator[Any, Any]:
        """获取连接

        Args:
            timeout: 获取连接超时时间

        Yields:
            远程通信实例

        Raises:
            ResourceExhaustedError: 连接池耗尽
            TimeoutError: 获取连接超时
        """
        # 确保连接池已初始化
        if self._initialize_task is None or self._initialize_task.done():
            await self.initialize()

        connection: AsyncRemote | None = None
        start_time: float = time.time()

        try:
            # 获取连接超时时间
            get_timeout = timeout or self.remote_config.connection_pool_timeout

            while True:
                async with self._pool_lock:
                    if self._pool:
                        connection = self._pool.pop(0)
                        break

                # 检查是否超时
                if time.time() - start_time > get_timeout:
                    raise TimeoutError("获取连接超时")

                # 短暂等待后重试
                await asyncio.sleep(0.1)

            self._logger.debug(
                "获取异步连接成功",
                extra={"remaining_connections": len(self._pool)},
            )
            yield connection

        except Exception:
            # 如果连接已建立但发生错误，尝试放回池中
            if connection and connection.is_connected:
                await self._return_connection(connection)
            raise
        finally:
            # 正常完成时归还连接
            if connection and connection.is_connected:
                await self._return_connection(connection)

    async def _return_connection(self, connection: AsyncRemote) -> None:
        """归还连接到池中"""
        if connection.is_connected:
            async with self._pool_lock:
                if len(self._pool) < self.pool_size:
                    self._pool.append(connection)
                    self._logger.debug(
                        "归还异步连接到池中",
                        extra={"current_connections": len(self._pool)},
                    )
                    return

        # 连接已断开或池已满，关闭连接
        try:
            await connection.close()
        except Exception as e:
            self._logger.warning(
                "关闭异步连接时发生错误",
                extra={"error": str(e)},
            )

    async def close(self) -> None:
        """关闭连接池"""
        async with self._pool_lock:
            for connection in self._pool:
                try:
                    await connection.close()
                except Exception as e:
                    self._logger.warning(
                        "关闭异步连接时发生错误",
                        extra={"error": str(e)},
                    )
            self._pool.clear()

        # 取消初始化任务
        if self._initialize_task and not self._initialize_task.done():
            self._initialize_task.cancel()
            try:
                await self._initialize_task
            except asyncio.CancelledError:
                pass

        # 重置初始化状态，允许重新初始化
        self._is_initialized = False
        self._initialize_task = None

        self._logger.info("异步连接池已关闭")

    @property
    def size(self) -> int:
        """连接池大小"""
        return self.pool_size

    async def get_active_connections(self) -> int:
        """活跃连接数"""
        async with self._pool_lock:
            return len(self._pool)

    async def get_available_connections(self) -> int:
        """可用连接数"""
        async with self._pool_lock:
            return len(self._pool)

    async def __aenter__(self) -> "AsyncConnectionPool":
        """异步上下文管理器入口"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """异步上下文管理器出口"""
        await self.close()
