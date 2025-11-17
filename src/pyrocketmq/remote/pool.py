"""
连接池实现 - 重构版本
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

from .async_remote import AsyncClientRequestFunc, AsyncRemote
from .config import RemoteConfig
from .errors import ConnectionError
from .sync_remote import ClientRequestFunc, Remote


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
            for _ in range(self.pool_size):
                remote: Remote = self._build_connection()
                self._pool.append(remote)

        self._logger.info(
            "连接池初始化完成",
            extra={"pool_size": self.pool_size},
        )

    def _extract_from_pool(self) -> Remote | None:
        """从连接池中提取一个连接

        Returns:
            Remote | None: 提取的连接，如果池为空则返回None
        """
        with self._pool_lock:
            if self._pool:
                return self._pool.pop(0)
            return None

    def _wait_for_available_connection(
        self, timeout: float, start_time: float
    ) -> Remote:
        """等待直到有可用连接或超时

        Args:
            timeout: 超时时间
            start_time: 开始等待的时间

        Returns:
            Remote: 可用的连接

        Raises:
            TimeoutError: 等待超时
        """
        while True:
            connection = self._extract_from_pool()
            if connection is not None:
                return connection

            # 检查是否超时
            if time.time() - start_time > timeout:
                raise TimeoutError("获取连接超时")

            # 短暂等待后重试
            time.sleep(0.1)

    def _validate_connection(self, connection: Remote, usage: str | None) -> Remote:
        """验证连接有效性，如果无效则重建

        Args:
            connection: 要验证的连接
            usage: 连接使用场景

        Returns:
            Remote: 有效的连接

        Raises:
            ConnectionError: 连接重建失败
        """
        if connection.is_connected:
            return connection

        # 连接无效，需要重建
        self._logger.warning(
            "获取到已断开的连接，正在淘汰并重建连接",
            extra={
                "address": self.address,
                "remaining_connections": len(self._pool),
                "usage": usage,
            },
        )

        # 关闭断开的连接
        try:
            connection.close()
        except Exception as e:
            self._logger.warning("关闭断开连接时发生错误", extra={"error": str(e)})

        # 重建连接
        conn: Remote = self._build_connection()

        # 添加新连接到池中
        self._add_connection(conn)
        return conn

    def _build_connection(self) -> Remote:
        """建立一个新的连接

        Returns:
            Remote: 新建的连接

        Raises:
            ConnectionError: 连接失败
        """
        try:
            from .factory import RemoteFactory

            new_connection: Remote = RemoteFactory.create_sync_remote(
                self.address, self.remote_config, self.transport_config
            )
            new_connection.connect()

            self._logger.info(
                "成功连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                },
            )

            return new_connection

        except Exception as e:
            self._logger.error(
                "连接失败", extra={"address": self.address, "error": str(e)}
            )
            raise ConnectionError("连接失败") from e

    def _log_connection_success(self, usage: str | None) -> None:
        """记录成功获取连接的日志

        Args:
            usage: 连接使用场景
        """
        log_extra: dict[str, str | int] = {"remaining_connections": len(self._pool)}
        if usage:
            log_extra["usage"] = usage

        self._logger.debug(
            "获取连接成功",
            extra=log_extra,
        )

    @contextmanager
    def get_connection(
        self, timeout: float | None = None, usage: str | None = None
    ) -> Generator[Any, None, None]:
        """获取连接

        Args:
            timeout: 获取连接超时时间
            usage: 连接使用场景说明

        Yields:
            远程通信实例

        Raises:
            ResourceExhaustedError: 连接池耗尽
            TimeoutError: 获取连接超时
            ConnectionError: 连接重建失败
        """
        connection: Remote | None = None
        start_time = time.time()

        try:
            # 获取连接超时时间
            get_timeout: float = timeout or self.remote_config.connection_pool_timeout

            # 等待并获取可用连接
            connection = self._wait_for_available_connection(get_timeout, start_time)

            # 验证连接有效性，必要时重建
            connection = self._validate_connection(connection, usage)

            # 记录成功日志
            self._log_connection_success(usage)

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

    def _add_connection(self, connection: Remote) -> None:
        """添加连接到池中"""
        with self._pool_lock:
            if len(self._pool) < self.pool_size:
                self._pool.append(connection)
                self._logger.debug(
                    "添加连接到池中",
                    extra={"current_connections": len(self._pool)},
                )
            else:
                self._logger.warning(
                    "连接池已满，无法添加连接",
                    extra={"current_connections": len(self._pool)},
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

    def register_request_processor(
        self, request_code: int, processor_func: ClientRequestFunc
    ) -> None:
        """为连接池中的所有remote注册请求处理器

        Args:
            request_code: 请求代码
            processor_func: 处理器函数
        """
        with self._pool_lock:
            for remote in self._pool:
                remote.register_request_processor_lazy(request_code, processor_func)
            self._logger.debug(
                "为连接池中的所有remote注册请求处理器",
                extra={
                    "request_code": request_code,
                    "connection_count": len(self._pool),
                },
            )

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

            for _ in range(self.pool_size):
                conn: AsyncRemote = await self._build_connection()
                self._pool.append(conn)

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

    async def _extract_from_pool(self) -> AsyncRemote | None:
        """从连接池中提取一个连接

        Returns:
            AsyncRemote | None: 提取的连接，如果池为空则返回None
        """
        async with self._pool_lock:
            if self._pool:
                return self._pool.pop(0)
            return None

    async def _wait_for_available_connection(
        self, timeout: float, start_time: float
    ) -> AsyncRemote:
        """等待直到有可用连接或超时

        Args:
            timeout: 超时时间
            start_time: 开始等待的时间

        Returns:
            AsyncRemote: 可用的连接

        Raises:
            TimeoutError: 等待超时
        """
        while True:
            connection = await self._extract_from_pool()
            if connection is not None:
                return connection

            # 检查是否超时
            if time.time() - start_time > timeout:
                raise TimeoutError("获取连接超时")

            # 短暂等待后重试
            await asyncio.sleep(0.1)

    async def _validate_connection(
        self, connection: AsyncRemote, usage: str | None
    ) -> AsyncRemote:
        """验证连接有效性，如果无效则重建

        Args:
            connection: 要验证的连接
            usage: 连接使用场景

        Returns:
            AsyncRemote: 有效的连接

        Raises:
            ConnectionError: 连接重建失败
        """
        if connection.is_connected:
            return connection

        # 连接无效，需要重建
        self._logger.warning(
            "获取到已断开的异步连接，正在淘汰并重建连接",
            extra={
                "address": self.address,
                "remaining_connections": len(self._pool),
                "usage": usage,
            },
        )

        # 关闭断开的连接
        try:
            await connection.close()
        except Exception as e:
            self._logger.warning("关闭断开异步连接时发生错误", extra={"error": str(e)})

        # 重建连接
        conn: AsyncRemote = await self._build_connection()
        # 添加新连接到池中
        await self._add_connection(conn)
        return conn

    async def _build_connection(self) -> AsyncRemote:
        """重建一个新的连接

        Returns:
            AsyncRemote: 新建的连接

        Raises:
            ConnectionError: 连接失败
        """
        try:
            from .factory import RemoteFactory

            new_connection = RemoteFactory.create_async_remote(
                self.address, self.remote_config, self.transport_config
            )
            await new_connection.connect()

            self._logger.info(
                "成功异步连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                },
            )

            return new_connection

        except Exception as e:
            self._logger.error(
                "异步连接失败", extra={"address": self.address, "error": str(e)}
            )
            raise ConnectionError("连接失败") from e

    def _log_connection_success(self, usage: str | None) -> None:
        """记录成功获取连接的日志

        Args:
            usage: 连接使用场景
        """
        log_extra: dict[str, str | int] = {"remaining_connections": len(self._pool)}
        if usage:
            log_extra["usage"] = usage

        self._logger.debug(
            "获取异步连接成功",
            extra=log_extra,
        )

    @asynccontextmanager
    async def get_connection(
        self, timeout: float | None = None, usage: str | None = None
    ) -> AsyncGenerator[Any, Any]:
        """获取连接

        Args:
            timeout: 获取连接超时时间
            usage: 连接使用场景说明

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

            # 等待并获取可用连接
            connection = await self._wait_for_available_connection(
                get_timeout, start_time
            )

            # 验证连接有效性，必要时重建
            connection = await self._validate_connection(connection, usage)

            # 记录成功日志
            self._log_connection_success(usage)

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

    async def _add_connection(self, connection: AsyncRemote) -> None:
        """添加新连接到池中"""
        async with self._pool_lock:
            self._pool.append(connection)

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

    async def register_request_processor(
        self, request_code: int, processor_func: AsyncClientRequestFunc
    ) -> None:
        """为异步连接池中的所有remote注册请求处理器

        Args:
            request_code: 请求代码
            processor_func: 处理器函数
        """
        # 确保连接池已初始化
        if self._initialize_task is None or self._initialize_task.done():
            await self.initialize()

        async with self._pool_lock:
            for remote in self._pool:
                await remote.register_request_processor_lazy(
                    request_code, processor_func
                )
            self._logger.debug(
                "为异步连接池中的所有remote注册请求处理器",
                extra={
                    "request_code": request_code,
                    "connection_count": len(self._pool),
                },
            )

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """异步上下文管理器出口"""
        await self.close()
