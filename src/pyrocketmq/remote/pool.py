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


class ConnectionInfo:
    """同步连接信息包装器，记录连接的创建时间和存活时间相关信息"""

    def __init__(self, connection: Remote, max_lifetime: float = 0.0):
        """
        初始化连接信息

        Args:
            connection: 实际的同步连接对象（Remote）
            max_lifetime: 最大存活时间（秒），0表示无限制
        """
        self.connection = connection
        self.max_lifetime = max_lifetime
        self.created_at = time.time()
        self.last_used_at = self.created_at
        self.is_expired = False

    @property
    def age(self) -> float:
        """获取连接的年龄（从创建到现在的时间，秒）"""
        return time.time() - self.created_at

    @property
    def idle_time(self) -> float:
        """获取连接的空闲时间（从最后使用到现在的时间，秒）"""
        return time.time() - self.last_used_at

    def update_last_used(self) -> None:
        """更新最后使用时间"""
        self.last_used_at = time.time()

    def is_connection_expired(self) -> bool:
        """检查连接是否已过期"""
        if self.max_lifetime <= 0:
            return False
        return self.age > self.max_lifetime

    def mark_as_expired(self) -> None:
        """标记连接为已过期"""
        self.is_expired = True

    def close(self) -> None:
        """同步关闭连接"""
        try:
            if hasattr(self.connection, "close"):
                # 同步连接
                self.connection.close()
        except Exception as e:
            logger = get_logger("remote.pool")
            logger.warning("关闭同步连接时发生错误", extra={"error": str(e)})


class AsyncConnectionInfo:
    """异步连接信息包装器，记录异步连接的创建时间和存活时间相关信息"""

    def __init__(self, connection: AsyncRemote, max_lifetime: float = 0.0):
        """
        初始化异步连接信息

        Args:
            connection: 实际的异步连接对象（AsyncRemote）
            max_lifetime: 最大存活时间（秒），0表示无限制
        """
        self.connection = connection
        self.max_lifetime = max_lifetime
        self.created_at = time.time()
        self.last_used_at = self.created_at
        self.is_expired = False

    @property
    def age(self) -> float:
        """获取连接的年龄（从创建到现在的时间，秒）"""
        return time.time() - self.created_at

    @property
    def idle_time(self) -> float:
        """获取连接的空闲时间（从最后使用到现在的时间，秒）"""
        return time.time() - self.last_used_at

    def update_last_used(self) -> None:
        """更新最后使用时间"""
        self.last_used_at = time.time()

    def is_connection_expired(self) -> bool:
        """检查连接是否已过期"""
        if self.max_lifetime <= 0:
            return False
        return self.age > self.max_lifetime

    def mark_as_expired(self) -> None:
        """标记连接为已过期"""
        self.is_expired = True

    async def close(self) -> None:
        """异步关闭连接"""
        try:
            # 异步连接
            await self.connection.close()
        except Exception as e:
            logger = get_logger("remote.pool")
            logger.warning("关闭异步连接时发生错误", extra={"error": str(e)})


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

        # 连接池 - 使用ConnectionInfo包装
        self._pool: list[ConnectionInfo] = []
        self._pool_lock: threading.Lock = threading.Lock()

        # 统计信息
        self._expired_connections_count: int = 0
        self._recreated_connections_count: int = 0

        # 请求处理器注册表
        self._request_processors: dict[int, ClientRequestFunc] = {}

        # 创建连接池
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """初始化连接池"""
        with self._pool_lock:
            for _ in range(self.pool_size):
                conn_info: ConnectionInfo = self._build_connection()
                self._pool.append(conn_info)

        self._logger.info(
            "连接池初始化完成",
            extra={
                "pool_size": self.pool_size,
                "max_lifetime": self.remote_config.connection_max_lifetime,
            },
        )

    def _extract_from_pool(self) -> ConnectionInfo | None:
        """从连接池中提取一个连接

        Returns:
            ConnectionInfo | None: 提取的连接信息，如果池为空则返回None
        """
        with self._pool_lock:
            if self._pool:
                return self._pool.pop(0)
            return None

    def _wait_for_available_connection(
        self, timeout: float, start_time: float
    ) -> ConnectionInfo:
        """等待直到有可用连接或超时

        Args:
            timeout: 超时时间
            start_time: 开始等待的时间

        Returns:
            ConnectionInfo: 可用的连接信息

        Raises:
            TimeoutError: 等待超时
        """
        while True:
            conn_info = self._extract_from_pool()
            if conn_info is not None:
                return conn_info

            # 检查是否超时
            if time.time() - start_time > timeout:
                raise TimeoutError("获取连接超时")

            # 短暂等待后重试
            time.sleep(0.1)

    def _validate_connection(
        self, conn_info: ConnectionInfo, usage: str | None
    ) -> Remote:
        """验证连接有效性，如果无效或已过期则重建

        Args:
            conn_info: 要验证的连接信息
            usage: 连接使用场景

        Returns:
            Remote: 有效的连接

        Raises:
            ConnectionError: 连接重建失败
        """
        connection = conn_info.connection

        # 首先检查连接是否已过期
        if conn_info.is_connection_expired():
            self._expired_connections_count += 1
            self._logger.debug(
                "连接已过期，正在淘汰并重建连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                    "usage": usage,
                    "connection_age": conn_info.age,
                    "max_lifetime": conn_info.max_lifetime,
                },
            )

            # 关闭过期连接
            conn_info.close()
            conn_info.mark_as_expired()

            # 重建连接
            new_conn_info: ConnectionInfo = self._build_connection()
            self._recreated_connections_count += 1

            # 添加新连接到池中
            self._add_connection(new_conn_info)
            return new_conn_info.connection

        # 然后检查连接是否已断开
        if not connection.is_connected:
            self._logger.warning(
                "获取到已断开的连接，正在淘汰并重建连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                    "usage": usage,
                    "connection_age": conn_info.age,
                },
            )

            # 关闭断开的连接
            conn_info.close()

            # 重建连接
            new_conn_info = self._build_connection()

            # 添加新连接到池中
            self._add_connection(new_conn_info)
            return new_conn_info.connection

        # 更新最后使用时间
        conn_info.update_last_used()
        return connection

    def _build_connection(self) -> ConnectionInfo:
        """建立一个新的连接

        Returns:
            ConnectionInfo: 新建的连接信息

        Raises:
            ConnectionError: 连接失败
        """
        try:
            from .factory import RemoteFactory

            new_connection: Remote = RemoteFactory.create_sync_remote(
                self.address, self.remote_config, self.transport_config
            )
            new_connection.connect()

            # 创建连接信息包装器
            conn_info = ConnectionInfo(
                connection=new_connection,
                max_lifetime=self.remote_config.connection_max_lifetime,
            )

            # 为新连接注册所有已注册的请求处理器
            for request_code, processor_func in self._request_processors.items():
                new_connection.register_request_processor_lazy(
                    request_code, processor_func
                )

            self._logger.debug(
                "成功连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                    "max_lifetime": self.remote_config.connection_max_lifetime,
                    "registered_processors": len(self._request_processors),
                },
            )

            return conn_info

        except Exception as e:
            self._logger.error(
                "连接失败", extra={"address": self.address, "error": str(e)}
            )
            raise ConnectionError("连接失败") from e

    def _log_connection_success(
        self, usage: str | None, conn_info: ConnectionInfo
    ) -> None:
        """记录成功获取连接的日志

        Args:
            usage: 连接使用场景
            conn_info: 连接信息
        """
        log_extra: dict[str, str | int | float] = {
            "remaining_connections": len(self._pool),
            "connection_age": conn_info.age,
            "connection_idle_time": conn_info.idle_time,
        }
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
        conn_info: ConnectionInfo | None = None
        connection: Remote | None = None
        start_time = time.time()

        try:
            # 获取连接超时时间
            get_timeout: float = timeout or self.remote_config.connection_pool_timeout

            # 等待并获取可用连接
            conn_info = self._wait_for_available_connection(get_timeout, start_time)

            # 验证连接有效性，必要时重建
            connection = self._validate_connection(conn_info, usage)

            # 记录成功日志
            self._log_connection_success(usage, conn_info)

            yield connection

        except Exception:
            # 如果连接已建立但发生错误，尝试放回池中
            if conn_info and conn_info.connection.is_connected:
                self._return_connection(conn_info)
            raise
        finally:
            # 正常完成时归还连接
            if conn_info and conn_info.connection.is_connected:
                self._return_connection(conn_info)

    def _return_connection(self, conn_info: ConnectionInfo) -> None:
        """归还连接到池中"""
        if conn_info.connection.is_connected and not conn_info.is_expired:
            with self._pool_lock:
                if len(self._pool) < self.pool_size:
                    self._pool.append(conn_info)
                    self._logger.debug(
                        "归还连接到池中",
                        extra={
                            "current_connections": len(self._pool),
                            "connection_age": conn_info.age,
                        },
                    )
                    return

        # 连接已断开、已过期或池已满，关闭连接
        conn_info.close()

    def _add_connection(self, conn_info: ConnectionInfo) -> None:
        """添加连接到池中"""
        with self._pool_lock:
            if len(self._pool) < self.pool_size:
                self._pool.append(conn_info)
                self._logger.debug(
                    "添加连接到池中",
                    extra={"current_connections": len(self._pool)},
                )
            else:
                self._logger.warning(
                    "连接池已满，无法添加连接",
                    extra={"current_connections": len(self._pool)},
                )
                # 池已满，关闭连接
                conn_info.close()

    def close(self) -> None:
        """关闭连接池"""
        with self._pool_lock:
            for conn_info in self._pool:
                try:
                    conn_info.close()
                except Exception as e:
                    self._logger.warning(
                        "关闭连接时发生错误",
                        extra={"error": str(e)},
                    )
            self._pool.clear()

        self._logger.info(
            "连接池已关闭",
            extra={
                "expired_connections_count": self._expired_connections_count,
                "recreated_connections_count": self._recreated_connections_count,
            },
        )

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
        # 保存处理器到注册表，供新连接使用
        self._request_processors[request_code] = processor_func

        with self._pool_lock:
            for conn_info in self._pool:
                conn_info.connection.register_request_processor_lazy(
                    request_code, processor_func
                )
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

    def cleanup_expired_connections(self) -> int:
        """清理已过期的连接

        Returns:
            int: 清理的连接数量
        """
        cleaned_count = 0

        with self._pool_lock:
            expired_connections: list[tuple[int, ConnectionInfo]] = []
            for i, conn_info in enumerate(self._pool):
                if conn_info.is_connection_expired():
                    expired_connections.append((i, conn_info))

            # 从后往前删除，避免索引问题
            for i, conn_info in reversed(expired_connections):
                self._pool.pop(i)
                conn_info.close()
                cleaned_count += 1
                self._expired_connections_count += 1

        if cleaned_count > 0:
            self._logger.info(
                "清理过期连接",
                extra={
                    "cleaned_count": cleaned_count,
                    "remaining_connections": len(self._pool),
                },
            )

        return cleaned_count


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

        # 连接池 - 使用ConnectionInfo包装
        self._pool: list[AsyncConnectionInfo] = []
        self._pool_lock: asyncio.Lock = asyncio.Lock()

        # 统计信息
        self._expired_connections_count: int = 0
        self._recreated_connections_count: int = 0

        # 请求处理器注册表
        self._request_processors: dict[int, AsyncClientRequestFunc] = {}

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
                conn_info: AsyncConnectionInfo = await self._build_connection()
                self._pool.append(conn_info)

            # 标记为已初始化
            self._is_initialized = True
            self._logger.info(
                "异步连接池初始化完成",
                extra={
                    "pool_size": self.pool_size,
                    "max_lifetime": self.remote_config.connection_max_lifetime,
                },
            )

    async def initialize(self) -> None:
        """初始化连接池（异步）"""
        if self._initialize_task is None or self._initialize_task.done():
            self._initialize_task = asyncio.create_task(
                self._initialize_pool(), name="pool-initialize"
            )

        await self._initialize_task

    async def _extract_from_pool(self) -> AsyncConnectionInfo | None:
        """从连接池中提取一个连接

        Returns:
            ConnectionInfo | None: 提取的连接信息，如果池为空则返回None
        """
        async with self._pool_lock:
            if self._pool:
                return self._pool.pop(0)
            return None

    async def _wait_for_available_connection(
        self, timeout: float, start_time: float
    ) -> AsyncConnectionInfo:
        """等待直到有可用连接或超时

        Args:
            timeout: 超时时间
            start_time: 开始等待的时间

        Returns:
            AsyncConnectionInfo: 可用的连接信息

        Raises:
            TimeoutError: 等待超时
        """
        while True:
            conn_info = await self._extract_from_pool()
            if conn_info is not None:
                return conn_info

            # 检查是否超时
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"获取连接超时, timeout:{timeout}, pool_size:{self.pool_size}"
                )

            # 短暂等待后重试
            await asyncio.sleep(0.1)

    async def _validate_connection(
        self, conn_info: AsyncConnectionInfo, usage: str | None
    ) -> AsyncRemote:
        """验证连接有效性，如果无效或已过期则重建

        Args:
            conn_info: 要验证的连接信息
            usage: 连接使用场景

        Returns:
            AsyncRemote: 有效的连接

        Raises:
            ConnectionError: 连接重建失败
        """
        connection = conn_info.connection

        # 首先检查连接是否已过期
        if conn_info.is_connection_expired():
            self._expired_connections_count += 1
            self._logger.debug(
                "异步连接已过期，正在淘汰并重建连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                    "usage": usage,
                    "connection_age": conn_info.age,
                    "max_lifetime": conn_info.max_lifetime,
                },
            )

            # 关闭过期连接
            await conn_info.close()
            conn_info.mark_as_expired()

            # 重建连接
            new_conn_info: AsyncConnectionInfo = await self._build_connection()
            self._recreated_connections_count += 1

            # 添加新连接到池中
            await self._add_connection(new_conn_info)
            return new_conn_info.connection

        # 然后检查连接是否已断开
        if not connection.is_connected:
            self._logger.warning(
                "获取到已断开的异步连接，正在淘汰并重建连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                    "usage": usage,
                    "connection_age": conn_info.age,
                },
            )

            # 关闭断开的连接
            await conn_info.close()

            # 重建连接
            new_conn_info = await self._build_connection()

            # 添加新连接到池中
            await self._add_connection(new_conn_info)
            return new_conn_info.connection

        # 更新最后使用时间
        conn_info.update_last_used()
        return connection

    async def _build_connection(self) -> AsyncConnectionInfo:
        """重建一个新的连接

        Returns:
            AsyncConnectionInfo: 新建的连接信息

        Raises:
            ConnectionError: 连接失败
        """
        try:
            from .factory import RemoteFactory

            new_connection = RemoteFactory.create_async_remote(
                self.address, self.remote_config, self.transport_config
            )
            await new_connection.connect()

            # 创建异步连接信息包装器
            conn_info = AsyncConnectionInfo(
                connection=new_connection,
                max_lifetime=self.remote_config.connection_max_lifetime,
            )

            # 为新连接注册所有已注册的请求处理器
            for request_code, processor_func in self._request_processors.items():
                await new_connection.register_request_processor_lazy(
                    request_code, processor_func
                )

            self._logger.info(
                "成功异步连接",
                extra={
                    "address": self.address,
                    "remaining_connections": len(self._pool),
                    "max_lifetime": self.remote_config.connection_max_lifetime,
                    "registered_processors": len(self._request_processors),
                },
            )

            return conn_info

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

        conn_info: AsyncConnectionInfo | None = None
        connection: AsyncRemote | None = None
        start_time: float = time.time()

        try:
            # 获取连接超时时间
            get_timeout = timeout or self.remote_config.connection_pool_timeout

            # 等待并获取可用连接
            conn_info = await self._wait_for_available_connection(
                get_timeout, start_time
            )

            # 验证连接有效性，必要时重建
            connection = await self._validate_connection(conn_info, usage)

            # 记录成功日志
            self._log_connection_success(usage)

            yield connection

        except Exception:
            # 如果连接已建立但发生错误，尝试放回池中
            if conn_info and conn_info.connection.is_connected:
                await self._return_connection(conn_info)
            raise
        finally:
            # 正常完成时归还连接
            if conn_info and conn_info.connection.is_connected:
                await self._return_connection(conn_info)

    async def _return_connection(self, conn_info: AsyncConnectionInfo) -> None:
        """归还连接到池中"""
        if conn_info.connection.is_connected and not conn_info.is_expired:
            async with self._pool_lock:
                if len(self._pool) < self.pool_size:
                    self._pool.append(conn_info)
                    self._logger.debug(
                        "归还异步连接到池中",
                        extra={"current_connections": len(self._pool)},
                    )
                    return

        # 连接已断开或池已满，关闭连接
        try:
            await conn_info.close()
        except Exception as e:
            self._logger.warning(
                "关闭异步连接时发生错误",
                extra={"error": str(e)},
            )

    async def _add_connection(self, connection: AsyncConnectionInfo) -> None:
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
        # 保存处理器到注册表，供新连接使用
        self._request_processors[request_code] = processor_func

        # 确保连接池已初始化
        if self._initialize_task is None or self._initialize_task.done():
            await self.initialize()

        async with self._pool_lock:
            for conn_info in self._pool:
                await conn_info.connection.register_request_processor_lazy(
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
