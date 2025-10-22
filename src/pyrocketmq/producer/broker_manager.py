"""
Broker连接管理器模块

该模块提供了RocketMQ Broker连接的完整管理功能，包括：
- Broker连接池：管理到多个Broker的连接池
- 健康检查：定期检查Broker连接的健康状态
- 故障转移：自动检测Broker故障并转移流量
- 连接生命周期管理：连接的创建、维护和销毁
- 负载均衡：在多个可用Broker间分配连接负载

核心组件：
1. BrokerConnectionPool: 管理单个Broker的连接池
2. BrokerManager: 管理多个Broker的连接和路由
3. HealthChecker: 执行健康检查任务
4. FailoverManager: 处理故障转移逻辑

使用示例:
    # 创建Broker管理器
    broker_manager = BrokerManager(config)

    # 启动管理器
    await broker_manager.start()

    # 获取可用连接
    connection = await broker_manager.get_connection("broker-a:10911")

    # 使用连接发送消息
    response = await connection.rpc(request)

    # 释放连接
    await broker_manager.release_connection("broker-a:10911", connection)

    # 关闭管理器
    await broker_manager.shutdown()
"""

import asyncio
import queue
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from pyrocketmq.logging import get_logger
from pyrocketmq.remote.async_remote import AsyncRemote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.sync_remote import Remote
from pyrocketmq.transport.config import TransportConfig


class BrokerState(Enum):
    """Broker状态枚举"""

    UNKNOWN = "unknown"  # 未知状态
    HEALTHY = "healthy"  # 健康状态
    DEGRADED = "degraded"  # 性能下降状态
    UNHEALTHY = "unhealthy"  # 不健康状态
    SUSPENDED = "suspended"  # 暂停使用状态
    FAILED = "failed"  # 故障状态
    RECOVERING = "recovering"  # 恢复中状态


@dataclass
class BrokerConnectionInfo:
    """Broker连接信息"""

    broker_addr: str  # Broker地址
    broker_name: Optional[str] = ""  # Broker名称
    connections: List[AsyncRemote] = field(default_factory=list)  # 连接池
    state: BrokerState = BrokerState.UNKNOWN  # 当前状态
    last_health_check: float = field(
        default_factory=time.time
    )  # 最后健康检查时间
    consecutive_failures: int = 0  # 连续失败次数
    total_requests: int = 0  # 总请求数
    failed_requests: int = 0  # 失败请求数
    avg_response_time: float = 0.0  # 平均响应时间
    last_used_time: float = field(default_factory=time.time)  # 最后使用时间

    def __post_init__(self):
        """初始化后处理"""
        if not self.broker_addr:
            raise ValueError("broker_addr cannot be empty")
        if not self.broker_name:
            # 从地址中提取broker名称
            self.broker_name = self.broker_addr.split(":")[0]

    @property
    def success_rate(self) -> float:
        """计算成功率"""
        if self.total_requests == 0:
            return 1.0
        return (
            self.total_requests - self.failed_requests
        ) / self.total_requests

    @property
    def failure_rate(self) -> float:
        """计算失败率"""
        return 1.0 - self.success_rate

    def update_request_stats(self, success: bool, response_time: float):
        """更新请求统计信息"""
        self.total_requests += 1
        if not success:
            self.failed_requests += 1

        # 更新平均响应时间（指数移动平均）
        alpha = 0.1  # 平滑因子
        self.avg_response_time = (
            alpha * response_time + (1 - alpha) * self.avg_response_time
        )
        self.last_used_time = time.time()

    def reset_stats(self):
        """重置统计信息"""
        self.total_requests = 0
        self.failed_requests = 0
        self.avg_response_time = 0.0
        self.consecutive_failures = 0


class BrokerConnectionPool:
    """Broker连接池

    管理到单个Broker的连接池，提供连接的获取、释放和维护功能。
    支持连接复用、健康检查和自动故障恢复。
    """

    def __init__(
        self,
        broker_addr: str,
        broker_name: str,
        transport_config: TransportConfig,
        remote_config: RemoteConfig,
        max_connections: int = 5,
        connection_timeout: float = 10.0,
    ):
        """初始化连接池

        Args:
            broker_addr: Broker地址，格式为"host:port"
            broker_name: Broker名称
            transport_config: 传输层配置
            remote_config: 远程通信配置
            max_connections: 最大连接数
            connection_timeout: 连接超时时间
        """
        self.broker_addr = broker_addr
        self.broker_name = broker_name
        self.transport_config = transport_config
        self.remote_config = remote_config
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout

        self._logger = get_logger(f"broker.pool.{broker_name}")

        # 连接池状态
        self._connections: List[AsyncRemote] = []
        self._available_connections: asyncio.Queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._closed = False

        # 统计信息
        self._total_created = 0
        self._total_destroyed = 0
        self._active_connections = 0

        self._logger.info(f"初始化Broker连接池: {broker_addr}")

    async def get_connection(self) -> AsyncRemote:
        """获取可用连接

        优先从连接池中获取可用连接，如果池中没有可用连接且未达到
        最大连接数限制，则创建新连接。

        Returns:
            AsyncRemote: 可用的连接实例

        Raises:
            ConnectionError: 连接失败或连接池已关闭
        """
        if self._closed:
            raise ConnectionError(f"连接池已关闭: {self.broker_addr}")

        try:
            # 尝试从队列获取可用连接（带超时）
            connection = await asyncio.wait_for(
                self._available_connections.get(),
                timeout=self.connection_timeout,
            )

            # 检查连接是否仍然有效
            if connection.is_connected:
                self._logger.debug(f"从连接池获取连接: {self.broker_addr}")
                return connection
            else:
                # 连接已断开，销毁并继续尝试
                self._logger.warning(
                    f"发现无效连接，销毁重建: {self.broker_addr}"
                )
                await self._destroy_connection(connection)

        except asyncio.TimeoutError:
            # 超时，说明没有可用连接
            pass

        # 创建新连接
        async with self._lock:
            if self._closed:
                raise ConnectionError(f"连接池已关闭: {self.broker_addr}")

            if len(self._connections) < self.max_connections:
                connection = await self._create_connection()
                self._logger.debug(f"创建新连接: {self.broker_addr}")
                return connection

        # 达到最大连接数限制，等待可用连接
        self._logger.info(f"连接池已满，等待可用连接: {self.broker_addr}")
        connection = await asyncio.wait_for(
            self._available_connections.get(), timeout=self.connection_timeout
        )

        if not connection.is_connected:
            await self._destroy_connection(connection)
            raise ConnectionError(f"获取到无效连接: {self.broker_addr}")

        return connection

    async def release_connection(self, connection: AsyncRemote) -> None:
        """释放连接回连接池

        Args:
            connection: 要释放的连接实例
        """
        if self._closed:
            # 连接池已关闭，直接销毁连接
            await self._destroy_connection(connection)
            return

        if connection.is_connected:
            # 连接仍然有效，放回连接池
            try:
                await self._available_connections.put(connection)
                self._logger.debug(f"连接已释放回连接池: {self.broker_addr}")
            except asyncio.QueueFull:
                # 队列已满（不应该发生），销毁连接
                self._logger.warning(
                    f"连接池队列已满，销毁连接: {self.broker_addr}"
                )
                await self._destroy_connection(connection)
        else:
            # 连接已断开，销毁
            self._logger.warning(f"连接已断开，销毁连接: {self.broker_addr}")
            await self._destroy_connection(connection)

    async def close(self) -> None:
        """关闭连接池

        销毁所有连接并释放资源。
        """
        self._closed = True
        self._logger.info(f"开始关闭连接池: {self.broker_addr}")

        async with self._lock:
            # 销毁所有连接
            connections_to_destroy = self._connections.copy()
            self._connections.clear()

        # 并发销毁所有连接
        if connections_to_destroy:
            destroy_tasks = [
                self._destroy_connection(conn)
                for conn in connections_to_destroy
            ]
            await asyncio.gather(*destroy_tasks, return_exceptions=True)

        # 清空队列
        while not self._available_connections.empty():
            try:
                self._available_connections.get_nowait()
            except asyncio.QueueEmpty:
                break

        self._logger.info(
            f"连接池已关闭: {self.broker_addr}, "
            f"总共创建={self._total_created}, "
            f"总共销毁={self._total_destroyed}"
        )

    async def health_check(self) -> bool:
        """执行健康检查

        检查连接池中是否有可用连接，如果没有则尝试创建一个。

        Returns:
            bool: 连接池是否健康
        """
        if self._closed:
            return False

        # 检查是否有可用连接
        if not self._available_connections.empty():
            return True

        # 尝试创建测试连接
        try:
            test_connection = await self._create_connection()
            await self._destroy_connection(test_connection)
            return True
        except Exception as e:
            self._logger.error(f"健康检查失败: {self.broker_addr}, error={e}")
            return False

    async def _create_connection(self) -> AsyncRemote:
        """创建新连接

        Returns:
            AsyncRemote: 新创建的连接实例

        Raises:
            ConnectionError: 连接创建失败
        """
        try:
            # 创建AsyncRemote实例
            connection = AsyncRemote(self.transport_config, self.remote_config)

            # 建立连接
            await connection.connect()

            # 添加到连接列表
            self._connections.append(connection)
            self._total_created += 1
            self._active_connections += 1

            self._logger.debug(
                f"创建连接成功: {self.broker_addr}, "
                f"当前连接数={len(self._connections)}"
            )

            return connection

        except Exception as e:
            self._logger.error(f"创建连接失败: {self.broker_addr}, error={e}")
            raise ConnectionError(
                f"无法连接到Broker {self.broker_addr}: {e}"
            ) from e

    async def _destroy_connection(self, connection: AsyncRemote) -> None:
        """销毁连接

        Args:
            connection: 要销毁的连接实例
        """
        try:
            # 从连接列表中移除
            if connection in self._connections:
                self._connections.remove(connection)
                self._total_destroyed += 1
                self._active_connections -= 1

            # 关闭连接
            await connection.close()

            self._logger.debug(f"销毁连接成功: {self.broker_addr}")

        except Exception as e:
            self._logger.error(f"销毁连接失败: {self.broker_addr}, error={e}")

    @property
    def active_connections_count(self) -> int:
        """获取活跃连接数"""
        return self._active_connections

    @property
    def available_connections_count(self) -> int:
        """获取可用连接数"""
        return self._available_connections.qsize()

    @property
    def total_connections_count(self) -> int:
        """获取总连接数"""
        return len(self._connections)

    def get_stats(self) -> Dict:
        """获取连接池统计信息

        Returns:
            Dict: 包含各种统计指标的字典
        """
        return {
            "broker_addr": self.broker_addr,
            "broker_name": self.broker_name,
            "active_connections": self.active_connections_count,
            "available_connections": self.available_connections_count,
            "total_connections": self.total_connections_count,
            "max_connections": self.max_connections,
            "total_created": self._total_created,
            "total_destroyed": self._total_destroyed,
            "is_closed": self._closed,
        }


class BrokerManager:
    """Broker连接管理器

    管理多个Broker的连接，提供统一的服务接口。包括：
    - Broker连接池的创建和管理
    - 健康检查和故障检测
    - 自动故障转移和恢复
    - 负载均衡和连接选择
    """

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: Optional[TransportConfig] = None,
        health_check_interval: float = 30.0,
        health_check_timeout: float = 5.0,
        max_consecutive_failures: int = 3,
        connection_pool_size: int = 5,
    ):
        """初始化Broker管理器

        Args:
            remote_config: 远程通信配置
            transport_config: 传输层配置
            health_check_interval: 健康检查间隔（秒）
            health_check_timeout: 健康检查超时时间（秒）
            max_consecutive_failures: 最大连续失败次数
            connection_pool_size: 每个Broker的连接池大小
        """
        self.remote_config = remote_config
        self.transport_config = transport_config
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        self.max_consecutive_failures = max_consecutive_failures
        self.connection_pool_size = connection_pool_size

        self._logger = get_logger("broker.manager")

        # Broker连接信息映射
        self._brokers: Dict[str, BrokerConnectionInfo] = {}
        self._broker_pools: Dict[str, BrokerConnectionPool] = {}
        self._lock = asyncio.Lock()

        # 后台任务
        self._health_check_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        self._logger.info("Broker管理器初始化完成")

    async def start(self) -> None:
        """启动Broker管理器

        启动健康检查等后台任务。
        """
        if self._health_check_task and not self._health_check_task.done():
            self._logger.warning("Broker管理器已经在运行")
            return

        self._shutdown_event.clear()
        self._health_check_task = asyncio.create_task(
            self._health_check_worker(), name="broker-health-check"
        )

        self._logger.info("Broker管理器已启动")

    async def shutdown(self) -> None:
        """关闭Broker管理器

        停止所有后台任务并关闭所有连接池。
        """
        self._logger.info("开始关闭Broker管理器")

        # 停止健康检查任务
        self._shutdown_event.set()
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # 关闭所有连接池
        async with self._lock:
            broker_pools = list(self._broker_pools.values())
            self._broker_pools.clear()

        if broker_pools:
            close_tasks = [pool.close() for pool in broker_pools]
            await asyncio.gather(*close_tasks, return_exceptions=True)

        self._logger.info("Broker管理器已关闭")

    async def add_broker(
        self, broker_addr: str, broker_name: Optional[str] = None
    ) -> None:
        """添加Broker

        Args:
            broker_addr: Broker地址，格式为"host:port"
            broker_name: Broker名称，为None时从地址提取
        """
        if not broker_name:
            broker_name = broker_addr.split(":")[0]

        async with self._lock:
            if broker_addr in self._brokers:
                self._logger.warning(f"Broker已存在: {broker_addr}")
                return

            # 创建连接信息
            broker_info = BrokerConnectionInfo(
                broker_addr=broker_addr,
                broker_name=broker_name,
                state=BrokerState.UNKNOWN,
            )
            self._brokers[broker_addr] = broker_info

            # 创建连接池
            if self.transport_config:
                # Remove host and port from transport_config dict to avoid duplication
                transport_config_dict = {
                    k: v
                    for k, v in self.transport_config.__dict__.items()
                    if k not in ("host", "port")
                }
                transport_config = TransportConfig(
                    host=broker_addr.split(":")[0],
                    port=int(broker_addr.split(":")[1]),
                    **transport_config_dict,
                )
            else:
                transport_config = TransportConfig(
                    host=broker_addr.split(":")[0],
                    port=int(broker_addr.split(":")[1]),
                )

            pool = BrokerConnectionPool(
                broker_addr=broker_addr,
                broker_name=broker_name,
                transport_config=transport_config,
                remote_config=self.remote_config,
                max_connections=self.connection_pool_size,
            )
            self._broker_pools[broker_addr] = pool

            self._logger.info(f"已添加Broker: {broker_addr} ({broker_name})")

    async def remove_broker(self, broker_addr: str) -> None:
        """移除Broker

        Args:
            broker_addr: Broker地址
        """
        async with self._lock:
            if broker_addr not in self._brokers:
                self._logger.warning(f"Broker不存在: {broker_addr}")
                return

            # 关闭连接池
            if broker_addr in self._broker_pools:
                pool = self._broker_pools.pop(broker_addr)
                await pool.close()

            # 移除Broker信息
            del self._brokers[broker_addr]

            self._logger.info(f"已移除Broker: {broker_addr}")

    async def get_connection(self, broker_addr: str) -> AsyncRemote:
        """获取Broker连接

        Args:
            broker_addr: Broker地址

        Returns:
            AsyncRemote: 可用的连接实例

        Raises:
            ConnectionError: 连接失败或Broker不可用
        """
        async with self._lock:
            if broker_addr not in self._broker_pools:
                raise ConnectionError(f"Broker不存在: {broker_addr}")

            broker_info = self._brokers[broker_addr]
            if broker_info.state in [BrokerState.FAILED, BrokerState.UNKNOWN]:
                raise ConnectionError(
                    f"Broker不可用: {broker_addr}, state={broker_info.state.value}"
                )

            pool = self._broker_pools[broker_addr]

        try:
            connection = await pool.get_connection()
            broker_info.last_used_time = time.time()
            return connection
        except Exception as e:
            # 记录失败
            broker_info.consecutive_failures += 1
            broker_info.update_request_stats(False, 0.0)

            # 如果连续失败次数过多，标记为故障
            if (
                broker_info.consecutive_failures
                >= self.max_consecutive_failures
            ):
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    f"Broker标记为故障: {broker_addr}, "
                    f"连续失败次数={broker_info.consecutive_failures}"
                )

            raise ConnectionError(
                f"无法获取连接: {broker_addr}, error={e}"
            ) from e

    async def release_connection(
        self, broker_addr: str, connection: AsyncRemote
    ) -> None:
        """释放Broker连接

        Args:
            broker_addr: Broker地址
            connection: 连接实例
        """
        async with self._lock:
            if broker_addr not in self._broker_pools:
                self._logger.warning(
                    f"Broker不存在，直接关闭连接: {broker_addr}"
                )
                await connection.close()
                return

            pool = self._broker_pools[broker_addr]
            broker_info = self._brokers[broker_addr]

        try:
            await pool.release_connection(connection)
            # 重置连续失败计数
            broker_info.consecutive_failures = 0
            if broker_info.state == BrokerState.FAILED:
                broker_info.state = BrokerState.RECOVERING
                self._logger.info(f"Broker开始恢复: {broker_addr}")
        except Exception as e:
            self._logger.error(f"释放连接失败: {broker_addr}, error={e}")

    def get_healthy_brokers(self) -> List[str]:
        """获取健康的Broker列表

        Returns:
            List[str]: 健康的Broker地址列表
        """
        healthy_brokers = []
        for broker_addr, broker_info in self._brokers.items():
            if broker_info.state == BrokerState.HEALTHY:
                healthy_brokers.append(broker_addr)
        return healthy_brokers

    def get_available_brokers(self) -> List[str]:
        """获取可用的Broker列表

        Returns:
            List[str]: 可用的Broker地址列表（健康和恢复中）
        """
        available_brokers = []
        for broker_addr, broker_info in self._brokers.items():
            if broker_info.state in [
                BrokerState.HEALTHY,
                BrokerState.RECOVERING,
            ]:
                available_brokers.append(broker_addr)
        return available_brokers

    def get_broker_stats(self, broker_addr: str) -> Optional[Dict]:
        """获取Broker统计信息

        Args:
            broker_addr: Broker地址

        Returns:
            Optional[Dict]: 统计信息字典，如果Broker不存在则返回None
        """
        if broker_addr not in self._brokers:
            return None

        broker_info = self._brokers[broker_addr]
        pool_info = self._broker_pools[broker_addr].get_stats()

        return {
            "broker_addr": broker_addr,
            "broker_name": broker_info.broker_name,
            "state": broker_info.state.value,
            "consecutive_failures": broker_info.consecutive_failures,
            "total_requests": broker_info.total_requests,
            "failed_requests": broker_info.failed_requests,
            "success_rate": broker_info.success_rate,
            "avg_response_time": broker_info.avg_response_time,
            "last_health_check": broker_info.last_health_check,
            "last_used_time": broker_info.last_used_time,
            "connection_pool": pool_info,
        }

    def get_all_brokers_stats(self) -> Dict[str, Dict]:
        """获取所有Broker的统计信息

        Returns:
            Dict[str, Dict]: 所有Broker的统计信息，key为broker_addr
        """
        stats = {}
        for broker_addr in self._brokers:
            stats[broker_addr] = self.get_broker_stats(broker_addr)
        return stats

    async def _health_check_worker(self) -> None:
        """健康检查工作协程

        定期对所有Broker执行健康检查。
        """
        self._logger.info("健康检查任务启动")

        while not self._shutdown_event.is_set():
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"健康检查任务异常: {e}")
                await asyncio.sleep(5.0)  # 出错后短暂等待

        self._logger.info("健康检查任务结束")

    async def _perform_health_checks(self) -> None:
        """执行健康检查

        对所有已注册的Broker执行健康检查。
        """
        current_time = time.time()
        brokers_to_check = list(self._brokers.items())

        # 并发执行健康检查
        check_tasks = [
            self._check_single_broker(broker_addr, broker_info, current_time)
            for broker_addr, broker_info in brokers_to_check
        ]

        if check_tasks:
            await asyncio.gather(*check_tasks, return_exceptions=True)

    async def _check_single_broker(
        self,
        broker_addr: str,
        broker_info: BrokerConnectionInfo,
        current_time: float,
    ) -> None:
        """检查单个Broker的健康状态

        Args:
            broker_addr: Broker地址
            broker_info: Broker连接信息
            current_time: 当前时间戳
        """
        try:
            async with self._lock:
                if broker_addr not in self._broker_pools:
                    return

                pool = self._broker_pools[broker_addr]

            # 执行健康检查
            start_time = time.time()
            is_healthy = await asyncio.wait_for(
                pool.health_check(), timeout=self.health_check_timeout
            )
            response_time = time.time() - start_time

            # 更新Broker状态
            broker_info.last_health_check = current_time
            broker_info.update_request_stats(is_healthy, response_time)

            if is_healthy:
                if broker_info.state != BrokerState.HEALTHY:
                    broker_info.state = BrokerState.HEALTHY
                    broker_info.consecutive_failures = 0
                    self._logger.info(f"Broker恢复健康: {broker_addr}")
            else:
                broker_info.consecutive_failures += 1
                if (
                    broker_info.consecutive_failures
                    >= self.max_consecutive_failures
                ):
                    if broker_info.state != BrokerState.FAILED:
                        broker_info.state = BrokerState.FAILED
                        self._logger.error(
                            f"Broker健康检查失败，标记为故障: {broker_addr}"
                        )
                else:
                    broker_info.state = BrokerState.UNHEALTHY
                    self._logger.warning(
                        f"Broker健康检查失败: {broker_addr}, "
                        f"连续失败次数={broker_info.consecutive_failures}"
                    )

        except asyncio.TimeoutError:
            broker_info.consecutive_failures += 1
            broker_info.last_health_check = current_time
            if (
                broker_info.consecutive_failures
                >= self.max_consecutive_failures
            ):
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    f"Broker健康检查超时，标记为故障: {broker_addr}"
                )
            else:
                broker_info.state = BrokerState.UNHEALTHY
                self._logger.warning(f"Broker健康检查超时: {broker_addr}")

        except Exception as e:
            broker_info.consecutive_failures += 1
            broker_info.last_health_check = current_time
            if (
                broker_info.consecutive_failures
                >= self.max_consecutive_failures
            ):
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    f"Broker健康检查异常，标记为故障: {broker_addr}, error={e}"
                )
            else:
                broker_info.state = BrokerState.UNHEALTHY
                self._logger.warning(
                    f"Broker健康检查异常: {broker_addr}, error={e}"
                )

    @property
    def is_running(self) -> bool:
        """检查管理器是否正在运行"""
        return (
            self._health_check_task is not None
            and not self._health_check_task.done()
        )

    @property
    def brokers_count(self) -> int:
        """获取管理的Broker数量"""
        return len(self._brokers)

    @property
    def healthy_brokers_count(self) -> int:
        """获取健康的Broker数量"""
        return len(self.get_healthy_brokers())


class SyncBrokerConnectionPool:
    """同步版本的Broker连接池

    管理到单个Broker的同步连接池，提供连接的获取、释放和维护功能。
    支持连接复用、健康检查和自动故障恢复。使用线程和同步原语实现。
    """

    def __init__(
        self,
        broker_addr: str,
        broker_name: str,
        transport_config: TransportConfig,
        remote_config: RemoteConfig,
        max_connections: int = 5,
        connection_timeout: float = 10.0,
    ):
        """初始化同步连接池

        Args:
            broker_addr: Broker地址，格式为"host:port"
            broker_name: Broker名称
            transport_config: 传输层配置
            remote_config: 远程通信配置
            max_connections: 最大连接数
            connection_timeout: 连接超时时间
        """
        self.broker_addr = broker_addr
        self.broker_name = broker_name
        self.transport_config = transport_config
        self.remote_config = remote_config
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout

        self._logger = get_logger(f"broker.pool.sync.{broker_name}")

        # 连接池状态 - 使用线程安全的数据结构
        self._connections: List[Remote] = []
        self._available_connections = queue.Queue()
        self._lock = threading.Lock()
        self._closed = False

        # 统计信息
        self._total_created = 0
        self._total_destroyed = 0
        self._active_connections = 0

        self._logger.info(f"初始化同步Broker连接池: {broker_addr}")

    def get_connection(self) -> Remote:
        """获取可用连接

        优先从连接池中获取可用连接，如果池中没有可用连接且未达到
        最大连接数限制，则创建新连接。

        Returns:
            Remote: 可用的连接实例

        Raises:
            ConnectionError: 连接失败或连接池已关闭
        """
        if self._closed:
            raise ConnectionError(f"连接池已关闭: {self.broker_addr}")

        try:
            # 尝试从队列获取可用连接（带超时）
            connection = self._available_connections.get(
                timeout=self.connection_timeout
            )

            # 检查连接是否仍然有效
            if connection.is_connected:
                self._logger.debug(f"从连接池获取连接: {self.broker_addr}")
                return connection
            else:
                # 连接已断开，销毁并继续尝试
                self._logger.warning(
                    f"发现无效连接，销毁重建: {self.broker_addr}"
                )
                self._destroy_connection(connection)

        except queue.Empty:
            # 超时，说明没有可用连接
            pass

        # 创建新连接
        with self._lock:
            if self._closed:
                raise ConnectionError(f"连接池已关闭: {self.broker_addr}")

            if len(self._connections) < self.max_connections:
                connection = self._create_connection()
                self._logger.debug(f"创建新连接: {self.broker_addr}")
                return connection

        # 达到最大连接数限制，等待可用连接
        self._logger.info(f"连接池已满，等待可用连接: {self.broker_addr}")
        connection = self._available_connections.get(
            timeout=self.connection_timeout
        )

        if not connection.is_connected:
            self._destroy_connection(connection)
            raise ConnectionError(f"获取到无效连接: {self.broker_addr}")

        return connection

    def release_connection(self, connection: Remote) -> None:
        """释放连接回连接池

        Args:
            connection: 要释放的连接实例
        """
        if self._closed:
            # 连接池已关闭，直接销毁连接
            self._destroy_connection(connection)
            return

        if connection.is_connected:
            # 连接仍然有效，放回连接池
            try:
                self._available_connections.put(connection, block=False)
                self._logger.debug(f"连接已释放回连接池: {self.broker_addr}")
            except queue.Full:
                # 队列已满（不应该发生），销毁连接
                self._logger.warning(
                    f"连接池队列已满，销毁连接: {self.broker_addr}"
                )
                self._destroy_connection(connection)
        else:
            # 连接已断开，销毁
            self._logger.warning(f"连接已断开，销毁连接: {self.broker_addr}")
            self._destroy_connection(connection)

    def close(self) -> None:
        """关闭连接池

        销毁所有连接并释放资源。
        """
        self._closed = True
        self._logger.info(f"开始关闭连接池: {self.broker_addr}")

        with self._lock:
            # 销毁所有连接
            connections_to_destroy = self._connections.copy()
            self._connections.clear()

        # 销毁所有连接
        for connection in connections_to_destroy:
            self._destroy_connection(connection)

        # 清空队列
        while not self._available_connections.empty():
            try:
                self._available_connections.get_nowait()
            except queue.Empty:
                break

        self._logger.info(
            f"连接池已关闭: {self.broker_addr}, "
            f"总共创建={self._total_created}, "
            f"总共销毁={self._total_destroyed}"
        )

    def health_check(self) -> bool:
        """执行健康检查

        检查连接池中是否有可用连接，如果没有则尝试创建一个。

        Returns:
            bool: 连接池是否健康
        """
        if self._closed:
            return False

        # 检查是否有可用连接
        if not self._available_connections.empty():
            return True

        # 尝试创建测试连接
        try:
            test_connection = self._create_connection()
            self._destroy_connection(test_connection)
            return True
        except Exception as e:
            self._logger.error(f"健康检查失败: {self.broker_addr}, error={e}")
            return False

    def _create_connection(self) -> Remote:
        """创建新连接

        Returns:
            Remote: 新创建的连接实例

        Raises:
            ConnectionError: 连接创建失败
        """
        try:
            # 创建Remote实例
            connection = Remote(self.transport_config, self.remote_config)

            # 建立连接
            connection.connect()

            # 添加到连接列表
            with self._lock:
                self._connections.append(connection)
                self._total_created += 1
                self._active_connections += 1

            self._logger.debug(
                f"创建连接成功: {self.broker_addr}, "
                f"当前连接数={len(self._connections)}"
            )

            return connection

        except Exception as e:
            self._logger.error(f"创建连接失败: {self.broker_addr}, error={e}")
            raise ConnectionError(
                f"无法连接到Broker {self.broker_addr}: {e}"
            ) from e

    def _destroy_connection(self, connection: Remote) -> None:
        """销毁连接

        Args:
            connection: 要销毁的连接实例
        """
        try:
            # 从连接列表中移除
            with self._lock:
                if connection in self._connections:
                    self._connections.remove(connection)
                    self._total_destroyed += 1
                    self._active_connections -= 1

            # 关闭连接
            connection.close()

            self._logger.debug(f"销毁连接成功: {self.broker_addr}")

        except Exception as e:
            self._logger.error(f"销毁连接失败: {self.broker_addr}, error={e}")

    @property
    def active_connections_count(self) -> int:
        """获取活跃连接数"""
        with self._lock:
            return self._active_connections

    @property
    def available_connections_count(self) -> int:
        """获取可用连接数"""
        return self._available_connections.qsize()

    @property
    def total_connections_count(self) -> int:
        """获取总连接数"""
        with self._lock:
            return len(self._connections)

    def get_stats(self) -> Dict:
        """获取连接池统计信息

        Returns:
            Dict: 包含各种统计指标的字典
        """
        return {
            "broker_addr": self.broker_addr,
            "broker_name": self.broker_name,
            "active_connections": self.active_connections_count,
            "available_connections": self.available_connections_count,
            "total_connections": self.total_connections_count,
            "max_connections": self.max_connections,
            "total_created": self._total_created,
            "total_destroyed": self._total_destroyed,
            "is_closed": self._closed,
        }


class SyncBrokerManager:
    """同步版本的Broker连接管理器

    管理多个Broker的同步连接，提供统一的服务接口。包括：
    - Broker连接池的创建和管理
    - 健康检查和故障检测
    - 自动故障转移和恢复
    - 负载均衡和连接选择

    使用线程和同步原语实现，适用于同步应用场景。
    """

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: Optional[TransportConfig] = None,
        health_check_interval: float = 30.0,
        health_check_timeout: float = 5.0,
        max_consecutive_failures: int = 3,
        connection_pool_size: int = 5,
    ):
        """初始化同步Broker管理器

        Args:
            remote_config: 远程通信配置
            transport_config: 传输层配置
            health_check_interval: 健康检查间隔（秒）
            health_check_timeout: 健康检查超时时间（秒）
            max_consecutive_failures: 最大连续失败次数
            connection_pool_size: 每个Broker的连接池大小
        """
        self.remote_config = remote_config
        self.transport_config = transport_config
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        self.max_consecutive_failures = max_consecutive_failures
        self.connection_pool_size = connection_pool_size

        self._logger = get_logger("broker.manager.sync")

        # Broker连接信息映射
        self._brokers: Dict[str, BrokerConnectionInfo] = {}
        self._broker_pools: Dict[str, SyncBrokerConnectionPool] = {}
        self._lock = threading.Lock()

        # 后台线程
        self._health_check_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()

        self._logger.info("同步Broker管理器初始化完成")

    def start(self) -> None:
        """启动Broker管理器

        启动健康检查等后台线程。
        """
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._logger.warning("同步Broker管理器已经在运行")
            return

        self._shutdown_event.clear()
        self._health_check_thread = threading.Thread(
            target=self._health_check_worker,
            daemon=True,
            name="broker-health-check-sync",
        )
        self._health_check_thread.start()

        self._logger.info("同步Broker管理器已启动")

    def shutdown(self) -> None:
        """关闭Broker管理器

        停止所有后台线程并关闭所有连接池。
        """
        self._logger.info("开始关闭同步Broker管理器")

        # 停止健康检查线程
        self._shutdown_event.set()
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=10.0)

        # 关闭所有连接池
        with self._lock:
            broker_pools = list(self._broker_pools.values())
            self._broker_pools.clear()

        for pool in broker_pools:
            pool.close()

        self._logger.info("同步Broker管理器已关闭")

    def add_broker(
        self, broker_addr: str, broker_name: Optional[str] = None
    ) -> None:
        """添加Broker

        Args:
            broker_addr: Broker地址，格式为"host:port"
            broker_name: Broker名称，为None时从地址提取
        """
        if not broker_name:
            broker_name = broker_addr.split(":")[0]

        with self._lock:
            if broker_addr in self._brokers:
                self._logger.warning(f"Broker已存在: {broker_addr}")
                return

            # 创建连接信息
            broker_info = BrokerConnectionInfo(
                broker_addr=broker_addr,
                broker_name=broker_name,
                state=BrokerState.UNKNOWN,
            )
            self._brokers[broker_addr] = broker_info

            # 创建连接池
            if self.transport_config:
                # Remove host and port from transport_config dict to avoid duplication
                transport_config_dict = {
                    k: v
                    for k, v in self.transport_config.__dict__.items()
                    if k not in ("host", "port")
                }
                transport_config = TransportConfig(
                    host=broker_addr.split(":")[0],
                    port=int(broker_addr.split(":")[1]),
                    **transport_config_dict,
                )
            else:
                transport_config = TransportConfig(
                    host=broker_addr.split(":")[0],
                    port=int(broker_addr.split(":")[1]),
                )

            pool = SyncBrokerConnectionPool(
                broker_addr=broker_addr,
                broker_name=broker_name,
                transport_config=transport_config,
                remote_config=self.remote_config,
                max_connections=self.connection_pool_size,
            )
            self._broker_pools[broker_addr] = pool

            self._logger.info(f"已添加Broker: {broker_addr} ({broker_name})")

    def remove_broker(self, broker_addr: str) -> None:
        """移除Broker

        Args:
            broker_addr: Broker地址
        """
        with self._lock:
            if broker_addr not in self._brokers:
                self._logger.warning(f"Broker不存在: {broker_addr}")
                return

            # 关闭连接池
            if broker_addr in self._broker_pools:
                pool = self._broker_pools.pop(broker_addr)
                pool.close()

            # 移除Broker信息
            del self._brokers[broker_addr]

            self._logger.info(f"已移除Broker: {broker_addr}")

    def get_connection(self, broker_addr: str) -> Remote:
        """获取Broker连接

        Args:
            broker_addr: Broker地址

        Returns:
            Remote: 可用的连接实例

        Raises:
            ConnectionError: 连接失败或Broker不可用
        """
        with self._lock:
            if broker_addr not in self._broker_pools:
                raise ConnectionError(f"Broker不存在: {broker_addr}")

            broker_info = self._brokers[broker_addr]
            if broker_info.state in [BrokerState.FAILED, BrokerState.UNKNOWN]:
                raise ConnectionError(
                    f"Broker不可用: {broker_addr}, state={broker_info.state.value}"
                )

            pool = self._broker_pools[broker_addr]

        try:
            connection = pool.get_connection()
            broker_info.last_used_time = time.time()
            return connection
        except Exception as e:
            # 记录失败
            broker_info.consecutive_failures += 1
            broker_info.update_request_stats(False, 0.0)

            # 如果连续失败次数过多，标记为故障
            if (
                broker_info.consecutive_failures
                >= self.max_consecutive_failures
            ):
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    f"Broker标记为故障: {broker_addr}, "
                    f"连续失败次数={broker_info.consecutive_failures}"
                )

            raise ConnectionError(
                f"无法获取连接: {broker_addr}, error={e}"
            ) from e

    def release_connection(self, broker_addr: str, connection: Remote) -> None:
        """释放Broker连接

        Args:
            broker_addr: Broker地址
            connection: 连接实例
        """
        with self._lock:
            if broker_addr not in self._broker_pools:
                self._logger.warning(
                    f"Broker不存在，直接关闭连接: {broker_addr}"
                )
                connection.close()
                return

            pool = self._broker_pools[broker_addr]
            broker_info = self._brokers[broker_addr]

        try:
            pool.release_connection(connection)
            # 重置连续失败计数
            broker_info.consecutive_failures = 0
            if broker_info.state == BrokerState.FAILED:
                broker_info.state = BrokerState.RECOVERING
                self._logger.info(f"Broker开始恢复: {broker_addr}")
        except Exception as e:
            self._logger.error(f"释放连接失败: {broker_addr}, error={e}")

    def get_healthy_brokers(self) -> List[str]:
        """获取健康的Broker列表

        Returns:
            List[str]: 健康的Broker地址列表
        """
        healthy_brokers = []
        for broker_addr, broker_info in self._brokers.items():
            if broker_info.state == BrokerState.HEALTHY:
                healthy_brokers.append(broker_addr)
        return healthy_brokers

    def get_available_brokers(self) -> List[str]:
        """获取可用的Broker列表

        Returns:
            List[str]: 可用的Broker地址列表（健康和恢复中）
        """
        available_brokers = []
        for broker_addr, broker_info in self._brokers.items():
            if broker_info.state in [
                BrokerState.HEALTHY,
                BrokerState.RECOVERING,
            ]:
                available_brokers.append(broker_addr)
        return available_brokers

    def get_broker_stats(self, broker_addr: str) -> Optional[Dict]:
        """获取Broker统计信息

        Args:
            broker_addr: Broker地址

        Returns:
            Optional[Dict]: 统计信息字典，如果Broker不存在则返回None
        """
        if broker_addr not in self._brokers:
            return None

        broker_info = self._brokers[broker_addr]
        pool_info = self._broker_pools[broker_addr].get_stats()

        return {
            "broker_addr": broker_addr,
            "broker_name": broker_info.broker_name,
            "state": broker_info.state.value,
            "consecutive_failures": broker_info.consecutive_failures,
            "total_requests": broker_info.total_requests,
            "failed_requests": broker_info.failed_requests,
            "success_rate": broker_info.success_rate,
            "avg_response_time": broker_info.avg_response_time,
            "last_health_check": broker_info.last_health_check,
            "last_used_time": broker_info.last_used_time,
            "connection_pool": pool_info,
        }

    def get_all_brokers_stats(self) -> Dict[str, Dict]:
        """获取所有Broker的统计信息

        Returns:
            Dict[str, Dict]: 所有Broker的统计信息，key为broker_addr
        """
        stats = {}
        for broker_addr in self._brokers:
            stats[broker_addr] = self.get_broker_stats(broker_addr)
        return stats

    def _health_check_worker(self) -> None:
        """健康检查工作线程

        定期对所有Broker执行健康检查。
        """
        self._logger.info("健康检查线程启动")

        while not self._shutdown_event.wait(self.health_check_interval):
            try:
                self._perform_health_checks()
            except Exception as e:
                self._logger.error(f"健康检查任务异常: {e}")
                time.sleep(5.0)  # 出错后短暂等待

        self._logger.info("健康检查线程结束")

    def _perform_health_checks(self) -> None:
        """执行健康检查

        对所有已注册的Broker执行健康检查。
        """
        current_time = time.time()

        # 获取所有broker的副本，避免长时间持有锁
        brokers_to_check = []
        with self._lock:
            brokers_to_check = list(self._brokers.items())

        # 并发执行健康检查（使用线程池）
        check_threads = []
        for broker_addr, broker_info in brokers_to_check:
            thread = threading.Thread(
                target=self._check_single_broker,
                args=(broker_addr, broker_info, current_time),
                daemon=True,
            )
            thread.start()
            check_threads.append(thread)

        # 等待所有检查完成（设置超时）
        for thread in check_threads:
            thread.join(timeout=self.health_check_timeout)

    def _check_single_broker(
        self,
        broker_addr: str,
        broker_info: BrokerConnectionInfo,
        current_time: float,
    ) -> None:
        """检查单个Broker的健康状态

        Args:
            broker_addr: Broker地址
            broker_info: Broker连接信息
            current_time: 当前时间戳
        """
        try:
            with self._lock:
                if broker_addr not in self._broker_pools:
                    return

                pool = self._broker_pools[broker_addr]

            # 执行健康检查
            start_time = time.time()
            is_healthy = pool.health_check()
            response_time = time.time() - start_time

            # 更新Broker状态
            broker_info.last_health_check = current_time
            broker_info.update_request_stats(is_healthy, response_time)

            if is_healthy:
                if broker_info.state != BrokerState.HEALTHY:
                    broker_info.state = BrokerState.HEALTHY
                    broker_info.consecutive_failures = 0
                    self._logger.info(f"Broker恢复健康: {broker_addr}")
            else:
                broker_info.consecutive_failures += 1
                if (
                    broker_info.consecutive_failures
                    >= self.max_consecutive_failures
                ):
                    if broker_info.state != BrokerState.FAILED:
                        broker_info.state = BrokerState.FAILED
                        self._logger.error(
                            f"Broker健康检查失败，标记为故障: {broker_addr}"
                        )
                else:
                    broker_info.state = BrokerState.UNHEALTHY
                    self._logger.warning(
                        f"Broker健康检查失败: {broker_addr}, "
                        f"连续失败次数={broker_info.consecutive_failures}"
                    )

        except Exception as e:
            broker_info.consecutive_failures += 1
            broker_info.last_health_check = current_time
            if (
                broker_info.consecutive_failures
                >= self.max_consecutive_failures
            ):
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    f"Broker健康检查异常，标记为故障: {broker_addr}, error={e}"
                )
            else:
                broker_info.state = BrokerState.UNHEALTHY
                self._logger.warning(
                    f"Broker健康检查异常: {broker_addr}, error={e}"
                )

    @property
    def is_running(self) -> bool:
        """检查管理器是否正在运行"""
        return (
            self._health_check_thread is not None
            and self._health_check_thread.is_alive()
        )

    @property
    def brokers_count(self) -> int:
        """获取管理的Broker数量"""
        with self._lock:
            return len(self._brokers)

    @property
    def healthy_brokers_count(self) -> int:
        """获取健康的Broker数量"""
        return len(self.get_healthy_brokers())
