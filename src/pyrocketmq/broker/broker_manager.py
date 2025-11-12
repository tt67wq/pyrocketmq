import logging
import queue
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from pyrocketmq.broker.connection_info import BrokerConnectionInfo, BrokerState
from pyrocketmq.logging import get_logger
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.sync_remote import Remote
from pyrocketmq.transport.config import TransportConfig


class BrokerConnectionPool:
    """同步版本的Broker连接池

    管理到单个Broker的同步连接池，提供连接的获取、释放和维护功能。
    支持连接复用、健康检查和自动故障恢复。使用线程和同步原语实现。
    """

    broker_addr: str
    broker_name: str
    transport_config: TransportConfig
    remote_config: RemoteConfig
    max_connections: int
    connection_timeout: float
    _logger: logging.Logger
    _connections: list[Remote]
    _available_connections: queue.Queue[Remote]
    _lock: threading.Lock
    _closed: bool
    _total_created: int
    _total_destroyed: int
    _active_connections: int

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
        self._connections = []
        self._available_connections = queue.Queue()
        self._lock = threading.Lock()
        self._closed = False

        # 统计信息
        self._total_created = 0
        self._total_destroyed = 0
        self._active_connections = 0

        self._logger.info(
            "初始化同步Broker连接池",
            extra={
                "broker_addr": broker_addr,
                "broker_name": broker_name,
                "max_connections": max_connections,
                "connection_timeout": connection_timeout,
                "timestamp": time.time(),
            },
        )

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
                self._logger.debug(
                    "从连接池获取连接",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "connection_id": id(connection),
                        "timestamp": time.time(),
                    },
                )
                return connection
            else:
                # 连接已断开，销毁并继续尝试
                self._logger.warning(
                    "发现无效连接，销毁重建",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "connection_id": id(connection),
                        "timestamp": time.time(),
                    },
                )
                self._destroy_connection(connection)

        except queue.Empty:
            # 超时，说明没有可用连接
            self._logger.debug(
                "连接池超时，尝试创建新连接",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "timeout": self.connection_timeout,
                    "timestamp": time.time(),
                },
            )

        # 创建新连接
        with self._lock:
            if self._closed:
                raise ConnectionError(f"连接池已关闭: {self.broker_addr}")

            if len(self._connections) < self.max_connections:
                connection = self._create_connection()
                self._logger.debug(
                    "创建新连接",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "timestamp": time.time(),
                    },
                )
                return connection

        # 达到最大连接数限制，等待可用连接
        self._logger.info(
            "连接池已满，等待可用连接",
            extra={
                "broker_addr": self.broker_addr,
                "broker_name": self.broker_name,
                "max_connections": self.max_connections,
                "timeout": self.connection_timeout,
                "timestamp": time.time(),
            },
        )
        connection = self._available_connections.get(timeout=self.connection_timeout)

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
                self._logger.debug(
                    "连接已释放回连接池",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "connection_id": id(connection),
                        "timestamp": time.time(),
                    },
                )
            except queue.Full:
                # 队列已满（不应该发生），销毁连接
                self._logger.warning(
                    "连接池队列已满，销毁连接",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "connection_id": id(connection),
                        "timestamp": time.time(),
                    },
                )
                self._destroy_connection(connection)
        else:
            # 连接已断开，销毁
            self._logger.warning(
                "连接已断开，销毁连接",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "connection_id": id(connection),
                    "timestamp": time.time(),
                },
            )
            self._destroy_connection(connection)

    def close(self) -> None:
        """关闭连接池

        销毁所有连接并释放资源。
        """
        self._closed = True
        self._logger.info(
            "开始关闭连接池",
            extra={
                "broker_addr": self.broker_addr,
                "broker_name": self.broker_name,
                "total_connections": len(self._connections),
                "active_connections": self._active_connections,
                "timestamp": time.time(),
            },
        )

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
                _ = self._available_connections.get_nowait()
            except queue.Empty:
                break

        self._logger.info(
            "连接池已关闭",
            extra={
                "broker_addr": self.broker_addr,
                "broker_name": self.broker_name,
                "total_created": self._total_created,
                "total_destroyed": self._total_destroyed,
                "timestamp": time.time(),
            },
        )

    def health_check(self) -> bool:
        """执行健康检查

        检查连接池中是否有可用连接，如果没有则尝试创建一个并放回连接池。

        Returns:
            bool: 连接池是否健康
        """
        if self._closed:
            return False

        # 检查是否有可用连接
        if not self._available_connections.empty():
            return True

        # 尝试创建测试连接并放回连接池复用
        try:
            test_connection = self._create_connection()
            self._logger.debug(
                "健康检查创建连接，放回连接池",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "connection_id": id(test_connection),
                    "timestamp": time.time(),
                },
            )

            # 将健康检查创建的连接放回连接池，避免浪费
            try:
                self._available_connections.put(test_connection, block=False)
                self._logger.debug(
                    "健康检查连接已放回连接池",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "connection_id": id(test_connection),
                        "timestamp": time.time(),
                    },
                )
            except queue.Full:
                # 如果连接池满了，销毁连接
                self._logger.warning(
                    "连接池已满，销毁健康检查连接",
                    extra={
                        "broker_addr": self.broker_addr,
                        "broker_name": self.broker_name,
                        "connection_id": id(test_connection),
                        "timestamp": time.time(),
                    },
                )
                self._destroy_connection(test_connection)

            return True
        except Exception as e:
            self._logger.error(
                "健康检查失败",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
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
                "创建连接成功",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "connection_id": id(connection),
                    "total_connections": len(self._connections),
                    "active_connections": self._active_connections,
                    "timestamp": time.time(),
                },
            )

            return connection

        except Exception as e:
            self._logger.error(
                "创建连接失败",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise ConnectionError(f"无法连接到Broker {self.broker_addr}: {e}") from e

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

            self._logger.debug(
                "销毁连接成功",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "connection_id": id(connection),
                    "active_connections": self._active_connections,
                    "timestamp": time.time(),
                },
            )

        except Exception as e:
            self._logger.error(
                "销毁连接失败",
                extra={
                    "broker_addr": self.broker_addr,
                    "broker_name": self.broker_name,
                    "connection_id": id(connection),
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )

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

    def get_stats(self) -> dict[str, str | int | bool]:
        """获取连接池统计信息

        Returns:
            dict: 包含各种统计指标的字典
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
    """同步版本的Broker连接管理器

    管理多个Broker的同步连接，提供统一的服务接口。包括：
    - Broker连接池的创建和管理
    - 健康检查和故障检测
    - 自动故障转移和恢复
    - 负载均衡和连接选择

    使用线程和同步原语实现，适用于同步应用场景。
    """

    remote_config: RemoteConfig
    transport_config: TransportConfig | None
    health_check_interval: float
    health_check_timeout: float
    max_consecutive_failures: int
    connection_pool_size: int
    _logger: logging.Logger
    _brokers: dict[str, BrokerConnectionInfo]
    _broker_pools: dict[str, BrokerConnectionPool]
    _lock: threading.Lock
    _health_check_thread: threading.Thread | None
    _shutdown_event: threading.Event

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: TransportConfig | None = None,
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
        self._brokers = {}
        self._broker_pools = {}
        self._lock = threading.Lock()

        # 后台线程
        self._health_check_thread = None
        self._shutdown_event = threading.Event()

        self._logger.info(
            "同步Broker管理器初始化完成",
            extra={
                "health_check_interval": self.health_check_interval,
                "health_check_timeout": self.health_check_timeout,
                "max_consecutive_failures": self.max_consecutive_failures,
                "connection_pool_size": self.connection_pool_size,
                "timestamp": time.time(),
            },
        )

    def start(self) -> None:
        """启动Broker管理器

        启动健康检查等后台线程。
        """
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._logger.warning(
                "同步Broker管理器已经在运行",
                extra={
                    "thread_name": self._health_check_thread.name,
                    "thread_id": self._health_check_thread.native_id,
                    "timestamp": time.time(),
                },
            )
            return

        self._shutdown_event.clear()
        self._health_check_thread = threading.Thread(
            target=self._health_check_worker,
            daemon=True,
            name="broker-health-check-sync",
        )
        self._health_check_thread.start()

        self._logger.info(
            "同步Broker管理器已启动",
            extra={
                "thread_name": self._health_check_thread.name,
                "thread_id": self._health_check_thread.native_id,
                "health_check_interval": self.health_check_interval,
                "timestamp": time.time(),
            },
        )

    def shutdown(self) -> None:
        """关闭Broker管理器

        停止所有后台线程并关闭所有连接池。
        """
        self._logger.info(
            "开始关闭同步Broker管理器",
            extra={
                "total_brokers": len(self._brokers),
                "total_pools": len(self._broker_pools),
                "is_health_check_running": self._health_check_thread
                and self._health_check_thread.is_alive(),
                "timestamp": time.time(),
            },
        )

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

        self._logger.info(
            "同步Broker管理器已关闭",
            extra={
                "closed_brokers_count": len(broker_pools),
                "timestamp": time.time(),
            },
        )

    def add_broker(self, broker_addr: str, broker_name: str | None = None) -> None:
        """添加Broker

        Args:
            broker_addr: Broker地址，格式为"host:port"
            broker_name: Broker名称，为None时从地址提取
        """
        # self._logger.info(
        #     "开始添加同步Broker",
        #     extra={
        #         "broker_addr": broker_addr,
        #         "broker_name": broker_name,
        #         "timestamp": time.time(),
        #     },
        # )

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
                "同步Broker地址解析成功",
                extra={
                    "broker_addr": broker_addr,
                    "host": host,
                    "port": port,
                    "timestamp": time.time(),
                },
            )
        except ValueError as e:
            self._logger.error(
                "同步Broker地址解析失败",
                extra={
                    "broker_addr": broker_addr,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise ValueError(f"无效的Broker地址格式: {broker_addr}") from e

        if not broker_name:
            broker_name = broker_addr.split(":")[0]
            self._logger.debug(
                "从地址提取同步Broker名称",
                extra={
                    "broker_addr": broker_addr,
                    "extracted_broker_name": broker_name,
                    "timestamp": time.time(),
                },
            )

        with self._lock:
            if broker_addr in self._brokers:
                # self._logger.warning(
                #     "同步Broker已存在，跳过添加",
                #     extra={
                #         "broker_addr": broker_addr,
                #         "broker_name": broker_name,
                #         "timestamp": time.time(),
                #     },
                # )
                return

            try:
                # 创建连接信息
                self._logger.debug(
                    "创建同步Broker连接信息",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "timestamp": time.time(),
                    },
                )
                broker_info = BrokerConnectionInfo(
                    broker_addr=broker_addr,
                    broker_name=broker_name,
                    state=BrokerState.UNKNOWN,
                )
                self._brokers[broker_addr] = broker_info
                self._logger.debug(
                    "同步Broker连接信息创建成功",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "broker_state": broker_info.state.value,
                        "timestamp": time.time(),
                    },
                )

                # 创建传输配置
                self._logger.debug(
                    "创建同步传输配置",
                    extra={
                        "broker_addr": broker_addr,
                        "has_existing_config": self.transport_config is not None,
                        "timestamp": time.time(),
                    },
                )
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
                self._logger.debug(
                    "同步传输配置创建成功",
                    extra={
                        "broker_addr": broker_addr,
                        "transport_host": transport_config.host,
                        "transport_port": transport_config.port,
                        "timeout": transport_config.timeout,
                        "timestamp": time.time(),
                    },
                )

                # 创建同步连接池
                self._logger.debug(
                    "创建同步连接池",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "max_connections": self.connection_pool_size,
                        "timestamp": time.time(),
                    },
                )
                pool = BrokerConnectionPool(
                    broker_addr=broker_addr,
                    broker_name=broker_name,
                    transport_config=transport_config,
                    remote_config=self.remote_config,
                    max_connections=self.connection_pool_size,
                )
                self._broker_pools[broker_addr] = pool
                self._logger.debug(
                    "同步连接池创建成功",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "pool_id": id(pool),
                        "timestamp": time.time(),
                    },
                )

                # 立即尝试建立连接
                self._logger.info(
                    "正在建立与同步Broker的初始连接",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "pool_id": id(pool),
                        "timestamp": time.time(),
                    },
                )
                try:
                    # 执行健康检查来建立初始连接
                    connection_success = pool.health_check()
                    if connection_success:
                        broker_info.state = BrokerState.HEALTHY
                        broker_info.consecutive_failures = 0
                        self._logger.info(
                            "与同步Broker建立初始连接成功",
                            extra={
                                "broker_addr": broker_addr,
                                "broker_name": broker_name,
                                "broker_state": broker_info.state.value,
                                "pool_id": id(pool),
                                "timestamp": time.time(),
                            },
                        )
                    else:
                        broker_info.state = BrokerState.UNHEALTHY
                        broker_info.consecutive_failures = 1
                        self._logger.warning(
                            "与同步Broker建立初始连接失败",
                            extra={
                                "broker_addr": broker_addr,
                                "broker_name": broker_name,
                                "broker_state": broker_info.state.value,
                                "pool_id": id(pool),
                                "timestamp": time.time(),
                            },
                        )
                except Exception as e:
                    broker_info.state = BrokerState.FAILED
                    broker_info.consecutive_failures = 1
                    self._logger.error(
                        "与同步Broker建立初始连接时发生异常",
                        extra={
                            "broker_addr": broker_addr,
                            "broker_name": broker_name,
                            "broker_state": broker_info.state.value,
                            "pool_id": id(pool),
                            "error_message": str(e),
                            "timestamp": time.time(),
                        },
                    )

                self._logger.info(
                    "同步Broker添加完成",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "broker_state": broker_info.state.value,
                        "total_brokers": len(self._brokers),
                        "total_pools": len(self._broker_pools),
                        "consecutive_failures": broker_info.consecutive_failures,
                        "timestamp": time.time(),
                    },
                )

            except Exception as e:
                # 添加失败时清理
                self._logger.error(
                    "添加同步Broker失败",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
                )
                if broker_addr in self._brokers:
                    del self._brokers[broker_addr]
                if broker_addr in self._broker_pools:
                    del self._broker_pools[broker_addr]
                raise

    def remove_broker(self, broker_addr: str) -> None:
        """移除Broker

        Args:
            broker_addr: Broker地址
        """
        with self._lock:
            if broker_addr not in self._brokers:
                self._logger.warning(
                    "Broker不存在，无法移除",
                    extra={
                        "broker_addr": broker_addr,
                        "timestamp": time.time(),
                    },
                )
                return

            # 关闭连接池
            if broker_addr in self._broker_pools:
                pool = self._broker_pools.pop(broker_addr)
                pool.close()

            # 移除Broker信息
            broker_name = self._brokers[broker_addr].broker_name
            del self._brokers[broker_addr]

            self._logger.info(
                "已移除Broker",
                extra={
                    "broker_addr": broker_addr,
                    "broker_name": broker_name,
                    "remaining_brokers": len(self._brokers),
                    "timestamp": time.time(),
                },
            )

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
            if broker_info.consecutive_failures >= self.max_consecutive_failures:
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    "Broker标记为故障",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_info.broker_name,
                        "consecutive_failures": broker_info.consecutive_failures,
                        "max_consecutive_failures": self.max_consecutive_failures,
                        "timestamp": time.time(),
                    },
                )

            raise ConnectionError(f"无法获取连接: {broker_addr}, error={e}") from e

    def release_connection(self, broker_addr: str, connection: Remote) -> None:
        """释放Broker连接

        Args:
            broker_addr: Broker地址
            connection: 连接实例
        """
        with self._lock:
            if broker_addr not in self._broker_pools:
                self._logger.warning(
                    "Broker不存在，直接关闭连接",
                    extra={
                        "broker_addr": broker_addr,
                        "operation_type": "release_connection",
                        "status": "warning",
                        "timestamp": time.time(),
                    },
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
                self._logger.info(
                    "Broker开始恢复",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_info.broker_name,
                        "previous_state": BrokerState.FAILED.value,
                        "new_state": broker_info.state.value,
                        "connection_id": id(connection),
                        "timestamp": time.time(),
                    },
                )
        except Exception as e:
            self._logger.error(
                "释放连接失败",
                extra={
                    "broker_addr": broker_addr,
                    "broker_name": broker_info.broker_name,
                    "connection_id": id(connection),
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )

    def get_healthy_brokers(self) -> list[str]:
        """获取健康的Broker列表

        Returns:
            list[str]: 健康的Broker地址列表
        """
        healthy_brokers: list[str] = []
        for broker_addr, broker_info in self._brokers.items():
            if broker_info.state == BrokerState.HEALTHY:
                healthy_brokers.append(broker_addr)
        return healthy_brokers

    def get_available_brokers(self) -> list[str]:
        """获取可用的Broker列表

        Returns:
            list[str]: 可用的Broker地址列表（健康和恢复中）
        """
        available_brokers: list[str] = []
        for broker_addr, broker_info in self._brokers.items():
            if broker_info.state in [
                BrokerState.HEALTHY,
                BrokerState.RECOVERING,
            ]:
                available_brokers.append(broker_addr)
        return available_brokers

    def get_broker_stats(self, broker_addr: str) -> dict[str, Any] | None:
        """获取Broker统计信息

        Args:
            broker_addr: Broker地址

        Returns:
            Dict | None: 统计信息字典，如果Broker不存在则返回None
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

    def get_all_brokers_stats(self) -> dict[str, dict[str, Any]]:
        """获取所有Broker的统计信息

        Returns:
            dict[str, dict]: 所有Broker的统计信息，key为broker_addr
        """
        stats = {}
        for broker_addr in self._brokers:
            stats[broker_addr] = self.get_broker_stats(broker_addr)
        return stats

    def _health_check_worker(self) -> None:
        """健康检查工作线程

        定期对所有Broker执行健康检查。
        """
        self._logger.info(
            "健康检查线程启动",
            extra={
                "thread_name": threading.current_thread().name,
                "thread_id": threading.current_thread().native_id,
                "health_check_interval": self.health_check_interval,
                "timestamp": time.time(),
            },
        )

        while not self._shutdown_event.wait(self.health_check_interval):
            try:
                self._perform_health_checks()
            except Exception as e:
                self._logger.error(
                    "健康检查任务异常",
                    extra={
                        "thread_name": threading.current_thread().name,
                        "thread_id": threading.current_thread().native_id,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
                )
                time.sleep(5.0)  # 出错后短暂等待

        self._logger.info(
            "健康检查线程结束",
            extra={
                "thread_name": threading.current_thread().name,
                "thread_id": threading.current_thread().native_id,
                "timestamp": time.time(),
            },
        )

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
        check_threads: list[threading.Thread] = []
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
                    self._logger.info(
                        "Broker恢复健康",
                        extra={
                            "broker_addr": broker_addr,
                            "broker_name": broker_info.broker_name,
                            "previous_state": broker_info.state.value,
                            "response_time": response_time,
                            "timestamp": time.time(),
                        },
                    )
            else:
                broker_info.consecutive_failures += 1
                if broker_info.consecutive_failures >= self.max_consecutive_failures:
                    if broker_info.state != BrokerState.FAILED:
                        broker_info.state = BrokerState.FAILED
                        self._logger.error(
                            "Broker健康检查失败，标记为故障",
                            extra={
                                "broker_addr": broker_addr,
                                "broker_name": broker_info.broker_name,
                                "consecutive_failures": broker_info.consecutive_failures,
                                "max_consecutive_failures": self.max_consecutive_failures,
                                "timestamp": time.time(),
                            },
                        )
                else:
                    broker_info.state = BrokerState.UNHEALTHY
                    self._logger.warning(
                        "Broker健康检查失败",
                        extra={
                            "broker_addr": broker_addr,
                            "broker_name": broker_info.broker_name,
                            "consecutive_failures": broker_info.consecutive_failures,
                            "max_consecutive_failures": self.max_consecutive_failures,
                            "timestamp": time.time(),
                        },
                    )

        except Exception as e:
            broker_info.consecutive_failures += 1
            broker_info.last_health_check = current_time
            if broker_info.consecutive_failures >= self.max_consecutive_failures:
                broker_info.state = BrokerState.FAILED
                self._logger.error(
                    "Broker健康检查异常，标记为故障",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_info.broker_name,
                        "consecutive_failures": broker_info.consecutive_failures,
                        "max_consecutive_failures": self.max_consecutive_failures,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
                )
            else:
                broker_info.state = BrokerState.UNHEALTHY
                self._logger.warning(
                    "Broker健康检查异常",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_info.broker_name,
                        "consecutive_failures": broker_info.consecutive_failures,
                        "max_consecutive_failures": self.max_consecutive_failures,
                        "error_message": str(e),
                        "timestamp": time.time(),
                    },
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

    @contextmanager
    def connection(self, broker_addr: str) -> Generator[Remote, None, None]:
        """with风格的connection获取方法

        自动获取和释放Broker连接，确保连接总是被正确释放。

        Args:
            broker_addr: Broker地址

        Yields:
            Remote: 可用的连接实例

        Raises:
            ConnectionError: 连接失败或Broker不可用

        Example:
            >>> manager = BrokerManager(...)
            >>> manager.start()
            >>> try:
            ...     with manager.connection("127.0.0.1:10911") as conn:
            ...         # 使用连接进行操作
            ...         response = conn.send_sync_request(request)
            ...         print(f"收到响应: {response}")
            ...     # 连接会自动释放
            ... finally:
            ...     manager.shutdown()
        """
        connection = None
        try:
            # 获取连接
            connection = self.get_connection(broker_addr)
            yield connection
        except ConnectionError as e:
            # 连接相关的异常，直接重新抛出
            self._logger.error(
                "连接获取失败",
                extra={
                    "broker_addr": broker_addr,
                    "error_message": str(e),
                    "timestamp": time.time(),
                },
            )
            raise
        finally:
            # 确保连接被释放
            if connection is not None:
                try:
                    self.release_connection(broker_addr, connection)
                except Exception as e:
                    self._logger.error(
                        "释放连接时发生异常",
                        extra={
                            "broker_addr": broker_addr,
                            "connection_id": id(connection),
                            "error_message": str(e),
                            "timestamp": time.time(),
                        },
                    )
                    # 不抛出异常，避免掩盖原始异常
