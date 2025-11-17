import logging
import threading
import time

from pyrocketmq.logging import get_logger
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.pool import ConnectionPool
from pyrocketmq.transport.config import TransportConfig


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
    connection_pool_size: int
    _logger: logging.Logger
    _broker_pools: dict[str, ConnectionPool]
    _lock: threading.Lock

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: TransportConfig | None = None,
        max_consecutive_failures: int = 3,
        connection_pool_size: int = 5,
    ):
        """初始化同步Broker管理器

        Args:
            remote_config: 远程通信配置
            transport_config: 传输层配置
            max_consecutive_failures: 最大连续失败次数
            connection_pool_size: 每个Broker的连接池大小
        """
        self.remote_config = remote_config
        self.transport_config = transport_config
        self.connection_pool_size = connection_pool_size

        self._logger = get_logger("broker.manager.sync")

        # Broker连接信息映射
        self._broker_pools = {}
        self._lock = threading.Lock()

        self._logger.info(
            "同步Broker管理器初始化完成",
            extra={
                "connection_pool_size": self.connection_pool_size,
                "timestamp": time.time(),
            },
        )

    def start(self) -> None:
        pass

    def shutdown(self) -> None:
        """关闭Broker管理器

        停止所有后台线程并关闭所有连接池。
        """

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
            try:
                # 创建连接信息
                self._logger.debug(
                    "同步Broker连接信息创建成功",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
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

                pool = ConnectionPool(
                    address=broker_addr,
                    pool_size=self.connection_pool_size,
                    remote_config=self.remote_config,
                    transport_config=transport_config,
                )
                self._broker_pools[broker_addr] = pool
                self._logger.debug(
                    "同步连接池创建成功",
                    extra={
                        "broker_addr": broker_addr,
                        "broker_name": broker_name,
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
                if broker_addr in self._broker_pools:
                    del self._broker_pools[broker_addr]
                raise

    def remove_broker(self, broker_addr: str) -> None:
        """移除Broker

        Args:
            broker_addr: Broker地址
        """
        with self._lock:
            # 关闭连接池
            if broker_addr in self._broker_pools:
                pool = self._broker_pools.pop(broker_addr)
                pool.close()

            self._logger.info(
                "已移除Broker",
                extra={
                    "broker_addr": broker_addr,
                    "timestamp": time.time(),
                },
            )

    def connection_pool(self, broker_addr: str) -> ConnectionPool | None:
        """获取Broker连接池

        Args:
            broker_addr: Broker地址

        Returns:
            ConnectionPool | None: 连接池实例，如果不存在则返回None
        """
        with self._lock:
            if self._broker_pools.get(broker_addr):
                return self._broker_pools[broker_addr]
            return None

    def must_connection_pool(self, broker_addr: str) -> ConnectionPool:
        """获取Broker连接池，如果不存在则创建

        Args:
            broker_addr: Broker地址

        Returns:
            ConnectionPool: 连接池实例
        """
        with self._lock:
            if self._broker_pools.get(broker_addr):
                return self._broker_pools[broker_addr]

        # 在锁外调用add_broker，因为add_broker内部有自己的锁
        self.add_broker(broker_addr)

        # 再次获取，确保返回正确的连接池
        with self._lock:
            return self._broker_pools[broker_addr]
