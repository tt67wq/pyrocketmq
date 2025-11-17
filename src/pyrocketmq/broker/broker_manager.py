import logging
import time

from pyrocketmq.logging import get_logger
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.pool import ConnectionPool
from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.utils import ReadWriteContext, ReadWriteLock


class BrokerManager:
    """同步版本的Broker连接管理器

    BrokerManager是pyrocketmq的核心组件之一，负责管理与多个RocketMQ Broker的连接。
    它提供统一的连接池管理、连接复用、故障检测和自动恢复功能，是Producer和Consumer
    与Broker通信的基础设施层。

    核心功能:
    -----------
    - **连接池管理**: 为每个Broker创建和维护独立的连接池，支持连接复用
    - **线程安全**: 使用读写锁(ReadWriteLock)确保多线程环境下的安全访问
    - **动态管理**: 支持运行时添加和移除Broker，无需重启应用
    - **故障容错**: 连接失败时提供错误处理和资源清理
    - **配置灵活**: 支持自定义传输层配置和连接池参数

    架构设计:
    -----------
    1. **分层架构**: 位于Remote层和Transport层之间，为上层提供统一的Broker接口
    2. **连接复用**: 通过ConnectionPool实现TCP连接的复用，减少连接建立开销
    3. **锁机制**: 使用读写锁支持多读单写，提高并发读取性能
    4. **懒加载**: 支持按需创建连接池，避免不必要的资源占用

    使用场景:
    -----------
    - Producer需要与多个Broker建立连接进行消息发送
    - Consumer需要连接到不同的Broker拉取消息
    - 需要管理Broker集群的连接生命周期
    - 要求高并发、线程安全的连接管理

    线程安全性:
    -----------
    所有公共方法都是线程安全的，支持多线程并发访问。内部使用ReadWriteLock
    保护共享状态，读取操作使用读锁，写入操作使用写锁，确保数据一致性。

    性能特点:
    -----------
    - 连接池复用TCP连接，避免频繁的连接建立和断开
    - 读写锁机制支持多线程并发读取，提高吞吐量
    - 懒加载策略减少启动时间和资源消耗
    - 结构化日志记录，便于性能监控和问题诊断

    示例用法:
    ---------
    >>> from pyrocketmq.broker import BrokerManager
    >>> from pyrocketmq.remote.config import RemoteConfig
    >>>
    >>> # 创建配置
    >>> remote_config = RemoteConfig(connection_pool_size=10)
    >>>
    >>> # 创建管理器
    >>> manager = BrokerManager(remote_config)
    >>>
    >>> # 添加Broker
    >>> manager.add_broker("localhost:10911")
    >>> manager.add_broker("broker1:10911", "broker-primary")
    >>>
    >>> # 获取连接池
    >>> pool = manager.connection_pool("localhost:10911")
    >>>
    >>> # 使用连接池发送请求
    >>> with pool.get_connection() as conn:
    >>>     response = conn.send_request(request)
    >>>
    >>> # 清理资源
    >>> manager.shutdown()

    注意事项:
    -----------
    - 必须调用shutdown()方法清理资源，避免连接泄漏
    - Broker地址格式必须为"host:port"，端口号范围1-65535
    - 同一时间添加多个相同地址的Broker时，只保留第一个
    - 传输配置中的host和port会被覆盖为指定的Broker地址
    """

    remote_config: RemoteConfig
    transport_config: TransportConfig | None
    connection_pool_size: int
    _logger: logging.Logger
    _broker_pools: dict[str, ConnectionPool]
    _rwlock: ReadWriteLock

    def __init__(
        self,
        remote_config: RemoteConfig,
        transport_config: TransportConfig | None = None,
    ):
        """初始化同步Broker管理器

        创建一个新的BrokerManager实例，配置连接管理和传输参数。初始化过程
        会设置内部的日志记录器、连接池映射表和读写锁，但不立即创建任何连接。

        初始化过程:
        -----------
        1. 保存配置参数到实例变量
        2. 从remote_config中提取connection_pool_size
        3. 创建专用的Logger实例用于日志记录
        4. 初始化空的Broker连接池映射表
        5. 创建ReadWriteLock实例用于线程安全

        Args:
            remote_config (RemoteConfig): 远程通信配置
                - connection_pool_size: 每个Broker的连接池大小
                - timeout: 连接超时时间
                - 其他远程通信相关参数
            transport_config (TransportConfig | None): 传输层配置
                - connect_timeout: 连接建立超时时间
                - read_timeout: 读取数据超时时间
                - tcp_keepalive: TCP保活设置
                - 其他传输层参数
                - 如果为None，将使用默认的传输配置

        Raises:
            无异常抛出，构造函数总是成功

        Example:
            >>> from pyrocketmq.remote.config import RemoteConfig
            >>> from pyrocketmq.transport.config import TransportConfig
            >>>
            >>> # 基础配置
            >>> remote_config = RemoteConfig(connection_pool_size=5)
            >>> manager = BrokerManager(remote_config)
            >>>
            >>> # 完整配置
            >>> transport_config = TransportConfig(connect_timeout=5000.0)
            >>> remote_config = RemoteConfig(
            ...     connection_pool_size=10,
            ...     timeout=30000.0
            ... )
            >>> manager = BrokerManager(remote_config, transport_config)

        Note:
            - 构造函数不会立即建立连接，连接在首次使用时懒加载创建
            - transport_config中的host和port会在创建具体连接时被覆盖
            - 所有配置参数都有合理的默认值，可以省略可选参数
        """
        self.remote_config = remote_config
        self.transport_config = transport_config
        self.connection_pool_size = remote_config.connection_pool_size

        self._logger = get_logger("broker.manager.sync")

        # Broker连接信息映射
        self._broker_pools = {}
        self._rwlock = ReadWriteLock()

        self._logger.info(
            "同步Broker管理器初始化完成",
            extra={
                "connection_pool_size": self.connection_pool_size,
                "timestamp": time.time(),
            },
        )

    def _validate_broker_address(self, broker_addr: str) -> None:
        """验证Broker地址格式

        对Broker地址进行基础格式验证，确保符合"host:port"的格式要求。
        这是添加Broker之前的第一道验证关卡，确保地址的基本有效性。

        验证规则:
        -----------
        1. 地址不能为空或None
        2. 地址必须包含冒号(:)分隔符
        3. 不验证主机名的具体格式和端口范围（由parse方法处理）

        Args:
            broker_addr (str): 待验证的Broker地址，格式应为"host:port"
                - host可以是IP地址或域名
                - port应为数字字符串

        Raises:
            ValueError: 当地址格式无效时抛出
                - 空地址或None地址
                - 缺少冒号分隔符的地址

        Example:
            >>> self._validate_broker_address("localhost:10911")  # 有效
            >>> self._validate_broker_address("192.168.1.100:10911")  # 有效
            >>> self._validate_broker_address("broker.example.com:10911")  # 有效
            >>> self._validate_broker_address("")  # 抛出ValueError
            >>> self._validate_broker_address("localhost")  # 抛出ValueError
            >>> self._validate_broker_address("localhost:abc")  # 不在这里验证

        Note:
            - 此方法只进行基础格式检查，详细的端口号验证在_parse_broker_address中完成
            - 验证失败会记录结构化日志，便于问题诊断
            - 这是私有方法，仅供内部其他方法调用
        """
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

    def _parse_broker_address(self, broker_addr: str) -> tuple[str, int]:
        """解析Broker地址，返回主机和端口

        将符合格式的Broker地址字符串解析为主机名和端口号的元组。
        此方法进行详细的地址解析和验证，确保主机名和端口号的有效性。

        解析流程:
        -----------
        1. 按冒号分割地址字符串
        2. 验证分割结果的数量是否为2
        3. 验证主机名不为空
        4. 将端口部分转换为整数并验证范围
        5. 返回(host, port)元组

        验证规则:
        -----------
        - 主机名: 不能为空字符串
        - 端口号: 必须为整数，范围在1-65535之间
        - 格式: 必须包含且仅包含一个冒号分隔符

        Args:
            broker_addr (str): 待解析的Broker地址，格式为"host:port"
                - host: 主机名（IP地址或域名）
                - port: 端口号字符串

        Returns:
            tuple[str, int]: 解析后的(host, port)元组
                - host: 主机名字符串
                - port: 端口号整数

        Raises:
            ValueError: 当地址格式无效时抛出
                - 缺少冒号分隔符或有多个分隔符
                - 端口部分无法转换为整数
                - 端口号超出有效范围(1-65535)
                - 主机名为空字符串

        Example:
            >>> self._parse_broker_address("localhost:10911")
            ('localhost', 10911)
            >>> self._parse_broker_address("192.168.1.100:8080")
            ('192.168.1.100', 8080)
            >>> self._parse_broker_address("broker.example.com:10911")
            ('broker.example.com', 10911)
            >>> self._parse_broker_address("localhost:0")  # 抛出ValueError
            >>> self._parse_broker_address("localhost:65536")  # 抛出ValueError

        Note:
            - 此方法会在解析成功时记录debug级别的日志
            - 解析失败会记录error级别的日志，包含详细的错误信息
            - 使用int()转换端口号，确保端口号为有效整数
            - 这是私有方法，仅供内部使用
        """
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
            return host, port
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

    def _extract_broker_name(
        self, broker_addr: str, broker_name: str | None = None
    ) -> str:
        """提取Broker名称

        根据提供的Broker地址和可选的Broker名称，确定最终使用的Broker名称。
        如果没有提供明确的broker_name，则从broker_addr中提取主机名作为名称。

        提取逻辑:
        -----------
        1. 如果提供了明确的broker_name且不为空，直接使用
        2. 如果broker_name为None或空字符串，从broker_addr中提取
        3. 提取方式为取broker_addr中冒号前的部分（主机名）

        使用场景:
        -----------
        - 添加Broker时用户可能提供了友好的名称，优先使用
        - 如果没有提供名称，使用地址中的主机名作为默认标识
        - 用于日志记录、监控和管理的Broker标识

        Args:
            broker_addr (str): Broker地址，格式为"host:port"
                - 用于提取默认名称（当broker_name为None时）
                - 格式已在上游方法中验证
            broker_name (str | None): 可选的Broker名称
                - 如果提供且不为空，将作为优先使用的名称
                - 如果为None，将从broker_addr中提取名称

        Returns:
            str: 最终确定的Broker名称
                - 优先使用提供的broker_name
                - 备选方案是从broker_addr提取的主机名

        Example:
            >>> self._extract_broker_name("localhost:10911")
            'localhost'
            >>> self._extract_broker_name("broker.example.com:10911")
            'broker.example.com'
            >>> self._extract_broker_name("192.168.1.100:10911", "primary-broker")
            'primary-broker'
            >>> self._extract_broker_name("localhost:10911", "")
            'localhost'

        Note:
            - 此方法主要用于日志记录和内部标识
            - 提取的名称不一定唯一，IP地址和域名可能相同
            - 提取过程会记录debug级别的日志
            - 这是私有方法，供内部add_broker等方法调用
        """
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
        return broker_name

    def _create_transport_config(self, broker_addr: str) -> TransportConfig:
        """为指定Broker创建传输配置

        基于Broker地址和初始化时提供的默认传输配置，为特定Broker创建
        专用的传输配置实例。此配置会覆盖host和port为指定Broker的地址，
        同时继承其他传输层参数。

        创建逻辑:
        -----------
        1. 解析Broker地址获取host和port
        2. 如果存在默认transport_config，复制其参数（排除host和port）
        3. 使用解析的host和port创建新的TransportConfig实例
        4. 返回配置好的TransportConfig对象

        配置合并规则:
        -----------
        - host, port: 强制使用指定Broker的地址
        - 其他参数: 继承默认transport_config的配置
        - 无默认配置时: 使用TransportConfig的默认参数

        Args:
            broker_addr (str): Broker地址，格式为"host:port"
                - 用于设置传输配置的连接目标
                - 地址格式已在上游验证

        Returns:
            TransportConfig: 专用于该Broker的传输配置实例
                - host, port: 设置为指定Broker的地址
                - 其他参数: 继承自默认配置或使用默认值

        Example:
            >>> # 有默认配置的情况
            >>> default_config = TransportConfig(
            ...     connect_timeout=5000.0,
            ...     read_timeout=30000.0
            ... )
            >>> manager = BrokerManager(remote_config, default_config)
            >>> config = manager._create_transport_config("broker.example.com:10911")
            >>> # config.host == "broker.example.com"
            >>> # config.port == 10911
            >>> # config.connect_timeout == 5000.0  # 继承默认值
            >>>
            >>> # 无默认配置的情况
            >>> manager = BrokerManager(remote_config)
            >>> config = manager._create_transport_config("localhost:10911")
            >>> # config使用TransportConfig的默认参数

        Note:
            - 此方法确保每个Broker使用正确的主机和端口
            - 避免配置冲突：覆盖默认配置中的host和port
            - 支持灵活的配置继承和定制
            - 这是私有方法，供_create_connection_pool使用
            - 创建成功会记录debug级别的日志
        """
        host, port = self._parse_broker_address(broker_addr)

        if self.transport_config:
            # 移除基础配置中的host和port，避免重复
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
            transport_config = TransportConfig(
                host=host,
                port=port,
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

        return transport_config

    def _create_connection_pool(
        self, broker_addr: str, transport_config: TransportConfig
    ) -> ConnectionPool:
        """创建Broker连接池

        为指定的Broker创建一个新的连接池实例。连接池负责管理与该Broker的
        多个TCP连接，支持连接复用、并发控制和自动故障恢复。

        创建过程:
        -----------
        1. 使用提供的transport_config和remote_config创建ConnectionPool
        2. 配置连接池大小和连接参数
        3. 初始化连接池的内部状态和管理机制
        4. 返回可用的连接池实例

        连接池特性:
        -----------
        - 连接复用: 避免频繁建立TCP连接的开销
        - 并发控制: 限制同时活跃的连接数量
        - 健康检查: 自动检测和恢复失效的连接
        - 线程安全: 支持多线程并发访问
        - 资源管理: 自动清理空闲和失效连接

        Args:
            broker_addr (str): Broker地址，格式为"host:port"
                - 作为连接池的标识符
                - 用于日志记录和监控
            transport_config (TransportConfig): 专用于该Broker的传输配置
                - 包含连接超时、读取超时等参数
                - 由_create_transport_config方法创建

        Returns:
            ConnectionPool: 新创建的连接池实例
                - 已配置好连接参数和池大小
                - 可以立即用于获取和管理连接

        Example:
            >>> manager = BrokerManager(remote_config)
            >>> transport_config = manager._create_transport_config("localhost:10911")
            >>> pool = manager._create_connection_pool("localhost:10911", transport_config)
            >>> # 现在可以使用连接池获取连接
            >>> with pool.get_connection() as conn:
            >>>     response = conn.send_request(request)

        Note:
            - 连接池创建后需要手动管理其生命周期
            - 连接池不会立即创建连接，采用懒加载策略
            - 创建过程会记录debug和info级别的日志
            - 这是私有方法，供add_broker等方法使用
            - 连接池的关闭需要在shutdown时处理
        """
        self._logger.debug(
            "创建同步连接池",
            extra={
                "broker_addr": broker_addr,
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

        self._logger.debug(
            "同步连接池创建成功",
            extra={
                "broker_addr": broker_addr,
                "timestamp": time.time(),
            },
        )

        return pool

    def start(self) -> None:
        """启动Broker管理器

        启动Broker管理器，初始化必要的后台服务和资源。当前实现为空，
        预留给未来扩展使用，如定期健康检查、自动重连机制等。

        预期功能:
        -----------
        - 启动定期健康检查线程
        - 初始化监控和统计服务
        - 启动连接池预热机制
        - 建立与所有已添加Broker的初始连接

        当前状态:
        -----------
        空实现，不需要显式调用。Broker管理器在添加第一个Broker时会
        自动初始化相关资源。

        Args:
            无

        Returns:
            None

        Example:
            >>> manager = BrokerManager(remote_config)
            >>> manager.start()  # 当前无效果
            >>> manager.add_broker("localhost:10911")

        Note:
            - 当前版本为空实现，保留用于未来功能扩展
            - 可以安全调用，不会产生副作用
            - 实际的初始化工作在add_broker时按需进行
        """
        pass

    def shutdown(self) -> None:
        """关闭Broker管理器

        安全关闭Broker管理器，清理所有资源并释放连接。这是应用关闭时
        必须调用的方法，确保所有TCP连接得到正确关闭，避免资源泄漏。

        关闭流程:
        -----------
        1. 获取写锁，确保线程安全的清理过程
        2. 复制当前所有连接池的引用
        3. 清空内部连接池映射表
        4. 关闭所有连接池，释放TCP连接
        5. 记录关闭统计信息到日志

        线程安全:
        -----------
        使用ReadWriteContext获取写锁，确保在关闭过程中没有其他线程
        正在访问连接池映射表。关闭操作是原子性的，不会导致状态不一致。

        资源清理:
        -----------
        - 关闭所有活跃的TCP连接
        - 停止连接池的后台管理线程
        - 释放网络套接字和缓冲区
        - 清理内部状态和映射表

        Args:
            无

        Returns:
            None

        Example:
            >>> manager = BrokerManager(remote_config)
            >>> manager.add_broker("localhost:10911")
            >>> manager.add_broker("broker1:10911")
            >>> # 使用管理器...
            >>> manager.shutdown()  # 清理所有资源

        Note:
            - 关闭后管理器不可再用，需要重新创建实例
            - 重复调用是安全的，不会产生副作用
            - 关闭过程会记录详细的结构化日志
            - 必须在应用退出前调用，避免连接泄漏
            - 使用写锁确保与其他操作的互斥
        """

        # 关闭所有连接池 - 使用写锁
        with ReadWriteContext(self._rwlock, write=True):
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
        """添加Broker到管理器

        将一个新的Broker添加到管理器中，为其创建连接池和相关资源。
        如果Broker已存在，则跳过添加操作。此操作是线程安全的。

        添加流程:
        -----------
        1. 验证Broker地址格式的有效性
        2. 提取或确定Broker名称（用于日志记录）
        3. 获取写锁，确保线程安全的添加过程
        4. 检查Broker是否已存在，避免重复添加
        5. 创建专用的传输配置
        6. 创建并配置连接池
        7. 将连接池注册到内部映射表
        8. 处理异常情况并进行清理

        懒加载特性:
        -----------
        - 连接池创建后不会立即建立TCP连接
        - 实际连接在首次使用时按需创建
        - 减少初始化时间和资源消耗

        线程安全:
        -----------
        使用ReadWriteContext获取写锁，确保多线程环境下的安全添加。
        同一时间只能有一个线程修改连接池映射表。

        Args:
            broker_addr (str): Broker地址，格式必须为"host:port"
                - host可以是IP地址(如"192.168.1.100")或域名(如"broker.example.com")
                - port必须是1-65535范围内的整数
                - 地址格式会被严格验证
            broker_name (str | None): 可选的Broker友好名称
                - 如果提供，将用于日志记录和监控
                - 如果为None，将从broker_addr中提取主机名作为名称
                - 名称主要用于标识，不影响连接行为

        Raises:
            ValueError: 当Broker地址格式无效时
                - 地址为空或None
                - 缺少冒号分隔符
                - 端口号超出有效范围
            Exception: 其他创建过程中的异常
                - 连接池创建失败
                - 配置参数错误

        Example:
            >>> manager = BrokerManager(remote_config)
            >>>
            >>> # 添加Broker（使用自动提取的名称）
            >>> manager.add_broker("localhost:10911")
            >>>
            >>> # 添加Broker（使用自定义名称）
            >>> manager.add_broker("192.168.1.100:10911", "primary-broker")
            >>>
            >>> # 添加域名格式的Broker
            >>> manager.add_broker("broker1.example.com:10911", "broker-cluster-1")
            >>>
            >>> # 重复添加相同地址的Broker（会被忽略）
            >>> manager.add_broker("localhost:10911")  # 不会重复创建
            >>>
            >>> # 错误的地址格式（会抛出异常）
            >>> try:
            >>>     manager.add_broker("invalid-address")
            >>> except ValueError as e:
            >>>     print(f"地址格式错误: {e}")

        Note:
            - 重复添加相同地址的Broker会被静默忽略
            - 添加失败时会自动清理已创建的资源
            - 整个过程会记录详细的结构化日志
            - 添加后立即可用，无需额外的初始化步骤
            - 使用写锁确保与其他操作的互斥
        """
        # 1. 验证地址格式
        self._validate_broker_address(broker_addr)

        # 2. 提取Broker名称
        broker_name = self._extract_broker_name(broker_addr, broker_name)

        with ReadWriteContext(self._rwlock, write=True):
            try:
                # 检查broker是否已经存在，避免重复添加
                if broker_addr in self._broker_pools:
                    self._logger.debug(
                        "Broker已存在，跳过添加",
                        extra={
                            "broker_addr": broker_addr,
                            "broker_name": broker_name,
                            "timestamp": time.time(),
                        },
                    )
                    return

                # 3. 创建传输配置
                transport_config = self._create_transport_config(broker_addr)

                # 4. 创建连接池
                pool = self._create_connection_pool(broker_addr, transport_config)

                # 5. 保存到映射表
                self._broker_pools[broker_addr] = pool

                self._logger.debug(
                    "同步Broker连接信息创建成功",
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
        """从管理器中移除指定的Broker

        安全移除指定的Broker及其所有相关资源，包括连接池和TCP连接。
        此操作是线程安全的，会正确处理并发访问和资源清理。

        移除流程:
        -----------
        1. 获取写锁，确保线程安全的移除过程
        2. 检查指定地址的Broker是否存在
        3. 如果存在，从映射表中移除连接池引用
        4. 关闭连接池，释放所有TCP连接和资源
        5. 记录移除操作的统计信息

        资源清理:
        -----------
        - 关闭所有活跃和空闲的TCP连接
        - 停止连接池的后台管理线程
        - 释放网络套接字和内存缓冲区
        - 清理连接池的内部状态

        线程安全:
        -----------
        使用ReadWriteContext获取写锁，确保移除过程中没有其他线程
        正在访问该Broker的连接池。移除操作是原子性的。

        幂等性:
        -----------
        多次移除同一个Broker地址是安全的，后续调用会被静默忽略。
        如果Broker不存在，方法会直接返回而不抛出异常。

        Args:
            broker_addr (str): 要移除的Broker地址，格式为"host:port"
                - 地址必须与之前add_broker时使用的地址完全一致
                - 不进行格式验证，直接匹配映射表中的键

        Returns:
            None

        Example:
            >>> manager = BrokerManager(remote_config)
            >>>
            >>> # 添加多个Broker
            >>> manager.add_broker("localhost:10911")
            >>> manager.add_broker("broker1.example.com:10911")
            >>> manager.add_broker("broker2.example.com:10911")
            >>>
            >>> # 移除单个Broker
            >>> manager.remove_broker("broker1.example.com:10911")
            >>>
            >>> # 重复移除（安全操作）
            >>> manager.remove_broker("broker1.example.com:10911")  # 不会报错
            >>>
            >>> # 移除不存在的Broker（安全操作）
            >>> manager.remove_broker("nonexistent:10911")  # 不会报错

        Note:
            - 移除操作是立即生效的，不会等待现有连接完成
            - 正在使用该连接池的线程可能会收到连接异常
            - 建议在应用空闲时进行移除操作
            - 移除后需要重新添加才能再次使用该Broker
            - 整个过程会记录详细的结构化日志
            - 使用写锁确保与其他操作的互斥
        """
        with ReadWriteContext(self._rwlock, write=True):
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
        """获取指定Broker的连接池

        安全获取指定Broker地址对应的连接池实例。如果Broker不存在或
        未添加到管理器中，则返回None。此操作是线程安全的。

        获取流程:
        -----------
        1. 获取读锁，允许并发读取操作
        2. 在连接池映射表中查找指定地址
        3. 如果找到，返回对应的ConnectionPool实例
        4. 如果未找到，返回None

        线程安全:
        -----------
        使用ReadWriteContext获取读锁，支持多线程并发读取。
        读取操作不会阻塞其他读取操作，但会与写入操作互斥。

        使用场景:
        -----------
        - 检查Broker是否已被添加到管理器
        - 获取现有连接池用于连接操作
        - 条件性操作前检查连接池是否存在
        - 监控和管理现有连接池状态

        返回值语义:
        -----------
        - ConnectionPool: 找到对应的连接池，可用于获取连接
        - None: Broker不存在或未添加，需要先调用add_broker

        Args:
            broker_addr (str): 要查询的Broker地址，格式为"host:port"
                - 地址必须与add_broker时使用的完全一致
                - 不进行格式验证，直接匹配映射表键值

        Returns:
            ConnectionPool | None: 连接池实例或None
                - 成功时返回ConnectionPool实例，可用于连接操作
                - 失败时返回None，表示Broker不存在

        Example:
            >>> manager = BrokerManager(remote_config)
            >>>
            >>> # 查询不存在的Broker
            >>> pool = manager.connection_pool("localhost:10911")
            >>> print(pool)  # None
            >>>
            >>> # 添加Broker后查询
            >>> manager.add_broker("localhost:10911")
            >>> pool = manager.connection_pool("localhost:10911")
            >>> print(pool)  # <ConnectionPool object at ...>
            >>>
            >>> # 使用连接池（需要检查None）
            >>> if pool:
            >>>     with pool.get_connection() as conn:
            >>>         response = conn.send_request(request)
            >>> else:
            >>>     print("Broker不存在，请先添加")
            >>>
            >>> # 查询多个Broker
            >>> brokers = ["localhost:10911", "broker1:10911", "broker2:10911"]
            >>> for addr in brokers:
            >>>     pool = manager.connection_pool(addr)
            >>>     if pool:
            >>>         print(f"{addr}: 已添加")
            >>>     else:
            >>>         print(f"{addr}: 未添加")

        Note:
            - 此方法不会自动创建连接池，只返回已存在的
            - 返回的连接池可能处于任何状态（活跃、空闲、关闭等）
            - 使用返回的连接池前建议检查其状态
            - 高频查询时使用读锁优化性能
            - 地址必须完全匹配，包括端口号
        """
        with ReadWriteContext(self._rwlock, write=False):
            if self._broker_pools.get(broker_addr):
                return self._broker_pools[broker_addr]
            return None

    def must_connection_pool(self, broker_addr: str) -> ConnectionPool:
        """获取Broker连接池，如果不存在则自动创建

        这是一个便利方法，确保返回指定Broker的有效连接池。如果Broker
        不存在，会自动调用add_broker创建连接池。此方法简化了"获取或创建"
        的常见模式。

        工作流程:
        -----------
        1. 首先尝试获取读锁检查连接池是否存在
        2. 如果存在，立即返回连接池实例
        3. 如果不存在，释放读锁并调用add_broker创建连接池
        4. 再次获取写锁，确保返回有效的连接池

        懒加载策略:
        -----------
        采用"检查-然后-执行"的懒加载模式，避免不必要的锁竞争。
        大多数情况下（连接池已存在）只需要读锁，性能更优。

        线程安全设计:
        -----------
        - 使用读锁进行快速检查，支持并发读取
        - 需要创建时使用写锁，确保创建过程的原子性
        - 双重检查模式避免竞态条件
        - add_broker内部的锁机制保证创建安全

        使用场景:
        -----------
        - 确保总能获得可用的连接池
        - 简化"获取或创建"的代码逻辑
        - 首次访问时自动初始化连接池
        - 不需要预先手动添加所有Broker

        与connection_pool的区别:
        -----------
        - connection_pool: 只查询，不创建，可能返回None
        - must_connection_pool: 查询+创建，总是返回有效实例

        Args:
            broker_addr (str): Broker地址，格式为"host:port"
                - 地址格式会在add_broker中被严格验证
                - 支持IP地址和域名格式
                - 端口号必须在有效范围内

        Returns:
            ConnectionPool: 有效的连接池实例
                - 总是返回非None的ConnectionPool实例
                - 可以立即用于获取连接和发送请求

        Raises:
            ValueError: 当Broker地址格式无效时
                - 由add_broker方法抛出
                - 包含详细的错误信息
            Exception: 连接池创建过程中的其他异常
                - 网络配置错误
                - 资源不足等

        Example:
            >>> manager = BrokerManager(remote_config)
            >>>
            >>> # 首次访问会自动创建连接池
            >>> pool = manager.must_connection_pool("localhost:10911")
            >>> # pool现在是有效的ConnectionPool实例
            >>>
            >>> # 直接使用，无需检查None
            >>> with pool.get_connection() as conn:
            >>>     response = conn.send_request(request)
            >>>
            >>> # 后续访问直接返回现有连接池
            >>> pool2 = manager.must_connection_pool("localhost:10911")
            >>> assert pool is pool2  # 同一个实例
            >>>
            >>> # 批量处理多个Broker
            >>> brokers = ["broker1:10911", "broker2:10911", "broker3:10911"]
            >>> pools = []
            >>> for addr in brokers:
            >>>     pool = manager.must_connection_pool(addr)  # 自动创建
            >>>     pools.append(pool)
            >>> # 现在所有brokers都有可用的连接池
            >>>
            >>> # 错误地址格式（会抛出异常）
            >>> try:
            >>>     pool = manager.must_connection_pool("invalid-address")
            >>> except ValueError as e:
            >>>     print(f"地址错误: {e}")

        Note:
            - 相比手动检查connection_pool + add_broker，此方法更简洁
            - 适合"总是需要可用连接池"的使用场景
            - 第一次访问会有创建开销，后续访问性能很好
            - 自动创建会记录相应的日志信息
            - 适用于生产环境中的按需连接池管理
        """
        with ReadWriteContext(self._rwlock, write=True):
            if self._broker_pools.get(broker_addr):
                return self._broker_pools[broker_addr]

        # 在锁外调用add_broker，因为add_broker内部有自己的锁
        self.add_broker(broker_addr)

        # 再次获取，确保返回正确的连接池
        with ReadWriteContext(self._rwlock, write=True):
            return self._broker_pools[broker_addr]
