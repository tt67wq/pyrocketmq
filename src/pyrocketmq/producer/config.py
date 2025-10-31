"""
Producer配置管理模块

该模块提供了RocketMQ Producer的完整配置管理功能，包括：
- Producer基础配置（生产者组、客户端ID等）
- 消息发送配置（超时、重试、压缩等）
- 路由和Broker连接配置
- 性能和监控配置
- 环境变量支持和预定义配置模板

使用示例:
    # 使用默认配置
    config = ProducerConfig()

    # 使用预定义环境配置
    config = get_config("production")

    # 从环境变量创建配置
    config = ProducerConfig.from_env()

    # 自定义配置
    config = create_custom_config(
        producer_group="my_producer",
        namesrv_addr="localhost:9876",
        retry_times=3
    )
"""

import os
import socket
from dataclasses import dataclass

from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig


@dataclass
class ProducerConfig:
    """
    RocketMQ Producer配置类

    提供完整的RocketMQ Producer配置选项，支持灵活的参数配置
    和环境变量覆盖。该配置类遵循RocketMQ官方客户端的配置规范，
    确保与RocketMQ服务器的完全兼容性。

    Attributes:
        producer_group: 生产者组名，同一组内的生产者共享相同的行为特征
        client_id: 客户端唯一标识符，为None时自动生成（格式：hostname#pid）
        namesrv_addr: NameServer地址列表，格式为"host1:port1;host2:port2"
        send_msg_timeout: 消息发送超时时间，单位毫秒
        retry_times: 发送失败时的重试次数
        retry_another_broker_when_not_store_ok: 当存储失败时是否重试其他Broker
        compress_msg_body_over_howmuch: 消息体压缩阈值，超过此大小的消息将被压缩
        max_message_size: 允许发送的最大消息大小，不能超过32MB
        default_topic_queue_nums: 默认主题的队列数量
        poll_name_server_interval: 轮询NameServer的间隔时间，单位毫秒
        poll_name_server_interval_short: 短间隔轮询NameServer的时间，单位毫秒
        update_topic_route_info_interval: 更新主题路由信息的间隔时间，单位毫秒
        heartbeat_broker_interval: 向Broker发送心跳的间隔时间，单位毫秒
        persist_all_consumer_interval: 持久化消费者信息的间隔时间，单位毫秒
        batch_size: 批量发送消息的默认数量
        batch_split_type: 批量消息的分割策略，支持"by_size"和"by_count"
        async_send_semaphore: 异步发送的信号量大小，控制并发发送数量
        async_send_failure_timeout: 异步发送失败的超时时间，单位毫秒
        send_latency_enable: 是否启用发送延迟统计
        transport_config: 传输层配置，包含TCP连接相关参数
        remote_config: 远程通信配置，包含RPC调用相关参数
        trace_message: 是否启用消息跟踪功能
        debug_enabled: 是否启用调试模式，启用后会输出详细的调试信息

    Example:
        >>> # 基本使用
        >>> config = ProducerConfig()
        >>>
        >>> # 自定义配置
        >>> config = ProducerConfig(
        ...     producer_group="my_producer_group",
        ...     namesrv_addr="192.168.1.100:9876",
        ...     send_msg_timeout=5000.0,
        ...     retry_times=3
        ... )
        >>>
        >>> # 从环境变量加载
        >>> config = ProducerConfig.from_env()
        >>>
        >>> # 使用链式调用修改配置
        >>> config = ProducerConfig().with_producer_group("my_group")\\
        ...                          .with_timeout(5000.0)\\
        ...                          .with_retry(3)
    """

    # ==================== 基础配置 ====================
    producer_group: str = "DEFAULT_PRODUCER"
    """生产者组名

    同一组内的生产者共享相同的行为特征，在RocketMQ中用于事务消息和消息去重。
    建议使用有意义的业务名称，如"order_producer"、"payment_producer"等。
    """

    client_id: str | None = None
    """客户端唯一标识符

    用于在RocketMQ服务器端标识不同的客户端实例。如果为None，系统会自动生成，
    格式为"hostname#pid"。在分布式环境中，建议手动指定具有业务意义的client_id。
    """

    namesrv_addr: str = "localhost:9876"
    """NameServer地址列表

    格式为"host1:port1;host2:port2"，多个地址用分号分隔。
    NameServer是RocketMQ的路由发现服务，生产者通过它获取Topic的路由信息。
    """

    # ==================== 消息发送配置 ====================
    send_msg_timeout: float = 3000.0  # 毫秒
    """消息发送超时时间

    单次消息发送的最大等待时间，超过此时间将抛出超时异常。
    根据网络状况和业务需求调整，建议生产环境设置为3000-5000毫秒。
    """

    retry_times: int = 2
    """发送失败重试次数

    当消息发送失败时的重试次数，不包括首次发送。
    设置为0表示不重试。建议生产环境设置为2-3次。
    """

    retry_another_broker_when_not_store_ok: bool = False
    """存储失败时是否重试其他Broker

    当Broker返回存储失败（非网络问题）时，是否尝试向其他Broker重试。
    启用此选项可以提高消息发送成功率，但可能导致消息重复。
    """

    # ==================== 消息配置 ====================
    compress_msg_body_over_howmuch: int = 4 * 1024 * 1024  # 4MB
    """消息压缩阈值

    当消息体大小超过此阈值时，将自动进行压缩以减少网络传输量。
    默认4MB，可以根据网络带宽和消息特征调整。
    """

    max_message_size: int = 4 * 1024 * 1024  # 4MB
    """最大消息大小限制

    允许发送的单条消息的最大大小，不能超过32MB（RocketMQ硬限制）。
    超过此大小的消息将被拒绝发送。
    """

    default_topic_queue_nums: int = 4
    """默认主题的队列数量

    当创建新主题时，默认的队列数量。更多的队列可以提高并发性能，
    但也会增加系统开销。建议根据业务量级设置，通常为4-8个。
    """

    # ==================== 路由配置 ====================
    poll_name_server_interval: int = 30000  # 毫秒
    """轮询NameServer的间隔时间

    定期从NameServer获取最新的路由信息的时间间隔。
    默认30秒，可以根据路由变化的频率调整。
    """

    poll_name_server_interval_short: int = 10000  # 毫秒
    """短间隔轮询NameServer的时间

    当路由信息不可用或异常时，使用更短的间隔进行重试。
    默认10秒，用于快速恢复路由信息。
    """

    update_topic_route_info_interval: int = 30000  # 毫秒
    """更新主题路由信息的间隔时间

    定期更新已缓存的主题路由信息的时间间隔。
    默认30秒，与poll_name_server_interval保持一致。
    """

    # ==================== 心跳配置 ====================
    heartbeat_broker_interval: int = 30000  # 毫秒
    """向Broker发送心跳的间隔时间

    定期向连接的Broker发送心跳包，维持连接的活跃状态。
    默认30秒，不宜设置过短或过长。
    """

    persist_all_consumer_interval: int = 5000  # 毫秒
    """持久化消费者信息的间隔时间

    定期将消费者相关信息持久化到Broker的时间间隔。
    默认5秒，确保消费者信息不会丢失。
    """

    # ==================== 批量消息配置 ====================
    batch_size: int = 32
    """批量发送消息的默认数量

    批量发送时，每批发送的消息数量。较大的批次可以提高吞吐量，
    但会增加延迟和内存使用。建议根据消息大小和延迟要求调整。
    """

    batch_split_type: str = "by_size"
    """批量消息的分割策略

    支持两种策略：
    - "by_size": 按消息大小分割批次，确保每批消息总大小适中
    - "by_count": 按消息数量分割批次，确保每批消息数量固定
    默认为"by_size"，推荐使用。
    """

    # ==================== 性能配置 ====================
    async_send_semaphore: int = 10000
    """异步发送信号量大小

    控制同时进行的异步发送操作的最大数量，用于限流和资源控制。
    根据系统资源和网络状况调整，避免过多并发导致系统压力过大。
    """

    async_send_failure_timeout: int = 3000  # 毫秒
    """异步发送失败的超时时间

    异步发送操作失败后的超时时间，超过此时间将认为发送彻底失败。
    默认3秒，可以根据网络状况调整。
    """

    send_latency_enable: bool = False
    """是否启用发送延迟统计

    启用后会统计消息发送的延迟信息，用于性能监控和优化。
    在生产环境中建议启用，以便及时发现性能问题。
    """

    # ==================== 路由策略配置 ====================
    routing_strategy: str = "round_robin"
    """路由策略

    指定选择消息队列的算法策略，支持以下选项：
    - "round_robin": 轮询策略，按顺序轮流选择队列，实现负载均衡
    - "random": 随机策略，随机选择可用队列，简单高效
    - "message_hash": 消息哈希策略，基于消息分片键计算哈希值选择队列
      确保相同分片键的消息路由到同一队列，保证消息顺序性

    默认为"round_robin"策略，适合大多数场景。
    对于需要消息顺序性的场景，建议使用"message_hash"策略。
    """

    # ==================== 传输层配置 ====================
    transport_config: TransportConfig | None = None
    """传输层配置

    包含TCP连接、超时、重连等底层网络配置。如果为None，
    将使用默认的TransportConfig配置。
    """

    remote_config: RemoteConfig | None = None
    """远程通信配置

    包含RPC调用、连接池、并发控制等通信层配置。如果为None，
    将使用默认的RemoteConfig配置。
    """

    # ==================== 调试配置 ====================
    trace_message: bool = False
    """是否启用消息跟踪功能

    启用后会记录消息的详细轨迹信息，包括发送时间、接收时间等。
    在问题排查和性能分析时很有用，但会增加一定的性能开销。
    """

    debug_enabled: bool = False
    """是否启用调试模式

    启用后会输出详细的调试日志信息，包括配置参数、网络状态等。
    仅在开发和调试时使用，生产环境建议关闭。
    """

    def __post_init__(self) -> None:
        """后初始化处理

        在dataclass创建实例后自动调用，用于完成以下初始化工作：
        1. 自动生成客户端ID（如果未指定）
        2. 创建默认的传输层配置
        3. 创建默认的远程通信配置
        4. 验证所有配置参数的有效性

        Raises:
            ValueError: 当配置参数无效时抛出异常
        """
        # 自动生成客户端ID
        if self.client_id is None:
            self.client_id = self._generate_client_id()

        # 创建默认传输层配置
        if self.transport_config is None:
            host, port = self._parse_namesrv_addr(self.namesrv_addr)
            self.transport_config = TransportConfig(host=host, port=port)

        # 创建默认远程通信配置
        if self.remote_config is None:
            self.remote_config = RemoteConfig()

        # 验证配置值
        self._validate_config()

    def _generate_client_id(self) -> str:
        """生成客户端唯一标识符

        使用主机名和进程ID组合生成唯一的客户端ID，
        格式为"hostname#pid"。这样可以确保在同一台机器上
        运行的多个Producer实例具有不同的client_id。

        Returns:
            str: 格式为"hostname#pid"的客户端ID
        """
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"{hostname}#{pid}"

    def _parse_namesrv_addr(self, addr: str) -> tuple[str, int]:
        """解析NameServer地址

        从地址字符串中解析出主机名和端口号。支持两种格式：
        - "hostname" (使用默认端口9876)
        - "hostname:port" (使用指定端口)

        Args:
            addr: NameServer地址字符串

        Returns:
            tuple[str, int]: 包含主机名和端口号的元组

        Note:
            当解析失败时，返回默认值("localhost", 9876)
        """
        try:
            if ":" in addr:
                host, port_str = addr.split(":", 1)
                port = int(port_str)
            else:
                host = addr
                port = 9876
            return host, port
        except Exception:
            return "localhost", 9876

    def _validate_config(self) -> None:
        """验证所有配置参数的有效性

        检查各个配置参数是否符合RocketMQ的要求和业务逻辑，
        确保Producer能够正常启动和运行。

        Raises:
            ValueError: 当任何配置参数无效时抛出异常，包含具体的错误信息

        Note:
            验证包括但不限于：
            - 生产者组名不能为空
            - 超时时间必须为正数
            - 重试次数不能为负数
            - 消息大小限制必须合理
            - 批量配置必须有效
        """
        if not self.producer_group or not self.producer_group.strip():
            raise ValueError("producer_group cannot be empty")

        if self.send_msg_timeout <= 0:
            raise ValueError("send_msg_timeout must be greater than 0")

        if self.retry_times < 0:
            raise ValueError("retry_times must be non-negative")

        if self.max_message_size <= 0:
            raise ValueError("max_message_size must be greater than 0")

        if self.max_message_size > 32 * 1024 * 1024:
            raise ValueError("max_message_size cannot exceed 32MB")

        if self.batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        if self.batch_split_type not in ["by_size", "by_count"]:
            raise ValueError("batch_split_type must be 'by_size' or 'by_count'")

        # 验证路由策略配置
        valid_routing_strategies = ["round_robin", "random", "message_hash"]
        if self.routing_strategy not in valid_routing_strategies:
            raise ValueError(
                f"routing_strategy must be one of {valid_routing_strategies}, "
                f"got '{self.routing_strategy}'"
            )

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        """从环境变量创建配置实例

        从系统环境变量中读取配置参数，支持灵活的外部配置管理。
        环境变量的命名格式为"PYROCKETMQ_<参数名>"，采用大写字母和下划线。

        支持的环境变量：
        - PYROCKETMQ_PRODUCER_GROUP: 生产者组名
        - PYROCKETMQ_CLIENT_ID: 客户端ID
        - PYROCKETMQ_NAMESRV_ADDR: NameServer地址
        - PYROCKETMQ_SEND_MSG_TIMEOUT: 发送超时时间（毫秒）
        - PYROCKETMQ_RETRY_TIMES: 重试次数
        - PYROCKETMQ_MAX_MESSAGE_SIZE: 最大消息大小
        - PYROCKETMQ_ROUTING_STRATEGY: 路由策略 (round_robin/random/message_hash)
        - PYROCKETMQ_DEBUG_ENABLED: 是否启用调试模式

        Returns:
            ProducerConfig: 从环境变量创建的配置实例

        Note:
            - 环境变量值解析失败时会忽略，使用默认值
            - 布尔值支持"true"/"1"/"yes"表示真，其他值表示假
            - 数值类型转换失败时会保持默认值
        """
        config = cls()

        if producer_group := os.getenv("PYROCKETMQ_PRODUCER_GROUP"):
            config.producer_group = producer_group

        if client_id := os.getenv("PYROCKETMQ_CLIENT_ID"):
            config.client_id = client_id

        if namesrv_addr := os.getenv("PYROCKETMQ_NAMESRV_ADDR"):
            config.namesrv_addr = namesrv_addr

        if send_timeout := os.getenv("PYROCKETMQ_SEND_MSG_TIMEOUT"):
            try:
                config.send_msg_timeout = float(send_timeout)
            except ValueError:
                pass

        if retry_times := os.getenv("PYROCKETMQ_RETRY_TIMES"):
            try:
                config.retry_times = int(retry_times)
            except ValueError:
                pass

        if max_size := os.getenv("PYROCKETMQ_MAX_MESSAGE_SIZE"):
            try:
                config.max_message_size = int(max_size)
            except ValueError:
                pass

        # 路由策略配置
        if routing_strategy := os.getenv("PYROCKETMQ_ROUTING_STRATEGY"):
            config.routing_strategy = routing_strategy

        if debug_enabled := os.getenv("PYROCKETMQ_DEBUG_ENABLED"):
            config.debug_enabled = debug_enabled.lower() in ("true", "1", "yes")

        return config

    def __str__(self) -> str:
        return (
            f"ProducerConfig("
            f"producer_group={self.producer_group!r}, "
            f"client_id={self.client_id!r}, "
            f"namesrv_addr={self.namesrv_addr!r}, "
            f"send_msg_timeout={self.send_msg_timeout}ms, "
            f"retry_times={self.retry_times})"
        )

    def __repr__(self) -> str:
        return self.__str__()


# 预定义配置模板
DEFAULT_CONFIG = ProducerConfig()

DEVELOPMENT_CONFIG = ProducerConfig(
    send_msg_timeout=5000.0,
    retry_times=1,
    batch_size=16,
    trace_message=True,
    debug_enabled=True,
)

PRODUCTION_CONFIG = ProducerConfig(
    send_msg_timeout=3000.0,
    retry_times=3,
    retry_another_broker_when_not_store_ok=True,
    batch_size=32,
    send_latency_enable=True,
    trace_message=False,
    debug_enabled=False,
    routing_strategy="round_robin",
)

ORDER_SEQUENCED_CONFIG = ProducerConfig(
    send_msg_timeout=5000.0,
    retry_times=5,
    retry_another_broker_when_not_store_ok=True,
    batch_size=1,  # 单条消息发送保证顺序
    send_latency_enable=True,
    trace_message=True,
    debug_enabled=False,
    routing_strategy="message_hash",
)

HIGH_PERFORMANCE_CONFIG = ProducerConfig(
    send_msg_timeout=1000.0,
    retry_times=1,
    compress_msg_body_over_howmuch=1024 * 1024,  # 1MB
    max_message_size=2 * 1024 * 1024,  # 2MB
    routing_strategy="random",
    batch_size=64,
    async_send_semaphore=50000,
    send_latency_enable=True,
)

TESTING_CONFIG = ProducerConfig(
    producer_group="TEST_PRODUCER",
    send_msg_timeout=10000.0,
    retry_times=0,
    batch_size=8,
    trace_message=True,
    debug_enabled=True,
)


def get_config(environment: str = "default") -> ProducerConfig:
    """根据环境获取预定义配置

    提供针对不同环境的预定义配置模板，简化配置管理。
    每个环境都经过优化，适合对应的开发和部署场景。

    Args:
        environment: 环境名称，支持以下值：
            - "default": 默认配置，适合一般场景
            - "development"/"dev": 开发环境配置，启用调试和跟踪
            - "production"/"prod": 生产环境配置，注重性能和稳定性
            - "high_performance"/"high-perf": 高性能配置，优化吞吐量
            - "testing"/"test": 测试环境配置，简化配置便于测试

    Returns:
        ProducerConfig: 对应环境的预定义配置实例

    Example:
        >>> dev_config = get_config("development")
        >>> prod_config = get_config("production")
        >>> high_perf_config = get_config("high_performance")

    Note:
        如果指定的环境不存在，将返回默认配置
    """
    env = environment.lower()

    config_map = {
        "default": DEFAULT_CONFIG,
        "development": DEVELOPMENT_CONFIG,
        "dev": DEVELOPMENT_CONFIG,
        "production": PRODUCTION_CONFIG,
        "prod": PRODUCTION_CONFIG,
        "high_performance": HIGH_PERFORMANCE_CONFIG,
        "high-perf": HIGH_PERFORMANCE_CONFIG,
        "testing": TESTING_CONFIG,
        "test": TESTING_CONFIG,
    }

    return config_map.get(env, DEFAULT_CONFIG)


def create_custom_config(
    producer_group: str = "DEFAULT_PRODUCER",
    namesrv_addr: str = "localhost:9876",
    **kwargs,
) -> ProducerConfig:
    """创建自定义配置的便捷函数

    提供简化的接口来创建自定义配置，特别适合只需要修改
    少数关键参数的场景。同时支持通过kwargs传递任意配置参数。

    Args:
        producer_group: 生产者组名，默认为"DEFAULT_PRODUCER"
        namesrv_addr: NameServer地址，默认为"localhost:9876"
        **kwargs: 其他配置参数，支持ProducerConfig的所有字段

    Returns:
        ProducerConfig: 自定义的配置实例

    Example:
        >>> # 基本自定义配置
        >>> config = create_custom_config(
        ...     producer_group="my_producer",
        ...     namesrv_addr="192.168.1.100:9876"
        ... )
        >>>
        >>> # 带额外参数的自定义配置
        >>> config = create_custom_config(
        ...     producer_group="order_producer",
        ...     retry_times=3,
        ...     send_msg_timeout=5000.0,
        ...     batch_size=16
        ... )

    Note:
        通过kwargs传递的参数会覆盖默认值和基础参数
    """
    return ProducerConfig(
        producer_group=producer_group, namesrv_addr=namesrv_addr, **kwargs
    )
