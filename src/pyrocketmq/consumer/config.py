"""
Consumer配置管理模块

提供RocketMQ Consumer的完整配置管理功能，包括消费策略、
线程池配置、偏移量存储等核心参数。

MVP版本专注于核心配置，后续版本会逐步扩展更多配置选项。
"""

import os
from dataclasses import dataclass, field
from typing import Any

from pyrocketmq.model import AllocateQueueStrategy, ConsumeFromWhere, MessageModel


@dataclass
class ConsumerConfig:
    """
    RocketMQ Consumer配置类

    提供完整的Consumer配置参数，包括基础连接配置、消费行为配置、
    性能调优配置等。采用dataclass设计，便于类型检查和序列化。

    设计原则:
    - MVP优先: 只包含最核心的配置项
    - 默认值合理: 提供经过验证的默认配置
    - 可扩展性: 预留扩展点便于后续功能增强
    - 环境友好: 支持从环境变量加载配置
    """

    # === 基础配置 ===
    consumer_group: str  # 消费者组名称(必需)
    namesrv_addr: str  # NameServer地址(必需)

    # === 消费行为配置 ===
    message_model: str = MessageModel.CLUSTERING  # 消费模式
    consume_from_where: str = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET  # 消费起始位置
    consume_timestamp: int = 0  # 时间戳消费的起始时间(毫秒)
    allocate_queue_strategy: str = AllocateQueueStrategy.AVERAGE  # 队列分配策略
    max_reconsume_times: int = 16  # 最大重试次数
    suspend_current_queue_time_millis: int = 1000  # 消费失败时挂起队列的时间(毫秒)

    # === 性能配置 ===
    consume_thread_min: int = 20  # 最小消费线程数
    consume_thread_max: int = 64  # 最大消费线程数
    consume_timeout: int = 15  # 消费超时时间(秒)
    consume_batch_size: int = 1  # 每次消费处理的批次大小
    pull_batch_size: int = 32  # 批量拉取消息数量
    pull_interval: int = 1000  # 拉取间隔(毫秒)，0表示持续拉取

    # === OffsetStore偏移量存储配置 ===
    # 持久化间隔配置
    persist_interval: int = 5000  # 毫秒，定期持久化间隔

    # 本地存储路径(广播模式使用)
    offset_store_path: str = "~/.rocketmq/offsets"

    # 内存缓存配置
    max_cache_count_per_queue: int = 1024  # 每个队列最大缓存消息数量
    max_cache_size_per_queue: int = 512  # 每个队列最大缓存字节数(MB)
    # 故障恢复配置
    enable_auto_recovery: bool = True  # 启用自动恢复
    max_retry_times: int = 3  # 最大重试次数

    # === 队列锁配置 ===
    lock_expire_time: float = 30000.0  # 队列锁过期时间(毫秒)，用于顺序消费的锁管理

    # === 高级配置 ===
    enable_auto_commit: bool = True  # 是否自动提交偏移量
    enable_message_trace: bool = False  # 是否启用消息追踪

    # === 跟踪配置 ===
    enable_trace: bool = True  # 是否启用 TraceDispatcher 进行消息跟踪
    trace_topic: str | None = None  # 跟踪消息发送到的主题
    trace_batch_size: int | None = None  # 跟踪消息批量发送的大小
    trace_msg_size: int | None = None  # 单条跟踪消息的最大大小

    # === 内部配置 ===
    _client_id: str = field(default="", init=False)  # 客户端ID(内部生成)
    _instance_name: str = field(default="DEFAULT", init=False)  # 实例名称
    _attributes: dict[str, Any] = field(default_factory=dict, init=False)  # 内部属性

    def __post_init__(self) -> None:
        """后处理，验证配置和生成内部ID"""

        # 验证必需配置
        if not self.consumer_group:
            raise ValueError("consumer_group 不能为空")
        if not self.namesrv_addr:
            raise ValueError("namesrv_addr 不能为空")

        # 验证数值配置的合理性
        if self.consume_thread_min <= 0:
            raise ValueError("consume_thread_min 必须大于0")
        if self.consume_thread_max < self.consume_thread_min:
            raise ValueError("consume_thread_max 不能小于 consume_thread_min")
        if self.consume_batch_size <= 0:
            raise ValueError("consume_batch_size 必须大于0")
        if self.pull_batch_size <= 0:
            raise ValueError("pull_batch_size 必须大于0")
        if self.pull_batch_size > 1024:
            raise ValueError("pull_batch_size 不能超过1024")
        if self.max_reconsume_times < 0:
            raise ValueError("max_reconsume_times 不能为负数")
        if self.suspend_current_queue_time_millis < 0:
            raise ValueError("suspend_current_queue_time_millis 不能为负数")
        if self.consume_timeout <= 0:
            raise ValueError("consume_timeout 必须大于0")

        # 验证OffsetStore配置
        if self.persist_interval <= 0:
            raise ValueError("persist_interval 必须大于0")
        if not (1 <= self.max_cache_count_per_queue <= 65535):
            raise ValueError("max_cache_count_per_queue 必须在1-65535范围内")
        if not (1 <= self.max_cache_size_per_queue <= 1024):
            raise ValueError("max_cache_size_per_queue 必须在1-1024范围内")
        if self.max_retry_times < 0:
            raise ValueError("max_retry_times 不能为负数")

        # 验证路径配置
        if not self.offset_store_path:
            raise ValueError("offset_store_path 不能为空")

        # 验证锁配置
        if self.lock_expire_time <= 0:
            raise ValueError("lock_expire_time 必须大于0")

        # 生成客户端ID
        import time

        self._client_id = f"{self.consumer_group}@{int(time.time() * 1000)}"

        # 从环境变量加载可选配置
        self._load_from_env()

    def _load_from_env(self) -> None:
        """从环境变量加载配置项"""

        # 性能配置环境变量
        if os.getenv("ROCKETMQ_CONSUME_THREAD_MIN"):
            self.consume_thread_min = int(os.getenv("ROCKETMQ_CONSUME_THREAD_MIN", 20))
        if os.getenv("ROCKETMQ_CONSUME_THREAD_MAX"):
            self.consume_thread_max = int(os.getenv("ROCKETMQ_CONSUME_THREAD_MAX", 64))
        if os.getenv("ROCKETMQ_CONSUME_BATCH_SIZE"):
            self.consume_batch_size = int(os.getenv("ROCKETMQ_CONSUME_BATCH_SIZE", 1))
        if os.getenv("ROCKETMQ_PULL_BATCH_SIZE"):
            pull_batch_size_env = int(os.getenv("ROCKETMQ_PULL_BATCH_SIZE", 32))
            if pull_batch_size_env > 1024:
                raise ValueError("ROCKETMQ_PULL_BATCH_SIZE 环境变量不能超过1024")
            self.pull_batch_size = pull_batch_size_env
        if os.getenv("ROCKETMQ_CONSUME_TIMEOUT"):
            self.consume_timeout = int(os.getenv("ROCKETMQ_CONSUME_TIMEOUT", 15))

        # 行为配置环境变量
        model = os.getenv("ROCKETMQ_MESSAGE_MODEL")
        if model:
            model = model.upper()
            if model in [MessageModel.BROADCASTING, MessageModel.CLUSTERING]:
                self.message_model = model

        where = os.getenv("ROCKETMQ_CONSUME_FROM_WHERE")
        if where:
            where = where.upper()
            if where in [
                ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
                ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
            ]:
                self.consume_from_where = where

        allocate_strategy = os.getenv("ROCKETMQ_ALLOCATE_STRATEGY")
        if allocate_strategy:
            allocate_strategy = allocate_strategy.upper()
            if allocate_strategy in [
                AllocateQueueStrategy.AVERAGE,
                AllocateQueueStrategy.HASH,
            ]:
                self.allocate_queue_strategy = allocate_strategy

        # OffsetStore配置环境变量
        if os.getenv("ROCKETMQ_PERSIST_INTERVAL"):
            self.persist_interval = int(os.getenv("ROCKETMQ_PERSIST_INTERVAL", 5000))
        if os.getenv("ROCKETMQ_OFFSET_STORE_PATH"):
            self.offset_store_path = os.getenv(
                "ROCKETMQ_OFFSET_STORE_PATH", "~/.rocketmq/offsets"
            )
        if os.getenv("ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE"):
            cache_count_env = int(
                os.getenv("ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE", 10000)
            )
            if not (1 <= cache_count_env <= 65535):
                raise ValueError(
                    "ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE 环境变量必须在1-65535范围内"
                )
            self.max_cache_count_per_queue = cache_count_env
        if os.getenv("ROCKETMQ_MAX_CACHE_SIZE_PER_QUEUE"):
            cache_size_env = int(os.getenv("ROCKETMQ_MAX_CACHE_SIZE_PER_QUEUE", 512))
            if not (1 <= cache_size_env <= 1024):
                raise ValueError(
                    "ROCKETMQ_MAX_CACHE_SIZE_PER_QUEUE 环境变量必须在1-1024范围内"
                )
            self.max_cache_size_per_queue = cache_size_env
        if os.getenv("ROCKETMQ_ENABLE_AUTO_RECOVERY"):
            self.enable_auto_recovery = (
                os.getenv("ROCKETMQ_ENABLE_AUTO_RECOVERY", "true").lower() == "true"
            )
        if os.getenv("ROCKETMQ_MAX_RETRY_TIMES"):
            self.max_retry_times = int(os.getenv("ROCKETMQ_MAX_RETRY_TIMES", 3))
        if os.getenv("ROCKETMQ_SUSPEND_CURRENT_QUEUE_TIME_MILLIS"):
            self.suspend_current_queue_time_millis = int(
                os.getenv("ROCKETMQ_SUSPEND_CURRENT_QUEUE_TIME_MILLIS", 1000)
            )
        if os.getenv("ROCKETMQ_LOCK_EXPIRE_TIME"):
            self.lock_expire_time = float(
                os.getenv("ROCKETMQ_LOCK_EXPIRE_TIME", 30000.0)
            )

    @property
    def client_id(self) -> str:
        """获取客户端ID"""
        return self._client_id

    @property
    def instance_name(self) -> str:
        """获取实例名称"""
        return self._instance_name

    def set_instance_name(self, name: str) -> None:
        """设置实例名称"""
        if not name:
            raise ValueError("instance_name 不能为空")
        self._instance_name = name
        # 重新生成客户端ID
        import time

        self._client_id = (
            f"{self.consumer_group}@{self._instance_name}@{int(time.time() * 1000)}"
        )

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "consumer_group": self.consumer_group,
            "namesrv_addr": self.namesrv_addr,
            "message_model": self.message_model,
            "consume_from_where": self.consume_from_where,
            "consume_timestamp": self.consume_timestamp,
            "allocate_queue_strategy": self.allocate_queue_strategy,
            "max_reconsume_times": self.max_reconsume_times,
            "suspend_current_queue_time_millis": self.suspend_current_queue_time_millis,
            "consume_thread_min": self.consume_thread_min,
            "consume_thread_max": self.consume_thread_max,
            "consume_timeout": self.consume_timeout,
            "consume_batch_size": self.consume_batch_size,
            "pull_batch_size": self.pull_batch_size,
            "pull_interval": self.pull_interval,
            # OffsetStore配置
            "persist_interval": self.persist_interval,
            "offset_store_path": self.offset_store_path,
            "max_cache_count_per_queue": self.max_cache_count_per_queue,
            "max_cache_size_per_queue": self.max_cache_size_per_queue,
            "enable_auto_recovery": self.enable_auto_recovery,
            "max_retry_times": self.max_retry_times,
            # 队列锁配置
            "lock_expire_time": self.lock_expire_time,
            # 高级配置
            "enable_auto_commit": self.enable_auto_commit,
            "enable_message_trace": self.enable_message_trace,
            # 跟踪配置
            "enable_trace": self.enable_trace,
            "trace_topic": self.trace_topic,
            "trace_batch_size": self.trace_batch_size,
            "trace_msg_size": self.trace_msg_size,
            "client_id": self.client_id,
            "instance_name": self.instance_name,
        }

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ConsumerConfig["
            f"group={self.consumer_group}, "
            f"namesrv={self.namesrv_addr}, "
            f"model={self.message_model}, "
            f"strategy={self.allocate_queue_strategy}, "
            f"threads=({self.consume_thread_min}-{self.consume_thread_max}), "
            f"consume_batch={self.consume_batch_size}, "
            f"pull_batch={self.pull_batch_size}"
            f"]"
        )


def create_consumer_config(
    consumer_group: str, namesrv_addr: str, **kwargs: Any
) -> ConsumerConfig:
    """
    创建Consumer配置的便利函数

    Args:
        consumer_group: 消费者组名称
        namesrv_addr: NameServer地址
        **kwargs: 其他配置参数

    Returns:
        ConsumerConfig实例

    Examples:
        >>> # 创建基本配置
        >>> config = create_consumer_config(
        ...     "test_group",
        ...     "localhost:9876"
        ... )
        >>>
        >>> # 创建自定义配置
        >>> config = create_consumer_config(
        ...     "test_group",
        ...     "localhost:9876",
        ...     consume_thread_max=32,
        ...     pull_batch_size=16,
        ...     allocate_queue_strategy=AllocateQueueStrategy.HASH
        ... )
        >>>
        >>> # 创建集群消费配置
        >>> config = create_consumer_config(
        ...     "order_consumer",
        ...     "broker1:9876;broker2:9876",
        ...     message_model=MessageModel.CLUSTERING,
        ...     consume_from_where=ConsumeFromWhere.FIRST_OFFSET
        ... )
    """
    return ConsumerConfig(
        consumer_group=consumer_group, namesrv_addr=namesrv_addr, **kwargs
    )


def create_config(**kwargs: Any) -> ConsumerConfig:
    """
    创建自定义配置

    这个函数允许用户通过关键字参数创建完全自定义的Consumer配置。
    所有参数都是可选的，只提供必需参数即可创建有效的配置。

    Args:
        **kwargs: 配置参数

    Returns:
        ConsumerConfig实例

    Examples:
        >>> # 高性能配置
        >>> high_perf_config = create_custom_config(
        ...     consumer_group="high_perf_consumer",
        ...     namesrv_addr="broker1:9876;broker2:9876",
        ...     consume_thread_max=128,
        ...     pull_batch_size=64,
        ...     pull_interval=0,
        ...     persist_interval=1000,  # 频繁持久化
        ...     max_cache_size=50000
        ... )
        >>>
        >>> # 资源节约配置
        >>> low_resource_config = create_custom_config(
        ...     consumer_group="low_resource_consumer",
        ...     namesrv_addr="localhost:9876",
        ...     consume_thread_min=1,
        ...     consume_thread_max=2,
        ...     pull_batch_size=8,
        ...     pull_interval=1000,
        ...     persist_interval=10000,  # 较低频持久化
        ...     max_cache_size=500
        ... )
        >>>
        >>> # 广播消费配置
        >>> broadcast_config = create_custom_config(
        ...     consumer_group="broadcast_consumer",
        ...     namesrv_addr="localhost:9876",
        ...     message_model=MessageModel.BROADCASTING,
        ...     offset_store_path="/tmp/broadcast_offsets",
        ...     persist_interval=2000
        ... )
    """

    # 确保必需参数存在
    if "consumer_group" not in kwargs:
        kwargs["consumer_group"] = "custom_consumer_group"
    if "namesrv_addr" not in kwargs:
        kwargs["namesrv_addr"] = "localhost:9876"

    return ConsumerConfig(**kwargs)
