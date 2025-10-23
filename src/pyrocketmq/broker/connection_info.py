import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from pyrocketmq.remote.async_remote import AsyncRemote


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
