"""Trace configuration for pyrocketmq."""

from dataclasses import dataclass
from enum import Enum
from typing import List


class AccessChannel(Enum):
    """Access channel enumeration for trace configuration."""

    LOCAL = 0  # connect to private IDC cluster
    CLOUD = 1  # connect to Cloud service


@dataclass
class TraceConfig:
    """Trace configuration for RocketMQ message tracing."""

    trace_topic: str
    group_name: str
    access: AccessChannel
    namesrv_addrs: List[str]
    max_batch_size: int = 20  # Default max batch size for trace messages
    max_msg_size: int = 4 * 1024 * 1024  # Default max message size (4MB)

    def __post_init__(self):
        """Post-initialization validation."""
        if not self.trace_topic:
            raise ValueError("trace_topic cannot be empty")
        if not self.group_name:
            raise ValueError("group_name cannot be empty")
        if not self.namesrv_addrs:
            raise ValueError("namesrv_addrs cannot be empty")
        if self.max_batch_size <= 0:
            raise ValueError("max_batch_size must be greater than 0")
        if self.max_msg_size <= 0:
            raise ValueError("max_msg_size must be greater than 0")

    @classmethod
    def create_local_config(
        cls,
        trace_topic: str,
        group_name: str,
        namesrv_addrs: List[str],
        max_batch_size: int = 20,
        max_msg_size: int = 4 * 1024 * 1024,
    ) -> "TraceConfig":
        """Create a trace config for local IDC cluster access."""
        return cls(
            trace_topic=trace_topic,
            group_name=group_name,
            access=AccessChannel.LOCAL,
            namesrv_addrs=namesrv_addrs,
            max_batch_size=max_batch_size,
            max_msg_size=max_msg_size,
        )

    @classmethod
    def create_cloud_config(
        cls,
        trace_topic: str,
        group_name: str,
        namesrv_addrs: List[str],
        max_batch_size: int = 20,
        max_msg_size: int = 4 * 1024 * 1024,
    ) -> "TraceConfig":
        """Create a trace config for cloud service access."""
        return cls(
            trace_topic=trace_topic,
            group_name=group_name,
            access=AccessChannel.CLOUD,
            namesrv_addrs=namesrv_addrs,
            max_batch_size=max_batch_size,
            max_msg_size=max_msg_size,
        )
