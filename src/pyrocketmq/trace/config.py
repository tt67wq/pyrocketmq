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

    def __post_init__(self):
        """Post-initialization validation."""
        if not self.trace_topic:
            raise ValueError("trace_topic cannot be empty")
        if not self.group_name:
            raise ValueError("group_name cannot be empty")
        if not self.namesrv_addrs:
            raise ValueError("namesrv_addrs cannot be empty")

    @classmethod
    def create_local_config(
        cls, trace_topic: str, group_name: str, namesrv_addrs: List[str]
    ) -> "TraceConfig":
        """Create a trace config for local IDC cluster access."""
        return cls(
            trace_topic=trace_topic,
            group_name=group_name,
            access=AccessChannel.LOCAL,
            namesrv_addrs=namesrv_addrs,
        )

    @classmethod
    def create_cloud_config(
        cls, trace_topic: str, group_name: str, namesrv_addrs: List[str]
    ) -> "TraceConfig":
        """Create a trace config for cloud service access."""
        return cls(
            trace_topic=trace_topic,
            group_name=group_name,
            access=AccessChannel.CLOUD,
            namesrv_addrs=namesrv_addrs,
        )
