"""
订阅相关数据结构

本模块定义了RocketMQ订阅相关的核心数据结构，
包括订阅条目和订阅冲突信息。这些数据结构主要用于
Consumer的订阅管理功能。

作者: pyrocketmq开发团队
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .client_data import MessageSelector, SubscriptionData


@dataclass
class SubscriptionEntry:
    """订阅条目数据结构

    存储单个Topic的完整订阅信息，包括选择器、时间戳等元数据。
    主要用于SubscriptionManager中的订阅管理。
    """

    topic: str  # 订阅的Topic
    selector: MessageSelector  # 消息选择器
    subscription_data: SubscriptionData  # 订阅数据
    created_at: datetime  # 创建时间
    updated_at: datetime  # 更新时间
    is_active: bool = True  # 是否活跃

    def update_timestamp(self) -> None:
        """更新时间戳为当前时间

        用于订阅信息更新时记录最新时间。
        """
        self.updated_at = datetime.now()

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            dict[str, Any]: 包含所有订阅信息的字典
        """
        return {
            "topic": self.topic,
            "selector": self.selector.to_dict(),
            "subscription_data": self.subscription_data.to_dict(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "is_active": self.is_active,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubscriptionEntry":
        """从字典创建订阅条目

        Args:
            data: 包含订阅信息的字典

        Returns:
            SubscriptionEntry: 订阅条目实例
        """
        selector = MessageSelector.from_dict(data["selector"])
        subscription_data = SubscriptionData.from_dict(data["subscription_data"])

        return cls(
            topic=data["topic"],
            selector=selector,
            subscription_data=subscription_data,
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            is_active=data.get("is_active", True),
        )

    def __str__(self) -> str:
        """返回订阅条目的字符串表示"""
        status = "active" if self.is_active else "inactive"
        return f"SubscriptionEntry(topic={self.topic}, selector={self.selector}, status={status})"

    def __repr__(self) -> str:
        """返回订阅条目的详细表示"""
        return (
            f"SubscriptionEntry(topic='{self.topic}', "
            f"selector={self.selector}, "
            f"created_at={self.created_at.isoformat()}, "
            f"updated_at={self.updated_at.isoformat()}, "
            f"is_active={self.is_active})"
        )


@dataclass
class SubscriptionConflict:
    """订阅冲突信息

    记录订阅冲突的详细信息，用于问题追踪和分析。
    当新的订阅与现有订阅发生冲突时，记录冲突的具体信息。
    """

    topic: str  # 冲突的Topic
    existing_selector: MessageSelector  # 现有选择器
    new_selector: MessageSelector  # 新选择器
    conflict_type: str  # 冲突类型
    timestamp: datetime  # 冲突发生时间
    description: str | None = None  # 冲突描述

    def __str__(self) -> str:
        """返回冲突信息的字符串表示"""
        base_info = f"SubscriptionConflict: {self.topic}, type={self.conflict_type}"
        selector_info = f"existing={self.existing_selector}, new={self.new_selector}"

        if self.description:
            return f"{base_info}, {selector_info}, desc={self.description}"
        return f"{base_info}, {selector_info}"

    def __repr__(self) -> str:
        """返回冲突信息的详细表示"""
        return (
            f"SubscriptionConflict(topic='{self.topic}', "
            f"existing_selector={self.existing_selector}, "
            f"new_selector={self.new_selector}, "
            f"conflict_type='{self.conflict_type}', "
            f"timestamp={self.timestamp.isoformat()})"
        )

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            dict[str, Any]: 包含冲突信息的字典
        """
        return {
            "topic": self.topic,
            "existing_selector": self.existing_selector.to_dict(),
            "new_selector": self.new_selector.to_dict(),
            "conflict_type": self.conflict_type,
            "timestamp": self.timestamp.isoformat(),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubscriptionConflict":
        """从字典创建冲突信息

        Args:
            data: 包含冲突信息的字典

        Returns:
            SubscriptionConflict: 冲突信息实例
        """
        return cls(
            topic=data["topic"],
            existing_selector=MessageSelector.from_dict(data["existing_selector"]),
            new_selector=MessageSelector.from_dict(data["new_selector"]),
            conflict_type=data["conflict_type"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            description=data.get("description"),
        )
