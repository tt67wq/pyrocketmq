"""
订阅管理器核心实现

SubscriptionManager是RocketMQ Consumer的核心组件，负责管理Topic订阅关系、
消息选择器和订阅数据。它提供了完整的订阅生命周期管理，确保Consumer能够
正确订阅和处理感兴趣的消息。

作者: pyrocketmq开发团队
"""

from datetime import datetime, timedelta
from typing import Any
from threading import RLock

from pyrocketmq.model.client_data import MessageSelector, SubscriptionData
from pyrocketmq.model import SubscriptionEntry, SubscriptionConflict
from pyrocketmq.consumer.subscription_exceptions import (
    SubscriptionError,
    InvalidTopicError,
    InvalidSelectorError,
    TopicNotSubscribedError,
    SubscriptionConflictError,
    SubscriptionLimitExceededError,
    SubscriptionDataError,
)


class SubscriptionManager:
    """订阅关系管理器

    管理Consumer的Topic订阅关系，提供线程安全的订阅操作。
    支持订阅的增删改查、冲突检测、数据持久化等功能。
    """

    def __init__(self, max_subscriptions: int = 1000) -> None:
        """初始化订阅管理器

        Args:
            max_subscriptions: 最大订阅数量限制，防止内存溢出
        """
        self._subscriptions: dict[str, SubscriptionEntry] = {}
        self._lock: RLock = RLock()  # 可重入锁，保证线程安全
        self._max_subscriptions: int = max_subscriptions
        self._conflict_history: list[SubscriptionConflict] = []

    # ==================== 核心订阅操作 ====================

    def subscribe(self, topic: str, selector: MessageSelector) -> bool:
        """订阅Topic

        Args:
            topic: Topic名称
            selector: 消息选择器

        Returns:
            bool: 订阅是否成功

        Raises:
            InvalidTopicError: Topic名称无效时抛出
            InvalidSelectorError: 选择器无效时抛出
            SubscriptionLimitExceededError: 超过订阅限制时抛出
            SubscriptionConflictError: 存在订阅冲突时抛出
        """
        with self._lock:
            # 验证参数
            if not self._validate_topic(topic):
                raise InvalidTopicError(f"Invalid topic: {topic}", topic=topic)

            if not self._validate_selector(selector):
                raise InvalidSelectorError(
                    f"Invalid selector: {selector}", selector=str(selector)
                )

            # 检查订阅数量限制
            if len(self._subscriptions) >= self._max_subscriptions:
                raise SubscriptionLimitExceededError(
                    f"Subscription limit exceeded: {self._max_subscriptions}",
                    current_count=len(self._subscriptions),
                    limit=self._max_subscriptions,
                )

            # 检查是否存在冲突
            conflict = self._check_subscription_conflict(topic, selector)
            if conflict:
                self._conflict_history.append(conflict)
                pass  # MVP版本移除指标记录
                raise SubscriptionConflictError(
                    f"Subscription conflict for topic {topic}: {conflict}",
                    topic=topic,
                    existing_selector=str(conflict.existing_selector),
                    new_selector=str(conflict.new_selector),
                    conflict_type=conflict.conflict_type,
                )

            # 创建或更新订阅
            now = datetime.now()

            try:
                subscription_data = SubscriptionData.from_selector(topic, selector)
            except Exception as e:
                raise SubscriptionDataError(
                    f"Failed to create subscription data: {e}",
                    topic=topic,
                    data_type="SubscriptionData",
                )

            if topic in self._subscriptions:
                # 更新现有订阅
                existing = self._subscriptions[topic]
                existing.selector = selector
                existing.subscription_data = subscription_data
                existing.updated_at = now
                existing.is_active = True
                pass  # MVP版本移除指标记录
            else:
                # 创建新订阅
                entry = SubscriptionEntry(
                    topic=topic,
                    selector=selector,
                    subscription_data=subscription_data,
                    created_at=now,
                    updated_at=now,
                )
                self._subscriptions[topic] = entry
                pass  # MVP版本移除指标记录

            # 更新统计信息
            self._update_metrics()
            return True

    def unsubscribe(self, topic: str) -> bool:
        """取消订阅Topic

        Args:
            topic: Topic名称

        Returns:
            bool: 取消订阅是否成功
        """
        with self._lock:
            if topic in self._subscriptions:
                del self._subscriptions[topic]
                pass  # MVP版本移除指标记录
                self._update_metrics()
                return True
            return False

    def update_selector(self, topic: str, new_selector: MessageSelector) -> bool:
        """更新消息选择器

        Args:
            topic: Topic名称
            new_selector: 新的消息选择器

        Returns:
            bool: 更新是否成功

        Raises:
            TopicNotSubscribedError: Topic未订阅时抛出
            SubscriptionConflictError: 存在冲突时抛出
        """
        with self._lock:
            if topic not in self._subscriptions:
                raise TopicNotSubscribedError(
                    f"Topic not subscribed: {topic}", topic=topic
                )

            # 检查冲突
            conflict = self._check_subscription_conflict(topic, new_selector)
            if conflict:
                self._conflict_history.append(conflict)
                pass  # MVP版本移除指标记录
                raise SubscriptionConflictError(
                    f"Selector update conflict for topic {topic}: {conflict}",
                    topic=topic,
                    existing_selector=str(conflict.existing_selector),
                    new_selector=str(conflict.new_selector),
                    conflict_type=conflict.conflict_type,
                )

            # 更新选择器
            try:
                subscription_data = SubscriptionData.from_selector(topic, new_selector)
            except Exception as e:
                raise SubscriptionDataError(
                    f"Failed to create subscription data: {e}",
                    topic=topic,
                    data_type="SubscriptionData",
                )

            entry = self._subscriptions[topic]
            entry.selector = new_selector
            entry.subscription_data = subscription_data
            entry.update_timestamp()

            pass  # MVP版本移除指标记录
            self._update_metrics()
            return True

    # ==================== 查询操作 ====================

    def get_subscription(self, topic: str) -> SubscriptionEntry | None:
        """获取Topic订阅信息

        Args:
            topic: Topic名称

        Returns:
            Optional[SubscriptionEntry]: 订阅条目，如果不存在则返回None
        """
        with self._lock:
            return self._subscriptions.get(topic)

    def get_all_subscriptions(self) -> list[SubscriptionEntry]:
        """获取所有订阅信息

        Returns:
            List[SubscriptionEntry]: 所有订阅条目的列表
        """
        with self._lock:
            return list(self._subscriptions.values())

    def get_active_subscriptions(self) -> list[SubscriptionEntry]:
        """获取所有活跃订阅

        Returns:
            List[SubscriptionEntry]: 活跃订阅条目的列表
        """
        with self._lock:
            return [entry for entry in self._subscriptions.values() if entry.is_active]

    def is_subscribed(self, topic: str) -> bool:
        """检查是否已订阅Topic

        Args:
            topic: Topic名称

        Returns:
            bool: 是否已订阅
        """
        with self._lock:
            return topic in self._subscriptions

    def get_subscription_count(self) -> int:
        """获取订阅数量

        Returns:
            int: 当前订阅数量
        """
        with self._lock:
            return len(self._subscriptions)

    def get_topics(self) -> list[str]:
        """获取所有订阅的Topic列表

        Returns:
            List[str]: Topic名称列表
        """
        with self._lock:
            return list(self._subscriptions.keys())

    # ==================== 数据转换操作 ====================

    def to_subscription_data_list(self) -> list[SubscriptionData]:
        """转换为SubscriptionData列表

        Returns:
            List[SubscriptionData]: 订阅数据列表
        """
        with self._lock:
            return [entry.subscription_data for entry in self._subscriptions.values()]

    def from_subscription_data_list(
        self, subscription_data_list: list[SubscriptionData]
    ) -> None:
        """从SubscriptionData列表恢复订阅关系

        Args:
            subscription_data_list: 订阅数据列表
        """
        with self._lock:
            self.clear_all()

            for sub_data in subscription_data_list:
                try:
                    selector = MessageSelector.from_dict(
                        {
                            "type": sub_data.expression_type,
                            "expression": sub_data.sub_string,
                        }
                    )

                    entry = SubscriptionEntry(
                        topic=sub_data.topic,
                        selector=selector,
                        subscription_data=sub_data,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                    )

                    self._subscriptions[sub_data.topic] = entry
                except Exception as e:
                    # 跳过无效的订阅数据，记录错误但不中断整个过程
                    print(
                        f"Warning: Failed to restore subscription for {sub_data.topic}: {e}"
                    )

            self._update_metrics()

    def export_subscriptions(self) -> dict[str, Any]:
        """导出订阅关系

        Returns:
            Dict[str, Any]: 导出的订阅数据
        """
        with self._lock:
            return {
                "subscriptions": [
                    entry.to_dict() for entry in self._subscriptions.values()
                ],
                "conflict_history": [
                    conflict.to_dict() for conflict in self._conflict_history
                ],
                "export_time": datetime.now().isoformat(),
                # metrics: self._metrics.to_dict(),  # MVP版本暂不支持指标
            }

    def import_subscriptions(self, data: dict[str, Any]) -> None:
        """导入订阅关系

        Args:
            data: 导入的订阅数据
        """
        with self._lock:
            self.clear_all()

            # 导入订阅数据
            for sub_data in data.get("subscriptions", []):
                try:
                    selector = MessageSelector.from_dict(sub_data["selector"])
                    subscription_data = SubscriptionData.from_dict(
                        sub_data["subscription_data"]
                    )

                    entry = SubscriptionEntry(
                        topic=sub_data["topic"],
                        selector=selector,
                        subscription_data=subscription_data,
                        created_at=datetime.fromisoformat(sub_data["created_at"]),
                        updated_at=datetime.fromisoformat(sub_data["updated_at"]),
                        is_active=sub_data.get("is_active", True),
                    )

                    self._subscriptions[entry.topic] = entry
                except Exception as e:
                    print(
                        f"Warning: Failed to import subscription for {sub_data.get('topic', 'unknown')}: {e}"
                    )

            # 导入冲突历史
            for conflict_data in data.get("conflict_history", []):
                try:
                    conflict = SubscriptionConflict(
                        topic=conflict_data["topic"],
                        existing_selector=MessageSelector.from_dict(
                            conflict_data["existing_selector"]
                        ),
                        new_selector=MessageSelector.from_dict(
                            conflict_data["new_selector"]
                        ),
                        conflict_type=conflict_data["conflict_type"],
                        timestamp=datetime.fromisoformat(conflict_data["timestamp"]),
                        description=conflict_data.get("description"),
                    )
                    self._conflict_history.append(conflict)
                except Exception as e:
                    print(f"Warning: Failed to import conflict record: {e}")

            self._update_metrics()

    # ==================== 管理操作 ====================

    def clear_all(self) -> None:
        """清除所有订阅关系"""
        with self._lock:
            self._subscriptions.clear()
            pass  # MVP版本移除指标记录
            self._update_metrics()

    def deactivate_subscription(self, topic: str) -> bool:
        """停用订阅（不删除，只是标记为非活跃）

        Args:
            topic: Topic名称

        Returns:
            bool: 操作是否成功
        """
        with self._lock:
            if topic in self._subscriptions:
                self._subscriptions[topic].is_active = False
                pass  # MVP版本移除指标记录
                self._update_metrics()
                return True
            return False

    def activate_subscription(self, topic: str) -> bool:
        """激活订阅

        Args:
            topic: Topic名称

        Returns:
            bool: 操作是否成功
        """
        with self._lock:
            if topic in self._subscriptions:
                self._subscriptions[topic].is_active = True
                pass  # MVP版本移除指标记录
                self._update_metrics()
                return True
            return False

    def cleanup_inactive_subscriptions(
        self, inactive_threshold: timedelta = timedelta(hours=24)
    ) -> int:
        """清理长时间非活跃的订阅

        Args:
            inactive_threshold: 非活跃时间阈值

        Returns:
            int: 清理的订阅数量
        """
        with self._lock:
            cutoff_time = datetime.now() - inactive_threshold
            to_remove: list[str] = []

            for topic, entry in self._subscriptions.items():
                if not entry.is_active and entry.updated_at < cutoff_time:
                    to_remove.append(topic)

            for topic in to_remove:
                del self._subscriptions[topic]

            if to_remove:
                pass  # MVP版本移除指标记录
                self._update_metrics()

            return len(to_remove)

    # ==================== 验证和冲突检测 ====================

    def validate_subscription(self, topic: str, selector: MessageSelector) -> bool:
        """验证订阅是否有效

        Args:
            topic: Topic名称
            selector: 消息选择器

        Returns:
            bool: 验证是否通过
        """
        return self._validate_topic(topic) and self._validate_selector(selector)

    def get_conflict_history(self, limit: int = 100) -> list[SubscriptionConflict]:
        """获取冲突历史

        Args:
            limit: 返回的冲突数量限制

        Returns:
            List[SubscriptionConflict]: 冲突历史列表
        """
        with self._lock:
            return self._conflict_history[-limit:]

    def clear_conflict_history(self) -> None:
        """清除冲突历史"""
        with self._lock:
            self._conflict_history.clear()

    # ==================== 私有方法 ====================

    def _validate_topic(self, topic: str) -> bool:
        """验证Topic名称

        Args:
            topic: Topic名称

        Returns:
            bool: 验证是否通过
        """
        if not topic or not isinstance(topic, str):
            return False

        # Topic名称验证规则（参考RocketMQ规范）
        return (
            len(topic) > 0
            and len(topic) <= 127
            and topic not in ("", "DEFAULT_TOPIC")
            and not topic.startswith("SYS_")
        )

    def _validate_selector(self, selector: MessageSelector) -> bool:
        """验证消息选择器

        Args:
            selector: 消息选择器

        Returns:
            bool: 验证是否通过
        """
        if not isinstance(selector, MessageSelector):
            return False

        try:
            selector.validate()
            return True
        except Exception:
            return False

    def _check_subscription_conflict(
        self, topic: str, new_selector: MessageSelector
    ) -> SubscriptionConflict | None:
        """检查订阅冲突

        Args:
            topic: Topic名称
            new_selector: 新选择器

        Returns:
            Optional[SubscriptionConflict]: 冲突信息，如果无冲突则返回None
        """
        if topic not in self._subscriptions:
            return None

        existing = self._subscriptions[topic]

        # 检查选择器类型冲突
        if existing.selector.type != new_selector.type:
            return SubscriptionConflict(
                topic=topic,
                existing_selector=existing.selector,
                new_selector=new_selector,
                conflict_type="SELECTOR_TYPE_MISMATCH",
                timestamp=datetime.now(),
                description=f"Selector type mismatch: {existing.selector.type} vs {new_selector.type}",
            )

        # 检查表达式冲突（仅对相同类型进行）
        if (
            existing.selector.type == new_selector.type
            and existing.selector.expression != new_selector.expression
        ):
            return SubscriptionConflict(
                topic=topic,
                existing_selector=existing.selector,
                new_selector=new_selector,
                conflict_type="EXPRESSION_MISMATCH",
                timestamp=datetime.now(),
                description=f"Expression mismatch: '{existing.selector.expression}' vs '{new_selector.expression}'",
            )

        return None

    def _update_metrics(self) -> None:
        """更新统计信息（MVP版本简化）"""
        # MVP版本暂不实现复杂指标
        pass

    def get_status_summary(self) -> dict[str, Any]:
        """获取状态摘要

        Returns:
            Dict[str, Any]: 包含关键状态信息的摘要
        """
        with self._lock:
            return {
                "subscription_count": len(self._subscriptions),
                "active_count": len(
                    [s for s in self._subscriptions.values() if s.is_active]
                ),
                "inactive_count": len(
                    [s for s in self._subscriptions.values() if not s.is_active]
                ),
                "total_conflicts": len(self._conflict_history),
                "recent_conflicts": len(
                    [
                        c
                        for c in self._conflict_history
                        if c.timestamp > datetime.now() - timedelta(hours=24)
                    ]
                ),
                "max_subscriptions": self._max_subscriptions,
                "utilization": len(self._subscriptions) / self._max_subscriptions,
                "last_updated": datetime.now().isoformat(),  # MVP版本使用当前时间
            }

    def __str__(self) -> str:
        """返回订阅管理器的字符串表示"""
        active_count = len([s for s in self._subscriptions.values() if s.is_active])
        return f"SubscriptionManager(subscriptions={len(self._subscriptions)}, active={active_count})"

    def __repr__(self) -> str:
        """返回订阅管理器的详细表示"""
        active_count = len([s for s in self._subscriptions.values() if s.is_active])
        return (
            f"SubscriptionManager(total={len(self._subscriptions)}, "
            f"active={active_count}, "
            f"conflicts={len(self._conflict_history)}, "
            f"max_subscriptions={self._max_subscriptions})"
        )
