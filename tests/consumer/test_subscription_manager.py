#!/usr/bin/env python3
"""
SubscriptionManager单元测试

测试订阅管理器的主要API功能，使用pytest框架
"""

import pytest

from pyrocketmq.consumer.subscription_exceptions import (
    SubscriptionLimitExceededError,
    TopicNotSubscribedError,
)
from pyrocketmq.consumer.subscription_manager import SubscriptionManager
from pyrocketmq.model import ExpressionType, MessageSelector


class TestSubscriptionManager:
    """SubscriptionManager测试类"""

    def test_init_default(self):
        """测试默认初始化"""
        manager = SubscriptionManager()
        assert manager._max_subscriptions == 1000
        assert manager._subscriptions == {}
        assert manager._conflict_history == []

    def test_init_custom_max(self):
        """测试自定义最大订阅数"""
        manager = SubscriptionManager(max_subscriptions=5)
        assert manager._max_subscriptions == 5

    def test_subscribe_basic(self):
        """测试基本订阅功能"""
        manager = SubscriptionManager(max_subscriptions=5)
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1 || TAG2")

        # 订阅成功
        result = manager.subscribe(topic, selector)
        assert result is True

        # 验证订阅存在
        assert manager.is_subscribed(topic) is True
        assert manager.get_subscription_count() == 1

        # 验证订阅内容
        subscription = manager.get_subscription(topic)
        assert subscription is not None
        assert subscription.topic == topic
        assert subscription.selector.expression == "TAG1 || TAG2"

    def test_unsubscribe_basic(self):
        """测试基本取消订阅功能"""
        manager = SubscriptionManager()
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 先订阅
        manager.subscribe(topic, selector)
        assert manager.is_subscribed(topic) is True

        # 取消订阅
        result = manager.unsubscribe(topic)
        assert result is True
        assert manager.is_subscribed(topic) is False
        assert manager.get_subscription_count() == 0

    def test_unsubscribe_not_subscribed(self):
        """测试取消未订阅的主题"""
        manager = SubscriptionManager()
        topic = "non-existent-topic"

        # 取消未订阅的主题应该返回False
        result = manager.unsubscribe(topic)
        assert result is False

    def test_get_subscription_basic(self):
        """测试获取订阅信息"""
        manager = SubscriptionManager()
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 订阅前获取
        subscription = manager.get_subscription(topic)
        assert subscription is None

        # 订阅后获取
        manager.subscribe(topic, selector)
        subscription = manager.get_subscription(topic)
        assert subscription is not None
        assert subscription.topic == topic

    def test_get_all_subscriptions(self):
        """测试获取所有订阅"""
        manager = SubscriptionManager()
        topics = ["topic1", "topic2", "topic3"]

        for i, topic in enumerate(topics):
            selector = MessageSelector(ExpressionType.TAG, f"TAG{i + 1}")
            manager.subscribe(topic, selector)

        # 获取所有订阅
        subscriptions = manager.get_all_subscriptions()
        assert len(subscriptions) == 3

        # 验证主题
        topic_names = [sub.topic for sub in subscriptions]
        for topic in topics:
            assert topic in topic_names

    def test_get_active_subscriptions(self):
        """测试获取活跃订阅"""
        manager = SubscriptionManager()
        topic1 = "active-topic"
        topic2 = "inactive-topic"

        # 订阅两个主题
        manager.subscribe(topic1, MessageSelector(ExpressionType.TAG, "TAG1"))
        manager.subscribe(topic2, MessageSelector(ExpressionType.TAG, "TAG2"))

        # 停用一个
        manager.deactivate_subscription(topic2)

        # 获取活跃订阅
        active_subs = manager.get_active_subscriptions()
        assert len(active_subs) == 1
        assert active_subs[0].topic == topic1

    def test_get_topics(self):
        """测试获取所有主题"""
        manager = SubscriptionManager()
        topics = ["topic1", "topic2", "topic3"]

        for topic in topics:
            manager.subscribe(topic, MessageSelector(ExpressionType.TAG, "TAG1"))

        # 获取主题列表
        result_topics = manager.get_topics()
        assert len(result_topics) == 3
        for topic in topics:
            assert topic in result_topics

    def test_validate_subscription(self):
        """测试订阅验证"""
        manager = SubscriptionManager()

        # 有效订阅验证
        valid = manager.validate_subscription(
            "valid-topic", MessageSelector(ExpressionType.TAG, "TAG1")
        )
        assert valid is True

        # 无效主题验证
        invalid = manager.validate_subscription(
            "", MessageSelector(ExpressionType.TAG, "TAG1")
        )
        assert invalid is False

        # 无效选择器验证
        invalid = manager.validate_subscription("topic", None)
        assert invalid is False

    def test_clear_all(self):
        """测试清除所有订阅"""
        manager = SubscriptionManager()

        # 添加一些订阅
        for i in range(5):
            manager.subscribe(
                f"topic{i}", MessageSelector(ExpressionType.TAG, f"TAG{i}")
            )

        assert manager.get_subscription_count() == 5

        # 清除所有
        manager.clear_all()
        assert manager.get_subscription_count() == 0
        assert len(manager.get_all_subscriptions()) == 0

    def test_activate_deactivate_subscription(self):
        """测试激活和停用订阅"""
        manager = SubscriptionManager()
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 订阅
        manager.subscribe(topic, selector)

        # 默认应该是激活的
        assert manager.get_subscription(topic).is_active is True

        # 停用
        result = manager.deactivate_subscription(topic)
        assert result is True
        assert manager.get_subscription(topic).is_active is False

        # 激活
        result = manager.activate_subscription(topic)
        assert result is True
        assert manager.get_subscription(topic).is_active is True

    def test_update_selector_raises_topic_not_subscribed(self):
        """测试更新未订阅主题的选择器应该抛出异常"""
        manager = SubscriptionManager()
        topic = "non-existent-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 应该抛出TopicNotSubscribedError
        with pytest.raises(TopicNotSubscribedError):
            manager.update_selector(topic, selector)

    def test_subscribe_max_limit(self):
        """测试最大订阅数限制"""
        manager = SubscriptionManager(max_subscriptions=2)

        # 订阅两个主题
        manager.subscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        manager.subscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

        # 第三个订阅应该失败
        with pytest.raises(SubscriptionLimitExceededError):
            manager.subscribe("topic3", MessageSelector(ExpressionType.TAG, "TAG3"))

    def test_import_export_subscriptions(self):
        """测试订阅导出导入"""
        manager = SubscriptionManager()

        # 添加订阅
        manager.subscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        manager.subscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

        # 导出
        export_data = manager.export_subscriptions()
        assert len(export_data) == 3  # 包含subscriptions, conflict_history, export_time
        assert len(export_data["subscriptions"]) == 2

        # 清空并重新导入
        manager.clear_all()
        assert manager.get_subscription_count() == 0

        # 导入
        imported = manager.import_subscriptions(export_data)
        assert imported == 2
        assert manager.get_subscription_count() == 2

        # 验证导入的数据
        assert manager.is_subscribed("topic1") is True
        assert manager.is_subscribed("topic2") is True

    def test_status_summary(self):
        """测试获取状态摘要"""
        manager = SubscriptionManager()

        # 添加订阅
        manager.subscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        manager.subscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))
        manager.deactivate_subscription("topic2")

        # 获取状态摘要
        summary = manager.get_status_summary()

        assert "total_subscriptions" in summary
        assert "active_subscriptions" in summary
        assert "inactive_subscriptions" in summary
        assert "max_subscriptions" in summary

        assert summary["total_subscriptions"] == 2
        assert summary["active_subscriptions"] == 1
        assert summary["inactive_subscriptions"] == 1


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
