#!/usr/bin/env python3
"""
AsyncSubscriptionManager单元测试

测试异步订阅管理器的主要API功能，使用pytest框架和pytest-asyncio插件
"""

import asyncio
import os
import sys
from datetime import datetime

import pytest

# 设置PYTHONPATH
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, os.path.join(project_root, "src"))

from pyrocketmq.consumer.subscription_exceptions import (
    InvalidSelectorError,
    InvalidTopicError,
    SubscriptionConflictError,
    SubscriptionDataError,
    SubscriptionLimitExceededError,
    TopicNotSubscribedError,
)
from pyrocketmq.consumer.subscription_manager import AsyncSubscriptionManager
from pyrocketmq.model import ExpressionType, MessageSelector


class TestAsyncSubscriptionManager:
    """AsyncSubscriptionManager测试类"""

    @pytest.fixture
    async def manager(self):
        """创建一个AsyncSubscriptionManager实例"""
        return AsyncSubscriptionManager()

    @pytest.fixture
    async def manager_with_limit(self):
        """创建一个有订阅数量限制的AsyncSubscriptionManager实例"""
        return AsyncSubscriptionManager(max_subscriptions=2)

    # ==================== 基础初始化测试 ====================

    @pytest.mark.asyncio
    async def test_init_default(self):
        """测试默认初始化"""
        manager = AsyncSubscriptionManager()
        assert manager._max_subscriptions == 1000
        assert manager._subscriptions == {}
        assert manager._conflict_history == []
        assert isinstance(manager._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_init_custom_max(self):
        """测试自定义最大订阅数"""
        manager = AsyncSubscriptionManager(max_subscriptions=5)
        assert manager._max_subscriptions == 5

    # ==================== 订阅/取消订阅测试 ====================

    @pytest.mark.asyncio
    async def test_asubscribe_basic(self, manager):
        """测试基本异步订阅功能"""
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1 || TAG2")

        # 订阅成功
        result = await manager.asubscribe(topic, selector)
        assert result is True

        # 验证订阅存在
        is_subscribed = await manager.ais_subscribed(topic)
        assert is_subscribed is True

        count = await manager.aget_subscription_count()
        assert count == 1

        # 验证订阅内容
        subscription = await manager.aget_subscription(topic)
        assert subscription is not None
        assert subscription.topic == topic
        assert subscription.selector.expression == "TAG1 || TAG2"

    @pytest.mark.asyncio
    async def test_asubscribe_invalid_topic(self, manager):
        """测试订阅无效主题"""
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 空主题应该抛出异常
        with pytest.raises(InvalidTopicError):
            await manager.asubscribe("", selector)

        with pytest.raises(InvalidTopicError):
            await manager.asubscribe(None, selector)

    @pytest.mark.asyncio
    async def test_asubscribe_invalid_selector(self, manager):
        """测试订阅无效选择器"""
        topic = "test-topic"

        # None选择器应该抛出异常
        with pytest.raises(InvalidSelectorError):
            await manager.asubscribe(topic, None)

    @pytest.mark.asyncio
    async def test_asubscribe_max_limit(self, manager_with_limit):
        """测试最大订阅数限制"""
        manager = manager_with_limit

        # 订阅两个主题
        await manager.asubscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        await manager.asubscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

        # 第三个订阅应该失败
        with pytest.raises(SubscriptionLimitExceededError):
            await manager.asubscribe(
                "topic3", MessageSelector(ExpressionType.TAG, "TAG3")
            )

    @pytest.mark.asyncio
    async def test_aunsubscribe_basic(self, manager):
        """测试基本异步取消订阅功能"""
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 先订阅
        await manager.asubscribe(topic, selector)
        is_subscribed = await manager.ais_subscribed(topic)
        assert is_subscribed is True

        # 取消订阅
        result = await manager.aunsubscribe(topic)
        assert result is True

        is_subscribed = await manager.ais_subscribed(topic)
        assert is_subscribed is False

        count = await manager.aget_subscription_count()
        assert count == 0

    @pytest.mark.asyncio
    async def test_aunsubscribe_not_subscribed(self, manager):
        """测试取消未订阅的主题"""
        topic = "non-existent-topic"

        # 取消未订阅的主题应该返回False
        result = await manager.aunsubscribe(topic)
        assert result is False

    # ==================== 选择器更新测试 ====================

    @pytest.mark.asyncio
    async def test_aupdate_selector_basic(self, manager):
        """测试基本异步更新选择器功能"""
        topic = "test-topic"
        old_selector = MessageSelector(ExpressionType.TAG, "TAG1")
        new_selector = MessageSelector(ExpressionType.SQL92, "amount > 100")

        # 先订阅
        await manager.asubscribe(topic, old_selector)

        # 更新选择器到不同的类型应该会触发冲突，这是预期的行为
        # 所以我们测试用相同的选择器（无变化更新）
        result = await manager.aupdate_selector(topic, old_selector)
        assert result is True

        # 验证选择器没有变化
        subscription = await manager.aget_subscription(topic)
        assert subscription is not None
        assert subscription.selector.expression == "TAG1"

        # 现在测试类型不同的更新应该失败
        with pytest.raises(SubscriptionConflictError):
            await manager.aupdate_selector(topic, new_selector)

    @pytest.mark.asyncio
    async def test_aupdate_selector_topic_not_subscribed(self, manager):
        """测试更新未订阅主题的选择器应该抛出异常"""
        topic = "non-existent-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        with pytest.raises(TopicNotSubscribedError):
            await manager.aupdate_selector(topic, selector)

    # ==================== 查询功能测试 ====================

    @pytest.mark.asyncio
    async def test_aget_subscription_basic(self, manager):
        """测试异步获取订阅信息"""
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 订阅前获取
        subscription = await manager.aget_subscription(topic)
        assert subscription is None

        # 订阅后获取
        await manager.asubscribe(topic, selector)
        subscription = await manager.aget_subscription(topic)
        assert subscription is not None
        assert subscription.topic == topic

    @pytest.mark.asyncio
    async def test_aget_all_subscriptions(self, manager):
        """测试异步获取所有订阅"""
        topics = ["topic1", "topic2", "topic3"]

        for i, topic in enumerate(topics):
            selector = MessageSelector(ExpressionType.TAG, f"TAG{i + 1}")
            await manager.asubscribe(topic, selector)

        # 获取所有订阅
        subscriptions = await manager.aget_all_subscriptions()
        assert len(subscriptions) == 3

        # 验证主题
        topic_names = [sub.topic for sub in subscriptions]
        for topic in topics:
            assert topic in topic_names

    @pytest.mark.asyncio
    async def test_aget_active_subscriptions(self, manager):
        """测试异步获取活跃订阅"""
        topic1 = "active-topic"
        topic2 = "inactive-topic"

        # 订阅两个主题
        await manager.asubscribe(topic1, MessageSelector(ExpressionType.TAG, "TAG1"))
        await manager.asubscribe(topic2, MessageSelector(ExpressionType.TAG, "TAG2"))

        # 停用一个
        await manager.adeactivate_subscription(topic2)

        # 获取活跃订阅
        active_subs = await manager.aget_active_subscriptions()
        assert len(active_subs) == 1
        assert active_subs[0].topic == topic1

    @pytest.mark.asyncio
    async def test_aget_topics(self, manager):
        """测试异步获取所有主题"""
        topics = ["topic1", "topic2", "topic3"]

        for topic in topics:
            await manager.asubscribe(topic, MessageSelector(ExpressionType.TAG, "TAG1"))

        # 获取主题列表
        result_topics = await manager.aget_topics()
        assert len(result_topics) == 3
        for topic in topics:
            assert topic in result_topics

    @pytest.mark.asyncio
    async def test_aget_subscription_count(self, manager):
        """测试异步获取订阅数量"""
        # 初始数量应该为0
        count = await manager.aget_subscription_count()
        assert count == 0

        # 添加订阅
        await manager.asubscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        count = await manager.aget_subscription_count()
        assert count == 1

        await manager.asubscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))
        count = await manager.aget_subscription_count()
        assert count == 2

        # 取消订阅
        await manager.aunsubscribe("topic1")
        count = await manager.aget_subscription_count()
        assert count == 1

    # ==================== 激活/停用功能测试 ====================

    @pytest.mark.asyncio
    async def test_aactivate_adeactivate_subscription(self, manager):
        """测试异步激活和停用订阅"""
        topic = "test-topic"
        selector = MessageSelector(ExpressionType.TAG, "TAG1")

        # 订阅
        await manager.asubscribe(topic, selector)

        # 默认应该是激活的
        subscription = await manager.aget_subscription(topic)
        assert subscription.is_active is True

        # 停用
        result = await manager.adeactivate_subscription(topic)
        assert result is True

        subscription = await manager.aget_subscription(topic)
        assert subscription.is_active is False

        # 激活
        result = await manager.aactivate_subscription(topic)
        assert result is True

        subscription = await manager.aget_subscription(topic)
        assert subscription.is_active is True

    @pytest.mark.asyncio
    async def test_aclear_all(self, manager):
        """测试异步清除所有订阅"""
        # 添加一些订阅
        for i in range(5):
            await manager.asubscribe(
                f"topic{i}", MessageSelector(ExpressionType.TAG, f"TAG{i}")
            )

        count = await manager.aget_subscription_count()
        assert count == 5

        # 清除所有
        await manager.aclear_all()
        count = await manager.aget_subscription_count()
        assert count == 0

        subscriptions = await manager.aget_all_subscriptions()
        assert len(subscriptions) == 0

    # ==================== 验证功能测试 ====================

    @pytest.mark.asyncio
    async def test_avalidate_subscription(self, manager):
        """测试异步订阅验证"""
        # 有效订阅验证
        valid = await manager.avalidate_subscription(
            "valid-topic", MessageSelector(ExpressionType.TAG, "TAG1")
        )
        assert valid is True

        # 无效主题验证
        invalid = await manager.avalidate_subscription(
            "", MessageSelector(ExpressionType.TAG, "TAG1")
        )
        assert invalid is False

        # 无效选择器验证
        invalid = await manager.avalidate_subscription("topic", None)
        assert invalid is False

    # ==================== 导入导出功能测试 ====================

    @pytest.mark.asyncio
    async def test_aimport_aexport_subscriptions(
        self, manager: AsyncSubscriptionManager
    ):
        """测试异步订阅导出导入"""
        # 添加订阅
        await manager.asubscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        await manager.asubscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

        # 导出
        export_data = await manager.aexport_subscriptions()
        assert len(export_data) == 3  # 包含subscriptions, conflict_history, export_time
        assert len(export_data["subscriptions"]) == 2

        # 清空并重新导入
        await manager.aclear_all()
        count = await manager.aget_subscription_count()
        assert count == 0

        # 导入 (方法返回None，不检查返回值)
        await manager.aimport_subscriptions(export_data)

        count = await manager.aget_subscription_count()
        assert count == 2

        # 验证导入的数据
        is_subscribed1 = await manager.ais_subscribed("topic1")
        is_subscribed2 = await manager.ais_subscribed("topic2")
        assert is_subscribed1 is True
        assert is_subscribed2 is True

    # ==================== 冲突历史测试 ====================

    @pytest.mark.asyncio
    async def test_aget_conflict_history(self, manager):
        """测试异步获取冲突历史"""
        # 初始应该为空
        history = await manager.aget_conflict_history()
        assert len(history) == 0

    @pytest.mark.asyncio
    async def test_aclear_conflict_history(self, manager):
        """测试异步清除冲突历史"""
        # 清除冲突历史
        await manager.aclear_conflict_history()
        history = await manager.aget_conflict_history()
        assert len(history) == 0

    # ==================== 状态摘要测试 ====================

    @pytest.mark.asyncio
    async def test_aget_status_summary(self, manager):
        """测试异步获取状态摘要"""
        # 添加订阅
        await manager.asubscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        await manager.asubscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))
        await manager.adeactivate_subscription("topic2")

        # 获取状态摘要
        summary = await manager.aget_status_summary()

        assert "subscription_count" in summary
        assert "active_count" in summary
        assert "inactive_count" in summary
        assert "max_subscriptions" in summary

        assert summary["subscription_count"] == 2
        assert summary["active_count"] == 1
        assert summary["inactive_count"] == 1

    # ==================== 清理功能测试 ====================

    @pytest.mark.asyncio
    async def test_acleanup_inactive_subscriptions(self, manager):
        """测试异步清理非活跃订阅"""
        from datetime import timedelta

        # 添加订阅
        await manager.asubscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        await manager.asubscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))
        await manager.adeactivate_subscription("topic2")

        count_before = await manager.aget_subscription_count()
        assert count_before == 2

        # 清理非活跃订阅（使用零阈值确保立即清理）
        cleaned_count = await manager.acleanup_inactive_subscriptions(
            inactive_threshold=timedelta(0)
        )

        count_after = await manager.aget_subscription_count()
        assert count_after == 1
        assert cleaned_count == 1

        # 验证只剩活跃订阅
        is_subscribed = await manager.ais_subscribed("topic1")
        assert is_subscribed is True
        is_subscribed = await manager.ais_subscribed("topic2")
        assert is_subscribed is False

    # ==================== 转换功能测试 ====================

    @pytest.mark.asyncio
    async def test_ato_subscription_data_list(self, manager):
        """测试异步转换为订阅数据列表"""
        # 添加订阅
        await manager.asubscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
        await manager.asubscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

        # 转换为订阅数据列表
        data_list = await manager.ato_subscription_data_list()
        assert len(data_list) == 2

        topics = [data.topic for data in data_list]
        assert "topic1" in topics
        assert "topic2" in topics

    # ==================== 并发测试 ====================

    @pytest.mark.asyncio
    async def test_concurrent_asubscribe(self, manager):
        """测试并发订阅"""
        topics = [f"topic{i}" for i in range(10)]
        selectors = [MessageSelector(ExpressionType.TAG, f"TAG{i}") for i in range(10)]

        # 并发订阅
        tasks = [
            manager.asubscribe(topic, selector)
            for topic, selector in zip(topics, selectors)
        ]
        results = await asyncio.gather(*tasks)

        # 所有订阅都应该成功
        assert all(results)

        # 验证所有订阅都存在
        count = await manager.aget_subscription_count()
        assert count == 10

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, manager):
        """测试并发操作"""
        # 先添加一些订阅
        for i in range(5):
            await manager.asubscribe(
                f"topic{i}", MessageSelector(ExpressionType.TAG, f"TAG{i}")
            )

        # 并发执行各种操作
        tasks = [
            manager.aget_subscription_count(),
            manager.aget_all_subscriptions(),
            manager.aget_active_subscriptions(),
            manager.aget_topics(),
        ]
        results = await asyncio.gather(*tasks)

        assert results[0] == 5  # subscription_count
        assert len(results[1]) == 5  # all_subscriptions
        assert len(results[2]) == 5  # active_subscriptions
        assert len(results[3]) == 5  # topics

    # ==================== 字符串表示测试 ====================

    @pytest.mark.asyncio
    async def test_str_representation(self, manager):
        """测试字符串表示"""
        # 添加订阅
        await manager.asubscribe(
            "test-topic", MessageSelector(ExpressionType.TAG, "TAG1")
        )

        str_repr = str(manager)
        assert "AsyncSubscriptionManager" in str_repr
        assert "subscriptions=1" in str_repr
        assert "active=1" in str_repr

    @pytest.mark.asyncio
    async def test_repr_representation(self, manager):
        """测试repr表示"""
        repr_str = repr(manager)
        assert "AsyncSubscriptionManager" in repr_str
        assert "max_subscriptions" in repr_str


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
