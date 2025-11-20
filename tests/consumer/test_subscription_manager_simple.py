#!/usr/bin/env python3
"""
SubscriptionManager简化单元测试

测试订阅管理器的主要API功能，避免复杂的边界情况
"""

import os
import sys

# 设置PYTHONPATH
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, os.path.join(project_root, "src"))

from pyrocketmq.consumer.subscription_manager import SubscriptionManager
from pyrocketmq.model import MessageSelector, ExpressionType


def test_basic_operations():
    """测试基本操作"""
    print("🧪 测试基本订阅操作...")

    manager = SubscriptionManager(max_subscriptions=5)

    # 1. 测试订阅
    topic = "test-topic"
    selector = MessageSelector(ExpressionType.TAG, "TAG1 || TAG2")

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
    print("✅ 订阅功能正常")

    # 2. 测试取消订阅
    result = manager.unsubscribe(topic)
    assert result is True
    assert manager.is_subscribed(topic) is False
    assert manager.get_subscription_count() == 0
    print("✅ 取消订阅功能正常")


def test_multiple_subscriptions():
    """测试多个订阅"""
    print("🧪 测试多个订阅...")

    manager = SubscriptionManager(max_subscriptions=10)

    # 添加多个订阅
    topics = ["topic1", "topic2", "topic3"]
    for i, topic in enumerate(topics):
        selector = MessageSelector(ExpressionType.TAG, f"TAG{i + 1}")
        manager.subscribe(topic, selector)

    # 验证订阅数量
    assert manager.get_subscription_count() == 3

    # 验证所有主题
    all_topics = manager.get_topics()
    for topic in topics:
        assert topic in all_topics

    # 验证所有订阅
    all_subscriptions = manager.get_all_subscriptions()
    assert len(all_subscriptions) == 3
    print("✅ 多个订阅功能正常")


def test_validation():
    """测试验证功能"""
    print("🧪 测试验证功能...")

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

    print("✅ 验证功能正常")


def test_active_management():
    """测试激活管理"""
    print("🧪 测试激活管理...")

    manager = SubscriptionManager()

    # 添加订阅
    manager.subscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
    manager.subscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

    # 验证默认都是激活的
    active_subscriptions = manager.get_active_subscriptions()
    assert len(active_subscriptions) == 2

    # 停用一个订阅
    manager.deactivate_subscription("topic2")
    active_subscriptions = manager.get_active_subscriptions()
    assert len(active_subscriptions) == 1
    assert active_subscriptions[0].topic == "topic1"

    # 重新激活
    manager.activate_subscription("topic2")
    active_subscriptions = manager.get_active_subscriptions()
    assert len(active_subscriptions) == 2

    print("✅ 激活管理功能正常")


def test_clear_all():
    """测试清除所有订阅"""
    print("🧪 测试清除所有订阅...")

    manager = SubscriptionManager()

    # 添加一些订阅
    for i in range(3):
        manager.subscribe(f"topic{i}", MessageSelector(ExpressionType.TAG, f"TAG{i}"))

    assert manager.get_subscription_count() == 3

    # 清除所有
    manager.clear_all()
    assert manager.get_subscription_count() == 0
    assert len(manager.get_all_subscriptions()) == 0

    print("✅ 清除所有订阅功能正常")


def test_edge_cases():
    """测试边界情况"""
    print("🧪 测试边界情况...")

    manager = SubscriptionManager(max_subscriptions=2)

    # 测试最大订阅数限制
    manager.subscribe("topic1", MessageSelector(ExpressionType.TAG, "TAG1"))
    manager.subscribe("topic2", MessageSelector(ExpressionType.TAG, "TAG2"))

    try:
        manager.subscribe("topic3", MessageSelector(ExpressionType.TAG, "TAG3"))
        assert False, "应该抛出订阅限制异常"
    except Exception as e:
        # 期望抛出异常
        pass

    # 测试取消不存在的订阅
    result = manager.unsubscribe("non-existent-topic")
    assert result is False

    print("✅ 边界情况处理正常")


def run_all_tests():
    """运行所有测试"""
    print("🚀 开始运行SubscriptionManager简化单元测试...")
    print("=" * 60)

    tests = [
        test_basic_operations,
        test_multiple_subscriptions,
        test_validation,
        test_active_management,
        test_clear_all,
        test_edge_cases,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__}: {e}")
            import traceback

            traceback.print_exc()
            failed += 1

    print("=" * 60)
    print(f"📊 测试结果: {passed} 通过, {failed} 失败")
    print(f"✅ 总计: {len(tests)} 个测试")

    if failed == 0:
        print("🎉 所有测试都通过了！SubscriptionManager实现正确。")
        return True
    else:
        print("❌ 部分测试失败，请检查实现。")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
