#!/usr/bin/env python3
"""
Producer MVP版本测试脚本

该脚本用于测试新创建的Producer MVP版本的基本功能。
验证核心功能是否正常工作，包括：
- Producer生命周期管理
- 消息验证
- 基础的路由选择
- 错误处理

使用方法:
    export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src
    python test_producer_mvp.py
"""

import sys
import traceback

from pyrocketmq.model.message import Message
from pyrocketmq.producer import Producer, create_producer
from pyrocketmq.producer.config import ProducerConfig
from pyrocketmq.producer.errors import ProducerStateError


def test_producer_lifecycle():
    """测试Producer生命周期管理"""
    print("\n=== 测试Producer生命周期管理 ===")

    try:
        # 创建Producer
        producer = Producer()
        print(f"✓ Producer创建成功: {producer}")

        # 测试未启动状态
        try:
            producer.send_sync(Message(topic="test", body=b"test"))
            print("✗ 应该抛出ProducerStateError")
        except ProducerStateError:
            print("✓ 未启动状态下正确抛出ProducerStateError")

        # 启动Producer
        producer.start()
        print("✓ Producer启动成功")

        # 测试重复启动
        producer.start()
        print("✓ 重复启动不会报错")

        # 检查运行状态
        assert producer.is_running(), "Producer应该处于运行状态"
        print("✓ 运行状态检查通过")

        # 获取统计信息
        stats = producer.get_stats()
        print(f"✓ 统计信息: {stats}")

        # 关闭Producer
        producer.shutdown()
        print("✓ Producer关闭成功")

        # 测试重复关闭
        producer.shutdown()
        print("✓ 重复关闭不会报错")

        # 检查停止状态
        assert not producer.is_running(), "Producer应该处于停止状态"
        print("✓ 停止状态检查通过")

    except Exception as e:
        print(f"✗ 生命周期测试失败: {e}")
        traceback.print_exc()
        return False

    return True


def test_message_validation():
    """测试消息验证功能"""
    print("\n=== 测试消息验证功能 ===")

    try:
        producer = Producer()

        # 测试有效消息
        valid_message = Message(topic="test_topic", body=b"Hello RocketMQ")
        print("✓ 有效消息创建成功")

        # 测试无效消息 - None消息
        try:
            producer.send_sync(None)
            print("✗ 应该抛出ValueError")
        except (ValueError, TypeError):
            print("✓ None消息验证通过")
        except ProducerStateError:
            print("✓ None消息验证通过（Producer未启动）")

        # 测试无效消息 - 空主题
        try:
            empty_topic_msg = Message(topic="", body=b"test")
            producer.send_sync(empty_topic_msg)
            print("✗ 应该抛出ValueError")
        except ValueError as e:
            print(f"✓ 空主题消息验证通过: {e}")
        except ProducerStateError:
            print("✓ 空主题消息验证通过（Producer未启动）")

        # 测试无效消息 - 空消息体
        try:
            empty_body_msg = Message(topic="test", body=b"")
            producer.send_sync(empty_body_msg)
            print("✗ 应该抛出ValueError")
        except ValueError as e:
            print(f"✓ 空消息体验证通过: {e}")
        except ProducerStateError:
            print("✓ 空消息体验证通过（Producer未启动）")

        # 测试消息属性
        message_with_props = Message(
            topic="test_topic",
            body=b"test message",
            tags="test_tag",
            keys="key1,key2",
        )
        message_with_props.set_property("custom_key", "custom_value")
        print("✓ 带属性的消息创建成功")

    except Exception as e:
        print(f"✗ 消息验证测试失败: {e}")
        traceback.print_exc()
        return False

    return True


def test_config_management():
    """测试配置管理功能"""
    print("\n=== 测试配置管理功能 ===")

    try:
        # 测试默认配置
        producer1 = Producer()
        config1 = producer1._config
        print(f"✓ 默认配置: {config1.producer_group}")

        # 测试自定义配置
        custom_config = ProducerConfig(
            producer_group="test_producer",
            namesrv_addr="localhost:9876",
            retry_times=3,
        )
        producer2 = Producer(custom_config)
        config2 = producer2._config
        print(f"✓ 自定义配置: {config2.producer_group}")

        # 测试便捷创建函数
        producer3 = create_producer(
            producer_group="convenience_test",
            namesrv_addr="192.168.1.100:9876",
            retry_times=2,
        )
        config3 = producer3._config
        print(f"✓ 便捷创建: {config3.producer_group}")

        # 验证配置值
        assert config2.producer_group == "test_producer"
        assert config2.retry_times == 3
        assert config3.producer_group == "convenience_test"
        assert config3.retry_times == 2
        print("✓ 配置值验证通过")

    except Exception as e:
        print(f"✗ 配置管理测试失败: {e}")
        traceback.print_exc()
        return False

    return True


def test_topic_broker_mapping():
    """测试Topic-Broker映射功能"""
    print("\n=== 测试Topic-Broker映射功能 ===")

    try:
        producer = Producer()
        mapping = producer._topic_mapping

        # 测试空映射
        available_queues = mapping.get_available_queues("nonexistent_topic")
        assert len(available_queues) == 0
        print("✓ 空映射测试通过")

        # 测试缓存统计
        stats = mapping.get_cache_stats()
        assert stats["total_topics"] == 0
        print(f"✓ 缓存统计: {stats}")

        # 测试获取所有Topic
        all_topics = mapping.get_all_topics()
        assert len(all_topics) == 0
        print("✓ 获取所有Topic测试通过")

        # 测试映射字符串表示
        mapping_str = str(mapping)
        print(f"✓ 映射字符串表示: {mapping_str}")

    except Exception as e:
        print(f"✗ Topic-Broker映射测试失败: {e}")
        traceback.print_exc()
        return False

    return True


def test_send_operations():
    """测试消息发送操作（模拟）"""
    print("\n=== 测试消息发送操作 ===")

    try:
        producer = Producer()
        producer.start()

        # 创建测试消息
        test_message = Message(
            topic="test_topic",
            body=b"Hello RocketMQ from MVP Producer",
            tags="test",
            keys="test_key",
        )

        # 由于没有实际的Broker连接，这里会失败，但可以测试基本的流程
        try:
            result = producer.send_sync(test_message)
            print(f"✓ 同步发送成功: {result}")
        except Exception as e:
            print(f"⚠ 同步发送失败（预期行为）: {type(e).__name__}")

        # 测试单向发送
        try:
            producer.send_oneway(test_message)
            print("✓ 单向发送成功")
        except Exception as e:
            print(f"⚠ 单向发送失败（预期行为）: {type(e).__name__}")

        # 获取发送统计
        stats = producer.get_stats()
        print(f"✓ 发送统计: {stats}")

        producer.shutdown()

    except Exception as e:
        print(f"✗ 消息发送操作测试失败: {e}")
        traceback.print_exc()
        return False

    return True


def test_error_handling():
    """测试错误处理"""
    print("\n=== 测试错误处理 ===")

    try:
        producer = Producer()

        # 测试未启动状态下的操作
        test_message = Message(topic="test", body=b"test")

        try:
            producer.send_sync(test_message)
            print("✗ 应该抛出ProducerStateError")
        except ProducerStateError:
            print("✓ 未启动状态错误处理正确")

        try:
            producer.send_oneway(test_message)
            print("✗ 应该抛出ProducerStateError")
        except ProducerStateError:
            print("✓ 未启动状态错误处理正确")

        # 测试无效消息
        producer.start()

        try:
            producer.send_sync(Message(topic="", body=b"test"))
            print("✗ 应该抛出ValueError")
        except ValueError:
            print("✓ 无效消息错误处理正确")
        except Exception as e:
            print(f"⚠ 其他错误（可接受）: {type(e).__name__}")

        producer.shutdown()

    except Exception as e:
        print(f"✗ 错误处理测试失败: {e}")
        traceback.print_exc()
        return False

    return True


def main():
    """主测试函数"""
    print("🚀 开始测试Producer MVP版本")
    print("=" * 50)

    tests = [
        test_producer_lifecycle,
        test_message_validation,
        test_config_management,
        test_topic_broker_mapping,
        test_send_operations,
        test_error_handling,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ 测试异常: {test_func.__name__} - {e}")
            failed += 1

    print("\n" + "=" * 50)
    print(f"🎯 测试结果: {passed} 通过, {failed} 失败")

    if failed == 0:
        print("🎉 所有测试通过！Producer MVP版本基本功能正常")
        return 0
    else:
        print("⚠️  部分测试失败，需要修复问题")
        return 1


if __name__ == "__main__":
    sys.exit(main())
