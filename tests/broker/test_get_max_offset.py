#!/usr/bin/env python3
"""
测试BrokerClient的get_max_offset方法
这个脚本演示如何使用BrokerClient获取队列的最大偏移量
"""

import json
import os
import sys

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pyrocketmq.broker import create_broker_client
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
    OffsetError,
)
from pyrocketmq.logging import LoggerFactory, LoggingConfig


def load_test_config():
    """从配置文件加载测试配置"""
    config_path = os.path.join(os.path.dirname(__file__), "test_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"配置文件未找到: {config_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"配置文件格式错误: {e}")
        return None


def test_get_max_offset():
    """测试get_max_offset方法"""
    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))

    # 加载配置
    config = load_test_config()
    if not config:
        print("无法加载配置文件，使用默认配置")
        config = {
            "host": "localhost",
            "port": 9876,
            "topic": "test_topic",
            "timeout": 10.0,
        }

    # 创建Broker客户端
    client = create_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 10.0),
    )

    try:
        # 连接到Broker
        print("连接到Broker...")
        client.connect()
        print(f"连接成功，客户端ID: {client.client_id}")

        # 测试参数
        topic = config.get("topic", "test_topic")

        print("\n🔗 连接信息:")
        print(f"  Broker地址: {config['host']}:{config['port']}")
        print(f"  超时时间: {config.get('timeout', 10.0)}秒")

        print("\n📊 获取最大偏移量测试:")
        print(f"  主题: {topic}")

        # 测试多个队列的最大偏移量查询
        queue_ids = [0, 1, 2, 3]

        for queue_id in queue_ids:
            try:
                print(f"\n获取队列 {queue_id} 的最大偏移量...")

                # 调用get_max_offset方法
                max_offset = client.get_max_offset(
                    topic=topic,
                    queue_id=queue_id,
                )

                # 输出结果
                print(f"✅ 队列 {queue_id} 最大偏移量获取成功:")
                print(f"  主题: {topic}")
                print(f"  队列ID: {queue_id}")
                print(f"  最大偏移量: {max_offset}")

            except OffsetError as e:
                print(
                    f"❌ 队列 {queue_id} 最大偏移量获取失败 (OffsetError): {e}"
                )

            except BrokerResponseError as e:
                print(
                    f"❌ 队列 {queue_id} 最大偏移量获取失败 (BrokerResponseError): {e}"
                )
                print(f"  响应代码: {e.response_code}")

            except BrokerTimeoutError as e:
                print(f"❌ 队列 {queue_id} 最大偏移量获取超时: {e}")

            except Exception as e:
                print(f"❌ 队列 {queue_id} 最大偏移量获取出现未知错误: {e}")

        # 测试无效参数的情况
        print("\n🧪 测试边界情况:")

        # 测试不存在的主题
        print("\n测试不存在的主题...")
        try:
            max_offset = client.get_max_offset(
                topic="non_existent_topic",
                queue_id=0,
            )
            print(f"意外成功 - 不存在主题的最大偏移量: {max_offset}")
        except BrokerResponseError as e:
            print(f"✅ 预期的错误 - 主题不存在: {e}")
        except Exception as e:
            print(f"❌ 未预期的错误: {e}")

        # 测试负数队列ID
        print("\n测试负数队列ID...")
        try:
            max_offset = client.get_max_offset(
                topic=topic,
                queue_id=-1,
            )
            print(f"意外成功 - 负数队列ID的最大偏移量: {max_offset}")
        except Exception as e:
            print(f"✅ 预期的错误 - 负数队列ID: {e}")

        # 测试不存在的队列ID
        print("\n测试不存在的队列ID...")
        try:
            max_offset = client.get_max_offset(
                topic=topic,
                queue_id=9999,
            )
            print(f"意外成功 - 不存在队列ID的最大偏移量: {max_offset}")
        except Exception as e:
            print(f"✅ 预期的错误 - 不存在的队列ID: {e}")

    except BrokerConnectionError as e:
        print(f"连接错误: {e}")
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 断开连接
        print("\n断开连接...")
        client.disconnect()
        print("断开连接完成")


def test_compare_with_consumer_offset():
    """比较最大偏移量与消费者偏移量的示例"""
    print("\n" + "=" * 50)
    print("比较最大偏移量与消费者偏移量示例")
    print("=" * 50)

    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 加载配置
    config = load_test_config()
    if not config:
        config = {
            "host": "localhost",
            "port": 9876,
            "topic": "test_topic",
            "consumer_group": "test_consumer_group",
            "timeout": 10.0,
        }

    # 创建客户端
    client = create_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 10.0),
    )

    try:
        client.connect()

        topic = config.get("topic", "test_topic")
        consumer_group = config.get("consumer_group", "test_consumer_group")
        queue_ids = [0, 1, 2, 3]

        print(f"比较主题 '{topic}' 中最大偏移量与消费者偏移量:")
        print(f"消费者组: {consumer_group}")

        for queue_id in queue_ids:
            print(f"\n队列 {queue_id}:")

            # 获取最大偏移量
            try:
                max_offset = client.get_max_offset(
                    topic=topic, queue_id=queue_id
                )
                print(f"  最大偏移量: {max_offset}")
            except Exception as e:
                print(f"  最大偏移量获取失败: {e}")
                max_offset = None

            # 获取消费者偏移量
            try:
                consumer_offset = client.query_consumer_offset(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id,
                )
                print(f"  消费者偏移量: {consumer_offset}")
            except Exception as e:
                print(f"  消费者偏移量获取失败: {e}")
                consumer_offset = None

            # 计算滞后
            if max_offset is not None and consumer_offset is not None:
                lag = max_offset - consumer_offset
                if lag > 0:
                    print(f"  消费滞后: {lag} 条消息")
                elif lag == 0:
                    print("  消费状态: 已跟上最新")
                else:
                    print("  消费状态: 异常 (消费者偏移量 > 最大偏移量)")

    except Exception as e:
        print(f"比较测试失败: {e}")
    finally:
        client.disconnect()


def test_batch_get_max_offsets():
    """批量获取多个队列最大偏移量的示例"""
    print("\n" + "=" * 50)
    print("批量获取最大偏移量示例")
    print("=" * 50)

    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 加载配置
    config = load_test_config()
    if not config:
        config = {
            "host": "localhost",
            "port": 9876,
            "topic": "test_topic",
            "timeout": 10.0,
        }

    # 创建客户端
    client = create_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 10.0),
    )

    try:
        client.connect()

        topic = config.get("topic", "test_topic")
        queue_ids = [0, 1, 2, 3, 4, 5]

        print(f"批量获取主题 '{topic}' 中多个队列的最大偏移量:")

        max_offsets = {}
        for queue_id in queue_ids:
            try:
                max_offset = client.get_max_offset(
                    topic=topic, queue_id=queue_id
                )
                max_offsets[queue_id] = max_offset
                print(f"  队列 {queue_id}: {max_offset}")
            except Exception as e:
                print(f"  队列 {queue_id}: 获取失败 - {e}")
                max_offsets[queue_id] = None

        print("\n结果汇总:")
        successful_queries = sum(
            1 for v in max_offsets.values() if v is not None
        )
        print(f"  成功获取: {successful_queries}/{len(queue_ids)} 个队列")

        # 计算总消息数
        total_messages = sum(v for v in max_offsets.values() if v is not None)
        print(f"  总消息数（估计）: {total_messages}")

        # 找出消息最多的队列
        if max_offsets:
            max_queue = max(
                max_offsets.items(),
                key=lambda x: x[1] if x[1] is not None else -1,
            )
            print(f"  消息最多的队列: {max_queue[0]} (消息数: {max_queue[1]})")

    except Exception as e:
        print(f"批量获取测试失败: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    test_get_max_offset()
    # test_compare_with_consumer_offset()
    # test_batch_get_max_offsets()
