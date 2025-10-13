#!/usr/bin/env python3
"""
测试BrokerClient的query_consumer_offset方法
这个脚本演示如何使用BrokerClient查询消费者偏移量
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


def test_query_consumer_offset():
    """测试query_consumer_offset方法"""
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
        consumer_group = config.get("consumer_group", "test_consumer_group")
        topic = config.get("topic", "test_topic")

        print("\n🔗 连接信息:")
        print(f"  Broker地址: {config['host']}:{config['port']}")
        print(f"  超时时间: {config.get('timeout', 10.0)}秒")

        print("\n📊 查询消费者偏移量测试:")
        print(f"  消费者组: {consumer_group}")
        print(f"  主题: {topic}")

        # 测试多个队列的偏移量查询
        queue_ids = [0, 1, 2, 3]

        for queue_id in queue_ids:
            try:
                print(f"\n查询队列 {queue_id} 的偏移量...")

                # 调用query_consumer_offset方法
                offset = client.query_consumer_offset(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id,
                )

                # 输出结果
                print(f"✅ 队列 {queue_id} 偏移量查询成功:")
                print(f"  消费者组: {consumer_group}")
                print(f"  主题: {topic}")
                print(f"  队列ID: {queue_id}")
                print(f"  当前偏移量: {offset}")

            except OffsetError as e:
                print(f"❌ 队列 {queue_id} 偏移量查询失败 (OffsetError): {e}")
                print("  这可能表示消费者组尚未开始消费该队列")

            except BrokerResponseError as e:
                print(
                    f"❌ 队列 {queue_id} 偏移量查询失败 (BrokerResponseError): {e}"
                )
                print(f"  响应代码: {e.response_code}")

            except BrokerTimeoutError as e:
                print(f"❌ 队列 {queue_id} 偏移量查询超时: {e}")

            except Exception as e:
                print(f"❌ 队列 {queue_id} 偏移量查询出现未知错误: {e}")

        # 测试无效参数的情况
        print("\n🧪 测试边界情况:")

        # 测试不存在的主题
        print("\n测试不存在的主题...")
        try:
            offset = client.query_consumer_offset(
                consumer_group=consumer_group,
                topic="non_existent_topic",
                queue_id=0,
            )
            print(f"意外成功 - 不存在主题的偏移量: {offset}")
        except BrokerResponseError as e:
            print(f"✅ 预期的错误 - 主题不存在: {e}")
        except Exception as e:
            print(f"❌ 未预期的错误: {e}")

        # 测试不存在的消费者组
        print("\n测试不存在的消费者组...")
        try:
            offset = client.query_consumer_offset(
                consumer_group="non_existent_consumer_group",
                topic=topic,
                queue_id=0,
            )
            print(f"意外成功 - 不存在消费者组的偏移量: {offset}")
        except (BrokerResponseError, OffsetError) as e:
            print(f"✅ 预期的错误 - 消费者组不存在: {e}")
        except Exception as e:
            print(f"❌ 未预期的错误: {e}")

        # 测试负数队列ID
        print("\n测试负数队列ID...")
        try:
            offset = client.query_consumer_offset(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=-1,
            )
            print(f"意外成功 - 负数队列ID的偏移量: {offset}")
        except Exception as e:
            print(f"✅ 预期的错误 - 负数队列ID: {e}")

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


def test_batch_query_offsets():
    """批量查询多个队列偏移量的示例"""
    print("\n" + "=" * 50)
    print("批量查询偏移量示例")
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

    def query_multiple_queues(client, consumer_group, topic, queue_ids):
        """批量查询多个队列的偏移量"""
        offsets = {}
        for queue_id in queue_ids:
            try:
                offset = client.query_consumer_offset(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id,
                )
                offsets[queue_id] = offset
                print(f"  队列 {queue_id}: {offset}")
            except Exception as e:
                print(f"  队列 {queue_id}: 查询失败 - {e}")
                offsets[queue_id] = None
        return offsets

    # 创建客户端
    client = create_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 10.0),
    )

    try:
        client.connect()

        consumer_group = config.get("consumer_group", "test_consumer_group")
        topic = config.get("topic", "test_topic")
        queue_ids = [0, 1, 2, 3, 4, 5]

        print(
            f"批量查询消费者组 '{consumer_group}' 在主题 '{topic}' 中的偏移量:"
        )
        offsets = query_multiple_queues(
            client, consumer_group, topic, queue_ids
        )

        print("\n查询结果汇总:")
        successful_queries = sum(1 for v in offsets.values() if v is not None)
        print(f"  成功查询: {successful_queries}/{len(queue_ids)} 个队列")
        print(
            f"  总偏移量: {sum(v for v in offsets.values() if v is not None)}"
        )

    except Exception as e:
        print(f"批量查询测试失败: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    test_query_consumer_offset()
    test_batch_query_offsets()
