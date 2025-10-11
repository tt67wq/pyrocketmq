#!/usr/bin/env python3
"""
测试BrokerClient的pull_message方法
这个脚本演示如何使用BrokerClient拉取消息
"""

import json
import os
import sys

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pyrocketmq.broker import create_broker_client
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


def test_pull_message():
    """测试pull_message方法"""
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
        queue_id = 0
        queue_offset = 0
        max_msg_nums = 10

        print("\n🔗 连接信息:")
        print(f"  Broker地址: {config['host']}:{config['port']}")
        print(f"  超时时间: {config.get('timeout', 10.0)}秒")

        print("\n📨 拉取消息测试:")
        print(f"  消费者组: {consumer_group}")
        print(f"  主题: {topic}")
        print(f"  队列ID: {queue_id}")
        print(f"  起始偏移量: {queue_offset}")
        print(f"  最大消息数: {max_msg_nums}")

        # 调用pull_message方法
        result = client.pull_message(
            consumer_group=consumer_group,
            topic=topic,
            queue_id=queue_id,
            queue_offset=queue_offset,
            max_msg_nums=max_msg_nums,
        )

        # 输出结果
        print("\n拉取结果:")
        print(f"  消息数量: {result.message_count}")
        print(f"  下次起始偏移量: {result.next_begin_offset}")
        print(f"  最小偏移量: {result.min_offset}")
        print(f"  最大偏移量: {result.max_offset}")
        print(
            f"  拉取耗时: {result.pull_rt:.3f}s"
            if result.pull_rt
            else "  拉取耗时: N/A"
        )

        if result.is_found:
            print("\n消息内容:")
            for i, msg in enumerate(result.messages):
                print(f"  消息 {i + 1}:")
                print(f"    ID: {msg.message_id}")
                print(f"    主题: {msg.topic}")
                print(f"    队列ID: {msg.queue_id}")
                print(f"    偏移量: {msg.queue_offset}")
                print(f"    标签: {msg.tags}")
                print(f"    键: {msg.keys}")
                print(f"    消息体长度: {len(msg.body)} bytes")
                if msg.properties:
                    print(f"    属性: {msg.properties}")
                print()
        else:
            print("  没有拉取到消息")

    except Exception as e:
        print(f"测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # 断开连接
        print("\n断开连接...")
        client.disconnect()
        print("断开连接完成")


if __name__ == "__main__":
    test_pull_message()
