#!/usr/bin/env python3
"""
测试BrokerClient的send_message方法
"""

import json
import os
import sys
import time

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyrocketmq.broker import create_broker_client
from pyrocketmq.logging import LoggerFactory, LoggingConfig


def load_config():
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


def main():
    # 加载配置
    config = load_config()
    if not config:
        print("无法加载配置文件，使用默认配置")
        config = {
            "host": "localhost",
            "port": 9876,
            "topic": "test_topic",
            "timeout": 10.0,
        }
    print("📋 配置信息:")
    print(f"  Broker: {config['host']}:{config['port']}")
    print(f"  Topic: {config['topic']}")
    print(f"  Timeout: {config['timeout']}s")

    # 设置日志
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 创建客户端
    client = create_broker_client(
        host=config["host"], port=config["port"], timeout=config["timeout"]
    )

    try:
        print("\n🔗 正在连接到Broker...")
        client.connect()
        print(f"✅ 连接成功! 客户端ID: {client.client_id}")

        # 准备测试消息
        test_messages = [
            {
                "body": b"Hello RocketMQ from Python!",
                "tags": "test",
                "keys": f"test_key_{int(time.time())}",
                "properties": {"source": "python_client", "version": "1.0"},
            },
            {
                "body": json.dumps(
                    {"message": "JSON message", "timestamp": time.time()}
                ).encode("utf-8"),
                "tags": "json",
                "keys": f"json_key_{int(time.time())}",
                "properties": {"content_type": "application/json"},
            },
            {
                "body": b"Batch message 1",
                "tags": "batch",
                "keys": f"batch_key_1_{int(time.time())}",
                "properties": {"batch_id": "batch_001", "index": "1"},
            },
        ]

        print("\n📤 开始发送消息...")
        results = []

        for i, msg_info in enumerate(test_messages):
            print(f"\n📨 发送消息 {i + 1}:")
            print(f"  主题: {config['topic']}")
            print(f"  标签: {msg_info['tags']}")
            print(f"  键: {msg_info['keys']}")
            print(f"  消息体长度: {len(msg_info['body'])} bytes")

            try:
                result = client.send_message(
                    producer_group="test_producer_group",
                    topic=config["topic"],
                    body=msg_info["body"],
                    queue_id=0,
                    tags=msg_info["tags"],
                    keys=msg_info["keys"],
                    properties=msg_info["properties"],
                )

                results.append(result)
                print("  ✅ 发送成功!")
                print(f"    消息ID: {result.msg_id}")
                print(f"    队列ID: {result.queue_id}")
                print(f"    队列偏移量: {result.queue_offset}")
                print(f"    区域ID: {result.region_id}")

            except Exception as e:
                print(f"  ❌ 发送失败: {e}")

        print("\n📊 发送结果汇总:")
        print(f"  总消息数: {len(test_messages)}")
        print(f"  成功数: {len(results)}")
        print(f"  失败数: {len(test_messages) - len(results)}")

        if results:
            print("\n📋 成功发送的消息详情:")
            for i, result in enumerate(results):
                print(
                    f"  消息{i + 1}: ID={result.msg_id}, Queue={result.queue_id}, Offset={result.queue_offset}"
                )

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()

    finally:
        print("\n🔌 断开连接...")
        client.disconnect()
        print("✅ 已断开连接")


if __name__ == "__main__":
    main()
