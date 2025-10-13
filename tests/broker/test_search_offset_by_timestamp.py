#!/usr/bin/env python3
"""
测试BrokerClient的search_offset_by_timestamp方法
这个脚本演示如何使用BrokerClient根据时间戳搜索偏移量
"""

import json
import os
import sys
import time
from datetime import datetime

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


def test_search_offset_by_timestamp():
    """测试search_offset_by_timestamp方法"""
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
            "queue_id": 0,
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
        default_queue_id = config.get("queue_id", 0)

        print("\n🔗 连接信息:")
        print(f"  Broker地址: {config['host']}:{config['port']}")
        print(f"  主题: {topic}")
        print(f"  默认队列ID: {default_queue_id}")
        print(f"  超时时间: {config.get('timeout', 10.0)}秒")

        print("\n🕐 根据时间戳搜索偏移量测试:")

        # 测试多个队列的偏移量搜索，优先使用配置中的queue_id，然后测试相邻队列
        if default_queue_id == 0:
            queue_ids = [0, 1, 2, 3]
        else:
            # 包含配置的queue_id以及相邻的队列
            queue_ids = list(
                set(
                    [
                        default_queue_id,
                        max(0, default_queue_id - 1),
                        default_queue_id + 1,
                        (default_queue_id + 2) % 8,  # 确保有一些多样性
                    ]
                )
            )[:4]  # 最多测试4个队列

        # 准备不同的时间戳进行测试
        current_timestamp = int(time.time() * 1000)  # 当前时间戳（毫秒）
        yesterday_timestamp = current_timestamp - (
            24 * 60 * 60 * 1000
        )  # 昨天的时间戳
        one_week_ago_timestamp = current_timestamp - (
            7 * 24 * 60 * 60 * 1000
        )  # 一周前的时间戳
        one_hour_later_timestamp = current_timestamp + (
            60 * 60 * 1000
        )  # 一小时后的时间戳

        timestamp_tests = [
            ("当前时间", current_timestamp),
            ("昨天时间", yesterday_timestamp),
            ("一周前", one_week_ago_timestamp),
            ("一小时后", one_hour_later_timestamp),
        ]

        for queue_id in queue_ids:
            queue_desc = (
                f"{queue_id} (配置队列)"
                if queue_id == default_queue_id
                else f"{queue_id}"
            )
            print(f"\n📍 测试队列 {queue_desc}:")

            for time_desc, timestamp in timestamp_tests:
                try:
                    # 将时间戳转换为可读格式
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    time_str = dt.strftime("%Y-%m-%d %H:%M:%S")

                    print(f"  🕐 搜索时间: {time_desc} ({time_str})")

                    # 调用search_offset_by_timestamp方法
                    offset = client.search_offset_by_timestamp(
                        topic=topic,
                        queue_id=queue_id,
                        timestamp=timestamp,
                    )

                    # 输出结果
                    if offset == -1:
                        print("    ✅ 搜索完成: 未找到对应偏移量（返回-1）")
                    else:
                        print(f"    ✅ 搜索成功: 找到偏移量 {offset}")

                except OffsetError as e:
                    print(f"    ❌ 搜索失败 (OffsetError): {e}")

                except BrokerResponseError as e:
                    print(f"    ❌ 搜索失败 (BrokerResponseError): {e}")
                    print(f"      响应代码: {e.response_code}")

                except BrokerTimeoutError as e:
                    print(f"    ❌ 搜索超时: {e}")

                except Exception as e:
                    print(f"    ❌ 搜索出现未知错误: {e}")

        # 测试特殊时间戳
        print("\n🧪 测试特殊时间戳:")

        special_timestamps = [
            ("时间戳0", 0),
            ("1970年", 86400000),  # 1970-01-02
            ("2020年开始", 1577836800000),  # 2020-01-01 00:00:00
            ("负数时间戳", -86400000),  # 1969-12-31
        ]

        for time_desc, timestamp in special_timestamps:
            try:
                dt = (
                    datetime.fromtimestamp(timestamp / 1000)
                    if timestamp > 0
                    else datetime(1970, 1, 1)
                )
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                print(f"  🕐 测试 {time_desc}: {time_str}")

                offset = client.search_offset_by_timestamp(
                    topic=topic,
                    queue_id=default_queue_id,  # 使用配置的队列
                    timestamp=timestamp,
                )

                if offset == -1:
                    print("    ✅ 未找到对应偏移量（返回-1）")
                else:
                    print(f"    ✅ 找到偏移量: {offset}")

            except Exception as e:
                print(f"    ❌ 测试失败: {e}")

        # 测试边界情况
        print("\n🚨 测试边界情况:")

        # 测试不存在的主题
        print("\n测试不存在的主题...")
        try:
            offset = client.search_offset_by_timestamp(
                topic="non_existent_topic",
                queue_id=default_queue_id,
                timestamp=current_timestamp,
            )
            print(f"意外成功 - 不存在主题的偏移量: {offset}")
        except BrokerResponseError as e:
            print(f"✅ 预期的错误 - 主题不存在: {e}")
        except Exception as e:
            print(f"❌ 未预期的错误: {e}")

        # 测试负数队列ID
        print("\n测试负数队列ID...")
        try:
            offset = client.search_offset_by_timestamp(
                topic=topic,
                queue_id=-1,
                timestamp=current_timestamp,
            )
            print(f"意外成功 - 负数队列ID的偏移量: {offset}")
        except Exception as e:
            print(f"✅ 预期的错误 - 负数队列ID: {e}")

        # 测试极大队列ID
        print("\n测试极大队列ID...")
        try:
            offset = client.search_offset_by_timestamp(
                topic=topic,
                queue_id=999999,
                timestamp=current_timestamp,
            )
            print(f"意外成功 - 极大队列ID的偏移量: {offset}")
        except Exception as e:
            print(f"✅ 预期的错误 - 极大队列ID: {e}")

        print("\n✅ 测试完成！")

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


if __name__ == "__main__":
    test_search_offset_by_timestamp()
