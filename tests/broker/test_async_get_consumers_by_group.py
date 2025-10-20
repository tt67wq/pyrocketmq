#!/usr/bin/env python3
"""
测试AsyncBrokerClient的get_consumers_by_group方法
这个脚本演示如何使用AsyncBrokerClient异步获取指定消费者组的消费者列表
"""

import asyncio
import json
import os
import sys

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pyrocketmq.broker import create_async_broker_client
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
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


async def test_get_consumers_by_group():
    """异步测试get_consumers_by_group方法"""
    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))

    # 加载配置
    config = load_test_config()
    if not config:
        print("无法加载配置文件，使用默认配置")
        config = {
            "host": "localhost",
            "port": 9876,
            "timeout": 10.0,
        }

    # 创建异步Broker客户端
    client = create_async_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 10.0),
    )

    try:
        # 连接到Broker
        print("连接到Broker...")
        await client.connect()
        print(f"连接成功，客户端ID: {client.client_id}")

        # 测试参数
        consumer_group = config.get("consumer_group", "test_consumer_group")

        print("\n🔗 连接信息:")
        print(f"  Broker地址: {config['host']}:{config['port']}")
        print(f"  超时时间: {config.get('timeout', 10.0)}秒")

        print("\n👥 获取消费者列表测试:")

        print(f"\n获取消费者组 '{consumer_group}' 的消费者列表...")

        try:
            # 调用异步get_consumers_by_group方法
            consumers = await client.get_consumers_by_group(
                consumer_group=consumer_group
            )

            # 输出结果
            print(f"✅ 消费者组 '{consumer_group}' 查询成功:")
            print(f"  消费者数量: {len(consumers)}")

            if consumers:
                print("  消费者ID列表:")
                for i, consumer_id in enumerate(consumers, 1):
                    print(f"    {i}. {consumer_id}")
            else:
                print("  该消费者组当前没有活跃的消费者")

        except BrokerResponseError as e:
            print(f"❌ 响应错误: {e}")
            print("  可能原因:")
            print("    - 消费者组不存在")
            print("    - 权限不足")
            print("    - Broker内部错误")
        except BrokerTimeoutError as e:
            print(f"❌ 请求超时: {e}")
            print("  建议:")
            print("    - 增加超时时间")
            print("    - 检查网络连接")
        except BrokerConnectionError as e:
            print(f"❌ 连接错误: {e}")
            print("  建议:")
            print("    - 检查Broker是否运行")
            print("    - 验证主机地址和端口")
        except Exception as e:
            print(f"❌ 未知错误: {e}")

    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        print("  请检查:")
        print("    - Broker是否正常运行")
        print("    - 网络连接是否正常")
        print("    - 配置参数是否正确")

    finally:
        # 断开连接
        print("\n断开连接...")
        try:
            await client.disconnect()
            print("✅ 连接已断开")
        except Exception as e:
            print(f"❌ 断开连接时发生错误: {e}")


async def test_error_scenarios():
    """异步测试错误场景"""
    print("\n🧪 错误场景测试:")

    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 加载配置
    config = load_test_config()
    if not config:
        config = {"host": "localhost", "port": 9876, "timeout": 5.0}

    # 创建异步Broker客户端
    client = create_async_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 5.0),
    )

    try:
        # 连接到Broker
        print("连接到Broker...")
        await client.connect()

        # 测试1: 不存在的消费者组
        print("\n测试1: 查询不存在的消费者组")
        try:
            consumers = await client.get_consumers_by_group(
                "non_existent_group"
            )
            print(f"结果: 返回 {len(consumers)} 个消费者")
        except BrokerResponseError as e:
            print(f"预期的响应错误: {e}")
        except Exception as e:
            print(f"意外错误: {e}")

        # 测试2: 空字符串消费者组
        print("\n测试2: 查询空字符串消费者组")
        try:
            consumers = await client.get_consumers_by_group("")
            print(f"结果: 返回 {len(consumers)} 个消费者")
        except Exception as e:
            print(f"错误: {e}")

    except Exception as e:
        print(f"❌ 错误场景测试失败: {e}")

    finally:
        await client.disconnect()


async def test_concurrent_requests():
    """异步测试并发请求"""
    print("\n🚀 并发请求测试:")

    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 加载配置
    config = load_test_config()
    if not config:
        config = {"host": "localhost", "port": 9876, "timeout": 10.0}

    # 创建异步Broker客户端
    client = create_async_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 10.0),
    )

    try:
        # 连接到Broker
        print("连接到Broker...")
        await client.connect()

        # 测试多个并发请求
        consumer_groups = [
            "test_consumer_group_1",
            "test_consumer_group_2",
            "test_consumer_group_3",
        ]

        print(f"\n并发查询 {len(consumer_groups)} 个消费者组...")

        # 创建并发任务
        tasks = []
        for group in consumer_groups:
            task = asyncio.create_task(client.get_consumers_by_group(group))
            tasks.append((group, task))

        # 等待所有任务完成
        start_time = asyncio.get_event_loop().time()

        for group, task in tasks:
            try:
                consumers = await task
                print(f"  ✅ 消费者组 '{group}': {len(consumers)} 个消费者")
            except Exception as e:
                print(f"  ❌ 消费者组 '{group}' 错误: {e}")

        end_time = asyncio.get_event_loop().time()
        print(f"\n并发请求完成，总耗时: {end_time - start_time:.3f}秒")

    except Exception as e:
        print(f"❌ 并发请求测试失败: {e}")

    finally:
        await client.disconnect()


async def test_performance():
    """异步性能测试"""
    print("\n⚡ 性能测试:")

    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="WARNING"))

    # 加载配置
    config = load_test_config()
    if not config:
        config = {"host": "localhost", "port": 9876, "timeout": 5.0}

    # 创建异步Broker客户端
    client = create_async_broker_client(
        host=config["host"],
        port=config["port"],
        timeout=config.get("timeout", 5.0),
    )

    try:
        # 连接到Broker
        print("连接到Broker...")
        await client.connect()

        # 性能测试参数
        test_group = "performance_test_group"
        request_count = 10

        print(f"\n执行 {request_count} 次请求的性能测试...")

        # 执行多次请求
        start_time = asyncio.get_event_loop().time()
        success_count = 0
        error_count = 0

        for i in range(request_count):
            try:
                consumers = await client.get_consumers_by_group(test_group)
                success_count += 1
                if i == 0:  # 只在第一次打印详细信息
                    print(f"  第1次请求: {len(consumers)} 个消费者")
            except Exception as e:
                error_count += 1
                if i == 0:  # 只在第一次打印错误
                    print(f"  第1次请求错误: {e}")

        end_time = asyncio.get_event_loop().time()
        total_time = end_time - start_time

        print("\n📊 性能测试结果:")
        print(f"  总请求数: {request_count}")
        print(f"  成功请求: {success_count}")
        print(f"  失败请求: {error_count}")
        print(f"  总耗时: {total_time:.3f}秒")
        print(f"  平均耗时: {total_time / request_count:.3f}秒/请求")
        print(f"  QPS: {request_count / total_time:.1f} 请求/秒")

    except Exception as e:
        print(f"❌ 性能测试失败: {e}")

    finally:
        await client.disconnect()


async def main():
    """异步主函数"""
    print("=" * 60)
    print("🧪 AsyncBrokerClient get_consumers_by_group 集成测试")
    print("=" * 60)

    try:
        # 执行主要测试
        await test_get_consumers_by_group()

        # 执行错误场景测试
        await test_error_scenarios()

        # 执行并发请求测试
        await test_concurrent_requests()

        # 执行性能测试
        await test_performance()

        print("\n" + "=" * 60)
        print("✅ 测试完成")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n❌ 测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())
