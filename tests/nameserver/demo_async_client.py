#!/usr/bin/env python3
"""
AsyncNameServerClient 演示程序

演示如何使用异步 NameServer 客户端查询 Topic 路由信息和 Broker 集群信息。
"""

import asyncio
import json
from pathlib import Path

from pyrocketmq.logging import LoggerFactory, LoggingConfig
from pyrocketmq.nameserver import AsyncNameServerClient, create_async_client
from pyrocketmq.nameserver.errors import NameServerError, NameServerTimeoutError


def load_config() -> dict:
    """加载配置文件"""
    config_path = Path(__file__).parent / "test_config.json"

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print("⚠️  配置文件未找到，使用默认配置")
        return {
            "host": "localhost",
            "port": 9876,
            "topic": "test_topic",
            "timeout": 30.0,
        }


async def demo_topic_route_info(client: AsyncNameServerClient, topic: str):
    """异步演示查询 Topic 路由信息"""
    print(f"\n🔍 查询 Topic 路由信息: {topic}")
    print("-" * 50)

    try:
        route_data = await client.query_topic_route_info(topic)

        print("✅ 查询成功!")
        print(f"📋 Topic: {topic}")
        print(f"📦 Order Topic: {route_data.order_topic_conf}")

        # 显示队列信息
        if route_data.queue_data_list:
            print(f"\n📊 队列信息 ({len(route_data.queue_data_list)} 个队列):")
            for i, queue_data in enumerate(route_data.queue_data_list):
                print(f"  队列 {i + 1}:")
                print(f"    🏷️  Broker Name: {queue_data.broker_name}")
                print(f"    📖 Read Queue Nums: {queue_data.read_queue_nums}")
                print(f"    ✏️  Write Queue Nums: {queue_data.write_queue_nums}")
                print(f"    🔐 Permission: {queue_data.perm}")
                print(f"    🏁 Topic Sys Flag: {queue_data.topic_syn_flag}")
        else:
            print("❌ 没有找到队列数据")

        # 显示 Broker 信息
        if route_data.broker_data_list:
            print(
                f"\n🏢 Broker 信息 ({len(route_data.broker_data_list)} 个 Broker):"
            )
            for i, broker_data in enumerate(route_data.broker_data_list):
                print(f"  Broker {i + 1}:")
                print(f"    🏷️  Cluster: {broker_data.cluster}")
                print(f"    📛 Broker Name: {broker_data.broker_name}")
                if broker_data.broker_addresses:
                    print("    📍 地址列表:")
                    for broker_id, addr in broker_data.broker_addresses.items():
                        role = (
                            "Master" if broker_id == 0 else f"Slave-{broker_id}"
                        )
                        print(f"      {role}: {addr}")
                else:
                    print("    📍 地址: 无")
        else:
            print("❌ 没有找到 Broker 数据")

    except NameServerTimeoutError as e:
        print(f"❌ 请求超时: {e}")
    except NameServerError as e:
        print(f"❌ NameServer 错误: {e}")
    except Exception as e:
        print(f"❌ 未知错误: {e}")


async def demo_broker_cluster_info(client: AsyncNameServerClient):
    """异步演示获取 Broker 集群信息"""
    print("\n🏢 获取 Broker 集群信息")
    print("-" * 50)

    try:
        cluster_info = await client.get_broker_cluster_info()

        print("✅ 获取成功!")

        # 显示集群地址表
        if cluster_info.cluster_addr_table:
            print(
                f"\n🌐 集群地址表 ({len(cluster_info.cluster_addr_table)} 个集群):"
            )
            for (
                cluster_name,
                broker_names,
            ) in cluster_info.cluster_addr_table.items():
                print(f"  🏷️  集群: {cluster_name}")
                print(f"    📋 Broker Names: {list(broker_names)}")
        else:
            print("❌ 没有找到集群地址表")

        # 显示 Broker 地址表
        if cluster_info.broker_addr_table:
            print(
                f"\n📍 Broker 地址表 ({len(cluster_info.broker_addr_table)} 个 Broker):"
            )
            for (
                broker_name,
                broker_data,
            ) in cluster_info.broker_addr_table.items():
                print(f"  🏷️  Broker: {broker_name}")
                print(f"    🌐 Cluster: {broker_data.cluster}")
                if broker_data.broker_addresses:
                    print("    📍 地址列表:")
                    for broker_id, addr in broker_data.broker_addresses.items():
                        role = (
                            "Master" if broker_id == 0 else f"Slave-{broker_id}"
                        )
                        print(f"      {role}: {addr}")
                else:
                    print("    📍 地址: 无")
        else:
            print("❌ 没有找到 Broker 地址表")

    except NameServerTimeoutError as e:
        print(f"❌ 请求超时: {e}")
    except NameServerError as e:
        print(f"❌ NameServer 错误: {e}")
    except Exception as e:
        print(f"❌ 未知错误: {e}")


async def demo_concurrent_queries(client: AsyncNameServerClient, topics: list):
    """演示并发查询多个 Topic"""
    print(f"\n🚀 并发查询 {len(topics)} 个 Topic 的路由信息")
    print("-" * 50)

    try:
        # 创建并发任务
        tasks = []
        for topic in topics:
            task = asyncio.create_task(demo_single_topic_query(client, topic))
            tasks.append(task)

        # 等待所有任务完成
        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        print(f"❌ 并发查询失败: {e}")


async def demo_single_topic_query(client: AsyncNameServerClient, topic: str):
    """查询单个 Topic 的路由信息"""
    try:
        route_data = await client.query_topic_route_info(topic)
        queue_count = (
            len(route_data.queue_data_list) if route_data.queue_data_list else 0
        )
        broker_count = (
            len(route_data.broker_data_list)
            if route_data.broker_data_list
            else 0
        )
        print(
            f"✅ Topic '{topic}': {queue_count} 个队列, {broker_count} 个 Broker"
        )
    except Exception as e:
        print(f"❌ Topic '{topic}' 查询失败: {e}")


async def main():
    """主函数"""
    print("🚀 AsyncNameServerClient 演示程序")
    print("=" * 60)

    # 设置日志 - 生产环境建议使用 INFO 级别
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 加载配置
    config = load_config()
    print("📋 连接配置:")
    print(f"   🌐 Host: {config['host']}")
    print(f"   🔌 Port: {config['port']}")
    print(f"   🎯 Topic: {config['topic']}")
    print(f"   ⏱️  Timeout: {config['timeout']}s")

    # 创建客户端
    try:
        print("\n🔗 创建 NameServer 客户端...")
        client = await create_async_client(
            host=config["host"], port=config["port"], timeout=config["timeout"]
        )

        # 使用上下文管理器自动处理连接和断开
        print("🔌 连接到 NameServer...")
        async with client:
            print(f"✅ 连接成功! 连接状态: {client.is_connected()}")

            # 演示 1: 查询单个 Topic 路由信息
            await demo_topic_route_info(client, config["topic"])

            # 演示 2: 获取 Broker 集群信息
            await demo_broker_cluster_info(client)

            # 演示 3: 并发查询多个 Topic
            test_topics = [config["topic"], "test_topic_2", "test_topic_3"]
            await demo_concurrent_queries(client, test_topics)

    except NameServerTimeoutError as e:
        print(f"❌ 连接超时: {e}")
        print("💡 提示:")
        print("   - 检查 NameServer 地址和端口是否正确")
        print("   - 确认网络连接是否正常")
        print("   - 检查防火墙设置")
        print("   - 尝试增加超时时间")
    except NameServerError as e:
        print(f"❌ NameServer 错误: {e}")
        print("💡 提示:")
        print("   - 检查 RocketMQ NameServer 是否正常运行")
        print("   - 确认客户端有访问权限")
    except Exception as e:
        print(f"❌ 未知错误: {e}")
        print("💡 启用 DEBUG 日志获取详细信息:")
        print(
            "   LoggerFactory.setup_default_config(LoggingConfig(level='DEBUG'))"
        )
        import traceback

        traceback.print_exc()

    print("\n🎉 演示程序结束")
    print("=" * 60)


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())
