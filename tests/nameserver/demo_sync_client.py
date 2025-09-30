#!/usr/bin/env python3
"""
SyncNameServerClient 演示程序

演示如何使用同步 NameServer 客户端查询 Topic 路由信息和 Broker 集群信息。
"""

import json
from pathlib import Path

from pyrocketmq.logging import LoggerFactory, LoggingConfig
from pyrocketmq.nameserver import SyncNameServerClient, create_sync_client
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


def demo_topic_route_info(client: SyncNameServerClient, topic: str):
    """演示查询 Topic 路由信息"""
    print(f"\n🔍 查询 Topic 路由信息: {topic}")
    print("-" * 50)

    try:
        route_data = client.query_topic_route_info(topic)

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


def demo_broker_cluster_info(client: SyncNameServerClient):
    """演示获取 Broker 集群信息"""
    print("\n🏢 获取 Broker 集群信息")
    print("-" * 50)

    try:
        cluster_info = client.get_broker_cluster_info()

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


def main():
    """主函数"""
    print("🚀 SyncNameServerClient 演示程序")
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
        client = create_sync_client(
            host=config["host"], port=config["port"], timeout=config["timeout"]
        )

        # 使用上下文管理器自动处理连接和断开
        print("🔌 连接到 NameServer...")
        with client:
            print(f"✅ 连接成功! 连接状态: {client.is_connected()}")

            # 演示 1: 查询 Topic 路由信息
            demo_topic_route_info(client, config["topic"])

            # 演示 2: 获取 Broker 集群信息
            demo_broker_cluster_info(client)

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
    main()
