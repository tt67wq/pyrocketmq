#!/usr/bin/env python3
"""
query_consumer_offset 方法演示脚本

这个脚本演示如何使用 BrokerClient 的 query_consumer_offset 方法来查询消费者偏移量。
该方法可以查询指定消费者组在特定主题和队列上的消费偏移量。
"""

import os
import sys
import time

# 添加 src 目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyrocketmq.broker import create_broker_client
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
    OffsetError,
)
from pyrocketmq.logging import LoggerFactory, LoggingConfig


def demo_basic_query():
    """基本的偏移量查询演示"""
    print("=" * 60)
    print("基本偏移量查询演示")
    print("=" * 60)

    # 设置日志级别
    LoggerFactory.setup_default_config(LoggingConfig(level="INFO"))

    # 配置参数
    host = "localhost"
    port = 9876
    timeout = 10.0

    # 测试参数
    consumer_group = "demo_consumer_group"
    topic = "demo_topic"
    queue_id = 0

    # 创建 Broker 客户端
    client = create_broker_client(host=host, port=port, timeout=timeout)

    try:
        print(f"🔗 连接到 Broker: {host}:{port}")
        client.connect()
        print(f"✅ 连接成功，客户端ID: {client.client_id}")

        print("\n📊 查询偏移量信息:")
        print(f"  消费者组: {consumer_group}")
        print(f"  主题: {topic}")
        print(f"  队列ID: {queue_id}")

        # 查询消费者偏移量
        offset = client.query_consumer_offset(
            consumer_group=consumer_group, topic=topic, queue_id=queue_id
        )

        print("\n✅ 查询结果:")
        print(f"  当前偏移量: {offset}")

    except BrokerConnectionError as e:
        print(f"❌ 连接错误: {e}")
    except OffsetError as e:
        print(f"⚠️  偏移量查询错误: {e}")
        print("  这可能表示消费者组尚未开始消费该队列")
    except BrokerResponseError as e:
        print(f"❌ Broker响应错误: {e}")
        if hasattr(e, "response_code"):
            print(f"  响应代码: {e.response_code}")
    except BrokerTimeoutError as e:
        print(f"❌ 请求超时: {e}")
    except Exception as e:
        print(f"❌ 未知错误: {e}")
    finally:
        print("\n🔐 断开连接...")
        client.disconnect()
        print("✅ 断开连接完成")


def demo_batch_query():
    """批量查询多个队列偏移量演示"""
    print("\n" + "=" * 60)
    print("批量查询偏移量演示")
    print("=" * 60)

    # 配置参数
    host = "localhost"
    port = 9876
    timeout = 10.0

    # 测试参数
    consumer_group = "demo_consumer_group"
    topic = "demo_topic"
    queue_ids = [0, 1, 2, 3]  # 查询多个队列

    # 创建 Broker 客户端
    client = create_broker_client(host=host, port=port, timeout=timeout)

    def query_multiple_queues(client, consumer_group, topic, queue_ids):
        """批量查询多个队列的偏移量"""
        results = {}
        total_offset = 0
        successful_count = 0

        for queue_id in queue_ids:
            try:
                offset = client.query_consumer_offset(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id,
                )
                results[queue_id] = offset
                total_offset += offset
                successful_count += 1
                print(f"  ✅ 队列 {queue_id}: {offset}")

            except OffsetError as e:
                results[queue_id] = None
                print(f"  ⚠️  队列 {queue_id}: 查询失败 - {e}")
            except Exception as e:
                results[queue_id] = None
                print(f"  ❌ 队列 {queue_id}: 未知错误 - {e}")

        return results, total_offset, successful_count

    try:
        print(f"🔗 连接到 Broker: {host}:{port}")
        client.connect()

        print("\n📊 批量查询偏移量信息:")
        print(f"  消费者组: {consumer_group}")
        print(f"  主题: {topic}")
        print(f"  队列列表: {queue_ids}")

        print("\n🔍 查询结果:")
        results, total_offset, successful_count = query_multiple_queues(
            client, consumer_group, topic, queue_ids
        )

        print("\n📈 统计信息:")
        print(f"  总队列数: {len(queue_ids)}")
        print(f"  成功查询: {successful_count}")
        print(f"  失败查询: {len(queue_ids) - successful_count}")
        print(f"  总偏移量: {total_offset}")
        print(
            f"  平均偏移量: {total_offset / successful_count if successful_count > 0 else 0:.2f}"
        )

        # 显示详细结果
        print("\n📋 详细结果:")
        for queue_id, offset in results.items():
            status = "✅" if offset is not None else "❌"
            offset_str = str(offset) if offset is not None else "N/A"
            print(f"  {status} 队列 {queue_id}: {offset_str}")

    except Exception as e:
        print(f"❌ 批量查询失败: {e}")
    finally:
        print("\n🔐 断开连接...")
        client.disconnect()


def demo_error_handling():
    """错误处理演示"""
    print("\n" + "=" * 60)
    print("错误处理演示")
    print("=" * 60)

    # 配置参数
    host = "localhost"
    port = 9876
    timeout = 5.0  # 较短的超时时间用于演示

    # 创建 Broker 客户端
    client = create_broker_client(host=host, port=port, timeout=timeout)

    try:
        print(f"🔗 尝试连接到 Broker: {host}:{port}")
        client.connect()

        # 测试场景1: 查询不存在的主题
        print("\n🧪 测试场景1: 查询不存在的主题")
        try:
            offset = client.query_consumer_offset(
                consumer_group="test_group",
                topic="non_existent_topic",
                queue_id=0,
            )
            print(f"  意外成功: {offset}")
        except BrokerResponseError as e:
            print(f"  ✅ 预期错误 - 主题不存在: {e}")
        except Exception as e:
            print(f"  ❌ 未预期错误: {e}")

        # 测试场景2: 查询不存在的消费者组
        print("\n🧪 测试场景2: 查询不存在的消费者组")
        try:
            offset = client.query_consumer_offset(
                consumer_group="non_existent_group",
                topic="demo_topic",
                queue_id=0,
            )
            print(f"  意外成功: {offset}")
        except (BrokerResponseError, OffsetError) as e:
            print(f"  ✅ 预期错误 - 消费者组不存在: {e}")
        except Exception as e:
            print(f"  ❌ 未预期错误: {e}")

        # 测试场景3: 查询负数队列ID
        print("\n🧪 测试场景3: 查询负数队列ID")
        try:
            offset = client.query_consumer_offset(
                consumer_group="test_group", topic="demo_topic", queue_id=-1
            )
            print(f"  意外成功: {offset}")
        except Exception as e:
            print(f"  ✅ 预期错误 - 无效队列ID: {e}")

    except BrokerConnectionError as e:
        print(f"❌ 连接失败: {e}")
        print("  请确保 RocketMQ Broker 正在运行并且地址正确")
    except Exception as e:
        print(f"❌ 测试失败: {e}")
    finally:
        if client.is_connected:
            print("\n🔐 断开连接...")
            client.disconnect()


def demo_performance_monitoring():
    """性能监控演示"""
    print("\n" + "=" * 60)
    print("性能监控演示")
    print("=" * 60)

    # 配置参数
    host = "localhost"
    port = 9876
    timeout = 10.0

    # 测试参数
    consumer_group = "perf_test_group"
    topic = "perf_test_topic"
    queue_count = 8
    iterations = 5

    # 创建 Broker 客户端
    client = create_broker_client(host=host, port=port, timeout=timeout)

    try:
        print(f"🔗 连接到 Broker: {host}:{port}")
        client.connect()

        print("\n⏱️  性能测试参数:")
        print(f"  消费者组: {consumer_group}")
        print(f"  主题: {topic}")
        print(f"  队列数: {queue_count}")
        print(f"  迭代次数: {iterations}")

        total_queries = 0
        total_time = 0
        successful_queries = 0

        for iteration in range(iterations):
            print(f"\n📊 第 {iteration + 1} 次迭代:")
            iteration_start = time.time()

            for queue_id in range(queue_count):
                query_start = time.time()
                try:
                    offset = client.query_consumer_offset(
                        consumer_group=consumer_group,
                        topic=topic,
                        queue_id=queue_id,
                    )
                    query_time = time.time() - query_start
                    total_queries += 1
                    total_time += query_time
                    successful_queries += 1
                    print(f"  队列 {queue_id}: {offset} ({query_time:.3f}s)")

                except Exception as e:
                    query_time = time.time() - query_start
                    total_queries += 1
                    total_time += query_time
                    print(f"  队列 {queue_id}: 失败 - {e} ({query_time:.3f}s)")

            iteration_time = time.time() - iteration_start
            print(f"  迭代耗时: {iteration_time:.3f}s")

        # 性能统计
        print("\n📈 性能统计:")
        print(f"  总查询数: {total_queries}")
        print(f"  成功查询: {successful_queries}")
        print(f"  失败查询: {total_queries - successful_queries}")
        print(f"  总耗时: {total_time:.3f}s")
        print(f"  平均查询时间: {total_time / total_queries:.3f}s")
        print(f"  查询成功率: {successful_queries / total_queries * 100:.1f}%")
        print(f"  QPS: {total_queries / total_time:.1f}")

    except Exception as e:
        print(f"❌ 性能测试失败: {e}")
    finally:
        if client.is_connected:
            print("\n🔐 断开连接...")
            client.disconnect()


def main():
    """主函数"""
    print("🚀 query_consumer_offset 方法演示")
    print("这个演示展示了如何使用 BrokerClient 查询消费者偏移量")

    try:
        # 基本查询演示
        demo_basic_query()

        # 批量查询演示
        demo_batch_query()

        # 错误处理演示
        demo_error_handling()

        # 性能监控演示
        demo_performance_monitoring()

    except KeyboardInterrupt:
        print("\n\n⚠️  演示被用户中断")
    except Exception as e:
        print(f"\n\n❌ 演示出现错误: {e}")
        import traceback

        traceback.print_exc()

    print("\n🎉 演示完成！")
    print("\n💡 使用提示:")
    print("1. 确保 RocketMQ Broker 正在运行")
    print("2. 调整脚本中的 host 和 port 参数")
    print("3. 使用实际存在的消费者组和主题进行测试")
    print("4. 查看日志以获得更详细的调试信息")


if __name__ == "__main__":
    main()
