#!/usr/bin/env python3
"""
简单批量消息Producer示例

这个示例展示了如何使用pyrocketmq Producer的新send_batch方法进行批量消息发送。
包括：
- 创建多个消息
- 使用producer.send_batch()直接批量发送
- 查看发送结果和统计

适用场景：
- 高吞吐量消息发送
- 批量处理业务场景
- 减少网络传输次数

使用方法:
    export PYTHONPATH=/path/to/pyrocketmq/src
    python examples/simple_batch_producer.py
"""

import sys
import time

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import create_producer
from pyrocketmq.producer.errors import ProducerError


def create_batch_messages(count: int = 5) -> list:
    """创建批量测试消息

    Args:
        count: 消息数量

    Returns:
        消息列表
    """
    messages = []
    for i in range(count):
        message = Message(
            topic="test_im_015",
            body=f"Batch message {i + 1} - {time.strftime('%H:%M:%S')}".encode(),
        )
        # 设置消息属性
        message.set_property("KEYS", f"batch_key_{i + 1}")
        message.set_property("BATCH_ID", f"batch_{int(time.time())}")
        message.set_tags("batch_tag")

        messages.append(message)

    return messages


def demonstrate_batch_features():
    """演示批量消息功能"""
    print("=== 批量消息功能演示 ===\n")

    # 创建测试消息
    messages = create_batch_messages(3)

    print(f"创建了 {len(messages)} 个消息:")
    for i, msg in enumerate(messages):
        print(
            f"  消息 {i + 1}: topic={msg.topic}, body_size={len(msg.body)}, keys={msg.get_property('KEYS')}"
        )

    print("\n--- Producer.send_batch() 功能说明 ---")
    print("✅ 新的批量发送方法支持:")
    print("  - 自动将多个消息编码为批量消息")
    print("  - 主题一致性验证")
    print("  - 高效的网络传输")
    print("  - 详细的错误处理和统计")
    print("  - 支持同步和异步发送")

    print("\n--- 使用示例 ---")
    print("```python")
    print("# 同步Producer")
    print("producer = create_producer('group', 'nameserver:9876')")
    print("producer.start()")
    print("")
    print("# 创建多个消息")
    print("msg1 = Message(topic='test', body=b'message1')")
    print("msg2 = Message(topic='test', body=b'message2')")
    print("")
    print("# 批量发送")
    print("result = producer.send_batch(msg1, msg2)")
    print("")
    print("# 异步Producer")
    print(
        "async_producer = await create_async_producer('group', 'nameserver:9876')"
    )
    print("await async_producer.start()")
    print("result = await async_producer.send_batch(msg1, msg2)")
    print("```")

    print("\n批量发送演示完成!\n")


def main():
    """主函数"""
    # 设置日志
    pyrocketmq.logging.setup_logging(LoggingConfig(level="INFO"))

    print("=== 简单批量消息Producer示例 ===\n")

    # 演示批量消息功能
    demonstrate_batch_features()

    # 创建Producer
    print("=== 开始Producer发送测试 ===")
    producer = create_producer(
        "GID_POETRY", "d1-dmq-namesrv.shizhuang-inc.net:31110"
    )

    # 批量发送测试
    batch_count = 0
    try:
        # 启动Producer
        print("启动Producer...")
        producer.start()
        print("Producer启动成功!")

        while True:
            try:
                # 创建批量消息
                messages = create_batch_messages(5)

                print(
                    f"\n发送第 {batch_count + 1} 批消息 ({len(messages)} 个消息):"
                )
                start_time = time.time()

                # 使用新的send_batch方法直接发送
                result = producer.send_batch(*messages)

                send_time = time.time() - start_time
                print(f"  ✅ 批量发送成功! 耗时: {send_time:.3f}s")
                print(f"  消息ID: {result.message_id if result else 'N/A'}")

                batch_count += 1

                # 每10批次输出一次统计
                if batch_count % 10 == 0:
                    stats = producer.get_stats()
                    print(f"\n📊 已发送 {batch_count} 批次消息")
                    print(f"   总发送: {stats['total_sent']}")
                    print(f"   总失败: {stats['total_failed']}")
                    print(f"   成功率: {stats['success_rate']:.1%}")

                # 等待一段时间再发送下一批
                time.sleep(2)

            except ProducerError as e:
                print(f"  ❌ 发送失败: {e}")
                print("  等待 5 秒后重试...")
                time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n\n🛑 用户中断，共发送了 {batch_count} 批次消息")

        # 输出最终统计
        stats = producer.get_stats()
        print("\n📊 最终统计:")
        print(f"   总发送: {stats['total_sent']}")
        print(f"   总失败: {stats['total_failed']}")
        print(f"   成功率: {stats['success_rate']:.1%}")

    except Exception as e:
        print(f"\n❌ 发生未知错误: {e}")

    finally:
        # 关闭Producer
        print("关闭Producer...")
        producer.shutdown()
        print("Producer已关闭")


if __name__ == "__main__":
    sys.exit(main())
