#! /usr/bin/env python3
# -*- coding: utf-8 -*-


import asyncio
import sys
from types import FrameType

import pyrocketmq.logging
from pyrocketmq.consumer import (
    create_async_concurrent_consumer,
    create_async_message_listener,
)
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model import ConsumeResult, MessageExt, create_tag_selector


def message_listener(messages: list[MessageExt]) -> ConsumeResult:
    import random

    # 概率返回RECONSUME_LATER，模拟消费失败需要重试的场景
    if random.randint(1, 6) == 1:
        print(f"模拟消费失败，返回RECONSUME_LATER进行重试，消息数量: {len(messages)}")
        return ConsumeResult.RECONSUME_LATER

    for message in messages:
        print(
            "【收到消息：】",
            str(message.body),
            "tags:",
            message.get_tags(),
            "keys:",
            message.get_keys(),
        )
    return ConsumeResult.SUCCESS


async def main():
    pyrocketmq.logging.setup_logging(
        LoggingConfig(level="INFO", file_path="async_consumer.log")
    )

    # 创建关闭事件，用于优雅关闭
    shutdown_event = asyncio.Event()

    def signal_handler(sig: int, frame: FrameType | None) -> None:
        """信号处理器：设置关闭事件"""
        print(f"\n收到信号 {sig}，准备关闭消费者...")
        shutdown_event.set()

    # 注册信号处理器
    import signal

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = create_async_concurrent_consumer(
        "GID_POETRY", "d1-dmq-namesrv.shizhuang-inc.net:31110"
    )

    await consumer.subscribe(
        "test_im_015",
        create_tag_selector("TAG1||TAG2"),
        create_async_message_listener(message_listener),
    )

    try:
        # 启动consumer
        print("启动消费者...")
        await consumer.start()
        print("消费者启动成功，开始处理消息...")

        # 等待关闭信号
        print("消费者正在运行，按 Ctrl+C 或发送 SIGTERM 信号停止...")
        await shutdown_event.wait()

    except Exception as e:
        print(f"消费者运行异常: {e}")

    finally:
        print("正在关闭消费者...")
        try:
            await consumer.shutdown()
            print("消费者已关闭")
        except Exception as e:
            print(f"关闭消费者时发生异常: {e}")


if __name__ == "__main__":
    try:
        # 运行异步主函数
        asyncio.run(main())
    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:
        print(f"程序运行异常: {e}")
        sys.exit(1)
