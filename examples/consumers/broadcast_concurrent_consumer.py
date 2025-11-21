#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pyrocketmq.logging
from pyrocketmq.consumer import create_concurrent_consumer, create_message_listener
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model import (
    SUBSCRIBE_ALL,
    ConsumeResult,
    MessageExt,
    MessageModel,
)


def message_listener(messages: list[MessageExt]) -> ConsumeResult:
    for message in messages:
        print(
            "!!!!!!!!!!!!!!!",
            str(message.body),
            "tags:",
            message.get_tags(),
            "keys:",
            message.get_keys(),
        )
    return ConsumeResult.SUCCESS


def main():
    pyrocketmq.logging.setup_logging(
        LoggingConfig(level="INFO", json_output=False, file_path="consumer.log")
    )
    consumer = create_concurrent_consumer(
        "GID_POETRY",
        "d1-dmq-namesrv.shizhuang-inc.net:31110",
        message_model=MessageModel.BROADCASTING,
    )
    consumer.subscribe(
        "test_im_015", SUBSCRIBE_ALL, create_message_listener(message_listener)
    )

    try:
        # 启动consumer
        print("启动消费者...")
        consumer.start()
        print("消费者启动成功，开始处理消息...")

        # 保持运行，直到收到Ctrl-C信号
        import signal
        import sys

        def signal_handler(_sig, _frame):
            print("\n收到中断信号，正在关闭消费者...")
            consumer.shutdown()
            print("消费者已关闭")
            sys.exit(0)

        # 注册信号处理器
        _ = signal.signal(signal.SIGINT, signal_handler)

        # 保持主线程运行
        print("消费者正在运行，按 Ctrl+C 停止...")
        while True:
            import time

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n收到键盘中断信号，正在关闭消费者...")

    except Exception as e:
        print(f"消费者运行异常: {e}")

    finally:
        try:
            consumer.shutdown()
            print("消费者已关闭")
        except Exception as e:
            print(f"关闭消费者时发生异常: {e}")


if __name__ == "__main__":
    main()
