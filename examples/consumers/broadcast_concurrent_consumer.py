#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from types import FrameType

from config_loader import load_config, parse_config_file_path

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
    try:
        # 从命令行参数解析配置文件路径
        config_path = parse_config_file_path()
        config = load_config(config_path)
        print(f"已加载配置文件: {config_path}")
        print(f"Topic: {config.topic}, Group: {config.group}, Nameserver: {config.nameserver}")

        # 设置日志
        pyrocketmq.logging.setup_logging(
            LoggingConfig(level="INFO", json_output=False, file_path="consumer.log")
        )

        # 创建广播模式的并发消费者
        consumer = create_concurrent_consumer(
            config.group,
            config.nameserver,
            message_model=MessageModel.BROADCASTING,
        )

        # 订阅主题，广播模式通常订阅所有消息
        consumer.subscribe(
            config.topic,
            SUBSCRIBE_ALL,
            create_message_listener(message_listener)
        )

    try:
        # 启动consumer
        print("启动消费者...")
        consumer.start()
        print("消费者启动成功，开始处理消息...")

        # 保持运行，直到收到Ctrl-C信号
        def signal_handler(_sig: int, _frame: FrameType | None):
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

    except FileNotFoundError as e:
        print(f"配置文件错误: {e}")
        print("请确保config.json文件存在且包含必需的配置项")
        sys.exit(1)
    except Exception as e:
        print(f"程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
