#! /usr/bin/env python3
# -*- coding: utf-8 -*-


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
    # 从命令行参数解析配置文件路径
    config_path = parse_config_file_path()
    config = load_config(config_path)
    print(f"已加载配置文件: {config_path}")
    print(
        f"Topic: {config.topic}, Group: {config.group}, Nameserver: {config.nameserver}"
    )

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
        config.topic, SUBSCRIBE_ALL, create_message_listener(message_listener)
    )

    try:
        # 启动consumer
        print("启动消费者...")
        consumer.start()
        print("消费者启动成功，开始处理消息...")

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
