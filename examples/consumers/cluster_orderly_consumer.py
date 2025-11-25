#! /usr/bin/env python3
# -*- coding: utf-8 -*-


from config_loader import load_config, parse_config_file_path

import pyrocketmq.logging
from pyrocketmq.consumer import (
    create_message_listener,
    create_orderly_consumer,
)
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model import (
    ConsumeResult,
    MessageExt,
    create_tag_selector,
)


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

    # 创建集群模式的顺序消费者
    consumer = create_orderly_consumer(config.group, config.nameserver)

    # 创建标签选择器，支持配置多个标签
    if config.tag:
        selector = create_tag_selector(config.tag)
    else:
        # 如果没有配置标签，订阅所有消息
        from pyrocketmq.model import SUBSCRIBE_ALL

        selector = SUBSCRIBE_ALL

    # 订阅主题
    consumer.subscribe(
        config.topic,
        selector,
        create_message_listener(message_listener),
    )

    try:
        # 启动consumer
        print("启动消费者...")
        consumer.start()
        print("消费者启动成功，开始处理消息...")

        while True:
            import time

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n收到键盘中断信号，正在关闭消费者...")

    except Exception as e:
        print(f"消费者运行异常: {e}")

    finally:
        consumer.shutdown()


if __name__ == "__main__":
    main()
