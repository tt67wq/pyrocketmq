#!/usr/bin/env python3
"""
Producer基础使用示例

这个示例展示了如何使用pyrocketmq Producer进行基本的消息发送操作。
包括：
- 创建Producer实例
- 发送同步消息
- 发送单向消息
- 查看发送结果
- 正确关闭Producer

适用场景：
- 初学者了解Producer基本用法
- 简单的消息发送需求
- 开发环境测试

使用方法:
    export PYTHONPATH=/path/to/pyrocketmq/src
    python examples/basic_producer.py
"""

import random
import sys
import time

from config_loader import load_config, parse_config_file_path

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import create_producer
from pyrocketmq.producer.errors import ProducerError


def main():
    """主函数"""
    try:
        # 从命令行参数解析配置文件路径
        config_path = parse_config_file_path()
        config = load_config(config_path)
        print(f"已加载配置文件: {config_path}")
        print(
            f"Topic: {config.topic}, Group: {config.group}, Nameserver: {config.nameserver}"
        )

        # 设置日志
        pyrocketmq.logging.setup_logging(
            LoggingConfig(level="INFO", json_output=False, file_path="producer.log")
        )

        # 创建Producer
        producer = create_producer(config.group, config.nameserver)
        producer.start()

        # 解析标签列表
        tags = config.tag.split("||") if config.tag else ["TAG1"]

        index = 0
        while True:
            try:
                # 发送单条消息
                tag = random.choice(tags)
                message = Message(
                    topic=config.topic,
                    body=f"Hello, This Is Single Msg From Python {index}".encode(),
                )
                message.set_tags(tag)
                message.set_keys("MSG_KEY" + str(index))
                ret = producer.send(message)
                print("Message sent ret:", ret)
                index += 1

                # 发送批量消息
                messages: list[Message] = []
                for _ in range(10):
                    tag = random.choice(tags)
                    message = Message(
                        topic=config.topic,
                        body=f"Hello, This Is Batch Msg From Python {index}".encode(),
                    )
                    message.set_tags(tag)
                    message.set_keys("MSG_KEY" + str(index))
                    messages.append(message)
                    index += 1
                ret = producer.send_batch(*messages)
                print("Batch message sent ret:", ret)

            except ProducerError as e:
                print(f"Failed to send message: {e}")
                time.sleep(5)
            else:
                time.sleep(1)

    except FileNotFoundError as e:
        print(f"配置文件错误: {e}")
        print("请确保config.json文件存在且包含必需的配置项")
        sys.exit(1)
    except Exception as e:
        print(f"程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
