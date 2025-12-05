#!/usr/bin/env python3

import asyncio
import random
import sys

from config_loader import load_config, parse_config_file_path

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import create_async_producer
from pyrocketmq.producer.errors import ProducerError


async def main():
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
            LoggingConfig(level="DEBUG", file_path="producer.log")
        )

        # 创建异步Producer
        producer = create_async_producer(config.group, config.nameserver)
        await producer.start()

        # 解析标签列表
        tags = config.tag.split("||") if config.tag else ["TAG1"]

        index = 0
        while True:
            try:
                tag = random.choice(tags)
                message = Message(
                    topic=config.topic,
                    body=f"This {index} Message: Hello, RocketMQ From Python!".encode(),
                )
                message.set_tags(tag)
                message.set_keys("KEYS")
                ret = await producer.send(message)
                print("Message sent ret:", ret)
                index += 1
            except ProducerError as e:
                print(f"Failed to send message: {e}")
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)

    except FileNotFoundError as e:
        print(f"配置文件错误: {e}")
        print("请确保config.json文件存在且包含必需的配置项")
        sys.exit(1)
    except Exception as e:
        print(f"程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
