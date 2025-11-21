#!/usr/bin/env python3

import asyncio

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import create_async_producer
from pyrocketmq.producer.errors import ProducerError


async def main():
    """主函数"""
    pyrocketmq.logging.setup_logging(LoggingConfig(level="DEBUG"))
    producer = create_async_producer(
        "GID_POETRY", "d1-dmq-namesrv.shizhuang-inc.net:31110"
    )
    await producer.start()
    index = 0
    while True:
        try:
            message = Message(
                topic="test_im_015",
                body=f"This {index} Message: Hello, RocketMQ From Python!".encode(),
            )
            message.set_tags("TAG1")
            message.set_keys("KEYS")
            ret = await producer.send(message)
            print("Message sent ret:", ret)
            index += 1
        except ProducerError as e:
            print(f"Failed to send message: {e}")
            await asyncio.sleep(5)
        else:
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
