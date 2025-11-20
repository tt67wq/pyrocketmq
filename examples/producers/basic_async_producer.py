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
    while True:
        try:
            message = Message(topic="test_im_015", body=b"Hello, RocketMQ From Python!")
            ret = await producer.send(message)
            print("Message sent ret:", ret)
        except ProducerError as e:
            print(f"Failed to send message: {e}")
            await asyncio.sleep(5)
        else:
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
