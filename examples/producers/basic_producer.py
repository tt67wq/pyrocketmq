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

import sys
import time

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import create_producer
from pyrocketmq.producer.errors import ProducerError


def main():
    """主函数"""
    pyrocketmq.logging.setup_logging(
        LoggingConfig(level="INFO", json_output=False, file_path="producer.log")
    )
    producer = create_producer("GID_POETRY", "d1-dmq-namesrv.shizhuang-inc.net:31110")
    producer.start()
    index = 0
    while True:
        try:
            message = Message(
                topic="test_im_015",
                body=f"Hello, This Is Single Msg From Python {index}".encode(),
            )
            message.set_tags("TAG1||TAG2")
            message.set_keys("KEY1 KEY2")
            ret = producer.send(message)
            print("Message sent ret:", ret)
            index += 1

            messages: list[Message] = []
            for _ in range(10):
                message = Message(
                    topic="test_im_015",
                    body=f"Hello, This Is Batch Msg From Python {index}".encode(),
                )
                messages.append(message)
                index += 1
            ret = producer.send_batch(*messages)
            print("Batch message sent ret:", ret)
        except ProducerError as e:
            print(f"Failed to send message: {e}")
            time.sleep(5)
        else:
            time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
