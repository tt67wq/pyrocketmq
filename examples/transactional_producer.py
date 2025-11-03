#!/usr/bin/env python3
"""
事务消息Producer使用示例

这个示例展示了如何使用pyrocketmq TransactionProducer进行事务消息发送操作。
包括：
- 创建事务消息Producer实例
- 实现TransactionListener接口
- 发送事务消息
- 处理本地事务执行和状态回查
- 查看事务发送结果
- 正确关闭Producer

适用场景：
- 需要确保消息与本地事务一致性的场景
- 订单创建、库存扣减等业务场景
- 分布式事务消息处理
- 开发环境测试

使用方法:
    export PYTHONPATH=/path/to/pyrocketmq/src
    python examples/transactional_producer.py
"""

import sys
import time

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import (
    create_transactional_producer,
)
from pyrocketmq.producer.errors import ProducerError
from pyrocketmq.producer.transaction import create_simple_transaction_listener


def main():
    """主函数"""
    # 设置日志
    pyrocketmq.logging.setup_logging(LoggingConfig(level="DEBUG"))

    print("=== 事务消息Producer示例 ===")

    # 创建事务监听器
    transaction_listener = create_simple_transaction_listener(True)

    # 创建事务Producer
    producer = create_transactional_producer(
        "GID_POETRY",
        "d1-dmq-namesrv.shizhuang-inc.net:31110",
        transaction_listener=transaction_listener,
    )

    # 启动Producer
    producer.start()
    print("事务Producer启动成功")

    while 1:
        try:
            message = Message(
                topic="test_im_015",
                body=b"Hello, RocketMQ From Python's Transactional Producer",
            )

            ret = producer.send_message_in_transaction(message)
            print("Message sent ret:", ret)
        except ProducerError as e:
            print(f"Failed to send message: {e}")
            time.sleep(5)
        else:
            time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
