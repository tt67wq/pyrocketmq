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

from config_loader import load_config, parse_config_file_path

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
    try:
        # 从命令行参数解析配置文件路径
        config_path = parse_config_file_path()
        config = load_config(config_path)
        print(f"已加载配置文件: {config_path}")
        print(
            f"Topic: {config.topic}, Group: {config.group}, Nameserver: {config.nameserver}"
        )

        # 设置日志
        pyrocketmq.logging.setup_logging(LoggingConfig(level="DEBUG", json_output=True))

        print("=== 事务消息Producer示例 ===")

        # 创建事务监听器
        transaction_listener = create_simple_transaction_listener(True)

        # 创建事务Producer
        producer = create_transactional_producer(
            config.group,
            config.nameserver,
            transaction_listener=transaction_listener,
        )

        # 启动Producer
        producer.start()
        print("事务Producer启动成功")

        index = 0
        while 1:
            try:
                index += 1
                message = Message(
                    topic=config.topic,
                    body=f"Hello, RocketMQ From Python's Transactional Producer - Message #{index}".encode(),
                )

                # 设置标签和键
                if config.tag:
                    tags = config.tag.split("||")
                    message.set_tags(tags[index % len(tags)])
                if config.keys:
                    message.set_keys(f"{config.keys}_{index}")
                else:
                    message.set_keys(f"TXN_MSG_KEY_{index}")

                ret = producer.send_message_in_transaction(message)
                print(f"Message {index} sent successfully")
                print("offset_msg_id=", ret.offset_msg_id)
                print("msg_id=", ret.msg_id)
                print("state=", ret.status)
                print("queue=", ret.message_queue)
                print("---")

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
