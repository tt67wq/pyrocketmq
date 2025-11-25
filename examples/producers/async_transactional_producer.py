#!/usr/bin/env python3
"""
异步事务消息Producer使用示例

这个示例展示了如何使用pyrocketmq AsyncTransactionProducer进行异步事务消息发送操作。
包括：
- 创建异步事务消息Producer实例
- 实现AsyncTransactionListener接口
- 异步发送事务消息
- 异步处理本地事务执行和状态回查
- 查看事务发送结果
- 正确关闭Producer

适用场景：
- 需要确保消息与本地事务一致性的异步场景
- 异步订单创建、库存扣减等业务场景
- 分布式事务消息处理
- 高并发异步处理环境

使用方法:
    export PYTHONPATH=/path/to/pyrocketmq/src
    python examples/async_transactional_producer.py
"""

import asyncio
import sys

from config_loader import load_config, parse_config_file_path

import pyrocketmq.logging
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import (
    create_async_transaction_producer,
)
from pyrocketmq.producer.errors import ProducerError
from pyrocketmq.producer.transaction import (
    create_async_simple_transaction_listener,
)


async def main():
    """异步主函数"""
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

        # 创建信号处理器用于优雅关闭
        shutdown_event = asyncio.Event()

        print("=== 异步事务消息Producer示例 ===")

        # 创建异步事务监听器
        transaction_listener = create_async_simple_transaction_listener(commit=True)

        # 创建异步事务Producer
        producer = create_async_transaction_producer(
            config.group,
            config.nameserver,
            transaction_listener=transaction_listener,
        )

        try:
            # 启动Producer
            await producer.start()
            print("异步事务Producer启动成功")

            # 消息计数器
            message_count = 0

            # 主循环：异步发送事务消息
            while not shutdown_event.is_set():
                try:
                    message_count += 1
                    message = Message(
                        topic=config.topic,
                        body=f"Hello, RocketMQ From Python's Async Transactional Producer - Message #{message_count}".encode(
                            "utf-8"
                        ),
                    )

                    # 设置标签和键
                    if config.tag:
                        tags = config.tag.split("||")
                        message.set_tags(tags[message_count % len(tags)])
                    if config.keys:
                        message.set_keys(f"{config.keys}_{message_count}")
                    else:
                        message.set_keys(f"ASYNC_TXN_MSG_KEY_{message_count}")

                    # 异步发送事务消息
                    ret = await producer.send_message_in_transaction(message)
                    print(
                        f"消息 #{message_count} 发送成功: transaction_id={ret.transaction_id}, status={ret.status_name}"
                    )

                    # 控制发送频率
                    await asyncio.sleep(1)

                except ProducerError as e:
                    print(f"消息 #{message_count} 发送失败: {e}")
                    # 错误时等待更长时间再重试
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            print("收到取消信号，准备关闭...")

        except KeyboardInterrupt:
            print("收到键盘中断，准备关闭...")

        except Exception as e:
            print(f"发生未预期错误: {e}")

        finally:
            # 优雅关闭Producer
            try:
                print("正在关闭异步事务Producer...")
                await producer.shutdown()
                print("异步事务Producer已关闭")
            except Exception as e:
                print(f"关闭Producer时发生错误: {e}")

    except FileNotFoundError as e:
        print(f"配置文件错误: {e}")
        print("请确保config.json文件存在且包含必需的配置项")
        sys.exit(1)
    except Exception as e:
        print(f"程序运行错误: {e}")
        sys.exit(1)


async def run_with_timeout(timeout: int = 60):
    """带超时控制的运行函数"""
    try:
        await asyncio.wait_for(main(), timeout=timeout)
    except asyncio.TimeoutError:
        print(f"运行时间达到 {timeout} 秒，自动退出")


if __name__ == "__main__":
    try:
        # 运行异步主函数
        asyncio.run(main())
    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:
        print(f"程序运行异常: {e}")
        sys.exit(1)
