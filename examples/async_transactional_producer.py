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
import signal

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
    # 设置日志
    pyrocketmq.logging.setup_logging(LoggingConfig(level="DEBUG", json_output=True))

    # 创建信号处理器用于优雅关闭
    shutdown_event = asyncio.Event()

    def signal_handler(signum, frame):
        print(f"\n收到信号 {signum}，准备关闭...")
        shutdown_event.set()

    # 注册信号处理器
    _ = signal.signal(signal.SIGINT, signal_handler)
    _ = signal.signal(signal.SIGTERM, signal_handler)

    print("=== 异步事务消息Producer示例 ===")

    # 创建异步事务监听器
    # 可以使用简单的实现或自定义实现
    transaction_listener = create_async_simple_transaction_listener(
        commit=True
    )  # 总是提交
    # transaction_listener = CustomAsyncTransactionListener()  # 自定义复杂逻辑

    # 创建异步事务Producer
    producer = create_async_transaction_producer(
        "GID_POETRY",
        "d1-dmq-namesrv.shizhuang-inc.net:31110",
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
                    topic="test_im_015",
                    body=f"Hello, RocketMQ From Python's Async Transactional Producer - Message #{message_count}".encode(
                        "utf-8"
                    ),
                )

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
