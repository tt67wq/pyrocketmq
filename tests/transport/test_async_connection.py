#!/usr/bin/env python3
"""异步连接状态机测试"""

import asyncio
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import AsyncConnectionStateMachine


async def test_async_connection():
    """测试异步连接状态机"""
    print("开始测试异步连接状态机...")

    # 创建配置
    config = TransportConfig(
        host="127.0.0.1",
        port=9876,  # RocketMQ默认端口
        timeout=5.0,
        max_retries=3,
        retry_interval=1.0,
        keep_alive=True,
    )

    # 创建异步连接状态机
    state_machine = AsyncConnectionStateMachine(config)

    # 激活初始状态
    await state_machine.start()

    print(f"初始状态: {state_machine.current_state_name}")
    print(f"是否已断开: {state_machine.is_disconnected}")
    print(f"是否已连接: {state_machine.is_connected}")
    print(f"是否正在连接: {state_machine.is_connecting}")
    print(f"是否已关闭: {state_machine.is_closed}")

    # 由于没有实际的RocketMQ服务器运行，连接会失败
    # 但我们可以测试状态机的状态转换逻辑
    await asyncio.sleep(0.1)
    print(f"等待后状态: {state_machine.current_state_name}")

    # 测试关闭
    await state_machine.stop()
    print(f"关闭后状态: {state_machine.current_state_name}")

    print("异步连接状态机测试完成！")


if __name__ == "__main__":
    asyncio.run(test_async_connection())
