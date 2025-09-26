#!/usr/bin/env python3
"""简单的异步连接状态机测试"""

import asyncio
import os
import sys

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import AsyncConnectionStateMachine


async def test_basic_state_machine():
    """测试基本状态机功能"""
    print("开始测试基本异步状态机功能...")

    # 创建配置
    config = TransportConfig(
        host="127.0.0.1",
        port=9876,
        timeout=5.0,
        max_retries=3,
        retry_interval=1.0,
        keep_alive=True,
    )

    # 创建异步连接状态机
    state_machine = AsyncConnectionStateMachine(config)
    await state_machine.start()

    print(f"初始状态: {state_machine.current_state_name}")
    print(f"是否已断开: {state_machine.is_disconnected}")
    print(f"是否已连接: {state_machine.is_connected}")
    print(f"是否正在连接: {state_machine.is_connecting}")
    print(f"是否已关闭: {state_machine.is_closed}")

    # 测试状态属性
    assert not state_machine.is_disconnected
    assert state_machine.is_connected
    assert not state_machine.is_connecting
    assert not state_machine.is_closed

    print("基本状态机功能测试通过！")


if __name__ == "__main__":
    asyncio.run(test_basic_state_machine())
