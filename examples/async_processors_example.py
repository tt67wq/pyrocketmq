#!/usr/bin/env python3
"""
示例：使用异步processors机制处理服务器主动通知
"""

import asyncio
import os
import sys

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyrocketmq.model import (
    LanguageCode,
    RemotingCommand,
    RequestCode,
    ResponseCode,
)
from pyrocketmq.remote.async_remote import AsyncRemote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig


async def async_heartbeat_processor(
    request: RemotingCommand, addr: tuple
) -> RemotingCommand:
    """异步心跳请求处理器"""
    print(f"收到心跳请求: code={request.code}, from={addr}")

    # 模拟异步操作（如数据库查询、网络请求等）
    await asyncio.sleep(0.1)

    # 创建响应
    response = RemotingCommand(
        code=ResponseCode.SUCCESS,
        language=LanguageCode.PYTHON,
        remark="Async heartbeat processed",
    )
    return response


async def async_consumer_info_processor(
    request: RemotingCommand, addr: tuple
) -> None:
    """异步消费者信息处理器（不返回响应）"""
    print(f"收到消费者信息查询: code={request.code}, from={addr}")

    # 模拟异步操作
    await asyncio.sleep(0.05)

    # 记录信息，不需要响应
    print("异步处理完成")
    return None


async def sync_to_async_wrapper(
    request: RemotingCommand, addr: tuple
) -> RemotingCommand:
    """将同步函数包装为异步的示例"""

    def sync_heartbeat_handler(
        req: RemotingCommand, address: tuple
    ) -> RemotingCommand:
        # 模拟同步处理
        print(f"同步处理心跳请求: code={req.code}, from={address}")
        return RemotingCommand(
            code=ResponseCode.SUCCESS,
            language=LanguageCode.PYTHON,
            remark="Sync heartbeat processed (wrapped)",
        )

    # 在异步函数中调用同步函数
    return sync_heartbeat_handler(request, addr)


async def main():
    """主函数"""
    print("=== Async Remote Processors 示例 ===")

    # 创建配置
    transport_config = TransportConfig(host="localhost", port=9876)
    remote_config = RemoteConfig()

    # 创建AsyncRemote实例
    async_remote = AsyncRemote(transport_config, remote_config)

    try:
        # 注册异步处理器
        print("注册异步请求处理器...")
        await async_remote.register_request_processor(
            RequestCode.HEART_BEAT, async_heartbeat_processor
        )
        await async_remote.register_request_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO, async_consumer_info_processor
        )
        await async_remote.register_request_processor(
            RequestCode.PULL_MESSAGE, sync_to_async_wrapper
        )

        print("异步处理器注册完成:")
        print(f"  - HEART_BEAT: {async_heartbeat_processor.__name__} (async)")
        print(
            f"  - GET_CONSUMER_RUNNING_INFO: {async_consumer_info_processor.__name__} (async, no response)"
        )
        print(
            f"  - PULL_MESSAGE: {sync_to_async_wrapper.__name__} (sync wrapped in async)"
        )

        print(
            "\n注意：此示例需要连接到真实的RocketMQ服务器才能测试processors功能"
        )
        print("异步处理器的优势：")
        print("- 可以执行异步操作（数据库查询、网络请求等）")
        print("- 不会阻塞消息接收线程")
        print("- 支持并发处理多个请求")

        # 取消注册处理器示例
        print("\n演示取消注册处理器...")
        result = await async_remote.unregister_request_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO
        )
        print(f"取消注册GET_CONSUMER_RUNNING_INFO: {result}")

    except Exception as e:
        print(f"示例运行失败: {e}")

    print("\n=== 示例结束 ===")


if __name__ == "__main__":
    asyncio.run(main())
