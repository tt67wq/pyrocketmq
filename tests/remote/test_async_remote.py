#!/usr/bin/env python3
"""
测试AsyncRemote类的基本功能
"""

import asyncio

# 添加项目路径
from pyrocketmq.logging import LoggerFactory, LoggingConfig
from pyrocketmq.model import (
    FlagType,
    LanguageCode,
    RemotingRequestFactory,
    RequestCode,
)
from pyrocketmq.model.factory import RemotingCommandBuilder
from pyrocketmq.remote.async_remote import AsyncRemote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig

LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))
logger = LoggerFactory.get_logger("test_async")


async def test_async_remote_connection():
    """测试AsyncRemote类的基本连接功能"""
    logger.info("开始测试AsyncRemote连接...")

    # 创建配置
    transport_config = TransportConfig(
        host="d1-dmq-namesrv.shizhuang-inc.net",
        port=31110,
        timeout=5.0,
        max_retries=0,  # 不重连
    )

    config = RemoteConfig(
        rpc_timeout=10.0,
        transport_config=transport_config,
    )

    # 创建AsyncRemote实例
    async with AsyncRemote(transport_config, config) as remote:
        try:
            # 等待一段时间，观察接收任务是否正常工作
            logger.info("连接成功，等待5秒观察接收任务...")
            await asyncio.sleep(5)

            # 检查连接状态
            logger.info(f"连接状态: {remote.is_connected}")
            logger.info(
                f"活跃等待者数量: {await remote.get_active_waiters_count()}"
            )

            # 测试发送一个请求（如果没有服务器，应该会失败）
            logger.info("测试发送请求...")
            try:
                command = RemotingCommandBuilder(
                    code=RequestCode.GET_BROKER_CLUSTER_INFO,
                    language=LanguageCode.PYTHON,
                    flag=FlagType.RPC_TYPE,
                ).build()

                response = await remote.rpc(command, timeout=5.0)
                logger.info(f"收到响应: {response}")

            except Exception as e:
                logger.info(f"发送请求失败（预期，因为没有服务器）: {e}")

            # 测试发送单向消息
            logger.info("测试发送单向消息...")
            try:
                oneway_command = (
                    RemotingRequestFactory.create_send_message_request(
                        topic="test_topic",
                        body=b"test message",
                        producer_group="test_group",
                    )
                )

                await remote.oneway(oneway_command)
                logger.info("单向消息发送成功")

            except Exception as e:
                logger.info(f"发送单向消息失败（预期，因为没有服务器）: {e}")

            # 再次检查状态
            logger.info(f"最终连接状态: {remote.is_connected}")
            logger.info(
                f"最终活跃等待者数量: {await remote.get_active_waiters_count()}"
            )

        except Exception as e:
            logger.error(f"测试过程中发生错误: {e}")
            raise


async def test_async_remote_timeout():
    """测试AsyncRemote超时处理"""
    logger.info("开始测试AsyncRemote超时处理...")

    # 创建配置
    transport_config = TransportConfig(
        host="127.0.0.1",  # 本地地址，假设没有服务器
        port=31110,
        timeout=5.0,
        max_retries=0,
    )

    config = RemoteConfig(
        rpc_timeout=2.0,  # 设置较短的超时时间
        transport_config=transport_config,
    )

    async with AsyncRemote(transport_config, config) as remote:
        try:
            # 测试超时
            logger.info("测试RPC超时...")
            start_time = asyncio.get_event_loop().time()

            try:
                command = RemotingCommandBuilder(
                    code=RequestCode.GET_BROKER_CLUSTER_INFO,
                    language=LanguageCode.PYTHON,
                    flag=FlagType.RPC_TYPE,
                ).build()
                response = await remote.rpc(command, timeout=1.0)
                logger.info(f"意外收到响应: {response}")
            except Exception as e:
                end_time = asyncio.get_event_loop().time()
                elapsed = end_time - start_time
                logger.info(f"请求超时，耗时: {elapsed:.2f}秒, 错误: {e}")

        except Exception as e:
            logger.error(f"超时测试过程中发生错误: {e}")


async def test_async_remote_stress():
    """测试AsyncRemote的并发处理能力"""
    logger.info("开始测试AsyncRemote并发处理...")

    # 创建配置
    transport_config = TransportConfig(
        host="d1-dmq-namesrv.shizhuang-inc.net",
        port=31110,
        timeout=5.0,
        max_retries=0,
    )

    config = RemoteConfig(
        rpc_timeout=5.0,
        max_waiters=100,
        transport_config=transport_config,
    )

    async with AsyncRemote(transport_config, config) as remote:
        try:
            # 创建多个并发请求
            tasks = []
            for i in range(5):
                logger.info(f"创建并发任务 {i + 1}")
                task = asyncio.create_task(
                    send_concurrent_request(remote, i + 1)
                )
                tasks.append(task)

            # 等待所有任务完成
            results = await asyncio.gather(*tasks, return_exceptions=True)

            logger.info(
                f"并发测试完成，结果: {len([r for r in results if not isinstance(r, Exception)])} 成功, {len([r for r in results if isinstance(r, Exception)])} 失败"
            )

        except Exception as e:
            logger.error(f"并发测试过程中发生错误: {e}")


async def send_concurrent_request(remote: AsyncRemote, task_id: int):
    """发送并发请求的辅助函数"""
    try:
        logger.info(f"任务 {task_id} 开始发送请求...")
        command = RemotingCommandBuilder(
            code=RequestCode.GET_BROKER_CLUSTER_INFO,
            language=LanguageCode.PYTHON,
            flag=FlagType.RPC_TYPE,
        ).build()
        response = await remote.rpc(command, timeout=3.0)
        logger.info(f"任务 {task_id} 收到响应")
        return response
    except Exception as e:
        logger.info(f"任务 {task_id} 失败: {e}")
        raise


async def main():
    """主测试函数"""
    logger.info("开始AsyncRemote测试套件...")

    try:
        # 基本连接测试
        await test_async_remote_connection()
        logger.info("=" * 50)

        # 超时测试
        # await test_async_remote_timeout()
        # logger.info("=" * 50)

        # # 并发测试
        # await test_async_remote_stress()
        # logger.info("=" * 50)

        logger.info("所有测试完成!")

    except Exception as e:
        logger.error(f"测试套件失败: {e}")


if __name__ == "__main__":
    asyncio.run(main())
