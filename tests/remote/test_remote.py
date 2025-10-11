#!/usr/bin/env python3
"""
测试Remote类的基本功能
"""

import time

# 添加项目路径
from pyrocketmq.logging import LoggerFactory, LoggingConfig
from pyrocketmq.model import RemotingRequestFactory
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.sync_remote import Remote
from pyrocketmq.transport.config import TransportConfig

LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))
logger = LoggerFactory.get_logger("test")


def test_remote_connection():
    """测试Remote类的基本连接功能"""
    logger.info("开始测试Remote连接...")

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

    # 创建Remote实例
    remote = Remote(transport_config, config)

    try:
        # 测试连接
        logger.info("尝试连接...")
        remote.connect()

        # 等待一段时间，观察接收线程是否正常工作
        logger.info("连接成功，等待5秒观察接收线程...")
        time.sleep(5)

        # 检查连接状态
        logger.info(f"连接状态: {remote.is_connected}")
        logger.info(f"活跃等待者数量: {remote.active_waiters_count}")

        # 测试发送一个请求（如果没有服务器，应该会失败）
        logger.info("测试发送请求...")
        try:
            command = RemotingRequestFactory.create_get_broker_info_request()

            response = remote.rpc(command, timeout=5.0)
            print(response)
            print(response.body)
            logger.info(f"收到响应: {response}")

        except Exception as e:
            logger.info(f"发送请求失败（预期，因为没有服务器）: {e}")

    finally:
        # 关闭连接
        logger.info("关闭连接...")
        remote.close()


if __name__ == "__main__":
    test_remote_connection()
