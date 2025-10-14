#!/usr/bin/env python3
"""
示例：使用processors机制处理服务器主动通知
"""

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
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.sync_remote import Remote
from pyrocketmq.transport.config import TransportConfig


def heartbeat_processor(
    request: RemotingCommand, addr: tuple
) -> RemotingCommand:
    """心跳请求处理器"""
    print(
        f"收到心跳请求: code={request.code}, from={addr}, remark={request.remark}"
    )

    # 创建响应
    response = RemotingCommand(
        code=ResponseCode.SUCCESS,
        language=LanguageCode.PYTHON,
        remark="Heartbeat processed",
    )
    return response


def consumer_info_processor(request: RemotingCommand, addr: tuple) -> None:
    """消费者信息处理器（不返回响应）"""
    print(f"收到消费者信息查询: code={request.code}, from={addr}")
    # 这里可以只记录信息，不需要响应
    return None


def main():
    """主函数"""
    print("=== Remote Processors 示例 ===")

    # 创建配置
    transport_config = TransportConfig(host="localhost", port=9876)
    remote_config = RemoteConfig()

    # 创建Remote实例
    remote = Remote(transport_config, remote_config)

    try:
        # 注册处理器
        print("注册请求处理器...")
        remote.register_request_processor(
            RequestCode.HEART_BEAT, heartbeat_processor
        )
        remote.register_request_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO, consumer_info_processor
        )

        print("处理器注册完成:")
        print(f"  - HEART_BEAT: {heartbeat_processor.__name__}")
        print(
            f"  - GET_CONSUMER_RUNNING_INFO: {consumer_info_processor.__name__}"
        )

        # 注意：由于我们只是演示processors机制，
        # 实际运行需要连接到真实的RocketMQ服务器
        print(
            "\n注意：此示例需要连接到真实的RocketMQ服务器才能测试processors功能"
        )
        print("可以通过以下方式测试:")
        print("1. 启动RocketMQ服务器")
        print("2. 修改host/port配置")
        print("3. 运行此示例")
        print("4. 服务器发送心跳或消费者信息查询时，会触发对应的处理器")

        # 取消注册处理器示例
        print("\n演示取消注册处理器...")
        result = remote.unregister_request_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO
        )
        print(f"取消注册GET_CONSUMER_RUNNING_INFO: {result}")

    except Exception as e:
        print(f"示例运行失败: {e}")

    print("\n=== 示例结束 ===")


if __name__ == "__main__":
    main()
