#!/usr/bin/env python3
"""
query_consumer_offset 方法的单元测试
"""

# 添加src目录到Python路径
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pyrocketmq.broker.client import BrokerClient
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerResponseError,
    OffsetError,
)
from pyrocketmq.model import RemotingCommand
from pyrocketmq.model.enums import ResponseCode
from pyrocketmq.remote.sync_remote import Remote


class TestQueryConsumerOffset(unittest.TestCase):
    """query_consumer_offset 方法的单元测试"""

    def setUp(self):
        """测试设置"""
        # 创建模拟的 Remote 实例
        self.mock_remote = MagicMock(spec=Remote)
        self.mock_remote.is_connected = True

        # 创建 BrokerClient 实例
        self.client = BrokerClient(remote=self.mock_remote, timeout=30.0)

        # 测试参数
        self.consumer_group = "test_consumer_group"
        self.topic = "test_topic"
        self.queue_id = 0

    def test_query_consumer_offset_success(self):
        """测试成功查询消费者偏移量"""
        # 模拟成功响应，偏移量在 ext_fields 中
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": "12345"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果
        self.assertEqual(offset, 12345)
        self.mock_remote.rpc.assert_called_once()

    def test_query_consumer_offset_success_no_ext_fields(self):
        """测试成功响应但没有ext_fields的情况"""
        # 模拟成功响应但没有ext_fields
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果，应该返回0
        self.assertEqual(offset, 0)

    def test_query_consumer_offset_success_no_offset_field(self):
        """测试成功响应但ext_fields中没有offset字段的情况"""
        # 模拟成功响应但ext_fields中没有offset字段
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"other_field": "value"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果，应该返回0
        self.assertEqual(offset, 0)

    def test_query_consumer_offset_query_not_found(self):
        """测试查询不到偏移量的情况"""
        # 模拟QUERY_NOT_FOUND响应
        mock_response = RemotingCommand(
            code=ResponseCode.QUERY_NOT_FOUND,
            remark="Consumer offset not found",
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出OffsetError
        with self.assertRaises(OffsetError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Consumer offset not found", str(context.exception))
        self.assertEqual(context.exception.topic, self.topic)
        self.assertEqual(context.exception.queue_id, self.queue_id)

    def test_query_consumer_offset_topic_not_exist(self):
        """测试主题不存在的情况"""
        # 模拟TOPIC_NOT_EXIST响应
        mock_response = RemotingCommand(
            code=ResponseCode.TOPIC_NOT_EXIST, remark="Topic not exist"
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出BrokerResponseError
        with self.assertRaises(BrokerResponseError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Topic not exist", str(context.exception))
        self.assertEqual(
            context.exception.response_code, ResponseCode.TOPIC_NOT_EXIST
        )

    def test_query_consumer_offset_general_error(self):
        """测试通用错误的情况"""
        # 模拟ERROR响应
        mock_response = RemotingCommand(
            code=ResponseCode.ERROR,
            remark="Consumer group not exist",
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出BrokerResponseError
        with self.assertRaises(BrokerResponseError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Query consumer offset error", str(context.exception))
        self.assertEqual(
            context.exception.response_code,
            ResponseCode.ERROR,
        )

    def test_query_consumer_offset_service_not_available(self):
        """测试服务不可用的情况"""
        # 模拟SERVICE_NOT_AVAILABLE响应
        mock_response = RemotingCommand(
            code=ResponseCode.SERVICE_NOT_AVAILABLE,
            remark="Service unavailable",
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出BrokerResponseError
        with self.assertRaises(BrokerResponseError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Service not available", str(context.exception))
        self.assertEqual(
            context.exception.response_code, ResponseCode.SERVICE_NOT_AVAILABLE
        )

    def test_query_consumer_offset_flush_disk_timeout(self):
        """测试刷盘超时的情况"""
        # 模拟FLUSH_DISK_TIMEOUT响应
        mock_response = RemotingCommand(
            code=ResponseCode.FLUSH_DISK_TIMEOUT, remark="Flush disk timeout"
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出BrokerResponseError
        with self.assertRaises(BrokerResponseError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Query consumer offset failed", str(context.exception))
        self.assertEqual(
            context.exception.response_code, ResponseCode.FLUSH_DISK_TIMEOUT
        )

    def test_query_consumer_offset_not_connected(self):
        """测试未连接状态下的调用"""
        # 设置未连接状态
        self.mock_remote.is_connected = False

        # 调用方法，应该抛出BrokerConnectionError
        with self.assertRaises(BrokerConnectionError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Not connected to Broker", str(context.exception))

    def test_query_consumer_offset_invalid_offset_value(self):
        """测试ext_fields中offset值格式错误的情况"""
        # 模拟成功响应但offset格式错误
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": "invalid_number"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出OffsetError
        with self.assertRaises(OffsetError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn(
            "Failed to parse offset from ext_fields", str(context.exception)
        )
        self.assertEqual(context.exception.topic, self.topic)
        self.assertEqual(context.exception.queue_id, self.queue_id)

    def test_query_consumer_offset_offset_none_value(self):
        """测试ext_fields中offset值为None的情况"""
        # 模拟成功响应但offset为None
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": None}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出OffsetError
        with self.assertRaises(OffsetError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn(
            "Failed to parse offset from ext_fields", str(context.exception)
        )

    def test_query_consumer_offset_unknown_error(self):
        """测试未知错误响应代码的情况"""
        # 模拟未知错误响应
        mock_response = RemotingCommand(
            code=999,  # 未知的响应代码
            remark="Unknown error",
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法，应该抛出BrokerResponseError
        with self.assertRaises(BrokerResponseError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn("Query consumer offset failed", str(context.exception))
        self.assertEqual(context.exception.response_code, 999)

    def test_query_consumer_offset_unexpected_exception(self):
        """测试意外异常的情况"""
        # 模拟rpc调用抛出异常
        self.mock_remote.rpc.side_effect = RuntimeError("Network error")

        # 调用方法，应该抛出OffsetError
        with self.assertRaises(OffsetError) as context:
            self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

        # 验证异常信息
        self.assertIn(
            "Unexpected error during query_consumer_offset",
            str(context.exception),
        )
        self.assertEqual(context.exception.topic, self.topic)
        self.assertEqual(context.exception.queue_id, self.queue_id)

    def test_query_consumer_offset_large_offset_value(self):
        """测试大偏移量值的处理"""
        # 模拟大偏移量值
        large_offset = 9223372036854775807  # 64位整数最大值
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": str(large_offset)}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果
        self.assertEqual(offset, large_offset)

    def test_query_consumer_offset_zero_value(self):
        """测试偏移量为0的情况"""
        # 模拟偏移量为0
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": "0"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果
        self.assertEqual(offset, 0)

    def test_query_consumer_offset_negative_value(self):
        """测试负偏移量值的情况"""
        # 模拟负偏移量值（虽然在实际场景中不太可能）
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": "-1"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果
        self.assertEqual(offset, -1)

    @patch("pyrocketmq.broker.client.time")
    def test_query_consumer_offset_timing(self, mock_time):
        """测试查询耗时计算"""
        # 模拟时间
        mock_time.time.side_effect = [1000.0, 1001.5]  # 查询耗时1.5秒

        # 模拟成功响应
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": "100"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 调用方法
        offset = self.client.query_consumer_offset(
            consumer_group=self.consumer_group,
            topic=self.topic,
            queue_id=self.queue_id,
        )

        # 验证结果
        self.assertEqual(offset, 100)
        # 验证时间调用
        self.assertEqual(mock_time.time.call_count, 2)

    def test_query_consumer_offset_request_creation(self):
        """测试请求创建的正确性"""
        # 模拟成功响应
        mock_response = RemotingCommand(
            code=ResponseCode.SUCCESS, ext_fields={"offset": "200"}
        )
        self.mock_remote.rpc.return_value = mock_response

        # 使用patch验证请求创建
        with patch(
            "pyrocketmq.broker.client.RemotingRequestFactory.create_query_consumer_offset_request"
        ) as mock_factory:
            mock_request = MagicMock()
            mock_factory.return_value = mock_request

            # 调用方法
            offset = self.client.query_consumer_offset(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

            # 验证工厂方法被正确调用
            mock_factory.assert_called_once_with(
                consumer_group=self.consumer_group,
                topic=self.topic,
                queue_id=self.queue_id,
            )

            # 验证远程调用使用了正确的请求
            self.mock_remote.rpc.assert_called_once_with(
                mock_request, timeout=30.0
            )

            # 验证结果
            self.assertEqual(offset, 200)


if __name__ == "__main__":
    unittest.main()
