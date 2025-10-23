"""
Producer MVP版本测试

测试Producer的基本功能，包括：
- 生命周期管理
- 消息发送
- 错误处理
- 统计信息

作者: pyrocketmq团队
版本: MVP 1.0
"""

import pytest

from pyrocketmq.model.message import Message
from pyrocketmq.producer.errors import ProducerStateError
from pyrocketmq.producer.producer import Producer, SendResult


class TestProducerMVP:
    """Producer MVP版本测试"""

    def test_producer_lifecycle(self):
        """测试Producer生命周期"""
        producer = Producer()

        # 初始状态
        assert not producer.is_running()

        # 启动
        producer.start()
        assert producer.is_running()

        # 重复启动（幂等性）
        producer.start()
        assert producer.is_running()

        # 关闭
        producer.shutdown()
        assert not producer.is_running()

        # 重复关闭（幂等性）
        producer.shutdown()
        assert not producer.is_running()

    def test_send_sync_running_check(self):
        """测试同步发送时的运行状态检查"""
        producer = Producer()
        message = Message(topic="test_topic", body=b"test")

        # 未启动状态下发送消息
        with pytest.raises(ProducerStateError, match="Producer is not running"):
            producer.send_sync(message)

    def test_send_oneway_running_check(self):
        """测试单向发送时的运行状态检查"""
        producer = Producer()
        message = Message(topic="test_topic", body=b"test")

        # 未启动状态下发送消息
        with pytest.raises(ProducerStateError, match="Producer is not running"):
            producer.send_oneway(message)

    def test_send_sync_success(self):
        """测试同步发送成功"""
        producer = Producer()
        producer.start()

        message = Message(topic="test_topic", body=b"test message")
        result = producer.send_sync(message)

        # 验证发送结果
        assert isinstance(result, SendResult)
        assert result.success
        assert result.message_id is not None
        assert result.topic == "test_topic"
        assert result.broker_name is not None
        assert result.queue_id is not None
        assert result.error is None
        assert result.send_timestamp is not None

        # 验证统计信息
        stats = producer.get_stats()
        assert stats["total_sent"] == 1
        assert stats["total_failed"] == 0

    def test_send_oneway_success(self):
        """测试单向发送成功"""
        producer = Producer()
        producer.start()

        message = Message(topic="test_topic", body=b"test message")
        # 单向发送不返回结果
        producer.send_oneway(message)

        # 验证统计信息（单向发送不计入统计）
        stats = producer.get_stats()
        assert stats["total_sent"] == 0
        assert stats["total_failed"] == 0

    def test_message_validation(self):
        """测试消息验证"""
        producer = Producer()
        producer.start()

        # 空消息
        with pytest.raises(ValueError, match="Message cannot be None"):
            producer.send_sync(None)

        # 空主题
        with pytest.raises(ValueError, match="Message topic cannot be empty"):
            producer.send_sync(Message(topic="", body=b"test"))

        # 空消息体
        with pytest.raises(ValueError, match="Message body cannot be empty"):
            producer.send_sync(Message(topic="test", body=b""))

        # 过长的主题
        long_topic = "a" * 128
        with pytest.raises(
            ValueError, match="Message topic length cannot exceed 127"
        ):
            producer.send_sync(Message(topic=long_topic, body=b"test"))

        # 过大的消息体
        large_body = b"a" * (4 * 1024 * 1024 + 1)  # 4MB + 1
        with pytest.raises(ValueError, match="Message body size"):
            producer.send_sync(Message(topic="test", body=large_body))

    def test_stats(self):
        """测试统计信息"""
        producer = Producer()
        producer.start()

        # 初始统计
        stats = producer.get_stats()
        assert stats["running"] is True
        assert stats["producer_group"] == "DEFAULT_PRODUCER"
        assert stats["client_id"] is not None
        assert stats["total_sent"] == 0
        assert stats["total_failed"] == 0
        assert stats["success_rate"] == "0.00%"

        # 发送消息
        message = Message(topic="test_topic", body=b"test")
        producer.send_sync(message)

        # 更新统计
        stats = producer.get_stats()
        assert stats["total_sent"] == 1
        assert stats["total_failed"] == 0
        assert stats["success_rate"] == "100.00%"

    def test_producer_string_representation(self):
        """测试Producer的字符串表示"""
        producer = Producer()
        producer.start()

        str_repr = str(producer)
        assert "Producer" in str_repr
        assert "DEFAULT_PRODUCER" in str_repr
        assert "running=True" in str_repr

    def test_send_result_creation(self):
        """测试SendResult的创建"""
        # 成功结果
        success_result = SendResult.success_result(
            message_id="test_id",
            topic="test_topic",
            broker_name="test_broker",
            queue_id=0,
        )
        assert success_result.success
        assert success_result.message_id == "test_id"
        assert success_result.topic == "test_topic"
        assert success_result.broker_name == "test_broker"
        assert success_result.queue_id == 0
        assert success_result.error is None

        # 失败结果
        error = Exception("test error")
        failure_result = SendResult.failure_result(
            topic="test_topic",
            error=error,
            message_id="test_id",
        )
        assert not failure_result.success
        assert failure_result.message_id == "test_id"
        assert failure_result.topic == "test_topic"
        assert failure_result.error == error

    def test_route_info_update(self):
        """测试路由信息更新"""
        producer = Producer()
        producer.start()

        # 尝试更新路由信息
        result = producer.update_route_info("test_topic")
        # TODO: 实现实际的路由更新逻辑
        # 目前只是检查函数调用的正确性
        assert isinstance(result, bool)

    def test_shutdown_with_active_operations(self):
        """测试在活跃操作时关闭Producer"""
        producer = Producer()
        producer.start()

        # 发送一些消息
        message = Message(topic="test_topic", body=b"test")
        producer.send_sync(message)
        producer.send_oneway(message)

        # 关闭Producer
        producer.shutdown()
        assert not producer.is_running()

    def test_multiple_instances(self):
        """测试多个Producer实例"""
        producer1 = Producer()
        producer2 = Producer()

        # 启动两个实例
        producer1.start()
        producer2.start()

        assert producer1.is_running()
        assert producer2.is_running()

        # 确保客户端ID不同
        stats1 = producer1.get_stats()
        stats2 = producer2.get_stats()
        assert stats1["client_id"] != stats2["client_id"]

        # 发送消息
        message = Message(topic="test_topic", body=b"test")
        result1 = producer1.send_sync(message)
        result2 = producer2.send_sync(message)

        assert result1.success
        assert result2.success
        assert result1.message_id != result2.message_id

        # 关闭两个实例
        producer1.shutdown()
        producer2.shutdown()

        assert not producer1.is_running()
        assert not producer2.is_running()
