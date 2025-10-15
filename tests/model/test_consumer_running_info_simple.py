"""
ConsumerRunningInfo encode 方法测试
简单版本，只测试核心功能
"""

import pytest

from pyrocketmq.model.client_data import (
    ConsumerRunningInfo,
    ConsumeStatus,
    ProcessQueueInfo,
)
from pyrocketmq.model.message_queue import MessageQueue


class TestConsumerRunningInfoEncodeSimple:
    """ConsumerRunningInfo encode 方法测试类（简单版）"""

    def test_encode_empty_info(self):
        """测试空信息的序列化"""
        info = ConsumerRunningInfo()
        encoded = info.encode()

        # 验证编码结果不为空
        assert encoded is not None
        assert len(encoded) > 0
        assert isinstance(encoded, bytes)

        # 验证可以解码为字符串
        json_str = encoded.decode("utf-8")
        assert isinstance(json_str, str)
        assert len(json_str) > 0

    def test_encode_with_properties(self):
        """测试包含属性的序列化"""
        info = ConsumerRunningInfo()
        info.set_property("consumerId", "test_client_123")
        info.set_property("consumeType", "PUSH")

        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证包含设置的属性（不考虑空格格式）
        assert "consumerId" in json_str
        assert "test_client_123" in json_str
        assert "consumeType" in json_str
        assert "PUSH" in json_str

    def test_encode_with_message_queue(self):
        """测试包含消息队列的序列化"""
        info = ConsumerRunningInfo()

        mq = MessageQueue(topic="test_topic", broker_name="broker1", queue_id=0)
        process_info = ProcessQueueInfo(commit_offset=1000)
        info.add_queue_info(mq, process_info)

        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证mqTable包含预期的内容
        assert "mqTable" in json_str
        assert "test_topic" in json_str
        assert "broker1" in json_str
        assert "commitOffset" in json_str
        assert "1000" in json_str

    def test_encode_with_status(self):
        """测试包含状态的序列化"""
        info = ConsumerRunningInfo()

        status = ConsumeStatus(
            pull_rt=25.6,
            pull_tps=200.0,
            consume_rt=30.2,
            consume_ok_tps=198.5,
            consume_failed_tps=1.5,
            consume_failed_msgs=25,
        )
        info.add_status("test_topic", status)

        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证状态表包含预期的内容
        assert "statusTable" in json_str
        assert "pullRT" in json_str
        assert "25.6" in json_str
        assert "pullTPS" in json_str
        assert "200.0" in json_str

    def test_encode_complex_scenario(self):
        """测试复杂场景的编码"""
        info = ConsumerRunningInfo()

        # 设置属性
        info.set_property("clientId", "complex_test")
        info.set_property("consumeType", "PUSH")

        # 添加消息队列
        mq = MessageQueue(topic="topic_a", broker_name="broker_a", queue_id=0)
        process_info = ProcessQueueInfo(
            commit_offset=5000, cached_msg_count=100, locked=True
        )
        info.add_queue_info(mq, process_info)

        # 添加状态
        status = ConsumeStatus(pull_rt=25.6, pull_tps=200.0)
        info.add_status("topic_a", status)

        # 执行编码
        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证数据完整性
        assert "clientId" in json_str
        assert "complex_test" in json_str
        assert "consumeType" in json_str
        assert "PUSH" in json_str
        assert "topic_a" in json_str
        assert "broker_a" in json_str
        assert "5000" in json_str
        assert "100" in json_str
        assert "25.6" in json_str
        assert "200.0" in json_str

    def test_encode_unicode_content(self):
        """测试Unicode内容的编码"""
        info = ConsumerRunningInfo()
        info.set_property("中文属性", "中文值")
        info.set_property("emoji", "🚀🎉")

        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证Unicode内容正确处理
        assert "中文属性" in json_str
        assert "中文值" in json_str
        assert "emoji" in json_str
        assert "🚀🎉" in json_str

    def test_encode_error_handling(self):
        """测试错误处理"""
        info = ConsumerRunningInfo()

        # 正常情况不应该抛出异常
        try:
            encoded = info.encode()
            assert encoded is not None
            assert len(encoded) > 0
        except Exception as e:
            pytest.fail(f"encode() should not raise exception: {e}")

    def test_encode_structure_elements(self):
        """测试编码包含所有必要的结构元素"""
        info = ConsumerRunningInfo()
        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证包含所有顶级字段
        assert "properties" in json_str
        assert "statusTable" in json_str
        assert "subscriptionSet" in json_str
        assert "mqTable" in json_str

    def test_encode_multiple_properties(self):
        """测试编码多个属性"""
        info = ConsumerRunningInfo()

        # 添加多个属性
        info.set_property("key1", "value1")
        info.set_property("key2", "value2")
        info.set_property("key3", "value3")

        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证所有属性都存在
        assert "key1" in json_str
        assert "value1" in json_str
        assert "key2" in json_str
        assert "value2" in json_str
        assert "key3" in json_str
        assert "value3" in json_str

    def test_encode_multiple_queues(self):
        """测试编码多个消息队列"""
        info = ConsumerRunningInfo()

        # 添加多个消息队列
        for i in range(3):
            mq = MessageQueue(
                topic=f"topic_{i}", broker_name=f"broker_{i}", queue_id=i
            )
            process_info = ProcessQueueInfo(commit_offset=i * 1000)
            info.add_queue_info(mq, process_info)

        encoded = info.encode()
        json_str = encoded.decode("utf-8")

        # 验证所有队列都存在
        for i in range(3):
            assert f"topic_{i}" in json_str
            assert f"broker_{i}" in json_str
            assert str(i * 1000) in json_str
