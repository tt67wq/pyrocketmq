"""
测试批量消息发送功能
"""

import os
import sys

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from pyrocketmq.model import MessageProperty
from pyrocketmq.model.enums import FlagType, LanguageCode, RequestCode
from pyrocketmq.model.factory import RemotingRequestFactory


class TestBatchSendMessage:
    """测试批量消息发送功能"""

    def test_create_send_batch_message_request(self):
        """测试创建发送批量消息请求"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Batch message content\nMessage2\nMessage3"
        queue_id = 1
        properties = {"batch": "true", "key1": "value1", "key2": "value2"}
        tags = "batch_tag"
        keys = "batch_key1,batch_key2"

        command = RemotingRequestFactory.create_send_batch_message_request(
            producer_group=producer_group,
            topic=topic,
            body=body,
            queue_id=queue_id,
            properties=properties,
            tags=tags,
            keys=keys,
        )

        # 验证基本信息
        assert command.code == RequestCode.SEND_BATCH_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body

        # 验证扩展字段
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)
        assert command.ext_fields["batch"] == "true"
        assert command.ext_fields["tags"] == tags
        assert command.ext_fields["keys"] == keys

        # 验证properties已被序列化
        serialized_properties = command.ext_fields["properties"]
        assert "batch" in serialized_properties
        assert "true" in serialized_properties
        assert "key1" in serialized_properties
        assert "value1" in serialized_properties
        assert "key2" in serialized_properties
        assert "value2" in serialized_properties

    def test_create_send_batch_message_request_without_properties(self):
        """测试创建发送批量消息请求（无properties）"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Simple batch message"

        command = RemotingRequestFactory.create_send_batch_message_request(
            producer_group=producer_group,
            topic=topic,
            body=body,
        )

        # 验证基本信息
        assert command.code == RequestCode.SEND_BATCH_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body

        # 验证扩展字段
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["batch"] == "true"
        assert command.ext_fields["queueId"] == "0"

        # 验证properties为空字符串
        assert command.ext_fields["properties"] == ""

    def test_create_send_batch_message_request_with_transaction_property(self):
        """测试创建发送批量消息请求（包含事务属性）"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Transactional batch message"
        properties = {
            MessageProperty.TRANSACTION_PREPARED: "true",
            "custom_prop": "custom_value",
        }

        command = RemotingRequestFactory.create_send_batch_message_request(
            producer_group=producer_group,
            topic=topic,
            body=body,
            properties=properties,
        )

        # 验证基本信息
        assert command.code == RequestCode.SEND_BATCH_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body

        # 验证扩展字段
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["batch"] == "true"

        # 验证事务属性已被序列化
        serialized_properties = command.ext_fields["properties"]
        assert MessageProperty.TRANSACTION_PREPARED in serialized_properties
        assert "true" in serialized_properties
        assert "custom_prop" in serialized_properties
        assert "custom_value" in serialized_properties

    def test_create_send_batch_message_request_all_parameters(self):
        """测试创建发送批量消息请求（所有参数）"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Complete batch message test"
        queue_id = 2
        properties = {
            "key1": "value1",
            "key2": "value2",
            "timestamp": "1234567890",
        }
        tags = "complete_tag"
        keys = "key1,key2,key3"

        command = RemotingRequestFactory.create_send_batch_message_request(
            producer_group=producer_group,
            topic=topic,
            body=body,
            queue_id=queue_id,
            properties=properties,
            tags=tags,
            keys=keys,
        )

        # 验证所有参数都被正确设置
        assert command.code == RequestCode.SEND_BATCH_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)
        assert command.ext_fields["batch"] == "true"
        assert command.ext_fields["tags"] == tags
        assert command.ext_fields["keys"] == keys

        # 验证所有properties都被序列化
        serialized_properties = command.ext_fields["properties"]
        for key, value in properties.items():
            assert key in serialized_properties
            assert value in serialized_properties
