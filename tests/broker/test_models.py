"""
测试 Broker models 模块
"""

import os
import sys

import pytest

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from pyrocketmq.broker.models import (
    ConsumeFromWhere,
    ConsumerData,
    ConsumeType,
    MessageExt,
    MessageModel,
    MessageQueue,
    OffsetResult,
    ProducerData,
    PullMessageResult,
    SendMessageResult,
)


class TestMessageQueue:
    """测试MessageQueue类"""

    def test_message_queue_creation(self):
        """测试MessageQueue创建"""
        mq = MessageQueue(
            topic="test_topic", broker_name="broker-a", queue_id=0
        )
        assert mq.topic == "test_topic"
        assert mq.broker_name == "broker-a"
        assert mq.queue_id == 0

    def test_message_queue_str(self):
        """测试MessageQueue字符串表示"""
        mq = MessageQueue(
            topic="test_topic", broker_name="broker-a", queue_id=0
        )
        str_repr = str(mq)
        assert "test_topic" in str_repr
        assert "broker-a" in str_repr
        assert "0" in str_repr

    def test_message_queue_equality(self):
        """测试MessageQueue相等性比较"""
        mq1 = MessageQueue(
            topic="test_topic", broker_name="broker-a", queue_id=0
        )
        mq2 = MessageQueue(
            topic="test_topic", broker_name="broker-a", queue_id=0
        )
        mq3 = MessageQueue(
            topic="test_topic", broker_name="broker-a", queue_id=1
        )

        assert mq1 == mq2
        assert mq1 != mq3
        assert hash(mq1) == hash(mq2)
        assert hash(mq1) != hash(mq3)

    def test_message_queue_to_dict(self):
        """测试MessageQueue转字典"""
        mq = MessageQueue(
            topic="test_topic", broker_name="broker-a", queue_id=0
        )
        data = mq.to_dict()
        expected = {
            "topic": "test_topic",
            "brokerName": "broker-a",
            "queueId": 0,
        }
        assert data == expected

    def test_message_queue_from_dict(self):
        """测试从字典创建MessageQueue"""
        data = {"topic": "test_topic", "brokerName": "broker-a", "queueId": 0}
        mq = MessageQueue.from_dict(data)
        assert mq.topic == "test_topic"
        assert mq.broker_name == "broker-a"
        assert mq.queue_id == 0


class TestMessageExt:
    """测试MessageExt类"""

    def test_message_ext_creation(self):
        """测试MessageExt创建"""
        msg = MessageExt(
            topic="test_topic",
            body=b"test message",
            tags="test_tag",
            message_id="msg123",
        )
        assert msg.topic == "test_topic"
        assert msg.body == b"test message"
        assert msg.tags == "test_tag"
        assert msg.message_id == "msg123"
        assert msg.born_timestamp is not None

    def test_message_ext_properties(self):
        """测试MessageExt属性操作"""
        msg = MessageExt(topic="test", body=b"test")

        # 测试设置和获取属性
        msg.set_property("key1", "value1")
        assert msg.get_property("key1") == "value1"
        assert msg.has_property("key1") is True
        assert msg.has_property("nonexistent") is False
        assert msg.get_property("nonexistent", "default") == "default"

    def test_message_ext_to_dict(self):
        """测试MessageExt转字典"""
        msg = MessageExt(
            topic="test_topic",
            body=b"test message",
            tags="test_tag",
            message_id="msg123",
            queue_id=0,
            queue_offset=100,
        )
        msg.set_property("key1", "value1")

        data = msg.to_dict()
        assert data["topic"] == "test_topic"
        assert data["body"] == "test message"
        assert data["tags"] == "test_tag"
        assert data["msgId"] == "msg123"
        assert data["queueId"] == 0
        assert data["queueOffset"] == 100
        assert data["properties"]["key1"] == "value1"

    def test_message_ext_from_dict(self):
        """测试从字典创建MessageExt"""
        data = {
            "topic": "test_topic",
            "body": "test message",
            "tags": "test_tag",
            "msgId": "msg123",
            "queueId": 0,
            "queueOffset": 100,
            "properties": {"key1": "value1"},
            "reconsumeTimes": 1,
        }
        msg = MessageExt.from_dict(data)

        assert msg.topic == "test_topic"
        assert msg.body == b"test message"
        assert msg.tags == "test_tag"
        assert msg.message_id == "msg123"
        assert msg.queue_id == 0
        assert msg.queue_offset == 100
        assert msg.get_property("key1") == "value1"
        assert msg.reconsume_times == 1


class TestProducerData:
    """测试ProducerData类"""

    def test_producer_data_creation(self):
        """测试ProducerData创建"""
        producer = ProducerData(group_name="test_producer_group")
        assert producer.group_name == "test_producer_group"

    def test_producer_data_to_dict(self):
        """测试ProducerData转字典"""
        producer = ProducerData(group_name="test_group")
        data = producer.to_dict()
        expected = {"groupName": "test_group"}
        assert data == expected

    def test_producer_data_from_dict(self):
        """测试从字典创建ProducerData"""
        data = {"groupName": "test_group"}
        producer = ProducerData.from_dict(data)
        assert producer.group_name == "test_group"

    def test_producer_data_from_bytes(self):
        """测试从字节数据创建ProducerData"""
        data_str = "{'groupName': 'test_group'}"
        data_bytes = data_str.encode("utf-8")
        producer = ProducerData.from_bytes(data_bytes)
        assert producer.group_name == "test_group"


class TestConsumerData:
    """测试ConsumerData类"""

    def test_consumer_data_creation(self):
        """测试ConsumerData创建"""
        consumer = ConsumerData(
            group_name="test_consumer_group",
            consume_type=ConsumeType.PUSH,
            message_model=MessageModel.CLUSTERING,
            consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
        )
        assert consumer.group_name == "test_consumer_group"
        assert consumer.consume_type == ConsumeType.PUSH
        assert consumer.message_model == MessageModel.CLUSTERING
        assert (
            consumer.consume_from_where
            == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )

    def test_consumer_data_to_dict(self):
        """测试ConsumerData转字典"""
        consumer = ConsumerData(
            group_name="test_group",
            consume_type=ConsumeType.PUSH,
            message_model=MessageModel.CLUSTERING,
            consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            subscription_data={"topic1": "*"},
        )
        data = consumer.to_dict()
        expected = {
            "groupName": "test_group",
            "consumeType": ConsumeType.PUSH,
            "messageModel": MessageModel.CLUSTERING,
            "consumeFromWhere": ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            "subscriptionData": {"topic1": "*"},
        }
        assert data == expected

    def test_consumer_data_from_dict(self):
        """测试从字典创建ConsumerData"""
        data = {
            "groupName": "test_group",
            "consumeType": ConsumeType.PUSH,
            "messageModel": MessageModel.CLUSTERING,
            "consumeFromWhere": ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            "subscriptionData": {"topic1": "*"},
        }
        consumer = ConsumerData.from_dict(data)
        assert consumer.group_name == "test_group"
        assert consumer.consume_type == ConsumeType.PUSH
        assert consumer.message_model == MessageModel.CLUSTERING
        assert (
            consumer.consume_from_where
            == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )
        assert consumer.subscription_data == {"topic1": "*"}

    def test_consumer_data_from_bytes(self):
        """测试从字节数据创建ConsumerData"""
        data_str = "{'groupName': 'test_group', 'consumeType': 'PUSH', 'messageModel': 'CLUSTERING', 'consumeFromWhere': 'CONSUME_FROM_LAST_OFFSET', 'subscriptionData': {}}"
        data_bytes = data_str.encode("utf-8")
        consumer = ConsumerData.from_bytes(data_bytes)
        assert consumer.group_name == "test_group"
        assert consumer.consume_type == ConsumeType.PUSH
        assert consumer.message_model == MessageModel.CLUSTERING
        assert (
            consumer.consume_from_where
            == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )


class TestSendMessageResult:
    """测试SendMessageResult类"""

    def test_send_message_result_creation(self):
        """测试SendMessageResult创建"""
        result = SendMessageResult(
            msg_id="msg123", queue_id=0, queue_offset=100
        )
        assert result.msg_id == "msg123"
        assert result.queue_id == 0
        assert result.queue_offset == 100
        assert result.region_id == "DefaultRegion"

    def test_send_message_result_to_dict(self):
        """测试SendMessageResult转字典"""
        result = SendMessageResult(
            msg_id="msg123",
            queue_id=0,
            queue_offset=100,
            transaction_id="tx123",
        )
        data = result.to_dict()
        expected = {
            "msgId": "msg123",
            "queueId": 0,
            "queueOffset": 100,
            "regionId": "DefaultRegion",
            "transactionId": "tx123",
        }
        assert data == expected

    def test_send_message_result_from_dict(self):
        """测试从字典创建SendMessageResult"""
        data = {
            "msgId": "msg123",
            "queueId": 0,
            "queueOffset": 100,
            "regionId": "DefaultRegion",
        }
        result = SendMessageResult.from_dict(data)
        assert result.msg_id == "msg123"
        assert result.queue_id == 0
        assert result.queue_offset == 100
        assert result.region_id == "DefaultRegion"

    def test_send_message_result_from_bytes(self):
        """测试从字节数据创建SendMessageResult"""
        data_str = "{'msgId': 'msg123', 'queueId': 0, 'queueOffset': 100, 'regionId': 'DefaultRegion'}"
        data_bytes = data_str.encode("utf-8")
        result = SendMessageResult.from_bytes(data_bytes)
        assert result.msg_id == "msg123"
        assert result.queue_id == 0
        assert result.queue_offset == 100
        assert result.region_id == "DefaultRegion"


class TestPullMessageResult:
    """测试PullMessageResult类"""

    def test_pull_message_result_creation(self):
        """测试PullMessageResult创建"""
        messages = [
            MessageExt(topic="test", body=b"msg1"),
            MessageExt(topic="test", body=b"msg2"),
        ]
        result = PullMessageResult(
            messages=messages,
            next_begin_offset=200,
            min_offset=0,
            max_offset=300,
        )
        assert len(result.messages) == 2
        assert result.next_begin_offset == 200
        assert result.min_offset == 0
        assert result.max_offset == 300
        assert result.is_found is True
        assert result.message_count == 2

    def test_pull_message_result_empty(self):
        """测试空消息的PullMessageResult"""
        result = PullMessageResult(
            messages=[], next_begin_offset=100, min_offset=0, max_offset=100
        )
        assert result.is_found is False
        assert result.message_count == 0

    def test_pull_message_result_to_dict(self):
        """测试PullMessageResult转字典"""
        messages = [MessageExt(topic="test", body=b"msg1", message_id="msg1")]
        result = PullMessageResult(
            messages=messages,
            next_begin_offset=200,
            min_offset=0,
            max_offset=300,
            suggest_which_broker_id=1,
            pull_rt=10.5,
        )
        data = result.to_dict()

        assert data["nextBeginOffset"] == 200
        assert data["minOffset"] == 0
        assert data["maxOffset"] == 300
        assert data["suggestWhichBrokerId"] == 1
        assert data["pullRT"] == 10.5
        assert data["isFound"] is True
        assert data["messageCount"] == 1
        assert len(data["messages"]) == 1
        assert data["messages"][0]["msgId"] == "msg1"

    def test_pull_message_result_from_bytes(self):
        """测试从字节数据创建PullMessageResult"""
        data_str = "{'messages': [], 'nextBeginOffset': 100, 'minOffset': 0, 'maxOffset': 100}"
        data_bytes = data_str.encode("utf-8")
        result = PullMessageResult.from_bytes(data_bytes)
        assert result.is_found is False
        assert result.message_count == 0
        assert result.next_begin_offset == 100
        assert result.min_offset == 0
        assert result.max_offset == 100


class TestOffsetResult:
    """测试OffsetResult类"""

    def test_offset_result_creation(self):
        """测试OffsetResult创建"""
        result = OffsetResult(offset=100, retry_times=2)
        assert result.offset == 100
        assert result.retry_times == 2

    def test_offset_result_to_dict(self):
        """测试OffsetResult转字典"""
        result = OffsetResult(offset=100, retry_times=2)
        data = result.to_dict()
        expected = {"offset": 100, "retryTimes": 2}
        assert data == expected

    def test_offset_result_from_dict(self):
        """测试从字典创建OffsetResult"""
        data = {"offset": 100, "retryTimes": 2}
        result = OffsetResult.from_dict(data)
        assert result.offset == 100
        assert result.retry_times == 2

    def test_offset_result_from_bytes(self):
        """测试从字节数据创建OffsetResult"""
        data_str = "{'offset': 100, 'retryTimes': 2}"
        data_bytes = data_str.encode("utf-8")
        result = OffsetResult.from_bytes(data_bytes)
        assert result.offset == 100
        assert result.retry_times == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
