"""
测试PropertyKey常量类
"""

from pyrocketmq.model.enums import PropertyKey


class TestPropertyKey:
    """PropertyKey常量类测试"""

    def test_key_separator(self):
        """测试键分隔符常量"""
        assert PropertyKey.KEY_SEPARATOR == " "

    def test_message_basic_properties(self):
        """测试消息基础属性常量"""
        assert PropertyKey.KEYS == "KEYS"
        assert PropertyKey.TAGS == "TAGS"
        assert PropertyKey.WAIT_STORE_MSG_OK == "WAIT"
        assert PropertyKey.DELAY_TIME_LEVEL == "DELAY"

    def test_retry_properties(self):
        """测试重试相关属性常量"""
        assert PropertyKey.RETRY_TOPIC == "RETRY_TOPIC"

    def test_transaction_properties(self):
        """测试事务相关属性常量"""
        assert PropertyKey.TRANSACTION_PREPARED == "TRAN_MSG"
        assert PropertyKey.TRANSACTION_ID == "__transactionId__"
        assert PropertyKey.PRODUCER_GROUP == "PGROUP"
        assert (
            PropertyKey.TRANSACTION_PREPARED_QUEUE_OFFSET
            == "TRAN_PREPARED_QUEUE_OFFSET"
        )
        assert PropertyKey.TRANSACTION_CHECK_TIMES == "TRANSACTION_CHECK_TIMES"
        assert (
            PropertyKey.CHECK_IMMUNITY_TIME_IN_SECONDS
            == "CHECK_IMMUNITY_TIME_IN_SECONDS"
        )

    def test_real_topic_and_queue_properties(self):
        """测试真实主题和队列信息属性常量"""
        assert PropertyKey.REAL_TOPIC == "REAL_TOPIC"
        assert PropertyKey.REAL_QUEUE_ID == "REAL_QID"

    def test_offset_properties(self):
        """测试偏移量相关属性常量"""
        assert PropertyKey.MIN_OFFSET == "MIN_OFFSET"
        assert PropertyKey.MAX_OFFSET == "MAX_OFFSET"

    def test_consumer_properties(self):
        """测试消费者相关属性常量"""
        assert PropertyKey.BUYER_ID == "BUYER_ID"
        assert PropertyKey.RECONSUME_TIME == "RECONSUME_TIME"
        assert PropertyKey.MAX_RECONSUME_TIMES == "MAX_RECONSUME_TIMES"
        assert PropertyKey.CONSUME_START_TIME == "CONSUME_START_TIME"

    def test_message_identification_properties(self):
        """测试消息标识和追踪属性常量"""
        assert PropertyKey.ORIGIN_MESSAGE_ID == "ORIGIN_MESSAGE_ID"
        assert PropertyKey.TRANSFER_FLAG == "TRANSFER_FLAG"
        assert PropertyKey.CORRECTION_FLAG == "CORRECTION_FLAG"
        assert PropertyKey.MQ2_FLAG == "MQ2_FLAG"
        assert PropertyKey.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX == "UNIQ_KEY"
        assert PropertyKey.TRACE_SWITCH == "TRACE_ON"

    def test_region_and_sharding_properties(self):
        """测试区域和分片属性常量"""
        assert PropertyKey.MSG_REGION == "MSG_REGION"
        assert PropertyKey.SHARDING_KEY == "SHARDING_KEY"

    def test_scheduled_message_properties(self):
        """测试定时消息属性常量"""
        assert PropertyKey.START_DELIVER_TIME == "START_DELIVER_TIME"

    def test_all_constants_exist(self):
        """测试所有常量都存在且不为空"""
        # 获取PropertyKey类的所有属性
        all_attributes = [
            attr for attr in dir(PropertyKey) if not attr.startswith("_")
        ]

        # 验证所有属性都有值
        for attr in all_attributes:
            value = getattr(PropertyKey, attr)
            assert isinstance(value, str), f"{attr} should be a string"
            assert len(value) > 0, f"{attr} should not be empty"

    def test_constants_match_go_implementation(self):
        """测试常量值与Go语言实现匹配"""
        # 这里验证关键常量与Go实现的一致性
        expected_constants = {
            "KEY_SEPARATOR": " ",
            "KEYS": "KEYS",
            "TAGS": "TAGS",
            "WAIT_STORE_MSG_OK": "WAIT",
            "DELAY_TIME_LEVEL": "DELAY",
            "RETRY_TOPIC": "RETRY_TOPIC",
            "TRANSACTION_PREPARED": "TRAN_MSG",
            "TRANSACTION_ID": "__transactionId__",
            "PRODUCER_GROUP": "PGROUP",
            "REAL_TOPIC": "REAL_TOPIC",
            "REAL_QUEUE_ID": "REAL_QID",
            "MIN_OFFSET": "MIN_OFFSET",
            "MAX_OFFSET": "MAX_OFFSET",
            "BUYER_ID": "BUYER_ID",
            "ORIGIN_MESSAGE_ID": "ORIGIN_MESSAGE_ID",
            "TRANSFER_FLAG": "TRANSFER_FLAG",
            "CORRECTION_FLAG": "CORRECTION_FLAG",
            "MQ2_FLAG": "MQ2_FLAG",
            "RECONSUME_TIME": "RECONSUME_TIME",
            "MSG_REGION": "MSG_REGION",
            "TRACE_SWITCH": "TRACE_ON",
            "UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX": "UNIQ_KEY",
            "MAX_RECONSUME_TIMES": "MAX_RECONSUME_TIMES",
            "CONSUME_START_TIME": "CONSUME_START_TIME",
            "TRANSACTION_PREPARED_QUEUE_OFFSET": "TRAN_PREPARED_QUEUE_OFFSET",
            "TRANSACTION_CHECK_TIMES": "TRANSACTION_CHECK_TIMES",
            "CHECK_IMMUNITY_TIME_IN_SECONDS": "CHECK_IMMUNITY_TIME_IN_SECONDS",
            "SHARDING_KEY": "SHARDING_KEY",
            "START_DELIVER_TIME": "START_DELIVER_TIME",
        }

        for constant_name, expected_value in expected_constants.items():
            actual_value = getattr(PropertyKey, constant_name)
            assert actual_value == expected_value, (
                f"{constant_name} should be '{expected_value}', got '{actual_value}'"
            )
