"""
测试RemotingRequestFactory类
"""

import time

from pyrocketmq.model.enums import FlagType, LanguageCode, RequestCode
from pyrocketmq.model.factory import RemotingRequestFactory


class TestRemotingRequestFactory:
    """RemotingRequestFactory测试类"""

    def test_create_send_message_request(self):
        """测试创建发送消息请求"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Hello, RocketMQ!"
        queue_id = 1
        properties = "key1=value1;key2=value2"
        tags = "test_tag"
        keys = "test_key"

        command = RemotingRequestFactory.create_send_message_request(
            producer_group=producer_group,
            topic=topic,
            body=body,
            queue_id=queue_id,
            properties=properties,
            tags=tags,
            keys=keys,
        )

        # 验证基本信息
        assert command.code == RequestCode.SEND_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body

        # 验证扩展字段
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)
        assert command.ext_fields["properties"] == properties
        assert command.ext_fields["tags"] == tags
        assert command.ext_fields["keys"] == keys

    def test_create_send_message_v2_request(self):
        """测试创建发送消息V2请求"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Hello, RocketMQ V2!"

        command = RemotingRequestFactory.create_send_message_v2_request(
            producer_group=producer_group, topic=topic, body=body
        )

        # 验证基本信息
        assert command.code == RequestCode.SEND_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body

        # 验证V2格式使用单字母字段名
        assert command.ext_fields["a"] == producer_group  # producerGroup
        assert command.ext_fields["b"] == topic  # topic
        assert command.ext_fields["c"] == "TBW102"  # defaultTopic

    def test_create_pull_message_request(self):
        """测试创建拉取消息请求"""
        consumer_group = "test_consumer"
        topic = "test_topic"
        queue_id = 1
        queue_offset = 100
        max_msg_nums = 32

        command = RemotingRequestFactory.create_pull_message_request(
            consumer_group=consumer_group,
            topic=topic,
            queue_id=queue_id,
            queue_offset=queue_offset,
            max_msg_nums=max_msg_nums,
        )

        # 验证基本信息
        assert command.code == RequestCode.PULL_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["consumerGroup"] == consumer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)
        assert command.ext_fields["queueOffset"] == str(queue_offset)
        assert command.ext_fields["maxMsgNums"] == str(max_msg_nums)

    def test_create_get_consumer_list_request(self):
        """测试创建获取消费者列表请求"""
        consumer_group = "test_consumer"

        command = RemotingRequestFactory.create_get_consumer_list_request(
            consumer_group
        )

        # 验证基本信息
        assert command.code == RequestCode.GET_CONSUMER_LIST_BY_GROUP
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["consumerGroup"] == consumer_group

    def test_create_get_max_offset_request(self):
        """测试创建获取最大偏移量请求"""
        topic = "test_topic"
        queue_id = 1

        command = RemotingRequestFactory.create_get_max_offset_request(
            topic, queue_id
        )

        # 验证基本信息
        assert command.code == RequestCode.GET_MAX_OFFSET
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)

    def test_create_query_consumer_offset_request(self):
        """测试创建查询消费者偏移量请求"""
        consumer_group = "test_consumer"
        topic = "test_topic"
        queue_id = 1

        command = RemotingRequestFactory.create_query_consumer_offset_request(
            consumer_group, topic, queue_id
        )

        # 验证基本信息
        assert command.code == RequestCode.QUERY_CONSUMER_OFFSET
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["consumerGroup"] == consumer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)

    def test_create_update_consumer_offset_request(self):
        """测试创建更新消费者偏移量请求"""
        consumer_group = "test_consumer"
        topic = "test_topic"
        queue_id = 1
        commit_offset = 200

        command = RemotingRequestFactory.create_update_consumer_offset_request(
            consumer_group, topic, queue_id, commit_offset
        )

        # 验证基本信息
        assert command.code == RequestCode.UPDATE_CONSUMER_OFFSET
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["consumerGroup"] == consumer_group
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)
        assert command.ext_fields["commitOffset"] == str(commit_offset)

    def test_create_search_offset_request(self):
        """测试创建搜索偏移量请求"""
        topic = "test_topic"
        queue_id = 1
        timestamp = int(time.time() * 1000)

        command = RemotingRequestFactory.create_search_offset_request(
            topic, queue_id, timestamp
        )

        # 验证基本信息
        assert command.code == RequestCode.SEARCH_OFFSET_BY_TIMESTAMP
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["queueId"] == str(queue_id)
        assert command.ext_fields["timestamp"] == str(timestamp)

    def test_create_get_route_info_request(self):
        """测试创建获取路由信息请求"""
        topic = "test_topic"

        command = RemotingRequestFactory.create_get_route_info_request(topic)

        # 验证基本信息
        assert command.code == RequestCode.GET_ROUTE_INFO_BY_TOPIC
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic

    def test_create_heartbeat_request(self):
        """测试创建心跳请求"""
        command = RemotingRequestFactory.create_heartbeat_request()

        # 验证基本信息
        assert command.code == RequestCode.HEART_BEAT
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.ext_fields == {}

    def test_create_end_transaction_request(self):
        """测试创建结束事务请求"""
        producer_group = "test_producer"
        tran_state_table_offset = 1000
        commit_log_offset = 2000
        commit_or_rollback = 1
        msg_id = "test_msg_id"
        transaction_id = "test_transaction_id"

        command = RemotingRequestFactory.create_end_transaction_request(
            producer_group=producer_group,
            tran_state_table_offset=tran_state_table_offset,
            commit_log_offset=commit_log_offset,
            commit_or_rollback=commit_or_rollback,
            msg_id=msg_id,
            transaction_id=transaction_id,
        )

        # 验证基本信息
        assert command.code == RequestCode.END_TRANSACTION
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["tranStateTableOffset"] == str(
            tran_state_table_offset
        )
        assert command.ext_fields["commitLogOffset"] == str(commit_log_offset)
        assert command.ext_fields["commitOrRollback"] == str(commit_or_rollback)
        assert command.ext_fields["msgId"] == msg_id
        assert command.ext_fields["transactionId"] == transaction_id

    def test_create_check_transaction_state_request(self):
        """测试创建检查事务状态请求"""
        tran_state_table_offset = 1000
        commit_log_offset = 2000
        msg_id = "test_msg_id"

        command = RemotingRequestFactory.create_check_transaction_state_request(
            tran_state_table_offset=tran_state_table_offset,
            commit_log_offset=commit_log_offset,
            msg_id=msg_id,
        )

        # 验证基本信息
        assert command.code == RequestCode.CHECK_TRANSACTION_STATE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["tranStateTableOffset"] == str(
            tran_state_table_offset
        )
        assert command.ext_fields["commitLogOffset"] == str(commit_log_offset)
        assert command.ext_fields["msgId"] == msg_id

    def test_create_consumer_send_msg_back_request(self):
        """测试创建消费者发送消息回退请求"""
        group = "test_consumer"
        offset = 100
        delay_level = 2
        origin_msg_id = "test_msg_id"
        origin_topic = "test_topic"

        command = RemotingRequestFactory.create_consumer_send_msg_back_request(
            group=group,
            offset=offset,
            delay_level=delay_level,
            origin_msg_id=origin_msg_id,
            origin_topic=origin_topic,
        )

        # 验证基本信息
        assert command.code == RequestCode.CONSUMER_SEND_MSG_BACK
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["group"] == group
        assert command.ext_fields["offset"] == str(offset)
        assert command.ext_fields["delayLevel"] == str(delay_level)
        assert command.ext_fields["originMsgId"] == origin_msg_id
        assert command.ext_fields["originTopic"] == origin_topic

    def test_create_create_topic_request(self):
        """测试创建主题请求"""
        topic = "test_topic"
        read_queue_nums = 16
        write_queue_nums = 16

        command = RemotingRequestFactory.create_create_topic_request(
            topic=topic,
            read_queue_nums=read_queue_nums,
            write_queue_nums=write_queue_nums,
        )

        # 验证基本信息
        assert command.code == RequestCode.CREATE_TOPIC
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["readQueueNums"] == str(read_queue_nums)
        assert command.ext_fields["writeQueueNums"] == str(write_queue_nums)

    def test_create_query_message_request(self):
        """测试创建查询消息请求"""
        topic = "test_topic"
        key = "test_key"
        max_num = 100
        begin_timestamp = int(time.time() * 1000) - 3600000  # 1小时前
        end_timestamp = int(time.time() * 1000)

        command = RemotingRequestFactory.create_query_message_request(
            topic=topic,
            key=key,
            max_num=max_num,
            begin_timestamp=begin_timestamp,
            end_timestamp=end_timestamp,
        )

        # 验证基本信息
        assert command.code == RequestCode.QUERY_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["key"] == key
        assert command.ext_fields["maxNum"] == str(max_num)
        assert command.ext_fields["beginTimestamp"] == str(begin_timestamp)
        assert command.ext_fields["endTimestamp"] == str(end_timestamp)

    def test_create_reset_offset_request(self):
        """测试创建重置偏移量请求"""
        topic = "test_topic"
        group = "test_consumer"
        timestamp = int(time.time() * 1000)
        is_force = True

        command = RemotingRequestFactory.create_reset_offset_request(
            topic=topic, group=group, timestamp=timestamp, is_force=is_force
        )

        # 验证基本信息
        assert command.code == RequestCode.RESET_CONSUMER_OFFSET
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic
        assert command.ext_fields["group"] == group
        assert command.ext_fields["timestamp"] == str(timestamp)
        assert command.ext_fields["isForce"] == "true"

    def test_create_get_all_topic_list_request(self):
        """测试创建获取所有主题列表请求"""
        command = RemotingRequestFactory.create_get_all_topic_list_request()

        # 验证基本信息
        assert command.code == RequestCode.GET_ALL_TOPIC_LIST_FROM_NAME_SERVER
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.ext_fields == {}

    def test_create_send_batch_message_request(self):
        """测试创建发送批量消息请求"""
        producer_group = "test_producer"
        topic = "test_topic"
        body = b"Batch message content"

        command = RemotingRequestFactory.create_send_batch_message_request(
            producer_group=producer_group, topic=topic, body=body
        )

        # 验证基本信息
        assert command.code == RequestCode.SEND_BATCH_MESSAGE
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE
        assert command.body == body

        # 验证批量标识
        assert command.ext_fields["batch"] == "true"
        assert command.ext_fields["producerGroup"] == producer_group
        assert command.ext_fields["topic"] == topic

    def test_create_save_msg_no_request(self):
        """测试创建保存消息编号请求"""
        msg_no = "test_msg_no"

        command = RemotingRequestFactory.create_save_msg_no_request(msg_no)

        # 验证基本信息
        assert command.code == RequestCode.PUT_MSG_NO
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["msgNo"] == msg_no

    def test_create_get_msg_no_request(self):
        """测试创建获取消息编号请求"""
        msg_no = "test_msg_no"

        command = RemotingRequestFactory.create_get_msg_no_request(msg_no)

        # 验证基本信息
        assert command.code == RequestCode.GET_MSG_NO
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["msgNo"] == msg_no

    def test_create_get_consumer_running_info_request(self):
        """测试创建获取消费者运行信息请求"""
        consumer_group = "test_consumer"
        client_id = "test_client_id"

        command = (
            RemotingRequestFactory.create_get_consumer_running_info_request(
                consumer_group=consumer_group, client_id=client_id
            )
        )

        # 验证基本信息
        assert command.code == RequestCode.GET_CONSUMER_RUNNING_INFO
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["consumerGroup"] == consumer_group
        assert command.ext_fields["clientId"] == client_id

    def test_create_consume_message_directly_request(self):
        """测试创建直接消费消息请求"""
        consumer_group = "test_consumer"
        client_id = "test_client_id"
        msg_id = "test_msg_id"
        broker_name = "test_broker"

        command = (
            RemotingRequestFactory.create_consume_message_directly_request(
                consumer_group=consumer_group,
                client_id=client_id,
                msg_id=msg_id,
                broker_name=broker_name,
            )
        )

        # 验证基本信息
        assert command.code == RequestCode.CONSUME_MESSAGE_DIRECTLY
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["consumerGroup"] == consumer_group
        assert command.ext_fields["clientId"] == client_id
        assert command.ext_fields["msgId"] == msg_id
        assert command.ext_fields["brokerName"] == broker_name

    def test_create_delete_topic_request(self):
        """测试创建删除主题请求"""
        topic = "test_topic"

        command = RemotingRequestFactory.create_delete_topic_request(topic)

        # 验证基本信息
        assert command.code == RequestCode.DELETE_TOPIC_IN_BROKER
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["topic"] == topic

    def test_create_view_message_request(self):
        """测试创建查看消息请求"""
        offset = 1000

        command = RemotingRequestFactory.create_view_message_request(offset)

        # 验证基本信息
        assert command.code == RequestCode.VIEW_MESSAGE_BY_ID
        assert command.language == LanguageCode.PYTHON
        assert command.flag == FlagType.RPC_TYPE

        # 验证扩展字段
        assert command.ext_fields["offset"] == str(offset)
