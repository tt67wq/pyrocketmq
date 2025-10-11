#!/usr/bin/env python3
"""
RemotingRequestFactory使用示例

演示如何使用RemotingRequestFactory快速创建各类RocketMQ请求
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyrocketmq.model.factory import RemotingRequestFactory


def demo_send_message():
    """演示发送消息请求创建"""
    print("=== 发送消息请求示例 ===")

    command = RemotingRequestFactory.create_send_message_request(
        producer_group="demo_producer_group",
        topic="demo_topic",
        body=b"Hello, RocketMQ from Python!",
        queue_id=1,
        properties="key1=value1;key2=value2",
        tags="demo_tag",
        keys="demo_key",
    )

    print(f"请求代码: {command.code} (RequestCode.SEND_MESSAGE)")
    print(f"生产者组: {command.ext_fields['producerGroup']}")
    print(f"主题: {command.ext_fields['topic']}")
    print(f"队列ID: {command.ext_fields['queueId']}")
    print(f"标签: {command.ext_fields['tags']}")
    print(f"消息键: {command.ext_fields['keys']}")
    print(f"消息体: {command.body}")
    print()


def demo_pull_message():
    """演示拉取消息请求创建"""
    print("=== 拉取消息请求示例 ===")

    command = RemotingRequestFactory.create_pull_message_request(
        consumer_group="demo_consumer_group",
        topic="demo_topic",
        queue_id=0,
        queue_offset=100,
        max_msg_nums=32,
        sub_expression="demo_tag || tag2",
    )

    print(f"请求代码: {command.code} (RequestCode.PULL_MESSAGE)")
    print(f"消费者组: {command.ext_fields['consumerGroup']}")
    print(f"主题: {command.ext_fields['topic']}")
    print(f"队列ID: {command.ext_fields['queueId']}")
    print(f"队列偏移量: {command.ext_fields['queueOffset']}")
    print(f"最大消息数: {command.ext_fields['maxMsgNums']}")
    print(f"订阅表达式: {command.ext_fields['subscription']}")
    print()


def demo_get_route_info():
    """演示获取路由信息请求创建"""
    print("=== 获取路由信息请求示例 ===")

    command = RemotingRequestFactory.create_get_route_info_request("demo_topic")

    print(f"请求代码: {command.code} (RequestCode.GET_ROUTE_INFO_BY_TOPIC)")
    print(f"主题: {command.ext_fields['topic']}")
    print()


def demo_heartbeat():
    """演示心跳请求创建"""
    print("=== 心跳请求示例 ===")

    command = RemotingRequestFactory.create_heartbeat_request()

    print(f"请求代码: {command.code} (RequestCode.HEART_BEAT)")
    print()


def demo_create_topic():
    """演示创建主题请求创建"""
    print("=== 创建主题请求示例 ===")

    command = RemotingRequestFactory.create_create_topic_request(
        topic="new_demo_topic", read_queue_nums=16, write_queue_nums=16, perm=6
    )

    print(f"请求代码: {command.code} (RequestCode.CREATE_TOPIC)")
    print(f"主题: {command.ext_fields['topic']}")
    print(f"读队列数: {command.ext_fields['readQueueNums']}")
    print(f"写队列数: {command.ext_fields['writeQueueNums']}")
    print(f"权限: {command.ext_fields['perm']}")
    print()


def demo_transaction():
    """演示事务相关请求创建"""
    print("=== 事务请求示例 ===")

    # 结束事务请求
    end_tx_cmd = RemotingRequestFactory.create_end_transaction_request(
        producer_group="demo_producer_group",
        tran_state_table_offset=1000,
        commit_log_offset=2000,
        commit_or_rollback=1,  # 1表示提交，0表示回滚
        msg_id="demo_msg_id",
        transaction_id="demo_transaction_id",
    )

    print("结束事务请求:")
    print(f"请求代码: {end_tx_cmd.code} (RequestCode.END_TRANSACTION)")
    print(f"生产者组: {end_tx_cmd.ext_fields['producerGroup']}")
    print(f"提交或回滚: {end_tx_cmd.ext_fields['commitOrRollback']}")
    print()

    # 检查事务状态请求
    check_tx_cmd = (
        RemotingRequestFactory.create_check_transaction_state_request(
            tran_state_table_offset=1000,
            commit_log_offset=2000,
            msg_id="demo_msg_id",
        )
    )

    print("检查事务状态请求:")
    print(
        f"请求代码: {check_tx_cmd.code} (RequestCode.CHECK_TRANSACTION_STATE)"
    )
    print(f"消息ID: {check_tx_cmd.ext_fields['msgId']}")
    print()


def demo_batch_message():
    """演示批量消息请求创建"""
    print("=== 批量消息请求示例 ===")

    batch_body = b"Message1\nMessage2\nMessage3"
    command = RemotingRequestFactory.create_send_batch_message_request(
        producer_group="demo_producer_group",
        topic="demo_topic",
        body=batch_body,
    )

    print(f"请求代码: {command.code} (RequestCode.SEND_BATCH_MESSAGE)")
    print(f"生产者组: {command.ext_fields['producerGroup']}")
    print(f"主题: {command.ext_fields['topic']}")
    print(f"是否批量: {command.ext_fields['batch']}")
    print(f"批量消息体: {command.body}")
    print()


def demo_offset_operations():
    """演示偏移量操作请求创建"""
    print("=== 偏移量操作请求示例 ===")

    # 查询消费者偏移量
    query_offset_cmd = (
        RemotingRequestFactory.create_query_consumer_offset_request(
            consumer_group="demo_consumer_group", topic="demo_topic", queue_id=0
        )
    )

    print("查询消费者偏移量请求:")
    print(
        f"请求代码: {query_offset_cmd.code} (RequestCode.QUERY_CONSUMER_OFFSET)"
    )
    print(f"消费者组: {query_offset_cmd.ext_fields['consumerGroup']}")
    print(f"主题: {query_offset_cmd.ext_fields['topic']}")
    print()

    # 更新消费者偏移量
    update_offset_cmd = (
        RemotingRequestFactory.create_update_consumer_offset_request(
            consumer_group="demo_consumer_group",
            topic="demo_topic",
            queue_id=0,
            commit_offset=200,
        )
    )

    print("更新消费者偏移量请求:")
    print(
        f"请求代码: {update_offset_cmd.code} (RequestCode.UPDATE_CONSUMER_OFFSET)"
    )
    print(f"提交偏移量: {update_offset_cmd.ext_fields['commitOffset']}")
    print()

    # 获取最大偏移量
    max_offset_cmd = RemotingRequestFactory.create_get_max_offset_request(
        topic="demo_topic", queue_id=0
    )

    print("获取最大偏移量请求:")
    print(f"请求代码: {max_offset_cmd.code} (RequestCode.GET_MAX_OFFSET)")
    print(f"主题: {max_offset_cmd.ext_fields['topic']}")
    print(f"队列ID: {max_offset_cmd.ext_fields['queueId']}")
    print()


def main():
    """主函数"""
    print("RemotingRequestFactory 使用示例")
    print("=" * 50)
    print()

    demo_send_message()
    demo_pull_message()
    demo_get_route_info()
    demo_heartbeat()
    demo_create_topic()
    demo_transaction()
    demo_batch_message()
    demo_offset_operations()

    print("所有示例演示完成!")
    print(
        "RemotingRequestFactory 提供了基于Go语言实现的快速创建各类RocketMQ请求的方法。"
    )
    print("支持所有主要的RocketMQ协议请求类型。")


if __name__ == "__main__":
    main()
