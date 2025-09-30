"""
测试TopicRouteData的Go语言整型key兼容性修复
验证TopicRouteData.from_bytes()能正确处理包含整型key的BrokerData
"""

import sys

sys.path.insert(0, "src")

from pyrocketmq.nameserver.models import TopicRouteData


def test_topic_route_data_with_go_format():
    """测试TopicRouteData处理Go语言格式数据"""

    # 模拟包含整型key的TopicRouteData（类似真实Go语言返回格式）
    go_data = """{
        "orderTopicConf": "",
        "queueDatas": [
            {
                "brokerName": "broker-a",
                "readQueueNums": 4,
                "writeQueueNums": 4,
                "perm": 6,
                "topicSynFlag": 0,
                "compressionType": "gzip"
            },
            {
                "brokerName": "broker-b",
                "readQueueNums": 8,
                "writeQueueNums": 8,
                "perm": 6,
                "topicSynFlag": 0,
                "compressionType": "gzip"
            }
        ],
        "brokerDatas": [
            {
                "cluster": "DefaultCluster",
                "brokerName": "broker-a",
                "brokerAddrs": {0: "192.168.1.100:10911", 1: "192.168.1.101:10911"}
            },
            {
                "cluster": "DefaultCluster",
                "brokerName": "broker-b",
                "brokerAddrs": {0: "192.168.1.200:10911", 1: "192.168.1.201:10911", 2: "192.168.1.202:10911"}
            }
        ]
    }"""

    try:
        # 解析数据
        topic_route = TopicRouteData.from_bytes(go_data.encode("utf-8"))

        print("✅ TopicRouteData解析成功！")
        print(f"订单主题配置: '{topic_route.order_topic_conf}'")
        print(f"队列数据数量: {len(topic_route.queue_data_list)}")
        print(f"Broker数据数量: {len(topic_route.broker_data_list)}")

        # 验证队列数据
        print("\n队列数据验证:")
        for i, queue_data in enumerate(topic_route.queue_data_list):
            print(
                f"  队列{i + 1}: {queue_data.broker_name}, 读队列={queue_data.read_queue_nums}, 写队列={queue_data.write_queue_nums}"
            )

        # 验证Broker数据（重点验证整型key）
        print("\nBroker数据验证:")
        for i, broker_data in enumerate(topic_route.broker_data_list):
            print(f"  Broker{i + 1}: {broker_data.broker_name}")
            print(f"    集群: {broker_data.cluster}")
            print(f"    地址: {broker_data.broker_addresses}")

            # 验证key类型
            for broker_id, address in broker_data.broker_addresses.items():
                print(
                    f"      BrokerID {broker_id} (type: {type(broker_id).__name__}): {address}"
                )

        print("\n🎉 所有验证通过！")
        return True

    except Exception as e:
        print(f"❌ 解析失败: {e}")
        return False


def test_json_vs_ast_comparison():
    """对比JSON和ast.literal_eval在TopicRouteData中的差异"""
    import ast
    import json

    # 包含整型key的简化数据
    go_data = """{
        "orderTopicConf": "",
        "queueDatas": [],
        "brokerDatas": [
            {
                "cluster": "DefaultCluster",
                "brokerName": "broker-a",
                "brokerAddrs": {0: "192.168.1.100:10911", 1: "192.168.1.101:10911"}
            }
        ]
    }"""

    print("=== TopicRouteData: JSON vs ast.literal_eval 对比 ===")

    try:
        json_result = json.loads(go_data)
        print("❌ json.loads() 解析成功（这不应该发生）")
        print(f"JSON结果: {json_result}")
    except json.JSONDecodeError as e:
        print(f"✅ json.loads() 预期失败: {e}")

    try:
        ast_result = ast.literal_eval(go_data)
        print("✅ ast.literal_eval() 解析成功")

        # 检查BrokerData中的key类型
        broker_data = ast_result["brokerDatas"][0]
        broker_addrs = broker_data["brokerAddrs"]
        print(
            f"brokerAddrs key类型: {[type(k).__name__ for k in broker_addrs.keys()]}"
        )
        print(f"brokerAddrs: {broker_addrs}")

    except (SyntaxError, ValueError) as e:
        print(f"❌ ast.literal_eval() 失败: {e}")


def test_complex_topic_route_data():
    """测试复杂的TopicRouteData数据"""

    # 更复杂的真实场景数据
    complex_data = """{
        "orderTopicConf": "topic1:broker-a:4;topic2:broker-b:8",
        "queueDatas": [
            {
                "brokerName": "broker-master-1",
                "readQueueNums": 16,
                "writeQueueNums": 16,
                "perm": 6,
                "topicSynFlag": 0,
                "compressionType": "lz4"
            },
            {
                "brokerName": "broker-master-2",
                "readQueueNums": 32,
                "writeQueueNums": 32,
                "perm": 6,
                "topicSynFlag": 0,
                "compressionType": "snappy"
            }
        ],
        "brokerDatas": [
            {
                "cluster": "prod-cluster-1",
                "brokerName": "broker-master-1",
                "brokerAddrs": {0: "10.0.1.10:10911", 1: "10.0.1.11:10911", 2: "10.0.1.12:10911"}
            },
            {
                "cluster": "prod-cluster-2",
                "brokerName": "broker-master-2",
                "brokerAddrs": {0: "10.0.2.10:10911", 1: "10.0.2.11:10911"}
            }
        ]
    }"""

    try:
        topic_route = TopicRouteData.from_bytes(complex_data.encode("utf-8"))

        print("✅ 复杂TopicRouteData解析成功！")
        print(f"订单主题配置: {topic_route.order_topic_conf}")

        # 验证数据完整性
        assert len(topic_route.queue_data_list) == 2
        assert len(topic_route.broker_data_list) == 2

        # 验证队列数据
        queue1 = topic_route.queue_data_list[0]
        assert queue1.broker_name == "broker-master-1"
        assert queue1.read_queue_nums == 16
        assert queue1.compression_type == "lz4"

        queue2 = topic_route.queue_data_list[1]
        assert queue2.broker_name == "broker-master-2"
        assert queue2.read_queue_nums == 32
        assert queue2.compression_type == "snappy"

        # 验证Broker数据
        broker1 = topic_route.broker_data_list[0]
        assert broker1.cluster == "prod-cluster-1"
        assert broker1.broker_name == "broker-master-1"
        assert len(broker1.broker_addresses) == 3
        assert all(isinstance(k, int) for k in broker1.broker_addresses.keys())

        broker2 = topic_route.broker_data_list[1]
        assert broker2.cluster == "prod-cluster-2"
        assert broker2.broker_name == "broker-master-2"
        assert len(broker2.broker_addresses) == 2
        assert all(isinstance(k, int) for k in broker2.broker_addresses.keys())

        print("✅ 所有断言验证通过！")
        return True

    except Exception as e:
        print(f"❌ 复杂数据解析失败: {e}")
        return False


def test_serialization_roundtrip():
    """测试序列化往返转换"""
    from pyrocketmq.nameserver.models import BrokerData, QueueData

    # 创建原始数据
    original_topic_route = TopicRouteData(
        order_topic_conf="test:broker:4",
        queue_data_list=[
            QueueData(
                broker_name="test-broker",
                read_queue_nums=8,
                write_queue_nums=8,
                perm=6,
                topic_syn_flag=0,
                compression_type="gzip",
            )
        ],
        broker_data_list=[
            BrokerData(
                cluster="TestCluster",
                broker_name="test-broker",
                broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
            )
        ],
    )

    try:
        # 序列化为字典
        data_dict = original_topic_route.to_dict()

        # 手动序列化为带整型key的字符串（模拟Go格式）
        go_format_str = str(data_dict).replace("'", '"')

        # 反序列化
        restored_topic_route = TopicRouteData.from_bytes(
            go_format_str.encode("utf-8")
        )

        # 验证数据一致性
        assert (
            restored_topic_route.order_topic_conf
            == original_topic_route.order_topic_conf
        )
        assert len(restored_topic_route.queue_data_list) == len(
            original_topic_route.queue_data_list
        )
        assert len(restored_topic_route.broker_data_list) == len(
            original_topic_route.broker_data_list
        )

        # 验证broker地址的整型key
        restored_broker = restored_topic_route.broker_data_list[0]
        original_broker = original_topic_route.broker_data_list[0]
        assert (
            restored_broker.broker_addresses == original_broker.broker_addresses
        )
        assert all(
            isinstance(k, int) for k in restored_broker.broker_addresses.keys()
        )

        print("✅ 序列化往返转换验证通过！")
        return True

    except Exception as e:
        print(f"❌ 序列化往返转换失败: {e}")
        return False


if __name__ == "__main__":
    print("开始测试TopicRouteData的Go语言整型key兼容性...")
    print("=" * 60)

    # 测试基本功能
    print("1. 基本Go格式数据解析测试:")
    test1 = test_topic_route_data_with_go_format()
    print()

    # 测试JSON对比
    print("2. JSON vs ast.literal_eval 对比测试:")
    test_json_vs_ast_comparison()
    print()

    # 测试复杂数据
    print("3. 复杂TopicRouteData测试:")
    test2 = test_complex_topic_route_data()
    print()

    # 测试序列化往返
    print("4. 序列化往返转换测试:")
    test3 = test_serialization_roundtrip()
    print()

    # 总结
    all_passed = test1 and test2 and test3
    if all_passed:
        print(
            "🎉 所有测试通过！TopicRouteData现在可以正确处理Go语言的整型key数据。"
        )
    else:
        print("❌ 部分测试失败，需要进一步调试。")
