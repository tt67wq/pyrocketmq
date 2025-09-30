"""
测试 BrokerData 反序列化功能
"""

import pytest

from pyrocketmq.nameserver.models import BrokerClusterInfo, BrokerData


def test_broker_data_from_bytes():
    """测试 BrokerData.from_bytes 方法"""
    # 测试数据 - 包含整数key的broker地址
    test_data = b'{"brokerAddrs":{0:"10.88.113.253:20911",1:"10.88.4.240:20911",2:"10.88.76.69:20911"},"brokerName":"sts-broker-d1-3","cluster":"d1"}'

    # 执行反序列化
    broker_data = BrokerData.from_bytes(test_data)

    # 验证结果
    assert broker_data.cluster == "d1"
    assert broker_data.broker_name == "sts-broker-d1-3"
    assert len(broker_data.broker_addresses) == 3

    # 验证broker地址被正确解析为整数key
    assert broker_data.broker_addresses[0] == "10.88.113.253:20911"
    assert broker_data.broker_addresses[1] == "10.88.4.240:20911"
    assert broker_data.broker_addresses[2] == "10.88.76.69:20911"


def test_broker_cluster_info_from_complete_data():
    """测试使用完整数据创建 BrokerClusterInfo"""
    # 完整的测试数据
    test_data = b'{"brokerAddrTable":{"sts-broker-d1-3":{"brokerAddrs":{0:"10.88.113.253:20911",1:"10.88.4.240:20911",2:"10.88.76.69:20911"},"brokerName":"sts-broker-d1-3","cluster":"d1"},"sts-broker-d1-2":{"brokerAddrs":{0:"10.88.116.48:20911",1:"10.88.4.96:20911",2:"10.88.77.65:20911"},"brokerName":"sts-broker-d1-2","cluster":"d1"}},"clusterAddrTable":{"d1":["sts-broker-d1-3","sts-broker-d1-2"]}}'

    # 解析数据
    import ast

    data_str = test_data.decode("utf-8")
    parsed_data = ast.literal_eval(data_str)

    # 创建 BrokerClusterInfo
    cluster_info = BrokerClusterInfo.from_dict(parsed_data)

    # 验证结果
    assert len(cluster_info.broker_addr_table) == 2
    assert len(cluster_info.cluster_addr_table) == 1
    assert "d1" in cluster_info.cluster_addr_table
    assert len(cluster_info.cluster_addr_table["d1"]) == 2

    # 验证第一个broker
    broker_1 = cluster_info.broker_addr_table["sts-broker-d1-3"]
    assert broker_1.cluster == "d1"
    assert broker_1.broker_name == "sts-broker-d1-3"
    assert len(broker_1.broker_addresses) == 3
    assert broker_1.broker_addresses[0] == "10.88.113.253:20911"

    # 验证第二个broker
    broker_2 = cluster_info.broker_addr_table["sts-broker-d1-2"]
    assert broker_2.cluster == "d1"
    assert broker_2.broker_name == "sts-broker-d1-2"
    assert len(broker_2.broker_addresses) == 3
    assert broker_2.broker_addresses[0] == "10.88.116.48:20911"


def test_broker_data_from_bytes_invalid_data():
    """测试处理无效数据"""
    # 测试无效JSON数据
    with pytest.raises(ValueError):
        BrokerData.from_bytes(b'{"invalid": json}')

    # 测试缺少必需字段
    with pytest.raises(ValueError):
        BrokerData.from_bytes(
            b'{"brokerName": "test"}'
        )  # 缺少cluster和brokerAddrs

    # 测试非UTF-8数据
    with pytest.raises(ValueError):
        BrokerData.from_bytes(b"\xff\xfe invalid utf-8")


def test_broker_data_from_bytes_string_keys():
    """测试处理字符串key的broker地址"""
    # 测试字符串格式的key（兼容性处理）
    test_data = b'{"brokerAddrs":{"0":"10.88.113.253:20911","1":"10.88.4.240:20911"},"brokerName":"sts-broker-d1-3","cluster":"d1"}'

    broker_data = BrokerData.from_bytes(test_data)

    assert broker_data.cluster == "d1"
    assert broker_data.broker_name == "sts-broker-d1-3"
    # 字符串key应该被转换为整数key
    assert 0 in broker_data.broker_addresses
    assert 1 in broker_data.broker_addresses
    assert broker_data.broker_addresses[0] == "10.88.113.253:20911"


if __name__ == "__main__":
    # 运行测试
    test_broker_data_from_bytes()
    test_broker_cluster_info_from_complete_data()
    test_broker_data_from_bytes_invalid_data()
    test_broker_data_from_bytes_string_keys()
    print("所有测试通过！")
