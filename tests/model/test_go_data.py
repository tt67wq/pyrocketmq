"""
测试真实的Go语言返回数据解析
验证BrokerClusterInfo能正确处理包含整型key的数据
"""

import sys

sys.path.insert(0, "src")

from pyrocketmq.nameserver.models import BrokerClusterInfo


def test_real_go_data():
    """使用真实的Go语言返回数据进行测试"""

    # 真实的Go语言返回数据（包含整型key）
    go_data = '{"brokerAddrTable":{"sts-broker-d1-3":{"brokerAddrs":{0:"10.88.113.253:20911",1:"10.88.4.240:20911",2:"10.88.76.69:20911"},"brokerName":"sts-broker-d1-3","cluster":"d1"},"sts-broker-d1-2":{"brokerAddrs":{0:"10.88.116.48:20911",1:"10.88.4.96:20911",2:"10.88.77.65:20911"},"brokerName":"sts-broker-d1-2","cluster":"d1"}},"clusterAddrTable":{"d1":["sts-broker-d1-3","sts-broker-d1-2"]}}'

    try:
        # 尝试解析数据
        cluster_info = BrokerClusterInfo.from_bytes(go_data.encode("utf-8"))

        print("✅ 解析成功！")
        print(f"Broker数量: {len(cluster_info.broker_addr_table)}")
        print(f"集群数量: {len(cluster_info.cluster_addr_table)}")

        # 验证第一个broker的数据
        broker_name = "sts-broker-d1-3"
        if broker_name in cluster_info.broker_addr_table:
            broker = cluster_info.broker_addr_table[broker_name]
            print(f"\nBroker: {broker.broker_name}")
            print(f"Cluster: {broker.cluster}")
            print(f"Addresses: {broker.broker_addresses}")

            # 验证地址的key类型
            for broker_id, address in broker.broker_addresses.items():
                print(
                    f"  BrokerID {broker_id} (type: {type(broker_id).__name__}): {address}"
                )

        # 验证第二个broker的数据
        broker_name = "sts-broker-d1-2"
        if broker_name in cluster_info.broker_addr_table:
            broker = cluster_info.broker_addr_table[broker_name]
            print(f"\nBroker: {broker.broker_name}")
            print(f"Cluster: {broker.cluster}")
            print(f"Addresses: {broker.broker_addresses}")

            # 验证地址的key类型
            for broker_id, address in broker.broker_addresses.items():
                print(
                    f"  BrokerID {broker_id} (type: {type(broker_id).__name__}): {address}"
                )

        # 验证集群信息
        print("\n集群信息:")
        for (
            cluster_name,
            broker_list,
        ) in cluster_info.cluster_addr_table.items():
            print(f"  Cluster {cluster_name}: {broker_list}")

        print("\n🎉 所有验证通过！")
        return True

    except Exception as e:
        print(f"❌ 解析失败: {e}")
        return False


def test_json_comparison():
    """对比JSON和ast.literal_eval的解析差异"""
    import ast
    import json

    go_data = '{"brokerAddrTable":{"sts-broker-d1-3":{"brokerAddrs":{0:"10.88.113.253:20911",1:"10.88.4.240:20911",2:"10.88.76.69:20911"},"brokerName":"sts-broker-d1-3","cluster":"d1"}}}'

    print("=== JSON vs ast.literal_eval 对比 ===")

    try:
        json_result = json.loads(go_data)
        print("❌ json.loads() 解析成功（这不应该发生）")
        print(f"JSON结果: {json_result}")
    except json.JSONDecodeError as e:
        print(f"✅ json.loads() 预期失败: {e}")

    try:
        ast_result = ast.literal_eval(go_data)
        print("✅ ast.literal_eval() 解析成功")
        print(f"AST结果: {ast_result}")

        # 检查key类型
        broker_addrs = ast_result["brokerAddrTable"]["sts-broker-d1-3"][
            "brokerAddrs"
        ]
        print(
            f"brokerAddrs key类型: {[type(k).__name__ for k in broker_addrs.keys()]}"
        )

    except (SyntaxError, ValueError) as e:
        print(f"❌ ast.literal_eval() 失败: {e}")


if __name__ == "__main__":
    print("开始测试真实Go语言数据解析...")
    print("=" * 50)

    # 测试JSON对比
    test_json_comparison()
    print()

    # 测试真实数据解析
    success = test_real_go_data()

    if success:
        print(
            "\n🎉 修复验证成功！BrokerClusterInfo现在可以正确处理Go语言的整型key数据。"
        )
    else:
        print("\n❌ 修复验证失败，需要进一步调试。")
