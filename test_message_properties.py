#!/usr/bin/env python3
"""
测试Message类的properties序列化功能
验证marshall_properties和unmarshal_properties方法的正确性
"""

import os
import sys

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pyrocketmq.model.message import (
    NAME_VALUE_SEPARATOR,
    PROPERTY_SEPARATOR,
    Message,
)


def test_marshall_properties():
    """测试properties序列化"""
    print("=== 测试 marshall_properties 方法 ===")

    # 创建消息并添加属性
    msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    msg.set_property("key1", "value1")
    msg.set_property("key2", "value2")
    msg.set_property("TAGS", "test_tag")

    # 序列化properties
    serialized = msg.marshall_properties()
    print(f"原始properties: {msg.properties}")
    print(f"序列化结果: {repr(serialized)}")

    # 验证序列化结果格式
    expected_parts = []
    for key, value in msg.properties.items():
        expected_parts.append(
            f"{key}{NAME_VALUE_SEPARATOR}{value}{PROPERTY_SEPARATOR}"
        )
    expected = "".join(expected_parts)

    assert serialized == expected, (
        f"序列化结果不匹配: {repr(serialized)} != {repr(expected)}"
    )
    print("✅ 序列化结果正确")


def test_unmarshal_properties():
    """测试properties反序列化"""
    print("\n=== 测试 unmarshal_properties 方法 ===")

    # 创建测试序列化字符串
    test_str = f"key1{NAME_VALUE_SEPARATOR}value1{PROPERTY_SEPARATOR}key2{NAME_VALUE_SEPARATOR}value2{PROPERTY_SEPARATOR}TAGS{NAME_VALUE_SEPARATOR}test_tag{PROPERTY_SEPARATOR}"

    # 创建消息并反序列化
    msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    msg.unmarshal_properties(test_str)

    print(f"反序列化字符串: {repr(test_str)}")
    print(f"反序列化结果: {msg.properties}")

    # 验证反序列化结果
    expected = {"key1": "value1", "key2": "value2", "TAGS": "test_tag"}

    assert msg.properties == expected, (
        f"反序列化结果不匹配: {msg.properties} != {expected}"
    )
    print("✅ 反序列化结果正确")


def test_round_trip():
    """测试序列化-反序列化往返过程"""
    print("\n=== 测试序列化-反序列化往返 ===")

    # 创建原始消息
    original_msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    original_msg.set_property("producer", "test_producer")
    original_msg.set_property("consumer", "test_consumer")
    original_msg.set_property("retry", "3")
    original_msg.set_property("delay", "1000")

    print(f"原始消息properties: {original_msg.properties}")

    # 序列化
    serialized = original_msg.marshall_properties()

    # 创建新消息并反序列化
    new_msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    new_msg.unmarshal_properties(serialized)

    print(f"往返后消息properties: {new_msg.properties}")

    # 验证往返过程
    assert original_msg.properties == new_msg.properties, (
        "往返过程properties不一致"
    )
    print("✅ 序列化-反序列化往返成功")


def test_empty_properties():
    """测试空properties的序列化"""
    print("\n=== 测试空properties ===")

    # 创建没有properties的消息
    msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")

    # 序列化空properties
    serialized = msg.marshall_properties()
    assert serialized == "", (
        f"空properties序列化应为空字符串，实际为: {repr(serialized)}"
    )
    print(f"空properties序列化结果: {repr(serialized)}")

    # 反序列化空字符串
    msg.unmarshal_properties("")
    assert msg.properties == {}, "反序列化空字符串后properties应为空字典"
    print("✅ 空properties处理正确")


def test_special_characters():
    """测试包含特殊字符的properties"""
    print("\n=== 测试特殊字符处理 ===")

    msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    msg.set_property("unicode", "测试中文")
    msg.set_property("symbols", "!@#$%^&*()")
    msg.set_property("spaces", "value with spaces")
    msg.set_property("empty", "")

    print(f"包含特殊字符的properties: {msg.properties}")

    # 序列化
    serialized = msg.marshall_properties()
    print(f"特殊字符序列化结果: {repr(serialized)}")

    # 反序列化
    new_msg = Message(topic="test_topic", body=b"Hello, RocketMQ!")
    new_msg.unmarshal_properties(serialized)

    print(f"特殊字符反序列化结果: {new_msg.properties}")

    # 验证结果
    assert msg.properties == new_msg.properties, "特殊字符处理不一致"
    print("✅ 特殊字符处理正确")


def main():
    """运行所有测试"""
    print("开始测试Message类的properties序列化功能...")
    print(f"PROPERTY_SEPARATOR: {repr(PROPERTY_SEPARATOR)}")
    print(f"NAME_VALUE_SEPARATOR: {repr(NAME_VALUE_SEPARATOR)}")

    try:
        test_marshall_properties()
        test_unmarshal_properties()
        test_round_trip()
        test_empty_properties()
        test_special_characters()

        print("\n🎉 所有测试通过！")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
