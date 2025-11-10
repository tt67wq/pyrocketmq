"""
NameServer 数据结构模型
定义与 RocketMQ NameServer 交互所需的核心数据结构。
所有数据结构都与 Go 语言实现保持兼容。
"""

import ast
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BrokerData:
    """Broker 信息"""

    cluster: str
    broker_name: str
    broker_addresses: dict[int, str]  # brokerId -> address

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "cluster": self.cluster,
            "brokerName": self.broker_name,
            "brokerAddrs": self.broker_addresses,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BrokerData":
        """从字典创建实例"""
        return cls(
            cluster=data["cluster"],
            broker_name=data["brokerName"],
            broker_addresses=data["brokerAddrs"],
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "BrokerData":
        """从字节数据创建实例，处理Go语言整数key的JSON兼容性问题

        Args:
            data: 原始响应体字节数据

        Returns:
            BrokerData: BrokerData实例

        Raises:
            ValueError: 数据格式无效时抛出异常
        """
        try:
            # 解码为字符串并使用ast.literal_eval解析
            # 这可以处理Go语言返回的整数key格式
            data_str = data.decode("utf-8")
            parsed_data: dict[str, Any] = ast.literal_eval(data_str)

            # 验证必需字段
            required_fields = ["cluster", "brokerName", "brokerAddrs"]
            for field_name in required_fields:
                if field_name not in parsed_data:
                    raise ValueError(f"Missing required field: {field_name}")

            # 确保brokerAddrs是字典格式
            broker_addrs: dict[str, Any] = parsed_data["brokerAddrs"]
            if not isinstance(broker_addrs, dict):
                raise ValueError("brokerAddrs must be a dictionary")

            # 将brokerId转换为整数key
            normalized_addrs: dict[int, str] = {}
            for broker_id_str, address in broker_addrs.items():
                try:
                    broker_id = int(broker_id_str)
                    normalized_addrs[broker_id] = str(address)
                except (ValueError, TypeError):
                    raise ValueError(f"Invalid brokerId: {broker_id_str}")

            return cls(
                cluster=str(parsed_data["cluster"]),
                broker_name=str(parsed_data["brokerName"]),
                broker_addresses=normalized_addrs,
            )

        except (UnicodeDecodeError, SyntaxError) as e:
            raise ValueError(f"Failed to parse BrokerData from bytes: {e}")
        except Exception as e:
            raise ValueError(f"Invalid BrokerData format: {e}")


@dataclass
class QueueData:
    """队列信息"""

    broker_name: str
    read_queue_nums: int
    write_queue_nums: int
    perm: int
    topic_syn_flag: int
    compression_type: str = "gzip"

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "brokerName": self.broker_name,
            "readQueueNums": self.read_queue_nums,
            "writeQueueNums": self.write_queue_nums,
            "perm": self.perm,
            "topicSynFlag": self.topic_syn_flag,
            "compressionType": self.compression_type,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueData":
        """从字典创建实例"""
        return cls(
            broker_name=data["brokerName"],
            read_queue_nums=data["readQueueNums"],
            write_queue_nums=data["writeQueueNums"],
            perm=data["perm"],
            topic_syn_flag=data["topicSynFlag"],
            compression_type=data.get("compressionType", "gzip"),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "QueueData":
        """从字节数据创建实例

        Args:
            data: 原始响应体字节数据

        Returns:
            QueueData: QueueData实例

        Raises:
            ValueError: 数据格式无效时抛出异常
        """
        try:
            data_str = data.decode("utf-8")
            parsed_data = json.loads(data_str)
            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to parse QueueData from bytes: {e}")
        except Exception as e:
            raise ValueError(f"Invalid QueueData format: {e}")


@dataclass
class TopicRouteData:
    """Topic 路由信息"""

    order_topic_conf: str = ""
    queue_data_list: list[QueueData] = field(default_factory=list)
    broker_data_list: list[BrokerData] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "orderTopicConf": self.order_topic_conf,
            "queueDatas": [q.to_dict() for q in self.queue_data_list],
            "brokerDatas": [b.to_dict() for b in self.broker_data_list],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TopicRouteData":
        """从字典创建实例"""
        return cls(
            order_topic_conf=data.get("orderTopicConf", ""),
            queue_data_list=[
                QueueData.from_dict(q) for q in data.get("queueDatas", [])
            ],
            broker_data_list=[
                BrokerData.from_dict(b) for b in data.get("brokerDatas", [])
            ],
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "TopicRouteData":
        """从字节数据创建实例，处理Go语言整数key的JSON兼容性问题

        Args:
            data: 原始响应体字节数据

        Returns:
            TopicRouteData: TopicRouteData实例

        Raises:
            ValueError: 数据格式无效时抛出异常
        """
        try:
            # 解码为字符串并使用ast.literal_eval解析
            # 这可以处理Go语言返回的整数key格式
            data_str = data.decode("utf-8")
            parsed_data: dict[str, Any] = ast.literal_eval(data_str)

            # 验证必需字段
            if not isinstance(parsed_data, dict):
                raise ValueError("Data must be a dictionary")

            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise ValueError(f"Failed to parse TopicRouteData from bytes: {e}")
        except Exception as e:
            raise ValueError(f"Invalid TopicRouteData format: {e}")


@dataclass
class BrokerClusterInfo:
    """Broker 集群信息"""

    broker_addr_table: dict[str, BrokerData] = field(default_factory=dict)
    cluster_addr_table: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "brokerAddrTable": {
                k: v.to_dict() for k, v in self.broker_addr_table.items()
            },
            "clusterAddrTable": self.cluster_addr_table,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BrokerClusterInfo":
        """从字典创建实例"""
        return cls(
            broker_addr_table={
                k: BrokerData.from_dict(v)
                for k, v in data.get("brokerAddrTable", {}).items()
            },
            cluster_addr_table=data.get("clusterAddrTable", {}),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "BrokerClusterInfo":
        """从字节数据创建实例，处理Go语言整数key的JSON兼容性问题

        Args:
            data: 原始响应体字节数据

        Returns:
            BrokerClusterInfo: BrokerClusterInfo实例

        Raises:
            ValueError: 数据格式无效时抛出异常
        """
        try:
            # 解码为字符串并使用ast.literal_eval解析
            # 这可以处理Go语言返回的整数key格式
            data_str = data.decode("utf-8")
            parsed_data: dict[str, Any] = ast.literal_eval(data_str)

            # 验证必需字段
            if not isinstance(parsed_data, dict):
                raise ValueError("Data must be a dictionary")

            return cls.from_dict(parsed_data)
        except (UnicodeDecodeError, SyntaxError) as e:
            raise ValueError(f"Failed to parse BrokerClusterInfo from bytes: {e}")
        except Exception as e:
            raise ValueError(f"Invalid BrokerClusterInfo format: {e}")
