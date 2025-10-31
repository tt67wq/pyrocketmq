"""
RocketMQ心跳数据结构定义
基于Go语言实现的HeartbeatData结构
"""

from dataclasses import dataclass, field


from .client_data import ConsumerData, ProducerData


@dataclass
class HeartbeatData:
    """
    心跳数据
    包含客户端的生产者和消费者信息
    """

    client_id: str  # 客户端ID
    producer_data_set: list[ProducerData] = field(
        default_factory=list
    )  # 生产者数据集合
    consumer_data_set: list[ConsumerData] = field(
        default_factory=list
    )  # 消费者数据集合

    def to_dict(self) -> dict:
        """转换为字典格式，用于JSON序列化"""
        return {
            "clientID": self.client_id,
            "producerDataSet": [
                producer.to_dict() for producer in self.producer_data_set
            ],
            "consumerDataSet": [
                consumer.to_dict() for consumer in self.consumer_data_set
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HeartbeatData":
        """从字典创建实例"""
        producer_data_set = [
            ProducerData.from_dict(producer_data)
            for producer_data in data.get("producerDataSet", [])
        ]

        consumer_data_set = [
            ConsumerData.from_dict(consumer_data)
            for consumer_data in data.get("consumerDataSet", [])
        ]

        return cls(
            client_id=data.get("clientID", ""),
            producer_data_set=producer_data_set,
            consumer_data_set=consumer_data_set,
        )

    def add_producer_data(self, producer_data: ProducerData) -> None:
        """添加生产者数据"""
        # 检查是否已存在相同的producer，避免重复
        for existing in self.producer_data_set:
            if existing.group_name == producer_data.group_name:
                return
        self.producer_data_set.append(producer_data)

    def add_consumer_data(self, consumer_data: ConsumerData) -> None:
        """添加消费者数据"""
        # 检查是否已存在相同的consumer，避免重复
        for existing in self.consumer_data_set:
            if existing.group_name == consumer_data.group_name:
                return
        self.consumer_data_set.append(consumer_data)

    def remove_producer_data(self, group_name: str) -> bool:
        """移除生产者数据

        Args:
            group_name: 生产者组名

        Returns:
            bool: 是否成功移除
        """
        for i, producer in enumerate(self.producer_data_set):
            if producer.group_name == group_name:
                del self.producer_data_set[i]
                return True
        return False

    def remove_consumer_data(self, group_name: str) -> bool:
        """移除消费者数据

        Args:
            group_name: 消费者组名

        Returns:
            bool: 是否成功移除
        """
        for i, consumer in enumerate(self.consumer_data_set):
            if consumer.group_name == group_name:
                del self.consumer_data_set[i]
                return True
        return False

    def get_producer_groups(self) -> list[str]:
        """获取所有生产者组名"""
        return [producer.group_name for producer in self.producer_data_set]

    def get_consumer_groups(self) -> list[str]:
        """获取所有消费者组名"""
        return [consumer.group_name for consumer in self.consumer_data_set]

    def is_empty(self) -> bool:
        """检查心跳数据是否为空"""
        return not self.producer_data_set and not self.consumer_data_set

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"HeartbeatData[clientID={self.client_id}, "
            f"producers={len(self.producer_data_set)}, "
            f"consumers={len(self.consumer_data_set)}]"
        )
