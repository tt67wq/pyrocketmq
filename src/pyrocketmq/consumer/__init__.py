"""
Consumer模块 - 消费者组件

提供完整的RocketMQ消费者功能，包括偏移量管理、订阅管理等。

主要组件：
- offset_store: 偏移量存储功能
- subscription_manager: 订阅关系管理（后续实现）
- consumer_config: 消费者配置（后续实现）

使用示例：
```python
from pyrocketmq.consumer import create_offset_store
from pyrocketmq.model import MessageModel

# 创建偏移量存储
store = await create_offset_store(
    consumer_group="my_group",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)
```
"""
