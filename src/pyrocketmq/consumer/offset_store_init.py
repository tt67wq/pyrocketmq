"""
OffsetStore模块 - 偏移量存储

提供完整的偏移量存储功能，支持集群模式和广播模式。

主要组件：
- OffsetStore: 抽象基类，定义偏移量存储接口
- RemoteOffsetStore: 集群模式远程存储
- LocalOffsetStore: 广播模式本地存储
- OffsetStoreFactory: 偏移量存储工厂
- OffsetStoreManager: 偏移量存储管理器

使用示例：
```python
from pyrocketmq.consumer.offset_store_factory import create_offset_store
from pyrocketmq.model import MessageModel

# 集群模式
remote_store = await create_offset_store(
    consumer_group="my_group",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)

# 广播模式
local_store = await create_offset_store(
    consumer_group="my_group",
    message_model=MessageModel.BROADCASTING,
    store_path="/path/to/offsets"
)
```
"""

from .offset_store import OffsetStore, ReadOffsetType, OffsetEntry, OffsetStoreMetrics
from .remote_offset_store import RemoteOffsetStore
from .local_offset_store import LocalOffsetStore
from .offset_store_factory import (
    OffsetStoreFactory,
    OffsetStoreManager,
    create_offset_store,
    close_offset_store,
    close_all_offset_stores,
    get_offset_store_manager,
    validate_offset_store_config,
)

__all__ = [
    # 基础类
    "OffsetStore",
    "ReadOffsetType",
    "OffsetEntry",
    "OffsetStoreMetrics",
    # 实现类
    "RemoteOffsetStore",
    "LocalOffsetStore",
    # 工厂和管理器
    "OffsetStoreFactory",
    "OffsetStoreManager",
    # 便捷函数
    "create_offset_store",
    "close_offset_store",
    "close_all_offset_stores",
    "get_offset_store_manager",
    "validate_offset_store_config",
]
