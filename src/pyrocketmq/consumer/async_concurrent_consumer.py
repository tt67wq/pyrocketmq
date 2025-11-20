# """
# AsyncConcurrentConsumer - 异步并发消费者实现

# AsyncConcurrentConsumer是pyrocketmq的异步并发消费者实现，支持高并发异步消息消费。
# 它使用asyncio和异步任务来并行处理多个队列的消息，提供高吞吐量的异步消息消费能力。

# 主要特性:
# - 异步并发消费，基于asyncio事件循环
# - 自动重平衡和队列分配
# - 异步偏移量管理
# - 完善的错误处理和恢复
# - 丰富的监控指标

# 作者: pyrocketmq开发团队
# """

# import asyncio
# import time
# from datetime import datetime
# from typing import Any

# # pyrocketmq导入
# from pyrocketmq.broker import AsyncBrokerClient, MessagePullError
# from pyrocketmq.consumer.allocate_queue_strategy import (
#     AllocateContext,
#     AllocateQueueStrategyFactory,
# )
# from pyrocketmq.consumer.async_base_consumer import AsyncBaseConsumer
# from pyrocketmq.consumer.async_consume_from_where_manager import (
#     AsyncConsumeFromWhereManager,
# )
# from pyrocketmq.consumer.async_listener import (
#     AsyncConsumeContext,
#     AsyncMessageListener,
#     ConsumeResult,
# )
# from pyrocketmq.consumer.config import ConsumerConfig
# from pyrocketmq.consumer.errors import (
#     ConsumerShutdownError,
#     ConsumerStartError,
#     MessageConsumeError,
# )
# from pyrocketmq.consumer.offset_store import ReadOffsetType
# from pyrocketmq.consumer.process_queue import ProcessQueue
# from pyrocketmq.logging import get_logger
# from pyrocketmq.model import (
#     ConsumeMessageDirectlyHeader,
#     ConsumeMessageDirectlyResult,
#     MessageExt,
#     MessageModel,
#     MessageQueue,
#     MessageSelector,
#     PullMessageResult,
#     RemotingCommand,
#     RemotingCommandBuilder,
#     RequestCode,
#     ResponseCode,
#     SubscriptionData,
#     SubscriptionEntry,
# )
# from pyrocketmq.remote import ConnectionPool

# logger = get_logger(__name__)


# class AsyncConcurrentConsumer(AsyncBaseConsumer):
#     """
#     异步并发消费者实现

#     支持异步并发消费消息的消费者实现。每个队列都有专门的异步任务负责拉取消息，
#     并使用异步任务池来并行处理消息，提供高吞吐量的异步消息消费能力。

#     核心特性:
#     - 异步并发拉取和处理消息
#     - 自动队列重平衡和分配
#     - 智能偏移量管理
#     - 完善的错误处理和重试机制
#     - 丰富的性能监控指标

#     使用示例:
#         >>> from pyrocketmq.consumer import AsyncConcurrentConsumer, ConsumerConfig
#         >>> from pyrocketmq.consumer.async_listener import AsyncMessageListener, ConsumeResult
#         >>>
#         >>> class MyAsyncListener(AsyncMessageListener):
#         ...     async def consume_message(self, messages, context):
#         ...         for msg in messages:
#         ...             print(f"Processing: {msg.body.decode()}")
#         ...         return ConsumeResult.SUCCESS
#         >>>
#         >>> config = ConsumerConfig(
#         ...     consumer_group="my_group",
#         ...     namesrv_addr="localhost:9876"
#         ... )
#         >>> consumer = AsyncConcurrentConsumer(config)
#         >>> await consumer.register_message_listener(MyAsyncListener())
#         >>> await consumer.subscribe("test_topic", create_tag_selector("*"))
#         >>> await consumer.start()
#     """

#     def __init__(self, config: ConsumerConfig) -> None:
#         """
#         初始化异步并发消费者

#         Args:
#             config (ConsumerConfig): 消费者配置参数
#         """
#         super().__init__(config)

#         # 异步任务管理
#         self._pull_tasks: dict[MessageQueue, asyncio.Task[None]] = {}
#         self._consume_task: asyncio.Task[None] | None = None
#         self._rebalance_task: asyncio.Task[None] | None = None

#         # 异步信号量控制并发数
#         self._consume_semaphore: asyncio.Semaphore | None = None
#         self._pull_semaphore: asyncio.Semaphore | None = None

#         # 处理队列
#         self._process_queue: asyncio.Queue[tuple[list[MessageExt], MessageQueue]] = (
#             asyncio.Queue()
#         )
#         self._msg_cache: dict[MessageQueue, ProcessQueue] = {}
#         self._assigned_queues: dict[MessageQueue, int] = {}

#         # 异步锁
#         self._cache_lock = asyncio.Lock()
#         self._stats_lock = asyncio.Lock()

#         # 统计信息
#         self._stats: dict[str, Any] = {
#             "start_time": 0,
#             "pull_count": 0,
#             "pull_success_count": 0,
#             "pull_error_count": 0,
#             "consume_count": 0,
#             "consume_success_count": 0,
#             "consume_error_count": 0,
#             "processed_message_count": 0,
#             "processed_message_size": 0,
#             "rebalance_count": 0,
#             "last_pull_time": 0,
#             "last_consume_time": 0,
#             "last_rebalance_time": 0,
#         }

#         # 重平衡事件
#         self._rebalance_event = asyncio.Event()

#         logger.info(
#             "AsyncConcurrentConsumer initialized",
#             extra={
#                 "consumer_group": self._config.consumer_group,
#                 "message_model": self._config.message_model,
#                 "consume_thread_max": self._config.consume_thread_max,
#                 "pull_batch_size": self._config.pull_batch_size,
#             },
#         )

#     async def start(self) -> None:
#         """启动异步并发消费者。

#         初始化并启动消费者的所有组件，包括：
#         - 建立与NameServer和Broker的网络连接
#         - 创建异步消息拉取和处理任务
#         - 执行初始队列分配和重平衡
#         - 启动心跳和重平衡后台任务

#         启动失败时会自动清理已分配的资源。

#         Raises:
#             ConsumerStartError: 当以下情况发生时抛出：
#                 - 未注册消息监听器
#                 - 消息监听器类型不匹配（需要AsyncMessageListener）
#                 - 网络连接失败
#                 - 异步任务创建失败
#                 - 其他初始化错误

#         Note:
#             此方法是协程，需要在async上下文中调用。
#             启动成功后，消费者会自动开始拉取和处理消息。
#         """
#         async with self._lock:
#             if self._is_running:
#                 logger.warning("AsyncConsumer is already running")
#                 return

#             try:
#                 logger.info(
#                     "Starting AsyncConcurrentConsumer",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                         "namesrv_addr": self._config.namesrv_addr,
#                     },
#                 )

#                 # 验证必要条件
#                 if not self._message_listener:
#                     raise ConsumerStartError(
#                         "No async message listener registered",
#                         context={"consumer_group": self._config.consumer_group},
#                     )

#                 if not isinstance(self._message_listener, AsyncMessageListener):
#                     raise ConsumerStartError(
#                         "Message listener must be AsyncMessageListener for AsyncConcurrentConsumer",
#                         context={"consumer_group": self._config.consumer_group},
#                     )

#                 # 启动AsyncBaseConsumer
#                 await super().start()

#                 # 创建异步信号量
#                 self._consume_semaphore = asyncio.Semaphore(
#                     self._config.consume_thread_max
#                 )
#                 self._pull_semaphore = asyncio.Semaphore(
#                     min(self._config.consume_thread_max, 10)
#                 )

#                 # 执行重平衡
#                 await self._do_rebalance()

#                 # 启动重平衡任务
#                 await self._start_rebalance_task()

#                 # 启动消息处理任务
#                 await self._start_consume_tasks()

#                 self._stats["start_time"] = time.time()

#                 logger.info(
#                     "AsyncConcurrentConsumer started successfully",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                         "assigned_queues": len(self._assigned_queues),
#                         "consume_concurrency": self._config.consume_thread_max,
#                         "pull_concurrency": min(self._config.consume_thread_max, 10),
#                     },
#                 )

#             except Exception as e:
#                 logger.error(
#                     f"Failed to start AsyncConcurrentConsumer: {e}",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                         "error": str(e),
#                     },
#                     exc_info=True,
#                 )
#                 # 清理资源
#                 await self._cleanup_on_start_failure()
#                 raise ConsumerStartError(
#                     f"Failed to start AsyncConcurrentConsumer: {e}"
#                 ) from e

#     async def shutdown(self) -> None:
#         """关闭异步并发消费者。

#         按照以下顺序安全关闭所有组件：
#         1. 停止运行状态标记
#         2. 停止所有消息拉取任务
#         3. 停止消息处理任务
#         4. 等待正在处理的消息完成
#         5. 持久化偏移量
#         6. 关闭网络连接

#         此方法会等待所有正在处理的消息完成，确保消息不丢失。

#         Raises:
#             ConsumerShutdownError: 当关闭过程中发生错误时抛出

#         Note:
#             此方法是协程，需要在async上下文中调用。
#             调用此方法后，消费者实例不可再次使用。
#         """
#         async with self._lock:
#             if not self._is_running:
#                 logger.warning("AsyncConsumer is not running")
#                 return

#             try:
#                 logger.info(
#                     "Shutting down AsyncConcurrentConsumer",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                     },
#                 )

#                 # 停止运行状态
#                 self._is_running = False

#                 # 停止重平衡事件
#                 self._rebalance_event.set()

#                 # 停止所有拉取任务
#                 await self._stop_pull_tasks()

#                 # 停止消息处理任务
#                 await self._stop_consume_tasks()

#                 # 等待处理完成
#                 await self._wait_for_processing_completion()

#                 # 关闭AsyncBaseConsumer
#                 await super().shutdown()

#                 # 获取最终统计信息
#                 final_stats = await self._get_final_stats()
#                 logger.info(
#                     "AsyncConcurrentConsumer shutdown completed",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                         "final_stats": final_stats,
#                     },
#                 )

#             except Exception as e:
#                 logger.error(
#                     f"Error during AsyncConcurrentConsumer shutdown: {e}",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                         "error": str(e),
#                     },
#                     exc_info=True,
#                 )
#                 raise ConsumerShutdownError(
#                     f"Failed to shutdown AsyncConcurrentConsumer: {e}"
#                 ) from e
#             finally:
#                 # 清理资源
#                 await self._cleanup_resources()

#     async def subscribe(self, topic: str, selector: MessageSelector) -> None:
#         """订阅指定主题。

#         Args:
#             topic (str): 要订阅的主题名称
#             selector (MessageSelector): 消息选择器，用于过滤消息

#         Raises:
#             SubscribeError: 当订阅失败时抛出
#         """
#         await super().subscribe(topic, selector)

#         # 如果消费者已启动，触发重平衡
#         if self._is_running:
#             await self._trigger_rebalance()

#     async def unsubscribe(self, topic: str) -> None:
#         """取消订阅指定主题。

#         Args:
#             topic (str): 要取消订阅的主题名称

#         Raises:
#             UnsubscribeError: 当取消订阅失败时抛出
#         """
#         await super().unsubscribe(topic)

#         # 如果消费者已启动，触发重平衡
#         if self._is_running:
#             await self._trigger_rebalance()

#     # ==================== 内部方法：重平衡机制 ====================

#     async def _do_rebalance(self) -> None:
#         """执行重平衡操作。

#         根据当前订阅信息和消费者组中的所有消费者，重新分配队列。
#         """
#         try:
#             logger.debug(
#                 "Starting rebalance",
#                 extra={"consumer_group": self._config.consumer_group},
#             )

#             # 获取所有订阅主题
#             topic_set = set(self._subscription_manager.get_topics())
#             if not topic_set:
#                 return

#             # 查找当前消费者组中的所有消费者
#             consumer_ids = await self._find_consumer_list()

#             # 执行队列分配
#             new_assigned_queues = await self._allocate_queues(topic_set, consumer_ids)

#             # 更新分配的队列
#             await self._update_assigned_queues(new_assigned_queues)

#             # 更新统计信息
#             async with self._stats_lock:
#                 self._stats["rebalance_count"] += 1
#                 self._stats["last_rebalance_time"] = time.time()

#             logger.info(
#                 "Rebalance completed",
#                 extra={
#                     "consumer_group": self._config.consumer_group,
#                     "assigned_queue_count": len(new_assigned_queues),
#                     "consumer_count": len(consumer_ids),
#                     "topics": list(topic_set),
#                 },
#             )

#         except Exception as e:
#             logger.error(
#                 f"Rebalance failed: {e}",
#                 extra={"consumer_group": self._config.consumer_group, "error": str(e)},
#                 exc_info=True,
#             )

#     async def _allocate_queues(
#         self, topics: set[str], consumer_ids: list[str]
#     ) -> set[MessageQueue]:
#         """为当前消费者分配队列。

#         Args:
#             topics: 订阅的主题集合
#             consumer_ids: 消费者ID列表

#         Returns:
#             分配给当前消费者的队列集合
#         """
#         # 获取主题路由信息
#         all_message_queues: set[MessageQueue] = set()
#         for topic in topics:
#             try:
#                 route_data = await self._namesrv_manager.query_topic_route_info(topic)
#                 for broker_data in route_data.broker_datas:
#                     for queue_id in range(route_data.queue_datas[0].read_queue_nums):
#                         message_queue = MessageQueue(
#                             broker_name=broker_data.broker_name,
#                             topic=topic,
#                             queue_id=queue_id,
#                         )
#                         all_message_queues.add(message_queue)
#             except Exception as e:
#                 logger.warning(
#                     f"Failed to get route info for topic {topic}: {e}",
#                     extra={"topic": topic, "error": str(e)},
#                 )

#         # 使用分配策略分配队列
#         allocate_context = AllocateContext(
#             consumer_group=self._config.consumer_group,
#             current_cid=self._client_id,
#             all_consumer_ids=consumer_ids,
#             message_queue_list=list(all_message_queues),
#         )

#         strategy = AllocateQueueStrategyFactory.get_strategy(
#             self._config.allocate_strategy
#         )
#         return strategy.allocate(allocate_context)

#     async def _update_assigned_queues(
#         self, new_assigned_queues: set[MessageQueue]
#     ) -> None:
#         """更新分配的队列。

#         Args:
#             new_assigned_queues: 新分配的队列集合
#         """
#         async with self._lock:
#             # 记录旧队列集合
#             old_queues = set(self._assigned_queues.keys())

#             # 停止已移除队列的拉取任务
#             removed_queues = old_queues - new_assigned_queues
#             for queue in removed_queues:
#                 task = self._pull_tasks.pop(queue, None)
#                 if task and not task.done():
#                     task.cancel()
#                     try:
#                         await task
#                     except asyncio.CancelledError:
#                         pass
#                 self._assigned_queues.pop(queue, None)

#             # 初始化新分配队列的偏移量
#             added_queues = new_assigned_queues - old_queues
#             for queue in added_queues:
#                 self._assigned_queues[queue] = 0  # 初始化偏移量为0，后续会更新

#             # 如果消费者正在运行，启动新队列的拉取任务
#             if self._is_running and added_queues:
#                 await self._start_pull_tasks_for_queues(added_queues)

#     async def _trigger_rebalance(self) -> None:
#         """触发重平衡"""
#         if self._is_running:
#             # 唤醒重平衡循环，使其立即执行重平衡
#             self._rebalance_event.set()

#     # ==================== 内部方法：消息拉取 ====================

#     async def _start_pull_tasks(self) -> None:
#         """启动所有队列的消息拉取任务"""
#         if not self._pull_semaphore or not self._assigned_queues:
#             return

#         await self._start_pull_tasks_for_queues(set(self._assigned_queues.keys()))

#     async def _start_pull_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
#         """为指定队列启动拉取任务"""
#         if not self._pull_semaphore:
#             raise ValueError("Pull semaphore is not initialized")

#         for queue in queues:
#             if queue not in self._pull_tasks:
#                 task = asyncio.create_task(self._pull_messages_loop(queue))
#                 self._pull_tasks[queue] = task

#     async def _stop_pull_tasks(self) -> None:
#         """停止所有消息拉取任务"""
#         if not self._pull_tasks:
#             return

#         # 取消所有拉取任务
#         for task in self._pull_tasks.values():
#             if not task.done():
#                 task.cancel()

#         # 等待所有任务完成
#         if self._pull_tasks:
#             await asyncio.gather(*self._pull_tasks.values(), return_exceptions=True)

#         self._pull_tasks.clear()

#     async def _pull_messages_loop(self, message_queue: MessageQueue) -> None:
#         """持续拉取指定队列的消息"""
#         suggest_broker_id = 0

#         while self._is_running:
#             try:
#                 # 获取处理队列
#                 pq = await self._get_or_create_process_queue(message_queue)
#                 if pq.need_flow_control():
#                     await asyncio.sleep(3.0)
#                     continue

#                 async with self._pull_semaphore:
#                     # 执行单次拉取操作
#                     pull_result = await self._perform_single_pull(
#                         message_queue, suggest_broker_id
#                     )

#                 if pull_result:
#                     # 处理拉取结果
#                     await self._handle_pulled_messages(message_queue, pull_result)

#                     # 应用拉取间隔
#                     await self._apply_pull_interval(len(pull_result.msg_found_list) > 0)
#                 else:
#                     # 拉取失败或无消息，应用默认间隔
#                     await self._apply_pull_interval(False)

#             except asyncio.CancelledError:
#                 logger.debug(
#                     f"Pull task for {message_queue} cancelled",
#                     extra={"queue": message_queue},
#                 )
#                 break
#             except Exception as e:
#                 logger.error(
#                     f"Error in pull loop for {message_queue}: {e}",
#                     extra={"queue": message_queue, "error": str(e)},
#                     exc_info=True,
#                 )
#                 # 出错时等待一段时间再重试
#                 await asyncio.sleep(1.0)

#     async def _perform_single_pull(
#         self, message_queue: MessageQueue, suggest_broker_id: int = 0
#     ) -> PullMessageResult | None:
#         """执行单次消息拉取操作"""
#         try:
#             # 获取当前偏移量
#             offset = await self._get_or_initialize_offset(message_queue)

#             # 构建拉取请求
#             sys_flag = await self._build_sys_flag(message_queue)

#             subscription_data = self._subscription_manager.get_subscription(
#                 message_queue.topic
#             )
#             if not subscription_data:
#                 return None

#             # 获取Broker连接
#             broker_addr = await self._namesrv_manager.get_broker_address(
#                 message_queue.broker_name
#             )
#             if not broker_addr:
#                 logger.warning(
#                     f"No broker address found for {message_queue.broker_name}",
#                     extra={"broker_name": message_queue.broker_name},
#                 )
#                 return None

#             pool = await self._broker_manager.get_connection_pool(broker_addr)
#             async with pool.get_connection("pull_message") as conn:
#                 broker_client = AsyncBrokerClient(conn)

#                 # 执行拉取
#                 pull_result = await broker_client.pull_message(
#                     consumer_group=self._config.consumer_group,
#                     topic=message_queue.topic,
#                     queue_id=message_queue.queue_id,
#                     offset=offset,
#                     max_nums=self._config.pull_batch_size,
#                     sys_flag=sys_flag,
#                     subscription_data=subscription_data,
#                 )

#                 # 更新统计信息
#                 async with self._stats_lock:
#                     self._stats["pull_count"] += 1
#                     self._stats["last_pull_time"] = time.time()
#                     if pull_result.pull_status == ResponseCode.SUCCESS:
#                         self._stats["pull_success_count"] += 1
#                     else:
#                         self._stats["pull_error_count"] += 1

#                 return pull_result

#         except Exception as e:
#             logger.error(
#                 f"Failed to pull messages from {message_queue}: {e}",
#                 extra={"queue": message_queue, "error": str(e)},
#                 exc_info=True,
#             )
#             async with self._stats_lock:
#                 self._stats["pull_count"] += 1
#                 self._stats["pull_error_count"] += 1
#                 self._stats["last_pull_time"] = time.time()
#             return None

#     async def _handle_pulled_messages(
#         self, message_queue: MessageQueue, pull_result: PullMessageResult
#     ) -> None:
#         """处理拉取到的消息"""
#         if not pull_result.msg_found_list:
#             return

#         try:
#             # 过滤消息
#             messages = await self._filter_messages_by_tags(
#                 message_queue, pull_result.msg_found_list
#             )

#             if messages:
#                 # 添加到缓存
#                 await self._add_messages_to_cache(message_queue, messages)

#                 # 提交处理
#                 await self._submit_messages_for_processing(message_queue, messages)

#                 logger.debug(
#                     f"Submitted {len(messages)} messages for processing",
#                     extra={
#                         "queue": message_queue,
#                         "message_count": len(messages),
#                     },
#                 )

#         except Exception as e:
#             logger.error(
#                 f"Failed to handle pulled messages from {message_queue}: {e}",
#                 extra={"queue": message_queue, "error": str(e)},
#                 exc_info=True,
#             )

#     async def _filter_messages_by_tags(
#         self, message_queue: MessageQueue, messages: list[MessageExt]
#     ) -> list[MessageExt]:
#         """根据标签过滤消息"""
#         subscription_data = self._subscription_manager.get_subscription(
#             message_queue.topic
#         )
#         if (
#             not subscription_data
#             or subscription_data.expression_type == "TAG_FILTER"
#             and subscription_data.sub_string == "*"
#         ):
#             return messages

#         filtered_messages = []
#         for message in messages:
#             if self._subscription_manager.is_match(message, subscription_data):
#                 filtered_messages.append(message)

#         return filtered_messages

#     async def _submit_messages_for_processing(
#         self, message_queue: MessageQueue, messages: list[MessageExt]
#     ) -> None:
#         """将消息按批次提交给处理队列"""
#         batch_size = self._config.consume_batch_size
#         message_count = len(messages)
#         batch_count = (message_count + batch_size - 1) // batch_size

#         logger.debug(
#             f"Splitting {message_count} messages into {batch_count} batches",
#             extra={
#                 "consumer_group": self._config.consumer_group,
#                 "topic": message_queue.topic,
#                 "queue_id": message_queue.queue_id,
#                 "message_count": message_count,
#                 "batch_size": batch_size,
#                 "batch_count": batch_count,
#             },
#         )

#         # 将消息按批次放入处理队列
#         for i in range(0, message_count, batch_size):
#             batch_messages = messages[i : i + batch_size]
#             await self._process_queue.put((batch_messages, message_queue))

#     async def _apply_pull_interval(self, has_messages: bool = True) -> None:
#         """应用智能拉取间隔控制"""
#         if self._config.pull_interval > 0:
#             if has_messages:
#                 logger.debug(
#                     "Messages pulled, continuing without interval",
#                     extra={"consumer_group": self._config.consumer_group},
#                 )
#             else:
#                 sleep_time = self._config.pull_interval / 1000.0
#                 logger.debug(
#                     f"No messages pulled, sleeping for {sleep_time}s",
#                     extra={
#                         "consumer_group": self._config.consumer_group,
#                         "sleep_time": sleep_time,
#                     },
#                 )
#                 await asyncio.sleep(sleep_time)

#     async def _get_or_create_process_queue(self, queue: MessageQueue) -> ProcessQueue:
#         """获取或创建指定队列的ProcessQueue"""
#         async with self._cache_lock:
#             if queue not in self._msg_cache:
#                 self._msg_cache[queue] = ProcessQueue(
#                     max_cache_count=self._config.max_cache_count_per_queue,
#                     max_cache_size_mb=self._config.max_cache_size_per_queue,
#                 )
#             return self._msg_cache[queue]

#     async def _add_messages_to_cache(
#         self, queue: MessageQueue, messages: list[MessageExt]
#     ) -> None:
#         """将消息添加到缓存"""
#         pq = await self._get_or_create_process_queue(queue)
#         for message in messages:
#             pq.add_message(message)

#         # 更新统计信息
#         total_size = sum(msg.body_size for msg in messages)
#         async with self._stats_lock:
#             self._stats["processed_message_count"] += len(messages)
#             self._stats["processed_message_size"] += total_size

#     async def _get_or_initialize_offset(self, queue: MessageQueue) -> int:
#         """获取或初始化队列偏移量"""
#         # 尝试从偏移量存储获取
#         try:
#             offset = await self._offset_store.read_offset(
#                 queue, ReadOffsetType.READ_FROM_MEMORY
#             )
#             if offset >= 0:
#                 return offset
#         except Exception as e:
#             logger.debug(
#                 f"Failed to read offset from memory for {queue}: {e}",
#                 extra={"queue": queue, "error": str(e)},
#             )

#         # 从存储中读取
#         try:
#             offset = await self._offset_store.read_offset(
#                 queue, ReadOffsetType.READ_FROM_STORE
#             )
#             if offset >= 0:
#                 async with self._cache_lock:
#                     self._assigned_queues[queue] = offset
#                 return offset
#         except Exception as e:
#             logger.debug(
#                 f"Failed to read offset from store for {queue}: {e}",
#                 extra={"queue": queue, "error": str(e)},
#             )

#         # 使用消费策略初始化偏移量
#         try:
#             offset = await self._consume_from_where_manager.get_consume_offset(
#                 queue, self._config.consume_from_where
#             )
#             async with self._cache_lock:
#                 self._assigned_queues[queue] = offset
#             return offset
#         except Exception as e:
#             logger.warning(
#                 f"Failed to initialize offset for {queue}: {e}, using 0",
#                 extra={"queue": queue, "error": str(e)},
#             )
#             async with self._cache_lock:
#                 self._assigned_queues[queue] = 0
#             return 0

#     async def _build_sys_flag(self, message_queue: MessageQueue) -> int:
#         """构建拉取请求的系统标志"""
#         sys_flag = 0

#         # 检查是否需要提交偏移量
#         try:
#             offset = await self._offset_store.read_offset(
#                 message_queue, ReadOffsetType.READ_FROM_MEMORY
#             )
#             if offset >= 0:
#                 sys_flag |= 0x40  # FLAG_COMMIT_OFFSET
#         except Exception:
#             pass

#         # 检查订阅信息
#         subscription_data = self._subscription_manager.get_subscription(
#             message_queue.topic
#         )
#         if subscription_data and subscription_data.class_filter_mode:
#             sys_flag |= 0x08  # FLAG_TAG_FILTER

#         return sys_flag

#     # ==================== 内部方法：消息处理 ====================

#     async def _start_consume_tasks(self) -> None:
#         """启动消息处理任务"""
#         if not self._consume_semaphore:
#             return

#         self._consume_task = asyncio.create_task(self._consume_messages_loop())

#     async def _stop_consume_tasks(self) -> None:
#         """停止消息处理任务"""
#         if self._consume_task and not self._consume_task.done():
#             self._consume_task.cancel()
#             try:
#                 await self._consume_task
#             except asyncio.CancelledError:
#                 pass
#         self._consume_task = None

#     async def _consume_messages_loop(self) -> None:
#         """持续处理消息"""
#         while self._is_running:
#             try:
#                 # 从处理队列获取消息
#                 messages, message_queue = await asyncio.wait_for(
#                     self._process_queue.get(), timeout=1.0
#                 )

#                 # 使用信号量控制并发数
#                 async with self._consume_semaphore:
#                     # 创建处理任务
#                     asyncio.create_task(
#                         self._process_messages_batch(messages, message_queue)
#                     )

#             except asyncio.TimeoutError:
#                 # 超时继续循环，检查是否停止
#                 continue
#             except asyncio.CancelledError:
#                 logger.debug("Consume messages loop cancelled")
#                 break
#             except Exception as e:
#                 logger.error(
#                     f"Error in consume messages loop: {e}",
#                     extra={"error": str(e)},
#                     exc_info=True,
#                 )
#                 await asyncio.sleep(1.0)

#     async def _process_messages_batch(
#         self, messages: list[MessageExt], message_queue: MessageQueue
#     ) -> None:
#         """处理一批消息"""
#         try:
#             # 创建消费上下文
#             context = AsyncConsumeContext(
#                 queue=message_queue,
#                 message_count=len(messages),
#                 is_first_time=False,  # TODO: 实现首次消费检测
#                 processing_start_time=time.time(),
#             )

#             # 调用监听器处理消息
#             consume_result = await self._message_listener.consume_message(
#                 messages, context
#             )

#             # 处理消费结果
#             if consume_result == ConsumeResult.SUCCESS:
#                 # 消费成功，更新偏移量
#                 await self._update_offset_from_cache(
#                     message_queue, messages[-1].queue_offset + 1
#                 )

#                 # 从缓存中移除消息
#                 await self._remove_messages_from_cache(message_queue, messages)

#                 logger.debug(
#                     f"Successfully consumed {len(messages)} messages",
#                     extra={
#                         "queue": message_queue,
#                         "message_count": len(messages),
#                     },
#                 )
#             else:
#                 # 消费失败，处理重试逻辑
#                 await self._handle_consume_failure(
#                     message_queue, messages, consume_result
#                 )

#             # 更新统计信息
#             async with self._stats_lock:
#                 self._stats["consume_count"] += 1
#                 self._stats["last_consume_time"] = time.time()
#                 if consume_result == ConsumeResult.SUCCESS:
#                     self._stats["consume_success_count"] += 1
#                 else:
#                     self._stats["consume_error_count"] += 1

#         except Exception as e:
#             logger.error(
#                 f"Error processing messages from {message_queue}: {e}",
#                 extra={
#                     "queue": message_queue,
#                     "message_count": len(messages),
#                     "error": str(e),
#                 },
#                 exc_info=True,
#             )

#             # 处理异常情况
#             await self._handle_consume_failure(
#                 message_queue, messages, ConsumeResult.RECONSUME_LATER
#             )

#     async def _handle_consume_failure(
#         self,
#         message_queue: MessageQueue,
#         messages: list[MessageExt],
#         result: ConsumeResult,
#     ) -> None:
#         """处理消费失败"""
#         if result == ConsumeResult.RECONSUME_LATER:
#             # 稍后重试，将消息重新放回队列
#             await asyncio.sleep(1.0)  # 简单延迟
#             await self._process_queue.put((messages, message_queue))
#         elif result == ConsumeResult.DISCARD:
#             # 丢弃消息，更新偏移量
#             if messages:
#                 await self._update_offset_from_cache(
#                     message_queue, messages[-1].queue_offset + 1
#                 )
#                 await self._remove_messages_from_cache(message_queue, messages)
#         else:
#             # 其他结果，也稍后重试
#             await asyncio.sleep(1.0)
#             await self._process_queue.put((messages, message_queue))

#     async def _update_offset_from_cache(self, queue: MessageQueue, offset: int) -> None:
#         """从缓存更新偏移量"""
#         async with self._cache_lock:
#             self._assigned_queues[queue] = offset
#         await self._offset_store.update_offset(queue, offset, increase_only=False)

#     async def _remove_messages_from_cache(
#         self, queue: MessageQueue, messages: list[MessageExt]
#     ) -> None:
#         """从缓存中移除消息"""
#         pq = await self._get_or_create_process_queue(queue)
#         for message in messages:
#             pq.remove_message(message)

#     async def _wait_for_processing_completion(self) -> None:
#         """等待正在处理的消息完成"""
#         # 等待处理队列为空
#         while not self._process_queue.empty():
#             await asyncio.sleep(0.1)

#         # 等待所有拉取任务完成
#         if self._pull_tasks:
#             await asyncio.gather(*self._pull_tasks.values(), return_exceptions=True)

#         # 等待消费任务完成
#         if self._consume_task and not self._consume_task.done():
#             await asyncio.wait_for(self._consume_task, timeout=30.0)

#     # ==================== 内部方法：重平衡任务 ====================

#     async def _start_rebalance_task(self) -> None:
#         """启动重平衡任务"""
#         self._rebalance_task = asyncio.create_task(self._rebalance_loop())

#     async def _rebalance_loop(self) -> None:
#         """重平衡循环"""
#         while self._is_running:
#             try:
#                 # 等待重平衡事件或超时
#                 try:
#                     await asyncio.wait_for(self._rebalance_event.wait(), timeout=20.0)
#                 except asyncio.TimeoutError:
#                     # 超时也执行重平衡
#                     pass

#                 # 清除事件
#                 self._rebalance_event.clear()

#                 # 执行重平衡
#                 if self._is_running:
#                     await self._do_rebalance()

#             except asyncio.CancelledError:
#                 logger.debug("Rebalance loop cancelled")
#                 break
#             except Exception as e:
#                 logger.error(
#                     f"Error in rebalance loop: {e}",
#                     extra={"error": str(e)},
#                     exc_info=True,
#                 )
#                 await asyncio.sleep(5.0)

#     async def _find_consumer_list(self) -> list[str]:
#         """查找消费者组中的所有消费者"""
#         try:
#             # 获取所有订阅的主题
#             topics = self._subscription_manager.get_all_topics()
#             if not topics:
#                 return []

#             consumer_ids = set()

#             # 从每个主题的Broker获取消费者列表
#             for topic in topics:
#                 try:
#                     route_data = await self._namesrv_manager.query_topic_route_info(
#                         topic
#                     )
#                     for broker_data in route_data.broker_datas:
#                         broker_addr = await self._namesrv_manager.get_broker_address(
#                             broker_data.broker_name
#                         )
#                         if broker_addr:
#                             pool = await self._broker_manager.get_connection_pool(
#                                 broker_addr
#                             )
#                             async with pool.get_connection(
#                                 "get_consumer_id_list"
#                             ) as conn:
#                                 broker_client = AsyncBrokerClient(conn)
#                                 topic_consumer_ids = (
#                                     await broker_client.get_consumer_id_list(
#                                         consumer_group=self._config.consumer_group,
#                                         topic=topic,
#                                     )
#                                 )
#                                 consumer_ids.update(topic_consumer_ids)
#                 except Exception as e:
#                     logger.warning(
#                         f"Failed to get consumer list from broker for topic {topic}: {e}",
#                         extra={"topic": topic, "error": str(e)},
#                     )

#             return list(consumer_ids)

#         except Exception as e:
#             logger.error(
#                 f"Failed to find consumer list: {e}",
#                 extra={"error": str(e)},
#                 exc_info=True,
#             )
#             return [self._client_id]  # 返回当前消费者ID

#     # ==================== 内部方法：资源管理和清理 ====================

#     async def _cleanup_on_start_failure(self) -> None:
#         """启动失败时的清理操作"""
#         try:
#             # 停止所有任务
#             await self._stop_pull_tasks()
#             await self._stop_consume_tasks()

#             # 清理资源
#             await self._cleanup_resources()

#         except Exception as e:
#             logger.error(
#                 f"Error during startup failure cleanup: {e}",
#                 extra={"error": str(e)},
#                 exc_info=True,
#             )

#     async def _cleanup_resources(self) -> None:
#         """清理资源"""
#         try:
#             # 清理缓存
#             async with self._cache_lock:
#                 self._msg_cache.clear()
#                 self._assigned_queues.clear()

#             # 清理任务
#             self._pull_tasks.clear()

#             # 清理信号量
#             self._consume_semaphore = None
#             self._pull_semaphore = None

#         except Exception as e:
#             logger.error(
#                 f"Error during resource cleanup: {e}",
#                 extra={"error": str(e)},
#                 exc_info=True,
#             )

#     async def _get_final_stats(self) -> dict[str, Any]:
#         """获取最终统计信息"""
#         async with self._stats_lock:
#             stats = self._stats.copy()

#         if stats["start_time"] > 0:
#             stats["running_time"] = time.time() - stats["start_time"]
#         else:
#             stats["running_time"] = 0

#         return stats

#     async def get_stats(self) -> dict[str, Any]:
#         """获取消费者统计信息"""
#         async with self._stats_lock:
#             stats = self._stats.copy()

#         if stats["start_time"] > 0:
#             stats["running_time"] = time.time() - stats["start_time"]
#         else:
#             stats["running_time"] = 0

#         # 添加缓存统计
#         async with self._cache_lock:
#             stats["assigned_queue_count"] = len(self._assigned_queues)
#             stats["cached_message_count"] = sum(
#                 pq.get_message_count() for pq in self._msg_cache.values()
#             )
#             stats["cached_message_size"] = sum(
#                 pq.get_message_size() for pq in self._msg_cache.values()
#             )

#         # 添加任务统计
#         stats["pull_task_count"] = len(self._pull_tasks)
#         stats["process_queue_size"] = self._process_queue.qsize()

#         return stats
