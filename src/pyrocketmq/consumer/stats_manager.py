#!/usr/bin/env python3
"""
Python实现的StatsManager MVP版本
用于收集和管理RocketMQ消费者的统计数据
"""

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass
class StatsSnapshot:
    """统计快照数据结构"""

    sum: int = 0
    tps: float = 0.0
    avgpt: float = 0.0


@dataclass
class CallSnapshot:
    """调用快照数据结构"""

    timestamp: int  # 毫秒时间戳
    times: int  # 调用次数
    value: int  # 累计值


@dataclass
class ConsumeStatus:
    """消费状态数据结构"""

    pull_rt: float = 0.0
    pull_tps: float = 0.0
    consume_rt: float = 0.0
    consume_ok_tps: float = 0.0
    consume_failed_tps: float = 0.0
    consume_failed_msgs: int = 0


class StatsItem:
    """单个统计项。

    管理单个统计指标的累计值、调用次数以及不同时间窗口的历史数据。
    提供线程安全的数据访问和计算功能。
    """

    def __init__(self, stats_name: str, stats_key: str) -> None:
        """初始化统计项。

        Args:
            stats_name (str): 统计项名称，用于标识统计类型。
            stats_key (str): 统计项键，通常由主题和消费组组成。
        """
        self.stats_name: str = stats_name
        self.stats_key: str = stats_key
        self.value: int = 0  # 累计值
        self.times: int = 0  # 调用次数

        # 不同时间窗口的数据存储
        self.minute_data: deque[CallSnapshot] = deque(maxlen=7)  # 分钟级数据，保留7个点
        self.hour_data: deque[CallSnapshot] = deque(maxlen=7)  # 小时级数据，保留7个点
        self.day_data: deque[CallSnapshot] = deque(maxlen=25)  # 天级数据，保留25个点

        self._lock: threading.RLock = threading.RLock()

    def add_value(self, inc_value: int, inc_times: int) -> None:
        """增加统计值。

        线程安全地更新累计值和调用次数。

        Args:
            inc_value (int): 增加的累计值，必须为非负整数。
            inc_times (int): 增加的调用次数，必须为正整数。
        """
        with self._lock:
            self.value += inc_value
            self.times += inc_times

    def sampling_in_seconds(self) -> None:
        """执行秒级采样。

        创建当前时间的快照并添加到分钟级数据队列中。
        用于高频率的性能监控和实时统计。
        """
        with self._lock:
            snapshot = CallSnapshot(
                timestamp=int(time.time() * 1000), times=self.times, value=self.value
            )
            self.minute_data.append(snapshot)

    def sampling_in_minutes(self) -> None:
        """执行分钟级采样。

        创建当前时间的快照并添加到小时级数据队列中。
        用于中期的趋势分析和性能监控。
        """
        with self._lock:
            snapshot = CallSnapshot(
                timestamp=int(time.time() * 1000), times=self.times, value=self.value
            )
            self.hour_data.append(snapshot)

    def sampling_in_hour(self) -> None:
        """执行小时级采样。

        创建当前时间的快照并添加到天级数据队列中。
        用于长期的容量规划和系统监控。
        """
        with self._lock:
            snapshot = CallSnapshot(
                timestamp=int(time.time() * 1000), times=self.times, value=self.value
            )
            self.day_data.append(snapshot)

    def compute_stats(self, data: deque[CallSnapshot]) -> StatsSnapshot:
        """根据历史数据计算统计信息。

        基于数据队列中的快照数据计算累计值、TPS和平均处理时间。

        Args:
            data (deque[CallSnapshot]): 包含历史快照数据的双端队列，至少需要2个数据点。

        Returns:
            StatsSnapshot: 包含累计值、TPS和平均处理时间的统计快照。
                当数据不足时返回空的统计快照（所有字段为默认值）。
        """
        if len(data) < 2:
            return StatsSnapshot()

        first: CallSnapshot = data[0]
        last: CallSnapshot = data[-1]

        sum_value: int = last.value - first.value
        time_diff: int = last.timestamp - first.timestamp
        times_diff: int = last.times - first.times

        tps: float = 0.0
        avgpt: float = 0.0

        if time_diff > 0:
            tps = (sum_value * 1000.0) / time_diff

        if times_diff > 0:
            avgpt = sum_value / times_diff

        return StatsSnapshot(sum=sum_value, tps=tps, avgpt=avgpt)

    def get_minute_stats(self) -> StatsSnapshot:
        """获取分钟级统计信息。

        基于最近约7个秒级采样点计算统计指标。
        适用于实时性能监控和告警。

        Returns:
            StatsSnapshot: 分钟级统计快照，包含累计值、TPS和平均处理时间。
        """
        with self._lock:
            return self.compute_stats(self.minute_data)

    def get_hour_stats(self) -> StatsSnapshot:
        """获取小时级统计信息。

        基于最近约7个分钟级采样点计算统计指标。
        适用于中期性能趋势分析。

        Returns:
            StatsSnapshot: 小时级统计快照，包含累计值、TPS和平均处理时间。
        """
        with self._lock:
            return self.compute_stats(self.hour_data)

    def get_day_stats(self) -> StatsSnapshot:
        """获取天级统计信息。

        基于最近约25个小时级采样点计算统计指标。
        适用于长期容量规划和系统性能评估。

        Returns:
            StatsSnapshot: 天级统计快照，包含累计值、TPS和平均处理时间。
        """
        with self._lock:
            return self.compute_stats(self.day_data)


class StatsItemSet:
    """统计项集合。

    管理同一类型的多个统计项，提供自动化的数据采样功能。
    启动后台线程定期执行秒级、分钟级和小时级采样。
    """

    def __init__(self, stats_name: str) -> None:
        """初始化统计项集合。

        Args:
            stats_name (str): 统计项集合名称，用于标识统计类型。
        """
        self.stats_name: str = stats_name
        self.stats_items: dict[str, StatsItem] = {}
        self._lock: threading.RLock = threading.RLock()
        self._closed: bool = False
        self._start_sampling()

    def _start_sampling(self) -> None:
        """启动采样线程。

        创建并启动三个后台线程分别执行不同频率的采样：
        - 秒级采样线程：每10秒执行一次
        - 分钟级采样线程：每10分钟执行一次
        - 小时级采样线程：每小时执行一次

        所有线程都设置为守护线程，不会阻止主程序退出。
        """

        def sampling_worker() -> None:
            while not self._closed:
                time.sleep(10)  # 10秒采样一次
                if self._closed:
                    break
                self._sampling_in_seconds()

        def minute_worker() -> None:
            while not self._closed:
                time.sleep(600)  # 10分钟采样一次
                if self._closed:
                    break
                self._sampling_in_minutes()

        def hour_worker() -> None:
            while not self._closed:
                time.sleep(3600)  # 1小时采样一次
                if self._closed:
                    break
                self._sampling_in_hour()

        # 启动采样线程
        self._sampling_thread: threading.Thread = threading.Thread(
            target=sampling_worker, daemon=True
        )
        self._minute_thread: threading.Thread = threading.Thread(
            target=minute_worker, daemon=True
        )
        self._hour_thread: threading.Thread = threading.Thread(
            target=hour_worker, daemon=True
        )

        self._sampling_thread.start()
        self._minute_thread.start()
        self._hour_thread.start()

    def _sampling_in_seconds(self) -> None:
        """对所有统计项执行秒级采样。

        线程安全地遍历所有统计项并调用其秒级采样方法。
        """
        with self._lock:
            for item in self.stats_items.values():
                item.sampling_in_seconds()

    def _sampling_in_minutes(self) -> None:
        """对所有统计项执行分钟级采样。

        线程安全地遍历所有统计项并调用其分钟级采样方法。
        """
        with self._lock:
            for item in self.stats_items.values():
                item.sampling_in_minutes()

    def _sampling_in_hour(self) -> None:
        """对所有统计项执行小时级采样。

        线程安全地遍历所有统计项并调用其小时级采样方法。
        """
        with self._lock:
            for item in self.stats_items.values():
                item.sampling_in_hour()

    def add_value(self, key: str, inc_value: int, inc_times: int) -> None:
        """添加统计值到指定的统计项。

        如果统计项不存在，会自动创建新的统计项。

        Args:
            key (str): 统计项键，通常格式为"topic@group"。
            inc_value (int): 增加的累计值，必须为非负整数。
            inc_times (int): 增加的调用次数，必须为正整数。
        """
        with self._lock:
            if key not in self.stats_items:
                self.stats_items[key] = StatsItem(self.stats_name, key)
            self.stats_items[key].add_value(inc_value, inc_times)

    def get_minute_stats(self, key: str) -> StatsSnapshot:
        """获取指定统计项的分钟级统计信息。

        Args:
            key (str): 统计项键，格式为"topic@group"。

        Returns:
            StatsSnapshot: 分钟级统计快照。
                如果统计项不存在，返回空的统计快照（所有字段为默认值）。
        """
        with self._lock:
            if key in self.stats_items:
                return self.stats_items[key].get_minute_stats()
            return StatsSnapshot()

    def get_hour_stats(self, key: str) -> StatsSnapshot:
        """获取指定统计项的小时级统计信息。

        Args:
            key (str): 统计项键，格式为"topic@group"。

        Returns:
            StatsSnapshot: 小时级统计快照。
                如果统计项不存在，返回空的统计快照（所有字段为默认值）。
        """
        with self._lock:
            if key in self.stats_items:
                return self.stats_items[key].get_hour_stats()
            return StatsSnapshot()

    def shutdown(self) -> None:
        """关闭统计集合。

        设置关闭标志，通知所有后台采样线程退出。
        线程会在下一次循环检查时安全退出。
        """
        self._closed = True


class StatsManager:
    """统计管理器主类。

    管理RocketMQ消费者的各种性能统计指标，包括：
    - 拉取TPS和响应时间
    - 消费TPS和响应时间
    - 消费成功/失败统计

    提供统一的接口来收集统计数据和生成消费状态报告。
    """

    def __init__(self) -> None:
        """初始化统计管理器。

        创建五种类型的统计项集合：
        - CONSUME_OK_TPS: 消费成功TPS统计
        - CONSUME_RT: 消费响应时间统计
        - CONSUME_FAILED_TPS: 消费失败TPS统计
        - PULL_TPS: 拉取TPS统计
        - PULL_RT: 拉取响应时间统计
        """
        # 初始化各种统计项集合
        self.consume_ok_tps: StatsItemSet = StatsItemSet("CONSUME_OK_TPS")
        self.consume_rt: StatsItemSet = StatsItemSet("CONSUME_RT")
        self.consume_failed_tps: StatsItemSet = StatsItemSet("CONSUME_FAILED_TPS")
        self.pull_tps: StatsItemSet = StatsItemSet("PULL_TPS")
        self.pull_rt: StatsItemSet = StatsItemSet("PULL_RT")

        self._started: bool = False
        self._lock: threading.RLock = threading.RLock()

    def _make_key(self, group: str, topic: str) -> str:
        """生成统计键。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。

        Returns:
            str: 格式为"topic@group"的统计键。
        """
        return f"{topic}@{group}"

    def increase_pull_rt(self, group: str, topic: str, rt: int) -> None:
        """增加拉取响应时间统计。

        记录消息从Broker拉取的响应时间。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。
            rt (int): 拉取响应时间，单位毫秒，必须为非负整数。
        """
        key: str = self._make_key(group, topic)
        self.pull_rt.add_value(key, rt, 1)

    def increase_pull_tps(self, group: str, topic: str, msgs: int) -> None:
        """增加拉取TPS统计。

        记录从Broker拉取的消息数量。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。
            msgs (int): 拉取的消息数量，必须为正整数。
        """
        key: str = self._make_key(group, topic)
        self.pull_tps.add_value(key, int(msgs), 1)

    def increase_consume_rt(self, group: str, topic: str, rt: int) -> None:
        """增加消费响应时间统计。

        记录消息消费处理的响应时间。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。
            rt (int): 消费响应时间，单位毫秒，必须为非负整数。
        """
        key: str = self._make_key(group, topic)
        self.consume_rt.add_value(key, rt, 1)

    def increase_consume_ok_tps(self, group: str, topic: str, msgs: int) -> None:
        """增加消费成功TPS统计。

        记录成功消费处理的消息数量。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。
            msgs (int): 成功消费的消息数量，必须为正整数。
        """
        key: str = self._make_key(group, topic)
        self.consume_ok_tps.add_value(key, int(msgs), 1)

    def increase_consume_failed_tps(self, group: str, topic: str, msgs: int) -> None:
        """增加消费失败TPS统计。

        记录消费处理失败的消息数量。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。
            msgs (int): 消费失败的消息数量，必须为正整数。
        """
        key: str = self._make_key(group, topic)
        self.consume_failed_tps.add_value(key, int(msgs), 1)

    def get_consume_status(self, group: str, topic: str) -> ConsumeStatus:
        """获取指定消费者组和主题的消费状态。

        综合各项统计数据生成完整的消费状态报告。
        对于消费响应时间，如果分钟级数据为空则使用小时级数据作为备选。

        Args:
            group (str): 消费者组名称。
            topic (str): 主题名称。

        Returns:
            ConsumeStatus: 包含以下字段的消费状态：
                - pull_rt: 拉取响应时间（毫秒）
                - pull_tps: 拉取TPS
                - consume_rt: 消费响应时间（毫秒）
                - consume_ok_tps: 消费成功TPS
                - consume_failed_tps: 消费失败TPS
                - consume_failed_msgs: 消费失败消息总数
        """
        key: str = self._make_key(group, topic)
        status: ConsumeStatus = ConsumeStatus()

        # 获取各项统计
        pull_rt_stats: StatsSnapshot = self.pull_rt.get_minute_stats(key)
        status.pull_rt = pull_rt_stats.avgpt

        pull_tps_stats: StatsSnapshot = self.pull_tps.get_minute_stats(key)
        status.pull_tps = pull_tps_stats.tps

        consume_rt_stats: StatsSnapshot = self.consume_rt.get_minute_stats(key)
        if consume_rt_stats.sum == 0:
            # 如果分钟级数据为空，使用小时级数据
            consume_rt_stats = self.consume_rt.get_hour_stats(key)
        status.consume_rt = consume_rt_stats.avgpt

        consume_ok_stats: StatsSnapshot = self.consume_ok_tps.get_minute_stats(key)
        status.consume_ok_tps = consume_ok_stats.tps

        consume_failed_stats: StatsSnapshot = self.consume_failed_tps.get_minute_stats(
            key
        )
        status.consume_failed_tps = consume_failed_stats.tps

        # 失败消息数使用小时级统计
        failed_hour_stats: StatsSnapshot = self.consume_failed_tps.get_hour_stats(key)
        status.consume_failed_msgs = failed_hour_stats.sum

        return status

    def shutdown(self) -> None:
        """关闭统计管理器。

        停止所有后台采样线程并清理资源。
        调用此方法后，统计管理器将不再收集新的统计数据。
        """
        self.consume_ok_tps.shutdown()
        self.consume_rt.shutdown()
        self.consume_failed_tps.shutdown()
        self.pull_tps.shutdown()
        self.pull_rt.shutdown()


# 示例使用代码
def example_usage() -> None:
    """统计管理器使用示例。

    演示如何创建和使用统计管理器来收集RocketMQ消费者的性能数据。
    包括数据收集、采样等待和统计报告生成的完整流程。

    示例流程：
    1. 配置日志系统
    2. 创建统计管理器实例
    3. 模拟收集各种统计数据
    4. 等待后台采样完成
    5. 获取并显示消费状态报告
    6. 安全关闭统计管理器
    """
    # 配置日志
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger: logging.Logger = logging.getLogger(__name__)

    # 创建统计管理器
    stats_manager: StatsManager = StatsManager()

    # 模拟数据收集
    group: str = "test_group"
    topic: str = "test_topic"

    logger.info("开始模拟统计数据收集...")

    # 模拟一些统计数据
    for i in range(100):
        stats_manager.increase_pull_tps(group, topic, 10)
        stats_manager.increase_pull_rt(group, topic, 50)
        stats_manager.increase_consume_ok_tps(group, topic, 8)
        stats_manager.increase_consume_rt(group, topic, 100)

        if i % 10 == 0:
            stats_manager.increase_consume_failed_tps(group, topic, 1)

        time.sleep(0.1)  # 间隔100ms

    logger.info("等待采样完成...")
    time.sleep(15)  # 等待至少一次采样

    # 获取消费状态
    status: ConsumeStatus = stats_manager.get_consume_status(group, topic)

    logger.info("消费状态统计:")
    logger.info(f"  拉取响应时间: {status.pull_rt:.2f}ms")
    logger.info(f"  拉取TPS: {status.pull_tps:.2f}")
    logger.info(f"  消费响应时间: {status.consume_rt:.2f}ms")
    logger.info(f"  消费成功TPS: {status.consume_ok_tps:.2f}")
    logger.info(f"  消费失败TPS: {status.consume_failed_tps:.2f}")
    logger.info(f"  消费失败消息数: {status.consume_failed_msgs}")

    # 关闭统计管理器
    stats_manager.shutdown()
    logger.info("统计管理器已关闭")


if __name__ == "__main__":
    example_usage()
