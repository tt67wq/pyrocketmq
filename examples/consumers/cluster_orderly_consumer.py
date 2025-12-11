#! /usr/bin/env python3
# -*- coding: utf-8 -*-


from config_loader import load_config, parse_config_file_path

import pyrocketmq.logging
from pyrocketmq.consumer import (
    create_message_listener,
    create_orderly_consumer,
)
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model import (
    ConsumeResult,
    MessageExt,
    create_tag_selector,
)


# 颜色代码定义
class Colors:
    RED = "\033[91m"  # 错误/失败信息 - 红色
    GREEN = "\033[92m"  # 成功信息 - 绿色
    YELLOW = "\033[93m"  # 警告信息 - 黄色
    BLUE = "\033[94m"  # 消息信息 - 蓝色
    MAGENTA = "\033[95m"  # 标签信息 - 洋红色
    CYAN = "\033[96m"  # 键值信息 - 青色
    WHITE = "\033[97m"  # 标题信息 - 白色
    BOLD = "\033[1m"  # 粗体
    END = "\033[0m"  # 结束颜色


def message_listener(messages: list[MessageExt]) -> ConsumeResult:
    import random

    if random.randint(1, 6) == 1:
        print(
            f"{Colors.RED}{Colors.BOLD}⚠️  模拟消费失败，返回SUSPEND_CURRENT_QUEUE_A_MOMENT进行重试，消息数量: {len(messages)}{Colors.END}"
        )
        return ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT

    for message in messages:
        print(
            f"{Colors.WHITE}{Colors.BOLD}【收到消息：】{Colors.END} "
            f"{Colors.BLUE}{message.body.decode('utf-8', errors='ignore')}{Colors.END} "
            f"{Colors.MAGENTA}tags: {message.get_tags()}{Colors.END} "
            f"{Colors.CYAN}keys: {message.get_keys()}{Colors.END} "
            f"{Colors.YELLOW}msg_id: {message.get_unique_client_message_id()}{Colors.END} "
        )

    print(f"{Colors.GREEN}✅ 消息处理成功，数量: {len(messages)}{Colors.END}")
    return ConsumeResult.SUCCESS


def main():
    # 从命令行参数解析配置文件路径
    config_path = parse_config_file_path()
    config = load_config(config_path)
    print(f"已加载配置文件: {config_path}")
    print(
        f"Topic: {config.topic}, Group: {config.group}, Nameserver: {config.nameserver}"
    )

    # 设置日志
    pyrocketmq.logging.setup_logging(
        LoggingConfig(level="INFO", json_output=False, file_path="consumer.log")
    )

    # 创建集群模式的顺序消费者
    consumer = create_orderly_consumer(config.group, config.nameserver)

    # 创建标签选择器，支持配置多个标签
    if config.tag:
        selector = create_tag_selector(config.tag)
    else:
        # 如果没有配置标签，订阅所有消息
        from pyrocketmq.model import SUBSCRIBE_ALL

        selector = SUBSCRIBE_ALL

    # 订阅主题
    consumer.subscribe(
        config.topic,
        selector,
        create_message_listener(message_listener),
    )

    try:
        # 启动consumer
        print("启动消费者...")
        consumer.start()
        print("消费者启动成功，开始处理消息...")

        while True:
            import time

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n收到键盘中断信号，正在关闭消费者...")

    except Exception as e:
        print(f"消费者运行异常: {e}")

    finally:
        consumer.shutdown()


if __name__ == "__main__":
    main()
