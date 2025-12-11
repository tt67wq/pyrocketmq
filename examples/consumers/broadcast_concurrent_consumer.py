#! /usr/bin/env python3
# -*- coding: utf-8 -*-


from config_loader import load_config, parse_config_file_path

import pyrocketmq.logging
from pyrocketmq.consumer import create_concurrent_consumer, create_message_listener
from pyrocketmq.logging import LoggingConfig
from pyrocketmq.model import (
    SUBSCRIBE_ALL,
    ConsumeResult,
    MessageExt,
    MessageModel,
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
    for message in messages:
        print(
            f"{Colors.WHITE}{Colors.BOLD}【收到消息：】{Colors.END} "
            f"{Colors.BLUE}{message.body.decode('utf-8', errors='ignore')}{Colors.END} "
            f"{Colors.MAGENTA}tags: {message.get_tags()}{Colors.END} "
            f"{Colors.CYAN}keys: {message.get_keys()}{Colors.END} "
            f"{Colors.YELLOW}msg_id: {message.get_unique_client_message_id()}{Colors.END} "
        )
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

    # 创建广播模式的并发消费者
    consumer = create_concurrent_consumer(
        config.group,
        config.nameserver,
        message_model=MessageModel.BROADCASTING,
    )

    # 订阅主题，广播模式通常订阅所有消息
    consumer.subscribe(
        config.topic, SUBSCRIBE_ALL, create_message_listener(message_listener)
    )

    try:
        # 启动consumer
        print("启动消费者...")
        consumer.start()
        print("消费者启动成功，开始处理消息...")

        # 保持主线程运行
        print("消费者正在运行，按 Ctrl+C 停止...")
        while True:
            import time

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n收到键盘中断信号，正在关闭消费者...")

    except Exception as e:
        print(f"消费者运行异常: {e}")

    finally:
        try:
            consumer.shutdown()
            print("消费者已关闭")
        except Exception as e:
            print(f"关闭消费者时发生异常: {e}")


if __name__ == "__main__":
    main()
