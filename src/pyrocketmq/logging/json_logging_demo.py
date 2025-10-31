#!/usr/bin/env python3
"""
pyrocketmq JSON日志功能演示

这个demo展示了如何在pyrocketmq中使用JSON格式的结构化日志。
"""

import asyncio
import time
import traceback
import logging
from functools import wraps
from typing import Any, Dict

# 导入pyrocketmq相关模块
from pyrocketmq.logging import get_logger, setup_logging
from pyrocketmq.logging.json_formatter import JsonFormatter, StructuredJsonFormatter
from pyrocketmq.logging.config import LoggingConfig
from pyrocketmq.model.message import Message
from pyrocketmq.producer import create_producer, create_async_producer


def demo_basic_json_logging():
    """演示基础JSON日志功能"""
    print("=" * 60)
    print("📋 演示1: 基础JSON日志功能")
    print("=" * 60)

    # 配置JSON日志
    config = LoggingConfig(
        level="INFO",
        console_output=True,
        format="%(message)s",  # 简化格式
    )
    setup_logging(config)

    # 创建JSON格式化器
    json_formatter = JsonFormatter(
        include_timestamp=True,
        include_level=True,
        include_logger=True,
        include_module=True,
        include_function=True,
        include_line=True,
        include_extra=True,
        service="json-demo",
        environment="demo",
    )

    # 获取logger并确保有handler，如果没有则创建一个
    logger = get_logger("basic_demo")
    if not logger.handlers:
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 设置JSON格式化器到所有handler
    for handler in logger.handlers:
        handler.setFormatter(json_formatter)

    # 基础日志输出
    logger.info("应用启动")
    logger.warning("这是一个警告消息")
    logger.error("这是一个错误消息")

    # 带额外字段的日志
    logger.info(
        "用户登录成功",
        extra={
            "user_id": 12345,
            "username": "john_doe",
            "ip_address": "192.168.1.100",
            "login_time": time.time(),
        },
    )

    print()


def demo_structured_json_logging():
    """演示结构化JSON日志功能"""
    print("=" * 60)
    print("📋 演示2: 结构化JSON日志功能")
    print("=" * 60)

    # 创建结构化格式化器
    structured_formatter = StructuredJsonFormatter(
        include_extra=True,
        service="rocketmq-producer",
        version="1.0.0",
        environment="demo",
    )

    logger = get_logger("structured_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置结构化JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(structured_formatter)

    # 结构化日志输出
    logger.info(
        "生产者启动",
        extra={
            "component": "producer",
            "operation": "startup",
            "config": {"producer_group": "demo_group", "nameserver": "localhost:9876"},
        },
    )

    logger.error(
        "连接失败",
        extra={
            "component": "producer",
            "operation": "connect",
            "broker": "localhost:10911",
            "error_code": 500,
            "retry_count": 3,
        },
    )

    print()


def demo_producer_json_logging():
    """演示Producer的JSON日志功能"""
    print("=" * 60)
    print("📋 演示3: Producer JSON日志功能")
    print("=" * 60)

    # 配置Producer专用JSON日志
    producer_formatter = JsonFormatter(
        include_timestamp=True,
        include_level=True,
        include_extra=True,
        service="rocketmq-producer",
        component="message-sending",
    )

    logger = get_logger("producer_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(producer_formatter)

    # 创建Producer
    producer = create_producer("json_demo_producer")

    try:
        producer.start()

        # 发送消息并记录JSON日志
        message = Message(topic="json_demo_topic", body=b"Hello JSON Logging!")

        logger.info(
            "开始发送消息",
            extra={
                "operation": "send_message",
                "topic": message.topic,
                "message_size": len(message.body),
                "timestamp": time.time(),
            },
        )

        result = producer.send(message)

        logger.info(
            "消息发送成功",
            extra={
                "operation": "send_message",
                "topic": message.topic,
                "message_id": result.msg_id,
                "queue_id": result.queue_id,
                "queue_offset": result.queue_offset,
                "success": True,
            },
        )

    except Exception as e:
        logger.error(
            "消息发送失败",
            extra={
                "operation": "send_message",
                "topic": message.topic,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "success": False,
            },
            exc_info=True,
        )

    finally:
        producer.shutdown()
        logger.info("Producer已关闭", extra={"operation": "shutdown", "success": True})

    print()


def demo_async_producer_json_logging():
    """演示异步Producer的JSON日志功能"""
    print("=" * 60)
    print("📋 演示4: 异步Producer JSON日志功能")
    print("=" * 60)

    # 配置异步Producer专用JSON日志
    async_formatter = JsonFormatter(
        include_timestamp=True,
        include_extra=True,
        service="async-rocketmq-producer",
        component="async-message-sending",
    )

    logger = get_logger("async_producer_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(async_formatter)

    async def async_demo():
        # 创建异步Producer
        producer = create_async_producer("async_json_demo_producer")

        try:
            await producer.start()

            # 异步发送消息
            message = Message(
                topic="async_json_demo_topic", body=b"Hello Async JSON Logging!"
            )

            logger.info(
                "开始异步发送消息",
                extra={
                    "operation": "async_send_message",
                    "topic": message.topic,
                    "message_size": len(message.body),
                    "async_operation": True,
                },
            )

            result = await producer.send(message)

            logger.info(
                "异步消息发送成功",
                extra={
                    "operation": "async_send_message",
                    "topic": message.topic,
                    "message_id": result.msg_id,
                    "queue_id": result.queue_id,
                    "async_operation": True,
                    "success": True,
                },
            )

        except Exception as e:
            logger.error(
                "异步消息发送失败",
                extra={
                    "operation": "async_send_message",
                    "topic": message.topic,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "async_operation": True,
                    "success": False,
                },
                exc_info=True,
            )

        finally:
            await producer.shutdown()
            logger.info(
                "异步Producer已关闭",
                extra={"operation": "async_shutdown", "success": True},
            )

    asyncio.run(async_demo())
    print()


def demo_error_handling_json_logging():
    """演示错误处理的JSON日志功能"""
    print("=" * 60)
    print("📋 演示5: 错误处理JSON日志功能")
    print("=" * 60)

    # 配置错误处理专用JSON日志
    error_formatter = JsonFormatter(
        include_timestamp=True,
        include_extra=True,
        service="error-handler",
        component="exception-handling",
    )

    logger = get_logger("error_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(error_formatter)

    def error_handling_wrapper(func):
        """错误处理装饰器"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                success = True
                error = None

                logger.info(
                    f"{func.__name__}执行成功",
                    extra={
                        "function": func.__name__,
                        "args_count": len(args),
                        "kwargs_keys": list(kwargs.keys()),
                        "duration_ms": round((time.time() - start_time) * 1000, 2),
                        "success": True,
                    },
                )

                return result

            except Exception as e:
                success = False
                error = str(e)

                logger.error(
                    f"{func.__name__}执行失败",
                    extra={
                        "function": func.__name__,
                        "args_count": len(args),
                        "kwargs_keys": list(kwargs.keys()),
                        "duration_ms": round((time.time() - start_time) * 1000, 2),
                        "success": False,
                        "error_type": type(e).__name__,
                        "error_message": error,
                        "traceback": traceback.format_exc(),
                    },
                    exc_info=True,
                )

                raise

        return wrapper

    @error_handling_wrapper
    def risky_operation_1():
        """可能出错的操作1"""
        time.sleep(0.1)  # 模拟处理时间
        return {"status": "success", "data": "operation_1_result"}

    @error_handling_wrapper
    def risky_operation_2():
        """可能出错的操作2"""
        raise ValueError("这是一个故意抛出的异常")

    @error_handling_wrapper
    def risky_operation_3():
        """可能出错的操作3"""
        raise ConnectionError("网络连接失败")

    # 测试错误处理
    print("测试正常操作...")
    result1 = risky_operation_1()
    print(f"结果1: {result1}")

    print("\n测试异常操作1...")
    try:
        risky_operation_2()
    except Exception:
        pass  # 异常已被记录

    print("\n测试异常操作2...")
    try:
        risky_operation_3()
    except Exception:
        pass  # 异常已被记录

    print()


def demo_performance_monitoring():
    """演示性能监控的JSON日志功能"""
    print("=" * 60)
    print("📋 演示6: 性能监控JSON日志功能")
    print("=" * 60)

    # 配置性能监控专用JSON日志
    perf_formatter = JsonFormatter(
        include_timestamp=True,
        include_extra=True,
        service="performance-monitor",
        component="timing",
    )

    logger = get_logger("performance_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(perf_formatter)

    def performance_monitor(operation_name: str):
        """性能监控装饰器"""

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()

                try:
                    result = func(*args, **kwargs)
                    success = True
                    error = None
                except Exception as e:
                    success = False
                    error = str(e)
                    raise
                finally:
                    end_time = time.time()
                    duration = (end_time - start_time) * 1000  # 毫秒

                    logger.info(
                        f"操作性能统计",
                        extra={
                            "operation": operation_name,
                            "duration_ms": round(duration, 2),
                            "success": success,
                            "error": error,
                            "args_count": len(args),
                            "kwargs_count": len(kwargs),
                            "start_time": start_time,
                            "end_time": end_time,
                        },
                    )

                return result

            return wrapper

        return decorator

    @performance_monitor("fast_operation")
    def fast_operation():
        """快速操作"""
        time.sleep(0.01)
        return "fast_result"

    @performance_monitor("slow_operation")
    def slow_operation():
        """慢速操作"""
        time.sleep(0.2)
        return "slow_result"

    @performance_monitor("failing_operation")
    def failing_operation():
        """失败操作"""
        time.sleep(0.05)
        raise RuntimeError("这是一个运行时错误")

    # 测试性能监控
    print("测试快速操作...")
    result1 = fast_operation()
    print(f"结果: {result1}")

    print("\n测试慢速操作...")
    result2 = slow_operation()
    print(f"结果: {result2}")

    print("\n测试失败操作...")
    try:
        failing_operation()
    except Exception:
        pass  # 异常已被记录

    print()


def demo_batch_operations():
    """演示批量操作的JSON日志功能"""
    print("=" * 60)
    print("📋 演示7: 批量操作JSON日志功能")
    print("=" * 60)

    # 配置批量操作专用JSON日志
    batch_formatter = JsonFormatter(
        include_timestamp=True,
        include_extra=True,
        service="batch-processor",
        component="batch-operations",
    )

    logger = get_logger("batch_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(batch_formatter)

    def log_batch_operation(operation_type: str, items: list):
        """记录批量操作日志"""
        batch_id = f"batch_{int(time.time())}"
        start_time = time.time()

        logger.info(
            f"批量操作开始",
            extra={
                "operation_type": operation_type,
                "total_items": len(items),
                "batch_id": batch_id,
                "start_time": start_time,
            },
        )

        # 模拟批量处理
        success_count = 0
        error_count = 0
        errors = []

        for i, item in enumerate(items):
            try:
                # 模拟处理时间
                processing_time = 0.01 + (i % 3) * 0.02  # 变化的处理时间
                time.sleep(processing_time)

                # 模拟处理逻辑
                if not item.get("valid", True):
                    raise ValueError(f"无效项目: {item}")

                success_count += 1

            except Exception as e:
                error_count += 1
                error_info = {
                    "item_index": i,
                    "item_id": item.get("id", f"item_{i}"),
                    "error": str(e),
                    "processing_time": processing_time,
                }
                errors.append(error_info)

                logger.error(
                    f"批量操作中处理项目失败",
                    extra={
                        "operation_type": operation_type,
                        "batch_id": batch_id,
                        **error_info,
                    },
                )

        end_time = time.time()
        total_duration = (end_time - start_time) * 1000  # 毫秒

        logger.info(
            f"批量操作完成",
            extra={
                "operation_type": operation_type,
                "batch_id": batch_id,
                "total_items": len(items),
                "success_count": success_count,
                "error_count": error_count,
                "success_rate": round(success_count / len(items) * 100, 2)
                if items
                else 0,
                "total_duration_ms": round(total_duration, 2),
                "avg_item_duration_ms": round(total_duration / len(items), 2)
                if items
                else 0,
                "end_time": end_time,
                "errors": errors if errors else None,
            },
        )

        return {
            "success_count": success_count,
            "error_count": error_count,
            "errors": errors,
        }

    # 创建测试数据
    items = [
        {"id": 1, "valid": True, "data": "item_1"},
        {"id": 2, "valid": True, "data": "item_2"},
        {"id": 3, "valid": False, "data": "item_3"},
        {"id": 4, "valid": True, "data": "item_4"},
        {"id": 5, "valid": True, "data": "item_5"},
    ]

    # 执行批量操作
    result = log_batch_operation("data_validation", items)
    print(f"批量操作结果: 成功 {result['success_count']}, 失败 {result['error_count']}")

    if result["errors"]:
        print(f"错误详情: {len(result['errors'])} 个错误")
        for error in result["errors"][:2]:  # 只显示前2个错误
            print(f"  - 项目 {error['item_id']}: {error['error']}")

    print()


def demo_custom_fields():
    """演示自定义字段的JSON日志功能"""
    print("=" * 60)
    print("📋 演示8: 自定义字段JSON日志功能")
    print("=" * 60)

    # 创建带有自定义字段的格式化器
    custom_formatter = JsonFormatter(
        include_timestamp=True,
        include_level=True,
        include_extra=True,
        service="custom-logger-demo",
        application="pyrocketmq-demo",
        version="1.0.0",
        environment="development",
        host="localhost",
    )

    logger = get_logger("custom_demo")

    # 确保logger有处理器
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        logger.propagate = False

    # 为所有处理器设置JSON格式化器
    for handler in logger.handlers:
        handler.setFormatter(custom_formatter)

    # 复杂的业务对象日志
    class Order:
        def __init__(self, order_id: str, customer_id: str, amount: float, items: list):
            self.order_id = order_id
            self.customer_id = customer_id
            self.amount = amount
            self.items = items
            self.created_at = time.time()

    class Customer:
        def __init__(self, customer_id: str, name: str, email: str, level: str):
            self.customer_id = customer_id
            self.name = name
            self.email = email
            self.level = level

    # 创建业务对象
    customer = Customer(
        customer_id="CUST_001", name="张三", email="zhangsan@example.com", level="VIP"
    )

    order = Order(
        order_id="ORDER_001",
        customer_id=customer.customer_id,
        amount=299.99,
        items=[
            {"product_id": "PROD_001", "name": "商品A", "price": 99.99, "quantity": 2},
            {"product_id": "PROD_002", "name": "商品B", "price": 100.01, "quantity": 1},
        ],
    )

    # 记录复杂的业务日志
    logger.info(
        "订单创建成功",
        extra={
            "event": "order_created",
            "order": {
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "amount": order.amount,
                "item_count": len(order.items),
                "created_at": order.created_at,
            },
            "customer": {
                "customer_id": customer.customer_id,
                "name": customer.name,
                "email": customer.email,
                "level": customer.level,
            },
            "metadata": {
                "source": "web_api",
                "user_agent": "Mozilla/5.0...",
                "request_id": "REQ_123456",
            },
        },
    )

    # 嵌套结构的日志
    logger.info(
        "支付处理完成",
        extra={
            "event": "payment_completed",
            "payment": {
                "payment_id": "PAY_001",
                "order_id": order.order_id,
                "amount": order.amount,
                "method": "credit_card",
                "status": "success",
                "transaction_id": "TXN_789012",
            },
            "processing_time_ms": 1234.56,
            "risk_score": 0.15,
            "fraud_check": {"status": "passed", "score": 0.15, "flags": []},
        },
    )

    # 数组和列表字段
    logger.info(
        "批量库存更新",
        extra={
            "event": "inventory_updated",
            "updates": [
                {
                    "product_id": "PROD_001",
                    "old_stock": 100,
                    "new_stock": 98,
                    "change": -2,
                },
                {
                    "product_id": "PROD_002",
                    "old_stock": 50,
                    "new_stock": 55,
                    "change": 5,
                },
                {
                    "product_id": "PROD_003",
                    "old_stock": 0,
                    "new_stock": 10,
                    "change": 10,
                },
            ],
            "total_products": 3,
            "total_change": 13,
        },
    )

    print()


def demo_json_vs_structured():
    """演示JsonFormatter vs StructuredJsonFormatter的区别"""
    print("=" * 60)
    print("📋 演示9: JsonFormatter vs StructuredJsonFormatter")
    print("=" * 60)

    # 创建两种格式化器
    json_formatter = JsonFormatter(
        include_timestamp=True,
        include_level=True,
        include_extra=True,
        service="comparison-demo",
    )

    structured_formatter = StructuredJsonFormatter(
        include_extra=True, service="comparison-demo"
    )

    logger1 = get_logger("json_fmt")
    logger2 = get_logger("structured_fmt")

    # 确保logger1有处理器
    if not logger1.handlers:
        console_handler1 = logging.StreamHandler()
        console_handler1.setLevel(logging.INFO)
        logger1.addHandler(console_handler1)
        logger1.propagate = False

    # 为logger1的所有处理器设置JSON格式化器
    for handler in logger1.handlers:
        handler.setFormatter(json_formatter)

    # 确保logger2有处理器
    if not logger2.handlers:
        console_handler2 = logging.StreamHandler()
        console_handler2.setLevel(logging.INFO)
        logger2.addHandler(console_handler2)
        logger2.propagate = False

    # 为logger2的所有处理器设置结构化JSON格式化器
    for handler in logger2.handlers:
        handler.setFormatter(structured_formatter)

    # 相同的业务数据
    business_data = {
        "operation": "send_message",
        "topic": "comparison_topic",
        "message_id": "MSG_123456",
        "retry_count": 2,
        "success": True,
        "metadata": {"source": "api", "version": "v1"},
    }

    print("JsonFormatter输出:")
    print("-" * 40)
    logger1.info("消息发送成功", extra=business_data)

    print("\nStructuredJsonFormatter输出:")
    print("-" * 40)
    logger2.info("消息发送成功", extra=business_data)

    print()


def main():
    """主函数，运行所有演示"""
    print("🚀 pyrocketmq JSON日志功能演示")
    print("本演示展示了JSON日志的各种使用场景和最佳实践")
    print()

    try:
        # 运行所有演示
        demo_basic_json_logging()
        demo_structured_json_logging()
        demo_producer_json_logging()
        demo_async_producer_json_logging()
        demo_error_handling_json_logging()
        demo_performance_monitoring()
        demo_batch_operations()
        demo_custom_fields()
        demo_json_vs_structured()

        print("=" * 60)
        print("✅ 所有演示完成！")
        print("=" * 60)
        print("💡 提示：")
        print("- 1. 在生产环境中，建议使用紧凑的JSON格式")
        print("- 2. 合理设计日志字段，避免过多的元数据")
        print("- 3. 使用结构化字段名称，便于日志分析")
        print("- 4. 考虑异步日志处理以提高性能")
        print("- 5. 配置日志轮转和清理策略")

    except Exception as e:
        print(f"❌ 演示过程中出现错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
