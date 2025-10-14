"""
RocketMQ模型层工具函数
"""

import json
import time
from typing import Any, Dict, List, Optional

from .command import RemotingCommand
from .enums import (
    FlagType,
    LanguageCode,
    LocalTransactionState,
    RequestCode,
    ResponseCode,
)
from .errors import ValidationError


def validate_command(command: RemotingCommand) -> bool:
    """验证命令对象的有效性

    Args:
        command: 要验证的命令对象

    Returns:
        是否有效

    Raises:
        ValidationError: 验证失败时抛出
    """
    if not isinstance(command.code, int):
        raise ValidationError(f"无效的code类型: {type(command.code)}")

    if not isinstance(command.language, LanguageCode):
        raise ValidationError(f"无效的language类型: {type(command.language)}")

    if not isinstance(command.version, int):
        raise ValidationError(f"无效的version类型: {type(command.version)}")

    if not isinstance(command.opaque, int):
        raise ValidationError(f"无效的opaque类型: {type(command.opaque)}")

    if not isinstance(command.flag, int):
        raise ValidationError(f"无效的flag类型: {type(command.flag)}")

    if command.remark is not None and not isinstance(command.remark, str):
        raise ValidationError(f"无效的remark类型: {type(command.remark)}")

    if not isinstance(command.ext_fields, dict):
        raise ValidationError(
            f"无效的ext_fields类型: {type(command.ext_fields)}"
        )

    # 验证ext_fields中的值类型
    for key, value in command.ext_fields.items():
        if not isinstance(key, str):
            raise ValidationError(f"ext_fields key必须是字符串: {key}")
        if not isinstance(value, str):
            raise ValidationError(f"ext_fields value必须是字符串: {value}")

    if command.body is not None and not isinstance(command.body, bytes):
        raise ValidationError(f"无效的body类型: {type(command.body)}")

    return True


def generate_opaque() -> int:
    """生成唯一的opaque值

    Returns:
        基于时间戳的唯一ID
    """
    return int(time.time() * 1000) % 1000000000


def is_success_response(command: RemotingCommand) -> bool:
    """检查是否为成功响应

    Args:
        command: 命令对象

    Returns:
        是否为成功响应
    """
    return command.is_response and command.code == ResponseCode.SUCCESS


def is_error_response(command: RemotingCommand) -> bool:
    """检查是否为错误响应

    Args:
        command: 命令对象

    Returns:
        是否为错误响应
    """
    return command.is_response and command.code != ResponseCode.SUCCESS


def get_command_summary(command: RemotingCommand) -> str:
    """获取命令摘要信息

    Args:
        command: 命令对象

    Returns:
        摘要字符串
    """
    type_str = "UNKNOWN"
    if command.is_request:
        type_str = "REQUEST"
    elif command.is_response:
        type_str = "RESPONSE"
    elif command.is_oneway:
        type_str = "ONEWAY"

    body_size = len(command.body) if command.body else 0
    ext_count = len(command.ext_fields)

    return f"{type_str}[code={command.code}, opaque={command.opaque}, body={body_size}B, ext={ext_count}]"


def get_topic_from_command(command: RemotingCommand) -> Optional[str]:
    """从命令中提取主题信息

    Args:
        command: 命令对象

    Returns:
        主题名称，如果不存在则返回None
    """
    return command.ext_fields.get("topic")


def get_group_from_command(command: RemotingCommand) -> Optional[str]:
    """从命令中提取组信息（生产者组或消费者组）

    Args:
        command: 命令对象

    Returns:
        组名称，如果不存在则返回None
    """
    # 优先返回消费者组
    consumer_group = command.ext_fields.get("consumerGroup")
    if consumer_group:
        return consumer_group

    # 其次返回生产者组
    return command.ext_fields.get("producerGroup")


def get_queue_id_from_command(command: RemotingCommand) -> Optional[int]:
    """从命令中提取队列ID

    Args:
        command: 命令对象

    Returns:
        队列ID，如果不存在或无效则返回None
    """
    queue_id_str = command.ext_fields.get("queueId")
    if queue_id_str:
        try:
            return int(queue_id_str)
        except ValueError:
            return None
    return None


def get_offset_from_command(command: RemotingCommand) -> Optional[int]:
    """从命令中提取偏移量

    Args:
        command: 命令对象

    Returns:
        偏移量，如果不存在或无效则返回None
    """
    offset_str = command.ext_fields.get("offset")
    if offset_str:
        try:
            return int(offset_str)
        except ValueError:
            return None
    return None


def copy_command_with_new_opaque(
    command: RemotingCommand, new_opaque: int
) -> RemotingCommand:
    """复制命令并设置新的opaque值

    Args:
        command: 原命令
        new_opaque: 新的opaque值

    Returns:
        复制的新命令
    """
    return RemotingCommand(
        code=command.code,
        language=command.language,
        version=command.version,
        opaque=new_opaque,
        flag=command.flag,
        remark=command.remark,
        ext_fields=command.ext_fields.copy(),
        body=command.body,  # bytes类型不需要copy
    )


def create_response_for_request(
    request: RemotingCommand,
    response_code: int,
    body: Optional[bytes] = None,
    remark: Optional[str] = None,
) -> RemotingCommand:
    """为请求创建对应的响应

    Args:
        request: 请求命令
        response_code: 响应代码
        body: 响应体，可选
        remark: 备注信息，可选

    Returns:
        响应命令
    """
    return RemotingCommand(
        code=response_code,
        language=request.language,
        version=request.version,
        opaque=request.opaque,
        flag=FlagType.RESPONSE_TYPE,
        remark=remark,
        ext_fields={},
        body=body,
    )


def filter_commands_by_topic(
    commands: List[RemotingCommand], topic: str
) -> List[RemotingCommand]:
    """按主题过滤命令列表

    Args:
        commands: 命令列表
        topic: 主题名称

    Returns:
        过滤后的命令列表
    """
    return [cmd for cmd in commands if get_topic_from_command(cmd) == topic]


def filter_commands_by_group(
    commands: List[RemotingCommand], group: str
) -> List[RemotingCommand]:
    """按组过滤命令列表

    Args:
        commands: 命令列表
        group: 组名称

    Returns:
        过滤后的命令列表
    """
    return [cmd for cmd in commands if get_group_from_command(cmd) == group]


def get_command_stats(commands: List[RemotingCommand]) -> Dict[str, int]:
    """获取命令统计信息

    Args:
        commands: 命令列表

    Returns:
        统计信息字典
    """
    stats = {
        "total": len(commands),
        "requests": 0,
        "responses": 0,
        "oneway": 0,
        "success": 0,
        "errors": 0,
    }

    for cmd in commands:
        if cmd.is_request:
            stats["requests"] += 1
        elif cmd.is_response:
            stats["responses"] += 1
            if is_success_response(cmd):
                stats["success"] += 1
            else:
                stats["errors"] += 1
        elif cmd.is_oneway:
            stats["oneway"] += 1

    return stats


def format_ext_fields_for_display(ext_fields: Dict[str, str]) -> str:
    """格式化扩展字段用于显示

    Args:
        ext_fields: 扩展字段字典

    Returns:
        格式化后的字符串
    """
    if not ext_fields:
        return "{}"

    lines = []
    for key, value in sorted(ext_fields.items()):
        lines.append(f"  {key}: {value}")

    return "{\n" + "\n".join(lines) + "\n}"


def command_to_dict(command: RemotingCommand) -> Dict[str, Any]:
    """将命令转换为字典格式

    Args:
        command: 命令对象

    Returns:
        字典格式的命令
    """
    return {
        "code": command.code,
        "language": command.language.name,
        "version": command.version,
        "opaque": command.opaque,
        "flag": command.flag,
        "type": "REQUEST"
        if command.is_request
        else "RESPONSE"
        if command.is_response
        else "ONEWAY",
        "remark": command.remark,
        "ext_fields": command.ext_fields,
        "body_size": len(command.body) if command.body else 0,
        "body_preview": command.body[:100].decode("utf-8", errors="ignore")
        + "..."
        if command.body and len(command.body) > 100
        else command.body.decode("utf-8", errors="ignore")
        if command.body
        else None,
    }


def commands_to_json(
    commands: List[RemotingCommand], indent: Optional[int] = 2
) -> str:
    """将命令列表转换为JSON格式

    Args:
        commands: 命令列表
        indent: 缩进空格数，None表示紧凑格式

    Returns:
        JSON格式字符串
    """
    command_dicts = [command_to_dict(cmd) for cmd in commands]
    return json.dumps(command_dicts, indent=indent, ensure_ascii=False)


def parse_command_from_json(json_str: str) -> RemotingCommand:
    """从JSON字符串解析命令

    Args:
        json_str: JSON格式字符串

    Returns:
        解析后的命令对象
    """
    data = json.loads(json_str)

    # 解析语言枚举
    language = LanguageCode[data["language"]]

    # 创建命令对象
    command = RemotingCommand(
        code=data["code"],
        language=language,
        version=data["version"],
        opaque=data["opaque"],
        flag=data["flag"],
        remark=data.get("remark"),
        ext_fields=data.get("ext_fields", {}),
        body=None,  # body在JSON中只包含预览信息
    )

    return command


def get_command_type_name(command: RemotingCommand) -> str:
    """获取命令类型名称

    Args:
        command: 命令对象

    Returns:
        类型名称字符串
    """
    if command.is_request:
        return "REQUEST"
    elif command.is_response:
        return "RESPONSE"
    elif command.is_oneway:
        return "ONEWAY"
    else:
        return "UNKNOWN"


def is_heartbeat_command(command: RemotingCommand) -> bool:
    """检查是否为心跳命令

    Args:
        command: 命令对象

    Returns:
        是否为心跳命令
    """
    return command.code == RequestCode.HEART_BEAT


def is_send_message_command(command: RemotingCommand) -> bool:
    """检查是否为发送消息命令

    Args:
        command: 命令对象

    Returns:
        是否为发送消息命令
    """
    return command.code in (
        RequestCode.SEND_MESSAGE,
        RequestCode.SEND_BATCH_MESSAGE,
    )


def is_pull_message_command(command: RemotingCommand) -> bool:
    """检查是否为拉取消息命令

    Args:
        command: 命令对象

    Returns:
        是否为拉取消息命令
    """
    return command.code == RequestCode.PULL_MESSAGE


def transaction_state(state: LocalTransactionState) -> int:
    """将本地事务状态转换为事务类型

    Args:
        state: 本地事务状态

    Returns:
        对应的事务类型常量
    """
    from .enums import (
        TRANSACTION_COMMIT_TYPE,
        TRANSACTION_NOT_TYPE,
        TRANSACTION_ROLLBACK_TYPE,
        LocalTransactionState,
    )

    if state == LocalTransactionState.COMMIT_MESSAGE_STATE:
        return TRANSACTION_COMMIT_TYPE
    elif state == LocalTransactionState.ROLLBACK_MESSAGE_STATE:
        return TRANSACTION_ROLLBACK_TYPE
    elif state == LocalTransactionState.UNKNOW_STATE:
        return TRANSACTION_NOT_TYPE
    else:
        return TRANSACTION_NOT_TYPE
