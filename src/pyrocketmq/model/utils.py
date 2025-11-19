"""
RocketMQ模型层工具函数
"""

import json
import os
import socket
import struct
import threading
import time
from typing import Any, Final

from .command import RemotingCommand
from .enums import (
    FlagType,
    LanguageCode,
    LocalTransactionState,
    RequestCode,
    ResponseCode,
)
from .errors import ValidationError

# ============================================================================
# 唯一ID生成相关常量和变量
# ============================================================================

# 起始时间戳（程序启动时记录）
START_TIMESTAMP: Final[int] = int(time.time())

# 全局变量用于ID生成
_counter: int = 0
_next_timestamp: int = 0
_counter_lock: threading.Lock = threading.Lock()


# 类加载器ID（Go代码中的classLoadId的Python等价物）
_CLASS_LOAD_ID: Final[int] = hash(time.time()) & 0xFFFFFFFF


def _get_client_ip4() -> bytes:
    """获取客户端IPv4地址（Go的utils.ClientIP4()的Python实现）"""
    try:
        # 尝试连接外部地址获取本地IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
    except Exception:
        # 如果失败，使用本地回环地址
        ip = "127.0.0.1"

    # 将IP地址转换为4字节
    return socket.inet_aton(ip)


def _get_pid() -> int:
    """获取进程ID（Go的Pid()的Python实现）"""
    return os.getpid() % 32768  # 确保在 int16 范围内


def _generate_prefix() -> str:
    """
    生成ID前缀，与Go实现完全一致

    Go代码参考：
    ```go
    func init() {
        buf := new(bytes.Buffer)

        ip, err := utils.ClientIP4()
        if err != nil {
            ip = utils.FakeIP()
        }
        _, _ = buf.Write(ip)
        _ = binary.Write(buf, binary.BigEndian, Pid())
        _ = binary.Write(buf, binary.BigEndian, classLoadId)
        prefix = strings.ToUpper(hex.EncodeToString(buf.Bytes()))
    }
    ```

    Returns:
        str: 大写的十六进制编码前缀
    """
    buf = bytearray()

    # 写入IP地址（4字节）
    try:
        ip_bytes = _get_client_ip4()
    except Exception:
        # 使用Go的FakeIP()等价物 - 使用固定的测试IP
        ip_bytes = socket.inet_aton("192.168.0.1")

    buf.extend(ip_bytes)

    # 写入进程ID（int16，大端序）
    buf.extend(struct.pack(">h", _get_pid()))

    # 写入类加载器ID（int32，大端序）
    buf.extend(struct.pack(">i", _CLASS_LOAD_ID & 0xFFFF))

    # 编码为大写的十六进制字符串
    return buf.hex().upper()


# 在模块初始化时生成前缀
_ID_PREFIX: str = _generate_prefix()


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
        raise ValidationError(f"无效的ext_fields类型: {type(command.ext_fields)}")

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


def get_topic_from_command(command: RemotingCommand) -> str | None:
    """从命令中提取主题信息

    Args:
        command: 命令对象

    Returns:
        主题名称，如果不存在则返回None
    """
    return command.ext_fields.get("topic")


def get_group_from_command(command: RemotingCommand) -> str | None:
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


def get_queue_id_from_command(command: RemotingCommand) -> int | None:
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


def get_offset_from_command(command: RemotingCommand) -> int | None:
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
    body: bytes | None = None,
    remark: str | None = None,
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
    commands: list[RemotingCommand], topic: str
) -> list[RemotingCommand]:
    """按主题过滤命令列表

    Args:
        commands: 命令列表
        topic: 主题名称

    Returns:
        过滤后的命令列表
    """
    return [cmd for cmd in commands if get_topic_from_command(cmd) == topic]


def filter_commands_by_group(
    commands: list[RemotingCommand], group: str
) -> list[RemotingCommand]:
    """按组过滤命令列表

    Args:
        commands: 命令列表
        group: 组名称

    Returns:
        过滤后的命令列表
    """
    return [cmd for cmd in commands if get_group_from_command(cmd) == group]


def get_command_stats(commands: list[RemotingCommand]) -> dict[str, int]:
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


def format_ext_fields_for_display(ext_fields: dict[str, str]) -> str:
    """格式化扩展字段用于显示

    Args:
        ext_fields: 扩展字段字典

    Returns:
        格式化后的字符串
    """
    if not ext_fields:
        return "{}"

    lines: list[str] = []
    for key, value in sorted(ext_fields.items()):
        lines.append(f"  {key}: {value}")

    return "{\n" + "\n".join(lines) + "\n}"


def command_to_dict(command: RemotingCommand) -> dict[str, Any]:
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
        "body_preview": command.body[:100].decode("utf-8", errors="ignore") + "..."
        if command.body and len(command.body) > 100
        else command.body.decode("utf-8", errors="ignore")
        if command.body
        else None,
    }


def commands_to_json(commands: list[RemotingCommand], indent: int | None = 2) -> str:
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


# ============================================================================
# 唯一ID生成相关函数
# ============================================================================


def _update_timestamp() -> None:
    """更新时间戳记录"""
    global _next_timestamp
    _next_timestamp = int(time.time())


def create_uniq_id() -> str:
    """
    创建唯一ID

    与Go实现完全对齐：
    1. 检查当前时间是否超过nextTimestamp，如果是则更新时间戳
    2. 递增计数器
    3. 将时间差和计数器写入字节缓冲区（大端序，int32）
    4. 编码为十六进制字符串并添加前缀

    Go代码参考：
    ```go
    func CreateUniqID() string {
        locker.Lock()
        defer locker.Unlock()

        if time.Now().Unix() > nextTimestamp {
            updateTimestamp()
        }
        counter++
        buf := new(bytes.Buffer)
        _ = binary.Write(buf, binary.BigEndian, int32((time.Now().Unix()-startTimestamp)*1000))
        _ = binary.Write(buf, binary.BigEndian, counter)

        return prefix + hex.EncodeToString(buf.Bytes())
    }
    ```

    Returns:
        str: 格式为 prefix + 十六进制编码的唯一ID
    """
    global _counter, _next_timestamp

    with _counter_lock:
        current_unix_time = int(time.time())

        # 检查当前时间是否超过nextTimestamp
        if current_unix_time > _next_timestamp:
            _update_timestamp()

        # 递增计数器
        _counter += 1
        # 如果_counter溢出int16，重置为0
        if _counter > 0xFFFF:
            _counter = 0

        # 计算时间差（毫秒），与Go实现保持一致
        time_diff_ms = (current_unix_time - START_TIMESTAMP) * 1000

        # 创建字节缓冲区并写入数据（大端序，int32）
        # 与Go的binary.Write(buf, binary.BigEndian, int32(...))保持一致
        buf = bytearray()

        # 写入时间差（int32，大端序）
        buf.extend(struct.pack(">i", time_diff_ms))

        # 写入计数器（int16，大端序）
        buf.extend(struct.pack(">h", _counter))

        # 编码为十六进制字符串
        hex_str = buf.hex()

        # 返回带动态生成前缀的唯一ID
        return f"{_ID_PREFIX}{hex_str}"


def parse_uniq_id(uniq_id: str) -> tuple[int, int] | None:
    """
    解析唯一ID，提取时间戳和计数器信息

    注意：前缀是动态生成的，长度为24字符（IP 4字节 + PID 4字节 + ClassLoadId 4字节 = 12字节 = 24字符hex）
    数据部分长度为6字节（int32时间差 4字节 + int16计数器 2字节 = 6字节 = 12字符hex）

    Args:
        uniq_id: 唯一ID字符串

    Returns:
        tuple[int, int] | None: (时间差毫秒, 计数器) 的元组，解析失败返回None
    """
    if not uniq_id.startswith(_ID_PREFIX):
        return None

    try:
        # 移除前缀
        hex_part = uniq_id[len(_ID_PREFIX) :]

        # 确保hex字符串长度正确（int32时间差4字节 + int16计数器2字节 = 6字节 = 12字符hex）
        if len(hex_part) != 12:
            return None

        # 将十六进制字符串转换为字节
        buf = bytes.fromhex(hex_part)

        # 解包数据（大端序）：int32时间差 + int16计数器
        time_diff_ms, counter = struct.unpack(">ih", buf)

        return time_diff_ms, counter

    except (ValueError, struct.error):
        return None


def get_timestamp_from_uniq_id(uniq_id: str) -> int | None:
    """
    从唯一ID中提取时间戳

    Args:
        uniq_id: 唯一ID字符串

    Returns:
        int | None: Unix时间戳，解析失败返回None
    """
    result = parse_uniq_id(uniq_id)
    if result is None:
        return None

    time_diff_ms, _ = result

    # 转换为秒并加上起始时间戳
    return START_TIMESTAMP + (time_diff_ms // 1000)


def is_valid_uniq_id(uniq_id: str) -> bool:
    """
    验证唯一ID格式是否有效

    Args:
        uniq_id: 待验证的唯一ID

    Returns:
        bool: 是否有效
    """
    return parse_uniq_id(uniq_id) is not None
