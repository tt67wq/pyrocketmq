"""
RocketMQ远程命令数据结构
"""

import json
from dataclasses import dataclass, field


from .enums import FlagType, LanguageCode


@dataclass
class RemotingCommand:
    """RocketMQ远程命令数据结构"""

    code: int
    language: LanguageCode = LanguageCode.PYTHON
    version: int = 1
    opaque: int = 0
    flag: int = 0
    remark: str | None = None
    ext_fields: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None

    def __post_init__(self):
        """后初始化处理"""
        if not isinstance(self.ext_fields, dict):
            self.ext_fields = dict(self.ext_fields)

    @property
    def is_request(self) -> bool:
        """是否为请求类型"""
        return self.flag & FlagType.RPC_TYPE == FlagType.RPC_TYPE

    @property
    def is_response(self) -> bool:
        """是否为响应类型"""
        return self.flag & FlagType.RESPONSE_TYPE == FlagType.RESPONSE_TYPE

    @property
    def is_oneway(self) -> bool:
        """是否为单向消息"""
        return self.flag & FlagType.RPC_ONEWAY == FlagType.RPC_ONEWAY

    def set_request(self) -> None:
        """设置为请求类型"""
        self.flag |= FlagType.RPC_TYPE

    def set_response(self) -> None:
        """设置为响应类型"""
        self.flag |= FlagType.RESPONSE_TYPE

    def set_oneway(self) -> None:
        """设置为单向消息"""
        self.flag |= FlagType.RPC_ONEWAY

    def add_ext_field(self, key: str, value: str) -> None:
        """添加扩展字段"""
        self.ext_fields[key] = value

    def get_ext_field(self, key: str, default: str | None = None) -> str | None:
        """获取扩展字段"""
        return self.ext_fields.get(key, default)

    def remove_ext_field(self, key: str) -> str | None:
        """移除扩展字段"""
        return self.ext_fields.pop(key, None)

    def has_ext_field(self, key: str) -> bool:
        """检查是否包含扩展字段"""
        return key in self.ext_fields

    def clear_ext_fields(self) -> None:
        """清空扩展字段"""
        self.ext_fields.clear()

    def get_ext_fields_copy(self) -> dict[str, str]:
        """获取扩展字段的拷贝"""
        return self.ext_fields.copy()

    def get_header_length(self) -> int:
        """获取header数据长度（字节）"""
        header_data = self._serialize_header()
        return len(header_data)

    def get_total_length(self) -> int:
        """获取总数据长度（字节）"""
        body_length = len(self.body) if self.body else 0
        return 4 + self.get_header_length() + body_length

    def _serialize_header(self) -> bytes:
        """序列化header数据为JSON格式"""
        header_dict: dict[str, str | int | dict[str, str]] = {
            "code": self.code,
            "language": int(self.language),
            "version": self.version,
            "opaque": self.opaque,
            "flag": self.flag,
        }

        if self.remark:
            header_dict["remark"] = self.remark

        if self.ext_fields:
            header_dict["extFields"] = self.ext_fields

        return json.dumps(
            header_dict, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    def __str__(self) -> str:
        """字符串表示"""
        parts = [
            f"RemotingCommand(code={self.code}",
            f"language={self.language.name}",
            f"version={self.version}",
            f"opaque={self.opaque}",
            f"flag={self.flag}",
        ]

        if self.remark:
            parts.append(f"remark='{self.remark}'")

        if self.ext_fields:
            parts.append(f"ext_fields={self.ext_fields}")

        if self.body:
            parts.append(f"body_len={len(self.body)}")

        return ", ".join(parts) + ")"

    def __repr__(self) -> str:
        """调试表示"""
        return self.__str__()

    def __eq__(self, other: object) -> bool:
        """相等性比较"""
        if not isinstance(other, RemotingCommand):
            return False

        return (
            self.code == other.code
            and self.language == other.language
            and self.version == other.version
            and self.opaque == other.opaque
            and self.flag == other.flag
            and self.remark == other.remark
            and self.ext_fields == other.ext_fields
            and self.body == other.body
        )

    def __hash__(self) -> int:
        """哈希值"""
        return hash(
            (
                self.code,
                self.language,
                self.version,
                self.opaque,
                self.flag,
                self.remark,
                frozenset(self.ext_fields.items()),
                self.body,
            )
        )
