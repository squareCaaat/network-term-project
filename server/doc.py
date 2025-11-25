"""문서 상태 관리 및 패치 유효성 검증 로직."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple


VALID_EDIT_OPS = {"INSERT", "DELETE", "REPLACE"}


@dataclass
class DocState:
    """단일 문서의 내용/버전/구독자 상태."""

    id: str
    content: str = ""
    version: int = 0
    subscribers: Set[str] = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def snapshot_payload(self) -> Dict[str, object]:
        return {
            "docId": self.id,
            "version": self.version,
            "content": self.content,
        }


def apply_operation(
    content: str, message: Dict[str, object]
) -> Tuple[bool, str, Optional[Dict[str, object]], str]:
    """클라이언트 연산을 적용하고 결과/패치를 반환."""
    op = str(message.get("op", "")).upper()
    if op not in VALID_EDIT_OPS:
        return False, content, None, "INVALID_OP"

    try:
        pos = int(message.get("pos", 0))
    except (TypeError, ValueError):
        return False, content, None, "INVALID_RANGE"

    if pos < 0 or pos > len(content):
        return False, content, None, "INVALID_RANGE"

    if op == "INSERT":
        text = message.get("text", "")
        if not isinstance(text, str):
            return False, content, None, "INVALID_PAYLOAD"
        new_content = content[:pos] + text + content[pos:]
        patch = {"type": "INSERT", "pos": pos, "text": text}
        return True, new_content, patch, ""

    if op == "DELETE":
        try:
            length = _coerce_length(message.get("len"))
        except ValueError:
            return False, content, None, "INVALID_RANGE"
        if pos + length > len(content):
            return False, content, None, "INVALID_RANGE"
        new_content = content[:pos] + content[pos + length :]
        patch = {"type": "DELETE", "pos": pos, "len": length}
        return True, new_content, patch, ""

    # REPLACE
    try:
        length = _coerce_length(message.get("len"))
    except ValueError:
        return False, content, None, "INVALID_RANGE"
    text = message.get("text", "")
    if not isinstance(text, str):
        return False, content, None, "INVALID_PAYLOAD"
    if pos + length > len(content):
        return False, content, None, "INVALID_RANGE"
    new_content = content[:pos] + text + content[pos + length :]
    patch = {"type": "REPLACE", "pos": pos, "len": length, "text": text}
    return True, new_content, patch, ""


def apply_patch_dict(content: str, patch: Dict[str, object]) -> str:
    """영속화 로그 등에 기록된 패치를 재적용."""
    ptype = str(patch.get("type", "")).upper()
    pos = int(patch.get("pos", 0))
    if pos < 0 or pos > len(content):
        raise ValueError("patch position out of range")

    if ptype == "INSERT":
        text = patch.get("text", "")
        if not isinstance(text, str):
            raise ValueError("invalid insert payload")
        return content[:pos] + text + content[pos:]

    if ptype == "DELETE":
        length = _coerce_length(patch.get("len"))
        if pos + length > len(content):
            raise ValueError("delete length overflow")
        return content[:pos] + content[pos + length :]

    if ptype == "REPLACE":
        length = _coerce_length(patch.get("len"))
        text = patch.get("text", "")
        if not isinstance(text, str):
            raise ValueError("invalid replace payload")
        if pos + length > len(content):
            raise ValueError("replace length overflow")
        return content[:pos] + text + content[pos + length :]

    raise ValueError(f"unsupported patch type: {ptype}")


def _coerce_length(value: Optional[object]) -> int:
    if value is None:
        raise ValueError("length missing")
    length = int(value)
    if length < 0:
        raise ValueError("length negative")
    return length


__all__ = [
    "DocState",
    "apply_operation",
    "apply_patch_dict",
]