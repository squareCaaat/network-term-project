"""JSON line 기반 프로토콜 유틸리티."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence


MAX_MESSAGE_BYTES = 1_000_000  # 1MB 제한 (오용 방지)


class ProtocolError(Exception):
    """프레이밍/파싱 중 발생하는 예외."""


class JsonLineFramer:
    """TCP 스트림을 JSON line 단위로 분리하는 헬퍼."""

    def __init__(self, *, max_message_bytes: int = MAX_MESSAGE_BYTES) -> None:
        self._buffer = bytearray()
        self._max_message_bytes = max_message_bytes

    def feed(self, chunk: bytes) -> List[Dict[str, Any]]:
        """새로운 바이트 청크를 넣고 완성된 메시지들을 반환."""
        if not chunk:
            return []
        self._buffer.extend(chunk)
        if len(self._buffer) > self._max_message_bytes:
            raise ProtocolError("message exceeds max size")

        messages: List[Dict[str, Any]] = []
        while True:
            newline_index = self._buffer.find(b"\n")
            if newline_index == -1:
                break
            line = self._buffer[:newline_index]
            del self._buffer[: newline_index + 1]
            line = line.strip()
            if not line:
                continue
            messages.append(parse_json_line(line))
        return messages

    def flush(self) -> None:
        """버퍼 초기화."""
        self._buffer.clear()


def parse_json_line(line: bytes) -> Dict[str, Any]:
    """단일 JSON line을 dict로 파싱."""
    try:
        return json.loads(line.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"bad json: {exc}") from exc


def encode_message(obj: Dict[str, Any]) -> bytes:
    """dict를 JSON line 바이트로 직렬화."""
    try:
        payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"cannot encode message: {exc}") from exc
    return (payload + "\n").encode("utf-8")


def encode_batch(messages: Sequence[Dict[str, Any]]) -> bytes:
    """여러 메시지를 한 번에 직렬화."""
    return b"".join(encode_message(msg) for msg in messages)


__all__ = [
    "JsonLineFramer",
    "ProtocolError",
    "encode_message",
    "encode_batch",
    "parse_json_line",
]