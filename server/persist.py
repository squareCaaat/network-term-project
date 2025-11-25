"""문서 스냅샷 및 오플로그 영속화 유틸."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, Tuple

from .doc import DocState, apply_patch_dict

LOGGER = logging.getLogger(__name__)


def ensure_storage(snapshot_dir: Path, oplog_dir: Path) -> Tuple[Path, Path]:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    oplog_dir.mkdir(parents=True, exist_ok=True)
    return snapshot_dir, oplog_dir


def load_doc_content(doc_id: str, snapshot_dir: Path, oplog_dir: Path) -> Tuple[str, int]:
    """스냅샷과 오플로그를 적용하여 최신 내용을 반환."""
    snapshot_dir, oplog_dir = ensure_storage(snapshot_dir, oplog_dir)
    content, version = _read_snapshot(doc_id, snapshot_dir)
    content, version = _replay_oplog(doc_id, content, version, oplog_dir)
    return content, version


def save_snapshot(doc: DocState, snapshot_dir: Path) -> None:
    """DocState를 스냅샷(JSON)으로 저장."""
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / f"{doc.id}.json"
    data = {
        "docId": doc.id,
        "version": doc.version,
        "content": doc.content,
    }
    tmp_fd, tmp_path = tempfile.mkstemp(dir=snapshot_dir, prefix=f".{doc.id}.", suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
        LOGGER.debug("snapshot saved: %s v%s", doc.id, doc.version)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def append_oplog(doc_id: str, version: int, patch: Dict[str, object], by: str, oplog_dir: Path) -> None:
    """오플로그(JSON Lines)에 패치 기록."""
    oplog_dir.mkdir(parents=True, exist_ok=True)
    entry = {
        "docId": doc_id,
        "version": version,
        "patch": patch,
        "by": by,
        "ts": time.time(),
    }
    path = oplog_dir / f"{doc_id}.logl"
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")


def _read_snapshot(doc_id: str, snapshot_dir: Path) -> Tuple[str, int]:
    path = snapshot_dir / f"{doc_id}.json"
    if not path.exists():
        return "", 0
    try:
        with path.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
        content = data.get("content", "")
        version = int(data.get("version", 0))
        return content, version
    except Exception as exc:
        LOGGER.warning("snapshot load failed (%s): %s", doc_id, exc)
        return "", 0


def _replay_oplog(doc_id: str, base_content: str, base_version: int, oplog_dir: Path) -> Tuple[str, int]:
    path = oplog_dir / f"{doc_id}.logl"
    if not path.exists():
        return base_content, base_version

    content = base_content
    version = base_version
    try:
        with path.open("r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    LOGGER.warning("skip bad oplog line (%s)", doc_id)
                    continue
                entry_version = int(entry.get("version", 0))
                if entry_version <= version:
                    continue
                patch = entry.get("patch")
                if not isinstance(patch, dict):
                    continue
                try:
                    content = apply_patch_dict(content, patch)
                except Exception as exc:  # pragma: no cover (방어 코드)
                    LOGGER.error("oplog patch failed (%s v%s): %s", doc_id, entry_version, exc)
                    break
                version = entry_version
    except FileNotFoundError:
        return base_content, base_version
    return content, version


__all__ = [
    "append_oplog",
    "ensure_storage",
    "load_doc_content",
    "save_snapshot",
]