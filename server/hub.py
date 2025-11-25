"""세션/문서 라우팅 및 서버 비즈니스 로직."""

from __future__ import annotations

import logging
import socket
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

from .doc import DocState, apply_operation
from .persist import append_oplog, ensure_storage, load_doc_content, save_snapshot
from .protocol import ProtocolError, encode_message

LOGGER = logging.getLogger(__name__)


class Session:
    """TCP 세션 상태."""

    def __init__(self, sid: str, sock: socket.socket, addr: tuple[str, int]):
        self.id = sid
        self.socket = sock
        self.addr = addr
        self.name = "anon"
        self.hello_received = False
        self.alive = True
        self.subscriptions: set[str] = set()
        self._writer_lock = threading.Lock()
        self.last_seen = time.monotonic()

    def send(self, payload: Dict[str, object]) -> None:
        if not self.alive:
            raise ConnectionError("session closed")
        data = encode_message(payload)
        with self._writer_lock:
            try:
                self.socket.sendall(data)
            except OSError as exc:
                self.alive = False
                raise ConnectionError("send failed") from exc

    def close(self) -> None:
        if not self.alive:
            return
        self.alive = False
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            try:
                self.socket.close()
            except OSError:
                pass

    def touch(self) -> None:
        self.last_seen = time.monotonic()


class ServerHub:
    """문서/세션간 메시지 라우팅을 담당."""

    def __init__(
        self,
        *,
        snapshot_dir: Path,
        oplog_dir: Path,
        snapshot_interval: int = 50,
        heartbeat_timeout: int = 120,
    ) -> None:
        self.snapshot_dir, self.oplog_dir = ensure_storage(snapshot_dir, oplog_dir)
        self.snapshot_interval = max(1, snapshot_interval)
        self.heartbeat_timeout = heartbeat_timeout
        self.sessions: Dict[str, Session] = {}
        self.docs: Dict[str, DocState] = {}
        self._sessions_lock = threading.Lock()
        self._docs_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()

    # ---------- 세션 관리 ----------
    def new_session(self, sock: socket.socket, addr: tuple[str, int]) -> Session:
        sid = f"S-{uuid.uuid4().hex[:8]}"
        session = Session(sid, sock, addr)
        with self._sessions_lock:
            self.sessions[sid] = session
        LOGGER.info("session connected: %s %s", sid, addr)
        return session

    def unregister_session(self, session: Session) -> None:
        with self._sessions_lock:
            self.sessions.pop(session.id, None)
        for doc_id in list(session.subscriptions):
            doc = self._get_doc_if_loaded(doc_id)
            if doc:
                with doc.lock:
                    doc.subscribers.discard(session.id)
        session.close()
        LOGGER.info("session closed: %s", session.id)

    # ---------- 문서 접근 ----------
    def _get_doc_if_loaded(self, doc_id: str) -> Optional[DocState]:
        with self._docs_lock:
            return self.docs.get(doc_id)

    def get_doc(self, doc_id: str) -> DocState:
        doc = self._get_doc_if_loaded(doc_id)
        if doc:
            return doc
        with self._docs_lock:
            doc = self.docs.get(doc_id)
            if doc:
                return doc
            content, version = load_doc_content(doc_id, self.snapshot_dir, self.oplog_dir)
            doc = DocState(doc_id, content, version)
            self.docs[doc_id] = doc
            return doc

    # ---------- 라우팅 ----------
    def route_message(self, session: Session, message: Dict[str, object]) -> None:
        session.touch()
        op = str(message.get("op", "")).upper()
        if not op:
            self.send_error(session, "INVALID_OP", hint="missing op")
            return

        if op == "HELLO":
            self._handle_hello(session, message)
        elif op == "SUBSCRIBE":
            self._handle_subscribe(session, message)
        elif op == "GET_SNAPSHOT":
            self._handle_snapshot(session, message)
        elif op in {"INSERT", "DELETE", "REPLACE"}:
            self._handle_edit(session, message)
        elif op == "PING":
            self._safe_send(session, {"ev": "PONG"})
        else:
            self.send_error(session, "UNKNOWN_OP", hint=op)

    def _handle_hello(self, session: Session, message: Dict[str, object]) -> None:
        name = message.get("name") or "anon"
        session.name = str(name)
        session.hello_received = True
        payload = {
            "ev": "WELCOME",
            "sessionId": session.id,
            "serverVersion": self._max_version(),
        }
        self._safe_send(session, payload)

    def _handle_subscribe(self, session: Session, message: Dict[str, object]) -> None:
        if not session.hello_received:
            self.send_error(session, "NOT_READY", hint="send HELLO first")
            return
        try:
            doc_id = self._normalize_doc_id(message.get("docId"))
        except ValueError:
            self.send_error(session, "INVALID_DOC", hint="docId required")
            return
        doc = self.get_doc(doc_id)
        with doc.lock:
            doc.subscribers.add(session.id)
        session.subscriptions.add(doc_id)
        snapshot = doc.snapshot_payload()
        snapshot["ev"] = "DOC_SNAPSHOT"
        self._safe_send(session, snapshot)

    def _handle_snapshot(self, session: Session, message: Dict[str, object]) -> None:
        try:
            doc_id = self._normalize_doc_id(message.get("docId"))
        except ValueError:
            self.send_error(session, "INVALID_DOC", hint="docId required")
            return
        doc = self.get_doc(doc_id)
        snapshot = doc.snapshot_payload()
        snapshot["ev"] = "DOC_SNAPSHOT"
        self._safe_send(session, snapshot)

    def _handle_edit(self, session: Session, message: Dict[str, object]) -> None:
        if not session.hello_received:
            self.send_error(session, "NOT_READY", hint="send HELLO first")
            return

        try:
            doc_id = self._normalize_doc_id(message.get("docId"))
        except ValueError:
            self.send_error(session, "INVALID_DOC", hint="docId required")
            return
        try:
            base_version = int(message.get("base"))
        except (TypeError, ValueError):
            base_version = -1
        doc = self.get_doc(doc_id)
        patch_result: Optional[Dict[str, object]] = None
        current_version: Optional[int] = None
        error_meta: Optional[tuple[str, Dict[str, object]]] = None

        with doc.lock:
            if base_version != doc.version:
                error_meta = (
                    "OUT_OF_DATE",
                    {"docId": doc.id, "serverVersion": doc.version},
                )
            else:
                ok, new_content, patch, err = apply_operation(doc.content, message)
                if not ok or patch is None:
                    error_meta = (err or "INVALID_PATCH", {})
                else:
                    doc.content = new_content
                    doc.version += 1
                    patch_result = patch
                    current_version = doc.version
                    append_oplog(doc.id, doc.version, patch, session.id, self.oplog_dir)
                    if current_version % self.snapshot_interval == 0:
                        save_snapshot(doc, self.snapshot_dir)

        if error_meta:
            code, extra = error_meta
            self.send_error(session, code, **extra)
            return
        if not patch_result or current_version is None:
            self.send_error(session, "SERVER_ERROR", hint="patch result missing")
            return

        applied_event = {
            "ev": "APPLIED",
            "docId": doc.id,
            "version": current_version,
            "patch": patch_result,
            "by": session.id,
        }
        broadcast_event = dict(applied_event)
        broadcast_event["ev"] = "BROADCAST"
        self._safe_send(session, applied_event)
        self._broadcast(doc, broadcast_event, exclude=session.id)

    # ---------- 헬퍼 ----------
    def _safe_send(self, session: Session, payload: Dict[str, object]) -> None:
        if not session.alive:
            return
        try:
            session.send(payload)
        except ConnectionError:
            self.unregister_session(session)
        except ProtocolError as exc:
            LOGGER.error("protocol encode failed: %s", exc)

    def send_error(self, session: Session, code: str, **extra: object) -> None:
        payload = {"ev": "ERROR", "code": code}
        payload.update(extra)
        self._safe_send(session, payload)

    def _broadcast(self, doc: DocState, payload: Dict[str, object], *, exclude: Optional[str] = None) -> None:
        with doc.lock:
            targets = list(doc.subscribers)
        for sid in targets:
            if sid == exclude:
                continue
            session = self._get_session(sid)
            if not session:
                with doc.lock:
                    doc.subscribers.discard(sid)
                continue
            self._safe_send(session, payload)

    def _get_session(self, sid: str) -> Optional[Session]:
        with self._sessions_lock:
            return self.sessions.get(sid)

    def _normalize_doc_id(self, value: object) -> str:
        doc_id = str(value or "").strip()
        if not doc_id:
            raise ValueError("docId required")
        return doc_id

    def _max_version(self) -> int:
        with self._docs_lock:
            if not self.docs:
                return 0
            return max(doc.version for doc in self.docs.values())

    # ---------- 워치독 ----------
    def _watchdog_loop(self) -> None:
        while not self._stop_event.wait(10):
            now = time.monotonic()
            stale: list[Session] = []
            with self._sessions_lock:
                for session in list(self.sessions.values()):
                    if not session.alive:
                        stale.append(session)
                    elif self.heartbeat_timeout and now - session.last_seen > self.heartbeat_timeout:
                        stale.append(session)
            for session in stale:
                LOGGER.info("session timeout: %s", session.id)
                self.unregister_session(session)

    def shutdown(self) -> None:
        self._stop_event.set()
        self._watchdog_thread.join(timeout=1.0)
        with self._sessions_lock:
            sessions = list(self.sessions.values())
        for session in sessions:
            self.unregister_session(session)


__all__ = [
    "ServerHub",
    "Session",
]