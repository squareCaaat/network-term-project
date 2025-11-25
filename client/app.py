#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyQt5 Collaborative Memo Board - Client Template
------------------------------------------------
기능 요약
- 서버(TCP)와 JSON-lines(한 줄에 한 메시지)로 통신
- HELLO -> SUBSCRIBE -> (선택) GET_SNAPSHOT 흐름
- 서버 이벤트(WELCOME/DOC_SNAPSHOT/APPLIED/BROADCAST/ERROR) 수신 및 적용
- 로컬 편집 이벤트 → 간단 diff → INSERT/DELETE/REPLACE 생성 → base(version) 포함 전송
- Out-of-date 수신 시 스냅샷 재요청 후 재시도(간단 전략)

TODO(필요시 확장)
- 인증 토큰 붙이기
- 재시도 큐/미발신 보관
- 간단 OT(오프셋 보정) 로직 추가
- 다문서 탭/권한/리치 텍스트 등
"""

import sys
import socket
import json
import threading
import time
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

from PyQt5 import QtCore, QtWidgets


# =====================
# 네트워크 워커
# =====================
class NetWorker(QtCore.QObject):
    connected = QtCore.pyqtSignal()
    disconnected = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(str)
    eventReceived = QtCore.pyqtSignal(dict)
    status = QtCore.pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self._sock: Optional[socket.socket] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._writer_lock = threading.Lock()
        self._alive = False
        self._host = None
        self._port = None

    def connect_to(self, host: str, port: int, timeout=5.0) -> bool:
        if self._alive:
            self.error.emit("Already connected")
            return True
        try:
            self.status.emit(f"Connecting {host}:{port} ...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, port))
            s.settimeout(None)
            self._sock = s
            self._alive = True
            self._host, self._port = host, port
            self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
            self._reader_thread.start()
            self.connected.emit()
            self.status.emit("Connected")
            return True
        except Exception as e:
            self._alive = False
            self._sock = None
            self.error.emit(f"Connect failed: {e}")
            return False

    def close(self):
        self._alive = False
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
        self._sock = None
        self.disconnected.emit()
        self.status.emit("Disconnected")

    def _reader_loop(self):
        buf = b""
        try:
            while self._alive and self._sock:
                chunk = self._sock.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8"))
                        self.eventReceived.emit(msg)
                    except Exception as e:
                        self.error.emit(f"Bad JSON: {e}")
        except Exception as e:
            self.error.emit(f"Reader error: {e}")
        finally:
            self.close()

    def send_json(self, obj: Dict[str, Any]):
        if not self._sock:
            self.error.emit("Not connected")
            return
        data = (json.dumps(obj) + "\n").encode("utf-8")
        with self._writer_lock:
            try:
                self._sock.sendall(data)
            except Exception as e:
                self.error.emit(f"Send failed: {e}")


# =====================
# 간단 diff & 패치 적용 유틸
# =====================
@dataclass
class Patch:
    type: str  # INSERT | DELETE | REPLACE
    pos: int
    length: int = 0
    text: str = ""


def compute_patch(old: str, new: str) -> Optional[Patch]:
    """단일 편집(삽입/삭제/치환) 가정 간단 diff.
    여러 키 입력이 한 번에 들어와도 보통 한 구간으로 수렴.
    """
    if old == new:
        return None
    i = 0
    minlen = min(len(old), len(new))
    while i < minlen and old[i] == new[i]:
        i += 1
    oi = len(old) - 1
    nj = len(new) - 1
    while oi >= i and nj >= i and old[oi] == new[nj]:
        oi -= 1
        nj -= 1

    if len(new) > len(old):  # INSERT
        ins_text = new[i : nj + 1]
        return Patch("INSERT", pos=i, text=ins_text)
    elif len(new) < len(old):  # DELETE
        del_len = (oi - i + 1)
        return Patch("DELETE", pos=i, length=del_len)
    else:  # REPLACE
        rep_text = new[i : nj + 1]
        rep_len = (oi - i + 1)
        return Patch("REPLACE", pos=i, length=rep_len, text=rep_text)


def apply_patch_to_text(content: str, patch: Dict[str, Any]) -> str:
    t = patch.get("type")
    pos = int(patch.get("pos", 0))
    if t == "INSERT":
        tx = patch.get("text", "")
        return content[:pos] + tx + content[pos:]
    if t == "DELETE":
        ln = int(patch.get("len") or patch.get("length", 0))
        return content[:pos] + content[pos + ln :]
    if t == "REPLACE":
        ln = int(patch.get("len") or patch.get("length", 0))
        tx = patch.get("text", "")
        return content[:pos] + tx + content[pos + ln :]
    return content


# =====================
# 메인 윈도우
# =====================
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Collab Note - PyQt Client (Template)")
        self.resize(900, 600)

        self.worker = NetWorker()
        self.thread = QtCore.QThread(self)
        self.worker.moveToThread(self.thread)
        self.thread.start()

        # 상태
        self.session_id: Optional[str] = None
        self.current_doc: str = "main"
        self.current_version: int = 0
        self.applying_remote: bool = False  # 원격 적용 중에는 textChanged 무시
        self.last_text_for_diff: str = ""
        self.name: str = "user"
        self.doc_synced: bool = False

        # UI
        self._build_ui()

        # 신호 연결
        self.worker.connected.connect(self.on_connected)
        self.worker.disconnected.connect(self.on_disconnected)
        self.worker.error.connect(self.on_error)
        self.worker.eventReceived.connect(self.on_event)
        self.worker.status.connect(self.set_status)

    # ---------- UI ----------
    def _build_ui(self):
        central = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(central)

        top = QtWidgets.QHBoxLayout()
        self.ed_host = QtWidgets.QLineEdit("127.0.0.1")
        self.ed_port = QtWidgets.QLineEdit("5055")
        self.ed_name = QtWidgets.QLineEdit("alice")
        self.ed_doc = QtWidgets.QLineEdit("main")
        self.btn_connect = QtWidgets.QPushButton("Connect")
        self.btn_sub = QtWidgets.QPushButton("Subscribe")
        self.lbl_version = QtWidgets.QLabel("ver: 0")

        for w, ph in [
            (self.ed_host, "host"),
            (self.ed_port, "port"),
            (self.ed_name, "name"),
            (self.ed_doc, "docId"),
        ]:
            w.setPlaceholderText(ph)

        top.addWidget(QtWidgets.QLabel("Host:"))
        top.addWidget(self.ed_host)
        top.addWidget(QtWidgets.QLabel("Port:"))
        top.addWidget(self.ed_port)
        top.addWidget(QtWidgets.QLabel("Name:"))
        top.addWidget(self.ed_name)
        top.addWidget(QtWidgets.QLabel("Doc:"))
        top.addWidget(self.ed_doc)
        top.addWidget(self.btn_connect)
        top.addWidget(self.btn_sub)
        top.addWidget(self.lbl_version)

        self.text = QtWidgets.QPlainTextEdit()
        self.text.setPlaceholderText("Start typing ...")

        layout.addLayout(top)
        layout.addWidget(self.text)
        self.setCentralWidget(central)

        # status bar
        self.status = QtWidgets.QStatusBar()
        self.setStatusBar(self.status)

        # event bindings
        self.btn_connect.clicked.connect(self.ui_connect)
        self.btn_sub.clicked.connect(self.ui_subscribe)
        self.text.textChanged.connect(self.on_text_changed)

    def set_status(self, s: str):
        self.status.showMessage(s, 5000)

    # ---------- 연결/구독 ----------
    @QtCore.pyqtSlot()
    def ui_connect(self):
        host = self.ed_host.text().strip()
        port = int(self.ed_port.text().strip() or 5055)
        self.name = self.ed_name.text().strip() or "user"
        ok = self.worker.connect_to(host, port)
        if ok:
            # HELLO 전송
            self.worker.send_json({"op": "HELLO", "name": self.name})
            # 자동 구독/동기화 요청
            self.request_doc_sync()

    @QtCore.pyqtSlot()
    def ui_subscribe(self):
        self.request_doc_sync()

    def request_doc_sync(self):
        self.current_doc = self.ed_doc.text().strip() or "main"
        self.doc_synced = False
        self.worker.send_json({"op": "SUBSCRIBE", "docId": self.current_doc})
        self.worker.send_json({"op": "GET_SNAPSHOT", "docId": self.current_doc})
        self.set_status(f"Syncing doc '{self.current_doc}' ...")

    # ---------- 이벤트 수신 ----------
    @QtCore.pyqtSlot()
    def on_connected(self):
        self.set_status("Connected. Send HELLO.")

    @QtCore.pyqtSlot()
    def on_disconnected(self):
        self.set_status("Disconnected")
        self.doc_synced = False

    @QtCore.pyqtSlot(str)
    def on_error(self, err: str):
        self.set_status(f"Error: {err}")

    @QtCore.pyqtSlot(dict)
    def on_event(self, ev: Dict[str, Any]):
        et = ev.get("ev")
        if et == "WELCOME":
            self.session_id = ev.get("sessionId")
            # 서버 전역 버전이 있을 수 있으나 문서 버전과 다를 수 있음
            self.set_status(f"WELCOME sid={self.session_id}")

        elif et == "DOC_SNAPSHOT":
            if ev.get("docId") != self.current_doc:
                return
            ver = int(ev.get("version", 0))
            content = ev.get("content", "")
            self.apply_remote_snapshot(content, ver)

        elif et in ("APPLIED", "BROADCAST"):
            if ev.get("docId") != self.current_doc:
                return
            ver = int(ev.get("version", 0))
            patch = ev.get("patch", {})
            author = ev.get("by")
            is_self_event = bool(author) and author == self.session_id and et == "APPLIED"
            self.apply_remote_patch(patch, ver, from_self=is_self_event)

        elif et == "ERROR":
            code = ev.get("code")
            if code == "OUT_OF_DATE":
                # 최신 스냅샷 재요청
                self.doc_synced = False
                self.worker.send_json({"op": "GET_SNAPSHOT", "docId": self.current_doc})
                self.set_status("Out-of-date → Resync snapshot")
            else:
                self.set_status(f"Server ERROR: {code}")

        elif et == "PONG":
            pass

    # ---------- 로컬 편집 → diff → 전송 ----------
    @QtCore.pyqtSlot()
    def on_text_changed(self):
        if self.applying_remote:
            return
        new_text = self.text.toPlainText()
        if not self.doc_synced:
            # 아직 스냅샷을 받지 못한 상태 → 편집 전송 보류
            self.last_text_for_diff = new_text
            return
        patch = compute_patch(self.last_text_for_diff, new_text)
        if not patch:
            self.last_text_for_diff = new_text
            return
        # base는 현재 로컬이 알고 있는 문서 버전
        base = int(self.current_version)
        msg = {"op": patch.type, "docId": self.current_doc, "base": base, "pos": patch.pos}
        if patch.type == "INSERT":
            msg["text"] = patch.text
        elif patch.type == "DELETE":
            msg["len"] = patch.length
        elif patch.type == "REPLACE":
            msg["len"] = patch.length
            msg["text"] = patch.text
        self.worker.send_json(msg)
        # 서버의 APPLIED/BROADCAST로 최종 버전이 확정되면 그때 current_version 갱신
        self.last_text_for_diff = new_text

    # ---------- 원격 적용 ----------
    def apply_remote_snapshot(self, content: str, version: int):
        self.applying_remote = True
        try:
            self.text.blockSignals(True)
            self.text.setPlainText(content)
            self.last_text_for_diff = content
        finally:
            self.text.blockSignals(False)
            self.applying_remote = False
        self.current_version = version
        self.lbl_version.setText(f"ver: {self.current_version}")
        self.doc_synced = True

    def apply_remote_patch(self, patch: Dict[str, Any], version: int, *, from_self: bool = False):
        """서버 패치를 로컬 편집기에 반영."""
        if from_self or not isinstance(patch, dict):
            self.current_version = version
            self.lbl_version.setText(f"ver: {self.current_version}")
            self.doc_synced = True
            return

        current = self.text.toPlainText()
        newc = apply_patch_to_text(current, patch)
        cursor = self.text.textCursor()
        new_cursor_pos = self._cursor_after_patch(cursor.position(), patch)
        new_cursor_pos = max(0, min(len(newc), new_cursor_pos))

        self.applying_remote = True
        try:
            self.text.blockSignals(True)
            self.text.setPlainText(newc)
            self.last_text_for_diff = newc
            cursor = self.text.textCursor()
            cursor.setPosition(new_cursor_pos)
            self.text.setTextCursor(cursor)
        finally:
            self.text.blockSignals(False)
            self.applying_remote = False

        self.current_version = version
        self.lbl_version.setText(f"ver: {self.current_version}")
        self.doc_synced = True

    def _cursor_after_patch(self, old_pos: int, patch: Dict[str, Any]) -> int:
        try:
            pos = int(patch.get("pos", 0))
        except (TypeError, ValueError):
            return old_pos
        ptype = patch.get("type")
        if ptype == "INSERT":
            text = patch.get("text", "")
            added = len(text) if isinstance(text, str) else 0
            if old_pos < pos:
                return old_pos
            return old_pos + added
        if ptype == "DELETE":
            length = self._patch_length(patch)
            if old_pos <= pos:
                return old_pos
            if old_pos <= pos + length:
                return pos
            return old_pos - length
        if ptype == "REPLACE":
            length = self._patch_length(patch)
            text = patch.get("text", "")
            added = len(text) if isinstance(text, str) else 0
            if old_pos <= pos:
                return old_pos
            if old_pos <= pos + length:
                return pos + added
            delta = added - length
            return old_pos + delta
        return old_pos

    def _patch_length(self, patch: Dict[str, Any]) -> int:
        value = patch.get("len", patch.get("length", 0))
        try:
            length = int(value)
        except (TypeError, ValueError):
            return 0
        return max(0, length)


# =====================
# 진입점
# =====================
def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
