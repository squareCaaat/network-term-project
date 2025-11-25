"""CollabServer TCP 진입점."""

from __future__ import annotations

import argparse
import logging
import socket
import threading
from pathlib import Path
from .hub import ServerHub
from .protocol import JsonLineFramer, ProtocolError


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Collaborative Memo Board - Server")
    parser.add_argument("--host", default="0.0.0.0", help="서버 바인드 호스트 (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5055, help="서버 포트 (default: 5055)")
    parser.add_argument("--backlog", type=int, default=128, help="listen backlog 크기")
    parser.add_argument("--snapshot-dir", type=Path, default=project_root / "snapshots", help="스냅샷 저장 경로")
    parser.add_argument("--oplog-dir", type=Path, default=project_root / "oplogs", help="오플로그 저장 경로")
    parser.add_argument("--snapshot-interval", type=int, default=50, help="스냅샷 저장 주기(연산 수)")
    parser.add_argument("--heartbeat-timeout", type=int, default=120, help="세션 타임아웃(초)")
    parser.add_argument("--log-level", default="INFO", help="로그 레벨 (DEBUG/INFO/...)")
    return parser.parse_args()


def client_worker(hub: ServerHub, session) -> None:
    framer = JsonLineFramer()
    sock = session.socket
    try:
        while session.alive:
            chunk = sock.recv(4096)
            if not chunk:
                break
            try:
                messages = framer.feed(chunk)
            except ProtocolError as exc:
                hub.send_error(session, "BAD_JSON", hint=str(exc))
                break
            for msg in messages:
                if not isinstance(msg, dict):
                    hub.send_error(session, "BAD_JSON", hint="message must be object")
                    continue
                try:
                    hub.route_message(session, msg)
                except Exception as exc:
                    logging.exception("route_message failed: session=%s", session.id)
                    hub.send_error(session, "SERVER_ERROR", hint=str(exc))
                    break
    except (ConnectionError, OSError):
        pass
    finally:
        hub.unregister_session(session)


def run_server(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    hub = ServerHub(
        snapshot_dir=args.snapshot_dir,
        oplog_dir=args.oplog_dir,
        snapshot_interval=args.snapshot_interval,
        heartbeat_timeout=args.heartbeat_timeout,
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((args.host, args.port))
        server_sock.listen(args.backlog)
        logging.info("CollabServer listening on %s:%s", args.host, args.port)

        try:
            while True:
                conn, addr = server_sock.accept()
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                session = hub.new_session(conn, addr)
                threading.Thread(target=client_worker, args=(hub, session), daemon=True).start()
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt → shutting down")
        finally:
            hub.shutdown()


def main() -> None:
    args = parse_args()
    run_server(args)


if __name__ == "__main__":
    main()