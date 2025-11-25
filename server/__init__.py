"""
CollabServer 패키지 초기화 모듈.

서버 구성 요소는 다음 하위 모듈에 정리되어 있다.
- protocol: JSON line 기반 프레이밍/직렬화
- doc: 문서 상태 및 패치 검증/적용
- persist: 스냅샷 및 오플로그 영속화
- hub: 세션/문서 라우팅 및 비즈니스 로직
- main: TCP 서버 진입점
"""

__all__ = [
    "hub",
    "doc",
    "protocol",
    "persist",
]