"""进程操作适配层：默认基于 subprocess.Popen，不经过 shell。"""
from __future__ import annotations

import subprocess
from typing import Optional, Protocol, Sequence


class ProcHandle(Protocol):
    """监管器所需的最小子进程句柄接口。"""

    pid: int

    def poll(self) -> Optional[int]: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...
    def wait(self, timeout: Optional[float] = None) -> int: ...


class PopenAdapter:
    """默认适配器：直接 exec 命令，不经过 shell。"""

    def spawn(self, argv: Sequence[str]) -> ProcHandle:
        return subprocess.Popen(list(argv))
