"""面向 POSIX 的站点进程监管器。

语义要点：
- submit 立即返回该站点单调递增的命令序号，不等待进程创建或退出。
- 每个站点任意时刻至多一个未回收子进程；新实例只能在旧实例回收后创建。
- stop/restart 淘汰接受前已存在或正在创建的实例；迟到的创建成功也必须终止回收。
- 当前实例自行退出由 poll 置为 error，不自动拉起；主动停止不算 error。
- SIGTERM 后 1 秒仍未退出则 SIGKILL 并回收。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from .adapter import PopenAdapter, ProcHandle

VALID_ACTIONS = ("start", "stop", "restart")
TERM_GRACE_SECONDS = 1.0


@dataclass
class _Command:
    seq: int
    action: str


@dataclass
class _ProcRecord:
    proc: ProcHandle
    creation_seq: int
    retired: bool = False
    reaped: bool = False


@dataclass
class _SiteState:
    site_id: str
    argv: Sequence[str]
    seq: int = 0
    settled_seq: int = 0
    retire_marker: int = 0
    status: str = "idle"
    pending: List[_Command] = field(default_factory=list)
    processing: Optional[_Command] = None
    current: Optional[_ProcRecord] = None
    cond: Optional[threading.Condition] = None
    worker: Optional[threading.Thread] = None


class Supervisor:
    """监管固定 site_id -> argv 映射的直接子进程。"""

    def __init__(self, sites: Dict[str, Sequence[str]], adapter: Optional[PopenAdapter] = None) -> None:
        if not sites:
            raise ValueError("sites must not be empty")
        self._adapter = adapter if adapter is not None else PopenAdapter()
        self._lock = threading.RLock()
        self._closed = False
        self._sites: Dict[str, _SiteState] = {}
        for site_id, argv in sites.items():
            st = _SiteState(site_id=site_id, argv=list(argv))
            st.cond = threading.Condition(self._lock)
            self._sites[site_id] = st

    # ------------------------------------------------------------------ API

    def submit(self, site_id: str, action: str) -> int:
        """接受命令并立即返回该站点单调递增的命令序号。"""
        if action not in VALID_ACTIONS:
            raise ValueError("invalid action: %r" % (action,))
        with self._lock:
            if self._closed:
                raise RuntimeError("supervisor is closed")
            st = self._sites[site_id]
            st.seq += 1
            seq = st.seq
            if action in ("stop", "restart"):
                st.retire_marker = seq
            st.pending.append(_Command(seq=seq, action=action))
            if st.worker is None:
                st.worker = threading.Thread(
                    target=self._worker_loop,
                    args=(st,),
                    name="site-supervisor-%s" % site_id,
                    daemon=True,
                )
                st.worker.start()
            st.cond.notify_all()
            return seq

    def poll(self) -> None:
        """检查已监管进程的退出情况并更新状态。"""
        with self._lock:
            targets = [
                (st, st.current)
                for st in self._sites.values()
                if st.current is not None
                and not st.current.retired
                and not st.current.reaped
            ]
        for st, rec in targets:
            rc = rec.proc.poll()
            if rc is None:
                continue
            self._deliver_exit(st, rec, rc)

    def snapshot(self, site_id: str) -> dict:
        """查询站点当前状态，区分处理中与已经落实。"""
        with self._lock:
            st = self._sites[site_id]
            settled = st.settled_seq >= st.seq
            pending_action = None
            if not settled:
                if st.pending:
                    pending_action = st.pending[-1].action
                elif st.processing is not None:
                    pending_action = st.processing.action
            pid = None
            if st.current is not None and not st.current.reaped:
                pid = st.current.proc.pid
            return {
                "site_id": st.site_id,
                "seq": st.seq,
                "settled_seq": st.settled_seq,
                "settled": settled,
                "status": st.status,
                "pid": pid,
                "pending_action": pending_action,
            }

    def close(self) -> None:
        """关闭监管器，不得遗留子进程。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for st in self._sites.values():
                st.cond.notify_all()
        for st in self._sites.values():
            self._retire_current(st)
        for st in self._sites.values():
            if st.worker is not None:
                st.worker.join()

    def __enter__(self) -> "Supervisor":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------- 内部实现

    def _worker_loop(self, st: _SiteState) -> None:
        while True:
            with self._lock:
                while not st.pending and not self._closed:
                    st.cond.wait()
                if self._closed:
                    st.pending.clear()
                    return
                # 合并被后续命令覆盖的请求，只执行最新一条；
                # 淘汰关系由 retire_marker 在 submit 时记录，不随合并撤销。
                cmd = st.pending[-1]
                st.pending.clear()
                st.processing = cmd
            try:
                self._execute(st, cmd)
            finally:
                with self._lock:
                    st.processing = None

    def _execute(self, st: _SiteState, cmd: _Command) -> None:
        if cmd.action == "stop":
            self._retire_current(st)
            self._settle(st, cmd, "stopped")
        elif cmd.action == "start":
            rec = self._current_if_reusable(st, cmd)
            if rec is not None:
                self._settle(st, cmd, "running")
                return
            self._retire_current(st)
            self._spawn_and_install(st, cmd)
        elif cmd.action == "restart":
            self._retire_current(st)
            self._spawn_and_install(st, cmd)

    def _current_if_reusable(self, st: _SiteState, cmd: _Command) -> Optional[_ProcRecord]:
        with self._lock:
            rec = st.current
            if rec is None or rec.creation_seq < st.retire_marker:
                return None
        # 已有实例若其实已退出但尚未被 poll 发现，则不可沿用。
        if rec.proc.poll() is not None:
            with self._lock:
                rec.reaped = True
                if st.current is rec:
                    st.current = None
            return None
        return rec

    def _spawn_and_install(self, st: _SiteState, cmd: _Command) -> None:
        with self._lock:
            if cmd.seq < st.retire_marker or self._closed:
                # 创建尚未发起即已被淘汰，直接放弃。
                self._settle_locked(st, cmd, None)
                return
        try:
            proc = self._adapter.spawn(st.argv)
        except Exception:
            self._settle(st, cmd, "error")
            return
        rec = _ProcRecord(proc=proc, creation_seq=cmd.seq)
        with self._lock:
            retired = cmd.seq < st.retire_marker or self._closed
            if not retired:
                st.current = rec
        if retired:
            # 被淘汰的创建迟到成功：必须终止并回收，不可供后续复用。
            self._terminate(rec)
            self._settle(st, cmd, None)
            return
        self._settle(st, cmd, "running")

    def _retire_current(self, st: _SiteState) -> None:
        with self._lock:
            rec = st.current
            st.current = None
            if rec is not None:
                rec.retired = True
        if rec is not None:
            self._terminate(rec)

    def _terminate(self, rec: _ProcRecord) -> None:
        with self._lock:
            rec.retired = True
            if rec.reaped:
                return
        if rec.proc.poll() is not None:
            with self._lock:
                rec.reaped = True
            return
        rec.proc.terminate()
        deadline = time.monotonic() + TERM_GRACE_SECONDS
        while time.monotonic() < deadline:
            if rec.proc.poll() is not None:
                with self._lock:
                    rec.reaped = True
                return
            time.sleep(0.01)
        rec.proc.kill()
        rec.proc.wait()
        with self._lock:
            rec.reaped = True

    def _settle(self, st: _SiteState, cmd: _Command, status: Optional[str]) -> None:
        with self._lock:
            self._settle_locked(st, cmd, status)

    def _settle_locked(self, st: _SiteState, cmd: _Command, status: Optional[str]) -> None:
        if status is not None:
            st.status = status
        if cmd.seq > st.settled_seq:
            st.settled_seq = cmd.seq

    # ------------------------------------------------------- 退出事件投递

    def _deliver_exit(self, st: _SiteState, rec: _ProcRecord, rc: int) -> None:
        """巡检观察到退出后的提交点（测试可包装以延迟投递）。"""
        self._apply_exit(st, rec, rc)

    def _apply_exit(self, st: _SiteState, rec: _ProcRecord, rc: int) -> None:
        with self._lock:
            rec.reaped = True
            if rec.retired:
                return
            if st.current is not rec:
                # 旧记录：不得覆盖已经落实的新状态。
                return
            st.current = None
            st.status = "error"
