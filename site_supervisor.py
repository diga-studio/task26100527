"""POSIX 站点进程监管库（仅使用 Python 标准库）。

构造时接收固定的 site_id -> argv 映射，直接以 argv 启动命令（不经过 shell）。
只管理自身创建的直接子进程，且任何时刻每个站点至多有一个尚未回收的子进程。

对外操作：
- submit(site_id, action): action 为 "start"/"stop"/"restart"，立即返回该站点
  单调递增的命令序号，不等待进程创建或退出。
- poll(): 巡检已监管进程的退出情况并推进状态机。
- snapshot(site_id): 查询站点当前状态，区分处理中（settled=False）与已落实。
- close(): 关闭监管器，保证不遗留子进程。

设计要点：
- 每个站点维护一个代次（gen）。stop/restart 被接受时代次递增，此前已存在或
  正在创建的实例即被淘汰；淘汰关系不可撤销，迟到成功的创建会被终止并回收。
- 旧启动的返回、旧进程的退出、旧巡检结果都通过代次/句柄一致性校验，无法把
  已经落实的新状态改回去。
- 状态机只在 poll()/close() 中推进；submit() 仅记录命令并立即返回序号。
"""

from __future__ import annotations

import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

START = "start"
STOP = "stop"
RESTART = "restart"
ACTIONS = (START, STOP, RESTART)

#: 正常终止信号发出后，等待进程自行退出的宽限时间（秒），超时强制结束。
DEFAULT_GRACE_SECONDS = 1.0


class ProcessAdapter:
    """进程操作适配层 / 测试挂点。默认实现见 SubprocessAdapter。"""

    def spawn(self, argv: Sequence[str]):
        """创建子进程并返回句柄；失败时抛出异常。"""
        raise NotImplementedError

    def terminate(self, handle) -> None:
        """发送正常终止信号（SIGTERM）。"""
        raise NotImplementedError

    def kill(self, handle) -> None:
        """强制结束（SIGKILL）。"""
        raise NotImplementedError

    def try_reap(self, handle) -> Optional[int]:
        """非阻塞回收；已退出返回退出码，仍在运行返回 None。"""
        raise NotImplementedError

    def pid(self, handle) -> int:
        raise NotImplementedError


class SubprocessAdapter(ProcessAdapter):
    """基于 subprocess.Popen 的默认适配器（不经过 shell）。"""

    def spawn(self, argv: Sequence[str]):
        return subprocess.Popen(list(argv))

    def terminate(self, handle) -> None:
        try:
            handle.terminate()
        except ProcessLookupError:
            pass

    def kill(self, handle) -> None:
        try:
            handle.kill()
        except ProcessLookupError:
            pass

    def try_reap(self, handle) -> Optional[int]:
        return handle.poll()

    def pid(self, handle) -> int:
        return handle.pid


@dataclass
class _Child:
    handle: object
    gen: int
    term_sent_at: Optional[float] = None
    kill_sent: bool = False


@dataclass
class _Spawn:
    gen: int


@dataclass
class _Site:
    site_id: str
    argv: List[str]
    seq: int = 0
    action: Optional[str] = None
    settled: bool = True
    status: str = "idle"
    error: Optional[str] = None
    gen: int = 0
    child: Optional[_Child] = None
    spawn: Optional[_Spawn] = None


class SiteSupervisor:
    """面向 POSIX 的站点进程监管器。"""

    def __init__(
        self,
        sites: Dict[str, Sequence[str]],
        adapter: Optional[ProcessAdapter] = None,
        grace_seconds: float = DEFAULT_GRACE_SECONDS,
        clock=time.monotonic,
    ) -> None:
        if not sites:
            raise ValueError("sites must not be empty")
        for site_id, argv in sites.items():
            if not argv:
                raise ValueError("argv for site %r must not be empty" % (site_id,))
        self._adapter = adapter if adapter is not None else SubprocessAdapter()
        self._grace = float(grace_seconds)
        self._clock = clock
        self._lock = threading.RLock()
        self._sites: Dict[str, _Site] = {
            site_id: _Site(site_id=site_id, argv=list(argv))
            for site_id, argv in sites.items()
        }
        self._spawn_results: "queue.Queue[Tuple[str, int, object, Optional[BaseException]]]" = queue.Queue()
        self._threads: List[threading.Thread] = []
        self._closed = False

    # ------------------------------------------------------------------ API

    def submit(self, site_id: str, action: str) -> int:
        """接受命令并立即返回该站点单调递增的命令序号。"""
        if action not in ACTIONS:
            raise ValueError("unknown action: %r" % (action,))
        with self._lock:
            if self._closed:
                raise RuntimeError("supervisor is closed")
            site = self._sites[site_id]
            site.seq += 1
            site.action = action
            site.settled = False
            site.error = None
            if action in (STOP, RESTART):
                # 淘汰接受前已存在或正在创建的实例；淘汰关系不可撤销。
                site.gen += 1
            return site.seq

    def poll(self) -> None:
        """巡检退出情况并推进各站点状态机。"""
        with self._lock:
            if self._closed:
                return
            self._drain_spawn_results()
            self._reap_children()
            for site in self._sites.values():
                self._advance(site, allow_spawn=True)

    def snapshot(self, site_id: str) -> dict:
        """返回站点当前状态；settled 区分处理中与已落实。"""
        with self._lock:
            site = self._sites[site_id]
            if site.action is None:
                status = "idle"
            elif not site.settled:
                status = "stopping" if site.action == STOP else "starting"
            else:
                status = site.status
            pid = None
            if site.child is not None and site.child.gen == site.gen:
                pid = self._adapter.pid(site.child.handle)
            return {
                "site_id": site_id,
                "seq": site.seq,
                "action": site.action,
                "settled": site.settled,
                "status": status,
                "pid": pid,
                "error": site.error,
            }

    def close(self) -> None:
        """关闭监管器；终止并回收全部子进程，不遗留任何子进程。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for site in self._sites.values():
                site.gen += 1  # 淘汰一切现存/在途实例
        while True:
            with self._lock:
                self._drain_spawn_results()
                self._reap_children()
                for site in self._sites.values():
                    self._advance(site, allow_spawn=False)
                done = all(
                    site.child is None and site.spawn is None
                    for site in self._sites.values()
                )
            if done:
                break
            time.sleep(0.01)
        for thread in self._threads:
            thread.join()

    def __enter__(self) -> "SiteSupervisor":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ------------------------------------------------------- 内部：事件汇入

    def _drain_spawn_results(self) -> None:
        """处理创建线程返回的结果（调用时需持有锁）。"""
        while True:
            try:
                site_id, gen, handle, exc = self._spawn_results.get_nowait()
            except queue.Empty:
                return
            site = self._sites[site_id]
            site.spawn = None
            if exc is not None:
                # 只有当前代次、且最新命令仍在等待实例时，创建失败才落实为 error；
                # 旧启动的失败返回不得改动新状态。
                if gen == site.gen and not site.settled and site.action in (START, RESTART):
                    site.settled = True
                    site.status = "error"
                    site.error = "spawn failed: %r" % (exc,)
                continue
            child = _Child(handle=handle, gen=gen)
            site.child = child
            # gen != site.gen 的迟到成功创建已被淘汰，将在 _advance 中终止回收。

    def _reap_children(self) -> None:
        """非阻塞巡检各站点子进程退出情况（调用时需持有锁）。"""
        for site in self._sites.values():
            child = site.child
            if child is None:
                continue
            code = self._adapter.try_reap(child.handle)
            if code is None:
                continue
            if child.gen != site.gen:
                # 被淘汰实例的退出属于清理流程，主动停止不算 error。
                site.child = None
            else:
                self._apply_unexpected_exit(site, child, code)

    def _apply_unexpected_exit(self, site: _Site, child: _Child, code: int) -> None:
        """当前实例自行退出 -> error（测试挂点：可覆写以延迟提交巡检结果）。

        通过句柄与代次一致性校验，旧的巡检结果无法覆盖已落实的新状态。
        """
        if site.child is not child or child.gen != site.gen:
            return
        site.child = None
        site.settled = True
        site.status = "error"
        site.error = "instance exited on its own (exit code %r)" % (code,)

    # ------------------------------------------------------- 内部：状态机

    def _advance(self, site: _Site, allow_spawn: bool) -> None:
        """推进单个站点（调用时需持有锁）。"""
        child = site.child
        if child is not None and child.gen != site.gen:
            # 被淘汰实例：先发正常终止信号，宽限 1 秒后仍未退出则强制结束。
            now = self._clock()
            if child.term_sent_at is None:
                self._adapter.terminate(child.handle)
                child.term_sent_at = now
            elif not child.kill_sent and now - child.term_sent_at >= self._grace:
                self._adapter.kill(child.handle)
                child.kill_sent = True

        if site.action is None or site.settled:
            return

        if site.action == STOP:
            # 所有创建操作都已返回、所有应清理子进程都已退出并回收后才完成。
            if site.child is None and site.spawn is None:
                site.settled = True
                site.status = "stopped"
            return

        # start / restart：需要当前代次的运行实例。
        if child is not None and child.gen == site.gen:
            # start 可沿用已有实例；restart 的实例必为接受命令之后创建
            # （接受时 gen 已递增，旧实例必为旧代次）。
            site.settled = True
            site.status = "running"
            return

        # 新实例只能在旧实例回收后创建；同一时刻至多一个在途创建。
        if allow_spawn and site.child is None and site.spawn is None:
            gen = site.gen
            site.spawn = _Spawn(gen=gen)
            thread = threading.Thread(
                target=self._spawn_worker,
                args=(site.site_id, gen),
                name="site-supervisor-spawn-%s" % (site.site_id,),
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

    def _spawn_worker(self, site_id: str, gen: int) -> None:
        argv = self._sites[site_id].argv  # 构造后不再变化，读取无需加锁
        try:
            handle = self._adapter.spawn(argv)
        except BaseException as exc:  # 创建失败 -> error（由主线程落实）
            self._spawn_results.put((site_id, gen, None, exc))
        else:
            self._spawn_results.put((site_id, gen, handle, None))
