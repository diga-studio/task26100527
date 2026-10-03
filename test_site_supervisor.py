"""SiteSupervisor 的 unittest 测试（使用真实 Python 子进程）。"""

import os
import signal
import sys
import tempfile
import threading
import time
import unittest

from site_supervisor import SiteSupervisor, SubprocessAdapter

PYTHON = sys.executable
SLEEPER = [PYTHON, "-c", "import time; time.sleep(30)"]
INSTANT_EXIT = [PYTHON, "-c", "pass"]
BAD_COMMAND = ["/nonexistent-site-supervisor-command-xyz"]


def term_ignoring_argv(ready_path):
    """安装 SIGTERM 忽略处理器后触碰 ready 文件，再进入睡眠。"""
    return [
        PYTHON,
        "-c",
        "import signal, sys, time, pathlib;"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
        "pathlib.Path(sys.argv[1]).touch();"
        "time.sleep(30)",
        ready_path,
    ]


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def poll_until(sup, predicate, timeout=10.0, interval=0.01):
    """持续 poll 直到 predicate() 为真或超时；返回是否成功。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sup.poll()
        if predicate():
            return True
        time.sleep(interval)
    return False


class RecordingAdapter(SubprocessAdapter):
    """记录句柄与信号次数的适配器。"""

    def __init__(self):
        self.handles = []
        self.term_count = 0
        self.kill_count = 0

    def spawn(self, argv):
        handle = super().spawn(argv)
        self.handles.append(handle)
        return handle

    def terminate(self, handle):
        self.term_count += 1
        super().terminate(handle)

    def kill(self, handle):
        self.kill_count += 1
        super().kill(handle)


class BlockingSpawnAdapter(SubprocessAdapter):
    """第一次 spawn 在子进程创建后阻塞，直到测试放行。"""

    def __init__(self, block_first_only=True):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.handles = []
        self.calls = 0
        self._block_first_only = block_first_only

    def spawn(self, argv):
        self.calls += 1
        handle = super().spawn(argv)  # 子进程已产生
        self.handles.append(handle)
        if self.calls == 1 or not self._block_first_only:
            self.entered.set()
            self.release.wait(timeout=10)
        return handle


class FailingFirstSpawnAdapter(SubprocessAdapter):
    """第一次 spawn 阻塞后失败，后续正常。"""

    def __init__(self):
        self.attempted = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def spawn(self, argv):
        self.calls += 1
        if self.calls == 1:
            self.attempted.set()
            self.release.wait(timeout=10)
            raise RuntimeError("boom")
        return super().spawn(argv)


class DelayedExitSupervisor(SiteSupervisor):
    """测试挂点：捕获首次“当前实例自行退出”的巡检结果，延迟提交。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._capture_enabled = True
        self.captured = []

    def _apply_unexpected_exit(self, site, child, code):
        if self._capture_enabled:
            self.captured.append((site.site_id, child, code))
            return
        super()._apply_unexpected_exit(site, child, code)

    def release_captured(self):
        with self._lock:
            self._capture_enabled = False
            pending = self.captured
            self.captured = []
            for site_id, child, code in pending:
                super()._apply_unexpected_exit(self._sites[site_id], child, code)


class SiteSupervisorTests(unittest.TestCase):
    def make_supervisor(self, argv=SLEEPER, **kwargs):
        return SiteSupervisor({"s": argv}, **kwargs)

    def test_start_spawns_real_child_then_stop_reaps(self):
        adapter = RecordingAdapter()
        sup = self.make_supervisor(adapter=adapter)
        try:
            self.assertEqual(sup.submit("s", "start"), 1)
            ok = poll_until(sup, lambda: sup.snapshot("s")["settled"])
            self.assertTrue(ok)
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "running")
            pid = snap["pid"]
            self.assertIsNotNone(pid)
            self.assertTrue(pid_alive(pid))

            self.assertEqual(sup.submit("s", "stop"), 2)
            ok = poll_until(sup, lambda: sup.snapshot("s")["settled"])
            self.assertTrue(ok)
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "stopped")
            self.assertIsNone(snap["pid"])
            self.assertFalse(pid_alive(pid))
            # 子进程已被回收（waitpid 完成）
            self.assertIsNotNone(adapter.handles[0].returncode)
        finally:
            sup.close()

    def test_start_reuses_running_instance(self):
        sup = self.make_supervisor()
        try:
            sup.submit("s", "start")
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            pid1 = sup.snapshot("s")["pid"]

            sup.submit("s", "start")
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "running")
            self.assertEqual(snap["pid"], pid1)  # 不得更换可沿用的实例
        finally:
            sup.close()
        self.assertFalse(pid_alive(pid1))  # 关闭后不得遗留子进程

    def test_restart_replaces_instance(self):
        sup = self.make_supervisor()
        try:
            sup.submit("s", "start")
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            pid1 = sup.snapshot("s")["pid"]

            sup.submit("s", "restart")
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "running")
            pid2 = snap["pid"]
            self.assertNotEqual(pid1, pid2)  # 必须使用新实例
            self.assertFalse(pid_alive(pid1))
            self.assertTrue(pid_alive(pid2))
        finally:
            sup.close()

    def test_self_exit_sets_error_and_no_respawn(self):
        sup = self.make_supervisor(argv=INSTANT_EXIT)
        try:
            sup.submit("s", "start")
            ok = poll_until(
                sup,
                lambda: sup.snapshot("s")["settled"]
                and sup.snapshot("s")["status"] == "error",
            )
            self.assertTrue(ok)
            snap = sup.snapshot("s")
            self.assertIn("exited", snap["error"])
            # 不自动拉起：继续巡检状态保持 error 且无新实例
            time.sleep(0.2)
            sup.poll()
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "error")
            self.assertIsNone(snap["pid"])
        finally:
            sup.close()

    def test_spawn_failure_sets_error(self):
        sup = self.make_supervisor(argv=BAD_COMMAND)
        try:
            sup.submit("s", "start")
            ok = poll_until(
                sup,
                lambda: sup.snapshot("s")["settled"]
                and sup.snapshot("s")["status"] == "error",
            )
            self.assertTrue(ok)
            self.assertIn("spawn failed", sup.snapshot("s")["error"])
        finally:
            sup.close()

    def test_sigterm_grace_then_sigkill(self):
        adapter = RecordingAdapter()
        with tempfile.TemporaryDirectory() as tmp:
            ready = os.path.join(tmp, "ready")
            sup = self.make_supervisor(
                argv=term_ignoring_argv(ready), adapter=adapter, grace_seconds=0.3
            )
            try:
                sup.submit("s", "start")
                self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
                pid = sup.snapshot("s")["pid"]
                # 等待子进程装好 SIGTERM 忽略处理器，消除竞态
                self.assertTrue(
                    poll_until(sup, lambda: os.path.exists(ready)),
                    "child did not install SIGTERM handler in time",
                )

                start = time.monotonic()
                sup.submit("s", "stop")
                self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
                elapsed = time.monotonic() - start
                self.assertEqual(sup.snapshot("s")["status"], "stopped")
                self.assertGreaterEqual(adapter.kill_count, 1)  # 宽限后强制结束
                self.assertGreaterEqual(elapsed, 0.3)
                self.assertFalse(pid_alive(pid))
                self.assertIsNotNone(adapter.handles[0].returncode)  # 已回收
            finally:
                sup.close()

    def test_stop_during_inflight_spawn_waits_and_reaps(self):
        adapter = BlockingSpawnAdapter()
        sup = self.make_supervisor(adapter=adapter)
        try:
            sup.submit("s", "start")
            # 创建已产生子进程但尚未向监管器返回
            self.assertTrue(poll_until(sup, lambda: adapter.entered.is_set()))
            time.sleep(0.05)

            sup.submit("s", "stop")
            # stop 不能提前完成（创建操作尚未返回）
            for _ in range(5):
                sup.poll()
                self.assertFalse(sup.snapshot("s")["settled"])
                time.sleep(0.02)

            adapter.release.set()  # 放行迟到的创建结果
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "stopped")
            self.assertIsNone(snap["pid"])
            # 迟到成功的子进程被终止并回收
            handle = adapter.handles[0]
            self.assertIsNotNone(handle.returncode)
            self.assertFalse(pid_alive(handle.pid))
        finally:
            adapter.release.set()
            sup.close()

    def test_obsoleted_spawn_not_reused_by_later_start(self):
        adapter = BlockingSpawnAdapter()
        sup = self.make_supervisor(adapter=adapter)
        try:
            sup.submit("s", "start")
            self.assertTrue(poll_until(sup, lambda: adapter.entered.is_set()))
            sup.submit("s", "stop")   # 淘汰在途创建
            sup.submit("s", "start")  # 后续 start 不得复用被淘汰的创建
            adapter.release.set()

            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "running")
            # 被淘汰的子进程已回收，当前实例是重新创建的
            first = adapter.handles[0]
            self.assertIsNotNone(first.returncode)
            self.assertFalse(pid_alive(first.pid))
            self.assertEqual(adapter.calls, 2)
            self.assertEqual(snap["pid"], adapter.handles[1].pid)
            self.assertTrue(pid_alive(snap["pid"]))
        finally:
            adapter.release.set()
            sup.close()

    def test_delayed_exit_result_does_not_overwrite_restart(self):
        sup = DelayedExitSupervisor({"s": SLEEPER})
        try:
            sup.submit("s", "start")
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
            pid1 = sup.snapshot("s")["pid"]

            # 旧实例自行退出（带外终止模拟），巡检观察到但结果被延迟提交
            os.kill(pid1, signal.SIGTERM)
            self.assertTrue(poll_until(sup, lambda: bool(sup.captured)))

            # 此间完成 restart 并启动新实例
            sup.submit("s", "restart")
            self.assertTrue(
                poll_until(
                    sup,
                    lambda: sup.snapshot("s")["settled"]
                    and sup.snapshot("s")["status"] == "running",
                )
            )
            snap = sup.snapshot("s")
            pid2 = snap["pid"]
            self.assertNotEqual(pid1, pid2)
            self.assertTrue(pid_alive(pid2))

            # 放行旧巡检结果：新实例的状态和 PID 不能被覆盖
            sup.release_captured()
            sup.poll()
            snap = sup.snapshot("s")
            self.assertEqual(snap["status"], "running")
            self.assertEqual(snap["pid"], pid2)
            self.assertTrue(pid_alive(pid2))
        finally:
            sup.close()

    def test_stale_spawn_failure_ignored(self):
        adapter = FailingFirstSpawnAdapter()
        sup = self.make_supervisor(adapter=adapter)
        try:
            sup.submit("s", "start")
            self.assertTrue(poll_until(sup, lambda: adapter.attempted.is_set()))
            sup.submit("s", "stop")   # 使第一次创建成为旧代次
            sup.submit("s", "start")  # 新命令
            adapter.release.set()     # 旧启动失败返回，不得影响新命令

            self.assertTrue(
                poll_until(
                    sup,
                    lambda: sup.snapshot("s")["settled"]
                    and sup.snapshot("s")["status"] == "running",
                )
            )
            snap = sup.snapshot("s")
            self.assertIsNone(snap["error"])
            self.assertTrue(pid_alive(snap["pid"]))
        finally:
            adapter.release.set()
            sup.close()

    def test_close_leaves_no_children(self):
        sup = self.make_supervisor()
        sup.submit("s", "start")
        self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
        pid = sup.snapshot("s")["pid"]
        sup.close()
        self.assertFalse(pid_alive(pid))
        sup.close()  # 幂等

    def test_seq_monotonic_and_validation(self):
        sup = self.make_supervisor()
        try:
            self.assertEqual(sup.submit("s", "start"), 1)
            self.assertEqual(sup.submit("s", "stop"), 2)
            self.assertEqual(sup.submit("s", "restart"), 3)
            with self.assertRaises(ValueError):
                sup.submit("s", "bogus")
            with self.assertRaises(KeyError):
                sup.submit("unknown", "start")
            with self.assertRaises(KeyError):
                sup.snapshot("unknown")
            self.assertTrue(poll_until(sup, lambda: sup.snapshot("s")["settled"]))
        finally:
            sup.close()
        with self.assertRaises(RuntimeError):
            sup.submit("s", "start")

    def test_merged_commands_final_stop_wins(self):
        sup = self.make_supervisor()
        try:
            sup.submit("s", "start")
            sup.submit("s", "stop")
            sup.submit("s", "start")
            sup.submit("s", "stop")  # 合并被覆盖的请求，最终落实 stop
            self.assertTrue(
                poll_until(
                    sup,
                    lambda: sup.snapshot("s")["settled"]
                    and sup.snapshot("s")["status"] == "stopped",
                )
            )
            self.assertIsNone(sup.snapshot("s")["pid"])
        finally:
            sup.close()


if __name__ == "__main__":
    unittest.main()
