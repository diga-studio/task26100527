"""Supervisor 的 unittest 测试，使用真实 Python 子进程。"""
import os
import signal
import sys
import threading
import time
import unittest

from site_supervisor import PopenAdapter, Supervisor

PY = sys.executable
SLEEP_ARGV = [PY, "-c", "import time; time.sleep(30)"]
QUICK_EXIT_ARGV = [PY, "-c", "pass"]
def ignore_term_argv(ready_path):
    return [
        PY,
        "-c",
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "open(sys.argv[1], 'w').close()\n"
        "time.sleep(30)",
        ready_path,
    ]
BAD_ARGV = ["/nonexistent/binary-that-does-not-exist"]


def wait_for(pred, timeout=10.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


class CapturingAdapter(PopenAdapter):
    """记录所有真实创建出来的子进程，便于断言回收。"""

    def __init__(self):
        self.procs = []
        self.lock = threading.Lock()

    def spawn(self, argv):
        proc = super().spawn(argv)
        with self.lock:
            self.procs.append(proc)
        return proc


class BlockingSpawnAdapter(CapturingAdapter):
    """子进程已创建但延迟向监管器返回的适配器（测试挂点）。"""

    def __init__(self):
        super().__init__()
        self.spawn_called = threading.Event()
        self.release = threading.Event()

    def spawn(self, argv):
        proc = CapturingAdapter.spawn(self, argv)
        self.spawn_called.set()
        self.release.wait(10)
        return proc


class SupervisorTest(unittest.TestCase):
    def make_supervisor(self, sites, adapter=None):
        sup = Supervisor(sites, adapter=adapter)
        self.addCleanup(sup.close)
        return sup

    # ---------------------------------------------------------- 基础语义

    def test_submit_returns_monotonic_seq(self):
        sup = self.make_supervisor({"a": SLEEP_ARGV})
        self.assertEqual(sup.submit("a", "start"), 1)
        self.assertEqual(sup.submit("a", "stop"), 2)
        self.assertEqual(sup.submit("a", "restart"), 3)
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))

    def test_submit_rejects_bad_input(self):
        sup = self.make_supervisor({"a": SLEEP_ARGV})
        with self.assertRaises(ValueError):
            sup.submit("a", "explode")
        with self.assertRaises(KeyError):
            sup.submit("nope", "start")
        with self.assertRaises(KeyError):
            sup.snapshot("nope")

    def test_start_runs_instance_and_settles(self):
        adapter = CapturingAdapter()
        sup = self.make_supervisor({"a": SLEEP_ARGV}, adapter)
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "running")
        self.assertIsNotNone(snap["pid"])
        os.kill(snap["pid"], 0)  # 进程真实存在

    def test_start_reuses_running_instance(self):
        sup = self.make_supervisor({"a": SLEEP_ARGV})
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        pid1 = sup.snapshot("a")["pid"]
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        self.assertEqual(sup.snapshot("a")["pid"], pid1)

    def test_stop_terminates_instance(self):
        adapter = CapturingAdapter()
        sup = self.make_supervisor({"a": SLEEP_ARGV}, adapter)
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        sup.submit("a", "stop")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "stopped")
        self.assertIsNone(snap["pid"])
        self.assertIsNotNone(adapter.procs[0].poll())  # 已回收

    def test_stop_without_instance_settles(self):
        sup = self.make_supervisor({"a": SLEEP_ARGV})
        sup.submit("a", "stop")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        self.assertEqual(sup.snapshot("a")["status"], "stopped")

    def test_restart_replaces_instance(self):
        adapter = CapturingAdapter()
        sup = self.make_supervisor({"a": SLEEP_ARGV}, adapter)
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        pid1 = sup.snapshot("a")["pid"]
        sup.submit("a", "restart")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "running")
        self.assertNotEqual(snap["pid"], pid1)
        self.assertIsNotNone(adapter.procs[0].poll())  # 旧进程已回收

    def test_spawn_failure_enters_error(self):
        sup = self.make_supervisor({"a": BAD_ARGV})
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "error")
        self.assertIsNone(snap["pid"])

    def test_self_exit_becomes_error_via_poll(self):
        sup = self.make_supervisor({"a": QUICK_EXIT_ARGV})
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        # 等待子进程真正退出后被 poll 发现
        self.assertTrue(
            wait_for(lambda: (sup.poll(), sup.snapshot("a")["status"] == "error")[1])
        )
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "error")
        self.assertIsNone(snap["pid"])
        # 不自动拉起
        sup.poll()
        time.sleep(0.1)
        sup.poll()
        self.assertEqual(sup.snapshot("a")["status"], "error")
        self.assertIsNone(sup.snapshot("a")["pid"])

    def test_sigterm_grace_then_sigkill(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            ready = os.path.join(tmp, "ready")
            adapter = CapturingAdapter()
            sup = self.make_supervisor({"a": ignore_term_argv(ready)}, adapter)
            sup.submit("a", "start")
            self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
            # 等子进程装好 SIGTERM 忽略处理器后再停
            self.assertTrue(wait_for(lambda: os.path.exists(ready)))
            self._check_sigkill(sup, adapter)

    def _check_sigkill(self, sup, adapter):
        t0 = time.monotonic()
        sup.submit("a", "stop")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        elapsed = time.monotonic() - t0
        self.assertGreaterEqual(elapsed, 0.9)  # 等满了 1 秒宽限
        self.assertEqual(adapter.procs[0].poll(), -signal.SIGKILL)

    def test_close_leaves_no_children(self):
        adapter = CapturingAdapter()
        sup = Supervisor({"a": SLEEP_ARGV, "b": SLEEP_ARGV}, adapter=adapter)
        sup.submit("a", "start")
        sup.submit("b", "start")
        self.assertTrue(
            wait_for(
                lambda: sup.snapshot("a")["settled"] and sup.snapshot("b")["settled"]
            )
        )
        sup.close()
        for proc in adapter.procs:
            self.assertIsNotNone(proc.poll())
        sup.close()  # 幂等

    def test_submit_after_close_raises(self):
        sup = Supervisor({"a": SLEEP_ARGV})
        sup.close()
        with self.assertRaises(RuntimeError):
            sup.submit("a", "start")

    def test_empty_sites_rejected(self):
        with self.assertRaises(ValueError):
            Supervisor({})

    # ---------------------------------------------------------- 挂点场景

    def test_stop_waits_for_inflight_creation(self):
        """创建已产生子进程但未返回时接受 stop：不得提前完成，放行后回收。"""
        adapter = BlockingSpawnAdapter()
        sup = self.make_supervisor({"a": SLEEP_ARGV}, adapter)
        sup.submit("a", "start")
        self.assertTrue(adapter.spawn_called.wait(10))  # 子进程已创建
        sup.submit("a", "stop")
        time.sleep(0.2)
        snap = sup.snapshot("a")
        self.assertFalse(snap["settled"])  # stop 不能提前完成
        self.assertEqual(snap["pending_action"], "stop")
        adapter.release.set()  # 放行创建返回
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "stopped")
        self.assertIsNone(snap["pid"])
        proc = adapter.procs[0]
        self.assertIsNotNone(proc.poll())  # 迟到成功的子进程被终止并回收
        self.assertLess(proc.poll(), 0)  # 被信号终止

    def test_stale_exit_does_not_overwrite_restart(self):
        """旧实例退出结果被延迟投递，期间 restart 完成；旧结果不得覆盖新实例。"""
        adapter = CapturingAdapter()
        sup = self.make_supervisor({"a": QUICK_EXIT_ARGV}, adapter)
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        old_proc = adapter.procs[0]
        self.assertTrue(wait_for(lambda: old_proc.poll() is not None))

        # 挂住退出结果的投递
        held = []
        orig_deliver = sup._deliver_exit

        def holding_deliver(st, rec, rc):
            held.append((st, rec, rc))

        sup._deliver_exit = holding_deliver
        sup.poll()  # 观察到旧实例退出，但结果被扣留
        self.assertEqual(len(held), 1)

        # 期间完成 restart，启动新的长运行实例
        sup._sites["a"].argv = SLEEP_ARGV
        sup.submit("a", "restart")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "running")
        new_pid = snap["pid"]
        self.assertNotEqual(new_pid, old_proc.pid)

        # 放行旧结果：新实例的状态和 PID 不能被覆盖
        sup._deliver_exit = orig_deliver
        for st, rec, rc in held:
            orig_deliver(st, rec, rc)
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "running")
        self.assertEqual(snap["pid"], new_pid)
        os.kill(new_pid, 0)  # 新实例仍然存活

    def test_retired_instance_not_reused_by_later_start(self):
        """stop 淘汰旧实例后，后续 start 不得复用它。"""
        adapter = CapturingAdapter()
        sup = self.make_supervisor({"a": SLEEP_ARGV}, adapter)
        sup.submit("a", "start")
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        pid1 = sup.snapshot("a")["pid"]
        sup.submit("a", "stop")
        sup.submit("a", "start")  # 合并执行也不能撤销 stop 的淘汰
        self.assertTrue(wait_for(lambda: sup.snapshot("a")["settled"]))
        snap = sup.snapshot("a")
        self.assertEqual(snap["status"], "running")
        self.assertNotEqual(snap["pid"], pid1)
        self.assertIsNotNone(adapter.procs[0].poll())


if __name__ == "__main__":
    unittest.main()
