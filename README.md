# task26100527

面向 POSIX 的站点进程监管库（纯 Python 标准库实现）。

## 结构

- `site_supervisor/adapter.py` — 进程操作适配层，默认基于 `subprocess.Popen`，直接 exec 不经过 shell；测试可替换/包装以注入挂点。
- `site_supervisor/supervisor.py` — 监管器核心：每站点一个工作线程，命令可合并但淘汰关系不撤销。
- `tests/test_supervisor.py` — unittest 测试，使用真实 Python 子进程验证创建、终止与回收。

## API

```python
from site_supervisor import Supervisor

sup = Supervisor({"web": ["/usr/bin/python3", "-m", "http.server"]})
seq = sup.submit("web", "start")     # 立即返回单调递增序号
sup.poll()                           # 巡检退出情况并更新状态
snap = sup.snapshot("web")           # {status, settled, pid, seq, settled_seq, pending_action}
sup.close()                          # 不遗留子进程
```

- `submit(site_id, action)`：`action ∈ {start, stop, restart}`，接受即返回，不等待进程创建或退出。
- `snapshot` 中 `settled=False` 表示最新命令仍在处理中；`status ∈ {idle, running, stopped, error}`。
- 当前实例自行退出（无论退出码）由 `poll()` 置为 `error`，不自动拉起；主动停止不算 error。
- SIGTERM 发出 1 秒后仍未退出则 SIGKILL 并回收。

## 运行测试

```sh
python3 -m unittest discover -s tests -v
```
