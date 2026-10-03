# task26100527
POSIX 站点进程监管库（仅标准库），见 `site_supervisor.py`，测试见 `test_site_supervisor.py`。

```python
from site_supervisor import SiteSupervisor

sup = SiteSupervisor({"web": ["/usr/bin/python3", "-m", "http.server"]})
seq = sup.submit("web", "start")      # 立即返回该站点单调递增的命令序号
while not sup.snapshot("web")["settled"]:
    sup.poll()                        # 巡检退出情况并推进状态机
print(sup.snapshot("web"))            # {'status': 'running', 'pid': ..., ...}
sup.submit("web", "stop")
sup.close()                           # 保证不遗留子进程
```

运行测试：`python3 -m unittest test_site_supervisor -v`
