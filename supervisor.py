#!/usr/bin/env python3
"""
supervisor —— 在容器里同时做两件事：
  1. 按固定时间点（默认 8/12/17 点，Asia/Shanghai）跑一次 lingowhale2rss.py
  2. 用内置 HTTP 服务器把生成的 .atom 文件暴露出去，给 Miniflux 订阅

只用标准库，不依赖系统 cron，避免 Alpine 装 cron 包和时区配置的额外坑。
"""

import http.server
import os
import socketserver
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo

    TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # noqa: BLE001
    TZ = None
    print("[!] zoneinfo 不可用，退回本地时区，请确认容器 TZ 设置正确", flush=True)

RUN_HOURS = [int(h) for h in os.environ.get("LW_RUN_HOURS", "8,12,17").split(",")]
DATA_DIR = os.environ.get("LW_DATA_DIR", "/app/data")
FEEDS_DIR = os.path.join(DATA_DIR, "feeds")
CACHE_PATH = os.path.join(DATA_DIR, "lw_cache.json")
PORT = int(os.environ.get("PORT", 8080))
SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lingowhale2rss.py")


def now():
    return datetime.now(TZ) if TZ else datetime.now()


def serve():
    """在 FEEDS_DIR 上起一个只读静态文件服务"""
    os.makedirs(FEEDS_DIR, exist_ok=True)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=FEEDS_DIR, **kw)

        def log_message(self, fmt, *args):
            print(f"[http] {self.address_string()} {fmt % args}", flush=True)

    with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler) as httpd:
        print(f"HTTP 服务已启动: 0.0.0.0:{PORT} -> {FEEDS_DIR}", flush=True)
        httpd.serve_forever()


def next_run(t):
    candidates = []
    for h in RUN_HOURS:
        c = t.replace(hour=h, minute=0, second=0, microsecond=0)
        if c <= t:
            c += timedelta(days=1)
        candidates.append(c)
    return min(candidates)


def run_job():
    print(f"[{now()}] 开始抓取", flush=True)
    cmd = [
        sys.executable,
        SCRIPT,
        "--out",
        FEEDS_DIR,
        "--cache",
        CACHE_PATH,
        "--max-items",
        os.environ.get("LW_MAX_ITEMS", "30"),
        "--delay",
        os.environ.get("LW_DELAY", "1.0"),
    ]
    base_url = os.environ.get("LW_BASE_URL", "")
    if base_url:
        cmd += ["--base-url", base_url]
    if os.environ.get("LW_NO_CONTENT") == "1":
        cmd.append("--no-content")

    try:
        subprocess.run(cmd, check=True)
        print(f"[{now()}] 抓取完成", flush=True)
    except subprocess.CalledProcessError as e:
        print(f"[{now()}] 抓取失败: {e}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[{now()}] 抓取异常: {e}", flush=True)


def scheduler():
    # 启动时先跑一次，避免部署后要空等到下一个整点才有数据
    run_job()
    while True:
        t = now()
        nxt = next_run(t)
        wait = (nxt - t).total_seconds()
        print(f"下次抓取: {nxt} (等待 {wait / 3600:.1f} 小时)", flush=True)
        time.sleep(max(wait, 1))
        run_job()


if __name__ == "__main__":
    threading.Thread(target=serve, daemon=True).start()
    scheduler()
