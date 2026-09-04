#!/usr/bin/env python3
"""
supervisor —— 在容器里同时做两件事：
  1. 按固定时间点（默认 7/10/13/16/19/22 点，Asia/Shanghai）跑一次 lingowhale2rss
  2. 用内置 HTTP 服务器把生成的 .atom 文件暴露出去，给 Miniflux 订阅

只用标准库，不依赖系统 cron，避免 Alpine 装 cron 包和时区配置的额外坑。

本版改动:
  - 默认抓取频率从 3 次/天提到 6 次/天。配合 feed 现在从缓存出，
    夜里 22:00→次日 07:00 的空档不会再丢文章，但缩短窗口能让新文章更快进
    Miniflux，也让单轮的请求量更平均。
  - 新增 LW_PER_CHANNEL / LW_FEED_MAX / LW_ENTRY_TYPES 三个环境变量，
    LW_MAX_ITEMS 已废弃（它是整组共享配额，正是漏文章的根源）。
  - 分组自动同步。LW_AUTO_GROUPS=0 退回代码内置分组；
    LW_INCLUDE_UNGROUPED=0 不给未分组的号出 feed；
    LW_PRUNE_FEEDS=0 保留分组改名后遗留的旧 .atom。
"""

import base64
import gc
import http.server
import hmac
import os
import socketserver
import threading
import time
from datetime import datetime, timedelta

import lingowhale2rss as lw2r

# 日报模块是可选的。缺文件、或者 report.py 自身有语法错误时，
# 这里必须降级而不是抛异常——supervisor 挂掉会连 HTTP 服务和抓取一起带走，
# Miniflux 那边直接断粮，代价远大于"今天没有日报"。
try:
    import report as rpt
except Exception as _e:  # noqa: BLE001
    rpt = None
    print(f"[!] 日报模块不可用({_e})，本次仅抓取，不生成日报", flush=True)

try:
    from zoneinfo import ZoneInfo

    TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # noqa: BLE001
    TZ = None
    print("[!] zoneinfo 不可用，退回本地时区，请确认容器 TZ 设置正确", flush=True)

RUN_HOURS = [int(h) for h in os.environ.get("LW_RUN_HOURS", "7,10,13,16,19,22").split(",")]
# 日报时间点。留空则不生成日报。默认 8 点——排在 7 点抓取之后，隔一小时够跑完
REPORT_HOURS = [
    int(h) for h in os.environ.get("LW_REPORT_HOURS", "8").split(",") if h.strip()
]
DATA_DIR = os.environ.get("LW_DATA_DIR", "/app/data")
FEEDS_DIR = os.path.join(DATA_DIR, "feeds")
CACHE_PATH = os.path.join(DATA_DIR, "lw_cache.json")
REPORTS_DIR = os.path.join(DATA_DIR, "reports")
PORT = int(os.environ.get("PORT", 8080))


def now():
    return datetime.now(TZ) if TZ else datetime.now()


AUTH_USER = os.environ.get("LW_HTTP_USER", "")
AUTH_PASS = os.environ.get("LW_HTTP_PASS", "")


def check_auth(header_value):
    """校验 Authorization: Basic xxx，使用 compare_digest 防时序攻击"""
    if not header_value or not header_value.startswith("Basic "):
        return False
    try:
        raw = base64.b64decode(header_value[6:]).decode("utf-8")
        user, _, pw = raw.partition(":")
    except Exception:  # noqa: BLE001
        return False
    return hmac.compare_digest(user, AUTH_USER) and hmac.compare_digest(pw, AUTH_PASS)


def serve():
    """在 FEEDS_DIR 上起一个只读静态文件服务，可选 HTTP Basic Auth"""
    os.makedirs(FEEDS_DIR, exist_ok=True)
    auth_enabled = bool(AUTH_USER and AUTH_PASS)
    if auth_enabled:
        print("HTTP Basic Auth 已启用", flush=True)
    else:
        print("[!] 未设置 LW_HTTP_USER/LW_HTTP_PASS，域名当前公开无密码", flush=True)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=FEEDS_DIR, **kw)

        def log_message(self, fmt, *args):
            print(f"[http] {self.address_string()} {fmt % args}", flush=True)

        def do_GET(self):
            if self.path == "/healthz":
                body = b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if auth_enabled and not check_auth(self.headers.get("Authorization")):
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="lingowhale-feeds"')
                self.end_headers()
                return
            super().do_GET()

    with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler) as httpd:
        print(f"HTTP 服务已启动: 0.0.0.0:{PORT} -> {FEEDS_DIR}", flush=True)
        httpd.serve_forever()


def run_job():
    print(f"[{now()}] 开始抓取", flush=True)
    if os.environ.get("LW_MAX_ITEMS"):
        print(
            "[!] LW_MAX_ITEMS 已废弃(整组共享配额是漏文章的根源)，"
            "请改用 LW_PER_CHANNEL / LW_FEED_MAX",
            flush=True,
        )
    try:
        entry_types = {
            int(x)
            for x in os.environ.get("LW_ENTRY_TYPES", "7").split(",")
            if x.strip()
        }
        lw2r.run(
            out=FEEDS_DIR,
            base_url=os.environ.get("LW_BASE_URL", ""),
            per_channel=int(os.environ.get("LW_PER_CHANNEL", "10")),
            feed_max=int(os.environ.get("LW_FEED_MAX", "120")),
            limit=int(os.environ.get("LW_LIMIT", "10")),
            delay=float(os.environ.get("LW_DELAY", "1.0")),
            no_content=os.environ.get("LW_NO_CONTENT") == "1",
            link_only=os.environ.get("LW_LINK_ONLY") == "1",
            cache_path=CACHE_PATH,
            cache_max_age_days=int(os.environ.get("LW_CACHE_MAX_AGE_DAYS", "14")),
            notify_days=int(os.environ.get("LW_NOTIFY_DAYS", "1")),
            entry_types=entry_types,
            auto_groups=os.environ.get("LW_AUTO_GROUPS", "1") != "0",
            include_ungrouped=os.environ.get("LW_INCLUDE_UNGROUPED", "1") != "0",
            prune_feeds=os.environ.get("LW_PRUNE_FEEDS", "1") != "0",
        )
        print(f"[{now()}] 抓取完成", flush=True)
    except SystemExit as e:
        # build_headers()/check_token_expiry() 配置有误时用 sys.exit() 报错。
        # 现在跟 supervisor 同一个进程，必须拦下来，否则会连 HTTP 服务一起被杀掉。
        print(f"[{now()}] 抓取因配置问题中止: {e}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[{now()}] 抓取异常: {e}", flush=True)
    gc.collect()


def report_job():
    """生成 AI 日报。失败只记日志，绝不能影响抓取和 HTTP 服务。"""
    if rpt is None:
        print(f"[{now()}] 日报模块未加载，跳过", flush=True)
        return
    if not os.environ.get("LLM_API_KEY"):
        print(f"[{now()}] 未设置 LLM_API_KEY，跳过日报", flush=True)
        return
    print(f"[{now()}] 开始生成日报", flush=True)
    try:
        rpt.generate(
            cache_path=CACHE_PATH,
            out_dir=FEEDS_DIR,
            reports_dir=REPORTS_DIR,
            base_url=os.environ.get("LW_BASE_URL", ""),
            window_hours=int(os.environ.get("LW_REPORT_WINDOW_HOURS", "26")),
            max_articles=int(os.environ.get("LW_REPORT_MAX_ARTICLES", "25")),
            max_chars=int(os.environ.get("LW_REPORT_MAX_CHARS", "3000")),
            report_slug=os.environ.get("LW_REPORT_SLUG", "daily-report"),
            push=os.environ.get("LW_REPORT_PUSH", "1") != "0",
        )
        print(f"[{now()}] 日报完成", flush=True)
    except SystemExit as e:
        print(f"[{now()}] 日报因配置问题中止: {e}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[{now()}] 日报异常: {e}", flush=True)
    gc.collect()


def next_event(t):
    """把抓取和日报两套时刻表合成一条时间线，同一时刻两件事都做。"""
    best, kinds = None, set()
    for hours, kind in ((RUN_HOURS, "fetch"), (REPORT_HOURS, "report")):
        for h in hours:
            c = t.replace(hour=h, minute=0, second=0, microsecond=0)
            if c <= t:
                c += timedelta(days=1)
            if best is None or c < best:
                best, kinds = c, {kind}
            elif c == best:
                kinds.add(kind)
    return best, kinds


def scheduler():
    # 启动时先抓一次，避免部署后要空等到下一个整点才有数据。
    # 日报不在启动时跑——容器重启一次就推一条微信太吵，
    # 想手动补一份用 LW_REPORT_ON_START=1。
    run_job()
    if os.environ.get("LW_REPORT_ON_START") == "1":
        report_job()

    while True:
        t = now()
        nxt, kinds = next_event(t)
        wait = (nxt - t).total_seconds()
        print(
            f"下次任务: {nxt} [{'+'.join(sorted(kinds))}] (等待 {wait / 3600:.1f} 小时)",
            flush=True,
        )
        time.sleep(max(wait, 1))
        if "fetch" in kinds:
            run_job()
        if "report" in kinds:
            report_job()


if __name__ == "__main__":
    threading.Thread(target=serve, daemon=True).start()
    scheduler()
