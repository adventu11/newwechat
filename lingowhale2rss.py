#!/usr/bin/env python3
"""
lingowhale2rss —— 把语鲸订阅的公众号转成 Atom 订阅源
只用标准库，无需 pip install。

用法:
    # 令牌放同目录的 lw.env 文件，格式:
    #   LW_ACCESS_TOKEN=eyJhbGci...
    #   LW_AUTH_TOKEN=eyJhbGci...
    #   LW_B_ID=...
    #   LW_GUEST_ID=...
    python lingowhale2rss.py --list-channels
    python lingowhale2rss.py --out ./feeds

生成的 .atom 文件放到任意静态服务器下，Miniflux 订阅即可。

--------------------------------------------------------------------------
分组现在是【自动同步】的:
  每轮抓取前先调 user_subscribe/list，用语鲸端当前的分组结构生成本轮任务。
  在语鲸里新建分组、往分组里加号、把号挪到别的分组，下一轮自动生效，
  不需要改代码。分组改名后 .atom 文件名跟着变(见 NAME_MAP 一节)。

  代码里的 FALLBACK_GROUPS 只在接口拉不到、且磁盘上也没有上一轮快照时兜底，
  日常不用维护。想回到旧的手工模式: --no-auto-groups
--------------------------------------------------------------------------
"""

import argparse
import base64
import gc
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from xml.sax.saxutils import escape

API = "https://api-public.lingowhale.com/api/lingowhale/v1/"
FEED_EP = API + "feed/subscription"
DETAIL_EP = API + "entry_detail/get"
SUBS_EP = API + "user_subscribe/list"

# 语鲸通用排序参数。注意: 缺失时接口不会报错，而是静默返回默认订阅，务必带上。
SORT_TYPE = 2

# 允许进入 feed 的 entry_type。7 = 文章。
# 如果发现某些推送始终不出现，先看运行日志末尾的"entry_type 分布"，
# 用 --entry-types 7,8 放行对应类型，无需改代码。
DEFAULT_ENTRY_TYPES = {7}

# 没有归到任何分组的公众号，统一收进这个 feed
UNGROUPED_NAME = "未分组"

# ---------------------------------------------------------------- 文件名映射
# 默认: .atom 文件名 = 语鲸端的分组名(经过路径字符清洗)。分组改名 → 文件名跟着变
# → Miniflux 里的订阅 URL 会失效，需要重新订阅一次。
#
# 如果某个分组你希望文件名永远钉死(比如已经在 Miniflux 里订阅好了不想动)，
# 在这里写一条映射即可，它优先于分组名:
#     NAME_MAP = {"医药": "yiyao", "注册": "zhuce"}
NAME_MAP = {}

# ---------------------------------------------------------------- 兜底分组
# 仅在 user_subscribe/list 拉取失败、且磁盘上也没有上一轮的分组快照时使用。
# 平时不用维护这里。
FALLBACK_GROUPS = {
    "yiyao": [
        "682b68f7da6f685c6aed8e4f",  # 21新健康
        "67cc08ae9a4297b6148b3da7",  # 药明康德
        "67cc08ae9a4297b6148b3eb8",  # E药经理人
        "67cc08ae9a4297b6148b3e05",  # 医药魔方
        "67cc08ac9a4297b6148b3a4f",  # 深蓝观
    ],
    "zhuce": [
        "6814e26c9c69993601c767ce",  # 注册圈
        "6814e28a288f7f1429cfe82f",  # iReg
    ],
}

# ---------------------------------------------------------------- 环境变量


def load_env_file(path="lw.env"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def build_headers():
    missing = [
        k
        for k in (
            "LW_ACCESS_TOKEN",
            "LW_AUTH_TOKEN",
            "LW_B_ID",
            "LW_GUEST_ID",
            "LW_U_ID",
        )
        if not os.environ.get(k)
    ]
    if missing:
        sys.exit("缺少环境变量: " + ", ".join(missing))
    return {
        "Access-Token": os.environ["LW_ACCESS_TOKEN"],
        "Auth-Token": os.environ["LW_AUTH_TOKEN"],
        "B-Id": os.environ["LW_B_ID"],
        "Guest-Id": os.environ["LW_GUEST_ID"],
        "U-Id": os.environ["LW_U_ID"],
        "Web-Site": "web",
        "Imei": "fingerPrint-web",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip",
        "Origin": "https://lingowhale.com",
        "Referer": "https://lingowhale.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
        ),
    }


# ---------------------------------------------------------------- HTTP


def post(url, headers, payload=None, retries=3):
    body = json.dumps(payload or {}).encode("utf-8")
    last = None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            j = json.loads(raw.decode("utf-8"))
            if j.get("code") != 0:
                raise RuntimeError(f"API code={j.get('code')} msg={j.get('msg')}")
            return j.get("data") or {}
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                sys.exit(f"鉴权失败 ({e.code})，令牌可能已过期，请重新抓取")
            last = e
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"请求失败 {url}: {last}")


# ---------------------------------------------------------------- 微信推送(Server酱)


def send_wechat_notify(title, content):
    """通过 Server酱 把消息推到微信。没配 SENDKEY 就跳过，不影响主流程。"""
    sendkey = os.environ.get("SERVERCHAN_SENDKEY", "")
    if not sendkey:
        print("[!] 未设置 SERVERCHAN_SENDKEY，跳过微信推送", file=sys.stderr)
        return False
    url = f"https://sctapi.ftqq.com/{sendkey}.send"
    data = urllib.parse.urlencode({"title": title, "desp": content}).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read().decode("utf-8"))
        ok = resp.get("code") == 0
        if not ok:
            print(f"[!] 微信推送失败: {resp}", file=sys.stderr)
        return ok
    except Exception as e:  # noqa: BLE001
        print(f"[!] 微信推送异常: {e}", file=sys.stderr)
        return False


# ---------------------------------------------------------------- JSON 落盘


def load_json(path, default=None):
    if not path:
        return {} if default is None else default
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {} if default is None else default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------- 令牌检查


def check_token_expiry(notify_state_path=None, notify_days=1):
    """
    检查 LW_ACCESS_TOKEN / LW_AUTH_TOKEN 的剩余有效期。

    剩余天数跌破 notify_days 时推一条微信消息；用 notify_state_path 记录
    "这个 exp 值是否已经通知过"，避免一天跑多次调度就收到多条重复消息——
    只有换了新令牌(exp 变化)才会重新触发提醒。
    """
    state = load_json(notify_state_path) if notify_state_path else {}
    state_changed = False

    for env_key, label in (
        ("LW_ACCESS_TOKEN", "Access-Token"),
        ("LW_AUTH_TOKEN", "Auth-Token"),
    ):
        tok = os.environ.get(env_key, "")
        parts = tok.split(".")
        if len(parts) != 3:
            continue
        try:
            p = parts[1] + "=" * (-len(parts[1]) % 4)
            claims = json.loads(base64.urlsafe_b64decode(p))
        except Exception:  # noqa: BLE001
            continue
        exp = claims.get("exp")
        if not exp:
            continue
        days = (exp - time.time()) / 86400
        if days < 0:
            sys.exit(f"{label} 已过期，请重新登录语鲸后抓取新令牌")
        if days < 3:
            print(f"[!] {label} 仅剩 {days:.1f} 天，请尽快更新", file=sys.stderr)
        else:
            print(f"{label} 剩余 {days:.1f} 天", file=sys.stderr)

        if days <= notify_days and notify_state_path:
            if state.get(env_key) != exp:  # 这个 exp 还没通知过，或者是新换的令牌
                ok = send_wechat_notify(
                    f"语鲸 {label} 即将过期",
                    f"{label} 还剩 {days:.1f} 天过期，"
                    f"请尽快登录语鲸抓取新令牌，并更新到部署的环境变量里。",
                )
                if ok:
                    state[env_key] = exp
                    state_changed = True

    if state_changed and notify_state_path:
        save_json(notify_state_path, state)


# ---------------------------------------------------------------- 分组同步


def parse_subscriptions(data, include_ungrouped=True):
    """
    把 user_subscribe/list 的返回拍平成 ({分组名: [channel_id]}, {channel_id: 号名})。

    同一个号被放进多个分组是允许的——抓取时按 channel 去重只拉一次，
    输出时按分组各出一份，不会重复请求。
    """
    groups = {}
    names = {}  # channel_id -> 公众号名，用于日志和排错

    for s in data.get("user_subscribes") or []:
        if "subscription_group" in s:
            g = s["subscription_group"] or {}
            name = (g.get("name") or "").strip()
            if not name:
                continue
            bucket = groups.setdefault(name, [])
            for c in g.get("channels") or []:
                cid = c.get("channel_id")
                if not cid:
                    continue
                names[cid] = c.get("name") or ""
                if cid not in bucket:
                    bucket.append(cid)
        elif "subscription_channel" in s and include_ungrouped:
            c = s["subscription_channel"] or {}
            cid = c.get("channel_id")
            if not cid:
                continue
            names[cid] = c.get("name") or ""
            bucket = groups.setdefault(UNGROUPED_NAME, [])
            if cid not in bucket:
                bucket.append(cid)

    groups = {k: v for k, v in groups.items() if v}  # 空分组不生成文件
    return groups, names


def fetch_groups(headers, include_ungrouped=True):
    data = post(SUBS_EP, headers, {"sort_type": SORT_TYPE})
    return parse_subscriptions(data, include_ungrouped=include_ungrouped)


def diff_groups(old, new, names):
    """打印分组结构的变化，一眼确认自动同步是否真的生效。"""
    if not old:
        print(f"分组快照初始化: {len(new)} 组", file=sys.stderr)
        return

    old_names, new_names = set(old), set(new)
    for n in sorted(new_names - old_names):
        print(f"[同步] 新增分组 {n} ({len(new[n])} 个号)", file=sys.stderr)
    for n in sorted(old_names - new_names):
        print(
            f"[同步] 分组消失 {n}（改名或已删除，旧 .atom 会被清理）", file=sys.stderr
        )

    for n in sorted(new_names & old_names):
        added = [c for c in new[n] if c not in old[n]]
        removed = [c for c in old[n] if c not in new[n]]
        for c in added:
            print(f"[同步] {n} 新增公众号 {names.get(c) or c}", file=sys.stderr)
        if removed:
            print(f"[同步] {n} 移除 {len(removed)} 个号", file=sys.stderr)


def resolve_groups(headers, auto=True, include_ungrouped=True, snapshot_path=None):
    """
    返回本轮要抓的 ({分组名: [channel_id]}, {channel_id: 号名})。

    优先用接口实时同步；接口挂了就退回磁盘上的上一轮快照；快照也没有才用
    代码里的 FALLBACK_GROUPS。这层兜底很重要——否则订阅接口一次抽风就会让
    所有 feed 变成空文件，Miniflux 那边看着就像文章集体消失。
    """
    if not auto:
        return dict(FALLBACK_GROUPS), {}

    try:
        groups, names = fetch_groups(headers, include_ungrouped=include_ungrouped)
        if not groups:
            raise RuntimeError("接口返回的订阅列表为空")
        if snapshot_path:
            diff_groups(load_json(snapshot_path), groups, names)
            save_json(snapshot_path, groups)
        total = sum(len(v) for v in groups.values())
        print(f"分组同步完成: {len(groups)} 组 / {total} 个订阅位", file=sys.stderr)
        return groups, names
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"[!] 分组同步失败: {e}", file=sys.stderr)

    snapshot = load_json(snapshot_path) if snapshot_path else {}
    if snapshot:
        print(f"[!] 退回上一轮分组快照 ({len(snapshot)} 组)", file=sys.stderr)
        return snapshot, {}

    print("[!] 无快照可用，退回代码内置的 FALLBACK_GROUPS", file=sys.stderr)
    return dict(FALLBACK_GROUPS), {}


def safe_filename(name):
    """分组名 → 文件名。清掉路径分隔符和 Windows 非法字符，空白折成下划线。"""
    s = (name or "").strip()
    for ch in '/\\:*?"<>|\r\n\t':
        s = s.replace(ch, "_")
    s = "_".join(s.split())
    s = s.strip(". ")
    return s or "group"


def feed_filenames(groups):
    """{分组名: 文件名(不含扩展名)}，处理 NAME_MAP 覆盖和清洗后的重名冲突。"""
    used, out = set(), {}
    for name in groups:
        base = NAME_MAP.get(name) or safe_filename(name)
        slug, i = base, 2
        while slug in used:
            slug = f"{base}_{i}"
            i += 1
        used.add(slug)
        out[name] = slug
    return out


def prune_feed_files(out_dir, keep):
    """删掉不再对应任何分组的 .atom（分组改名/删除后留下的孤儿文件）"""
    keep = {f"{s}.atom" for s in keep}
    try:
        existing = os.listdir(out_dir)
    except OSError:
        return
    for fn in existing:
        if fn.endswith(".atom") and fn not in keep:
            try:
                os.remove(os.path.join(out_dir, fn))
                print(f"[清理] 删除孤儿 feed {fn}", file=sys.stderr)
            except OSError as e:  # noqa: PERF203
                print(f"[!] 删除 {fn} 失败: {e}", file=sys.stderr)


# ---------------------------------------------------------------- 拉取


def fetch_channel_feed(headers, channel_id, max_items, limit, delay, cutoff=0):
    """
    按 cursor 翻页拉【单个公众号】的文章列表。

    单号拉取是关键: 旧版把整组 channel_ids 一起丢给接口再取前 N 条，
    等于让同组所有号抢一个配额，日更号必然把低频号挤掉。

    cutoff: unix 时间戳。翻到比它更老的文章就停——反正缓存也只保留这么久。
    """
    items, cursor = [], ""
    while len(items) < max_items:
        data = post(
            FEED_EP,
            headers,
            {
                "cursor": cursor,
                "sort_type": SORT_TYPE,
                "limit": limit,
                "filter_unread": False,
                "channel_ids": [channel_id],
            },
        )
        batch = data.get("feed_list") or []
        if not batch:
            break
        items.extend(batch)

        oldest = min((b.get("pub_time") or 0) for b in batch)
        if cutoff and oldest and oldest < cutoff:
            break
        if not data.get("has_more"):
            break
        cursor = data.get("cursor") or ""
        if not cursor:
            break
        time.sleep(delay)
    return items[:max_items]


def fetch_detail(headers, entry_id, entry_type):
    url = f"{DETAIL_EP}?entry_id={entry_id}&entry_type={entry_type}"
    data = post(url, headers, {})
    return data.get("resource") or {}


# ---------------------------------------------------------------- Atom


def rfc3339(ts):
    if not ts:
        ts = time.time()
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_atom(title, self_url, entries):
    out = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom">',
        f"<title>{escape(title)}</title>",
        f'<id>{escape(self_url or "urn:lingowhale:" + title)}</id>',
        f"<updated>{rfc3339(None)}</updated>",
        "<generator>lingowhale2rss</generator>",
    ]
    if self_url:
        out.append(f'<link rel="self" href="{escape(self_url)}"/>')

    for e in entries:
        link = e.get("orig_url") or ""
        eid = e.get("entry_id") or link
        content = e.get("html") or e.get("content") or e.get("abstract") or ""
        out.append("<entry>")
        out.append(f'<title type="html">{escape(e.get("title") or "(无标题)")}</title>')
        out.append(f"<id>urn:lingowhale:{escape(eid)}</id>")
        if link:
            out.append(f'<link rel="alternate" href="{escape(link)}"/>')
        out.append(f'<updated>{rfc3339(e.get("pub_time"))}</updated>')
        out.append(f'<published>{rfc3339(e.get("pub_time"))}</published>')
        author = e.get("author") or e.get("channel") or "语鲸"
        out.append(f"<author><name>{escape(author)}</name></author>")
        if e.get("channel"):
            out.append(f'<category term="{escape(e["channel"])}"/>')
        out.append(f'<content type="html">{escape(content)}</content>')
        out.append("</entry>")

    out.append("</feed>")
    return "\n".join(out)


# ---------------------------------------------------------------- 频道清单


def list_channels(headers):
    groups, names = fetch_groups(headers, include_ungrouped=True)
    for name, ids in groups.items():
        print(f"\n# 分组: {name}  ({len(ids)} 个号)  -> {safe_filename(name)}.atom")
        for cid in ids:
            print(f"    {cid}  {names.get(cid, '')}")


# ---------------------------------------------------------------- 核心流程
# 抽成独立函数，方便 supervisor.py 直接 import 调用，避免另起一个 Python
# 进程(subprocess)——两个解释器同时常驻在内存紧张的环境里会直接顶爆限额。


def run(
    out="./feeds",
    base_url="",
    per_channel=10,
    feed_max=120,
    limit=10,
    delay=1.0,
    no_content=False,
    link_only=False,
    cache_path="./lw_cache.json",
    cache_max_age_days=14,
    notify_days=1,
    entry_types=None,
    auto_groups=True,
    include_ungrouped=True,
    prune_feeds=True,
):
    entry_types = set(entry_types or DEFAULT_ENTRY_TYPES)
    headers = build_headers()

    data_dir = os.path.dirname(os.path.abspath(cache_path))
    notify_state_path = os.path.join(data_dir, "lw_notify_state.json")
    snapshot_path = os.path.join(data_dir, "lw_groups.json")
    check_token_expiry(notify_state_path=notify_state_path, notify_days=notify_days)

    os.makedirs(out, exist_ok=True)

    groups, channel_names = resolve_groups(
        headers,
        auto=auto_groups,
        include_ungrouped=include_ungrouped,
        snapshot_path=snapshot_path if auto_groups else None,
    )
    slugs = feed_filenames(groups)

    cache = load_json(cache_path)
    cutoff = time.time() - cache_max_age_days * 86400 if cache_max_age_days > 0 else 0

    # link_only/no_content 模式下不需要正文，把已缓存条目里的 html 直接清空。
    # 这一行是"自愈"：哪怕磁盘上的缓存文件是之前全文模式攒下的大文件，
    # 本次运行加载进内存后立刻瘦身，且下面 save_json() 会把瘦身结果写回磁盘。
    if link_only or no_content:
        for v in cache.values():
            v["html"] = ""

    # 同一个号可能同时属于多个分组，按 channel 去重，只拉一次
    all_channels = []
    for ids in groups.values():
        for cid in ids:
            if cid not in all_channels:
                all_channels.append(cid)

    new_details = 0
    failed_details = 0
    type_hist = {}

    print(f"开始抓取 {len(all_channels)} 个公众号…", file=sys.stderr)
    for cid in all_channels:
        label = channel_names.get(cid) or cid
        try:
            raw = fetch_channel_feed(
                headers, cid, per_channel, limit, delay, cutoff=cutoff
            )
        except Exception as e:  # noqa: BLE001
            # 单个号失败不该拖垮全局：本轮跳过，缓存里的旧条目照常出 feed，
            # 下一轮再补。
            print(f"  [!] 列表失败 {label}: {e}", file=sys.stderr)
            continue

        kept = 0
        for it in raw:
            et = it.get("entry_type")
            type_hist[et] = type_hist.get(et, 0) + 1
            if et not in entry_types:
                continue

            eid = it.get("entry_id")
            if not eid:
                continue
            kept += 1

            meta = {
                "entry_id": eid,
                # channel_id 是分组归属的唯一依据。存它而不是存分组名，
                # 这样分组改名、或者把号挪到别的分组，老缓存都能自动跟着走。
                "channel_id": cid,
                "title": it.get("title"),
                "pub_time": it.get("pub_time"),
                "channel": (it.get("channel") or {}).get("name", ""),
                "abstract": (it.get("abstract") or "")
                .replace("<hl>", "")
                .replace("</hl>", ""),
            }

            row = cache.get(eid)
            if row is not None:
                # 老条目：补齐列表侧字段（旧版缓存只存了正文相关的几项，
                # 没有 channel_id/title 就没法从缓存重建 feed）
                for k, v in meta.items():
                    if v or k not in row:
                        row[k] = v
                need_detail = not no_content and not row.get("orig_url")
            else:
                row = dict(meta)
                row.setdefault("orig_url", "")
                row.setdefault("author", "")
                row.setdefault("html", "")
                need_detail = not no_content

            if need_detail:
                try:
                    res = fetch_detail(headers, eid, et)
                    row.update(
                        {
                            # 语鲸返回的是 http，换成 https 只是去掉一个明显的
                            # "机器访问"信号，不能根治环境异常校验，但没有副作用
                            "orig_url": res.get("orig_url", "").replace(
                                "http://mp.weixin.qq.com",
                                "https://mp.weixin.qq.com",
                                1,
                            ),
                            "author": (res.get("author") or {}).get("name", ""),
                            # link_only 模式下不需要正文，压根不存，
                            # 而不是存了再在输出阶段丢弃——减少的是缓存本身的体积
                            "html": "" if link_only else res.get("html", ""),
                        }
                    )
                    new_details += 1
                    time.sleep(delay)
                except Exception as e:  # noqa: BLE001
                    # 拿不到 orig_url 就先不写进 feed。旧版会输出一条没有 <link>
                    # 的条目，Miniflux 按 GUID 落库后即使下轮补到链接也未必回填，
                    # 肉眼看就是"这篇丢了"。这里仍写缓存(留住标题/时间)，
                    # orig_url 为空，输出阶段跳过，下一轮自动重试详情。
                    failed_details += 1
                    print(f"  [!] 详情失败 {eid}: {e}", file=sys.stderr)

            cache[eid] = row

        if kept:
            print(f"  {label}: +{kept}", file=sys.stderr)
        time.sleep(delay)
        del raw

    gc.collect()

    # ------------------------------------------------------------ 清理缓存
    # 按发布时间清理过期条目，防止 lw_cache.json 无限增长。
    # 缓存现在同时是 feed 的数据源，所以 cache_max_age_days 直接决定了
    # 订阅源里能看到多久以前的文章。
    if cache_max_age_days > 0:
        before = len(cache)
        cache = {
            k: v for k, v in cache.items() if (v.get("pub_time") or cutoff) >= cutoff
        }
        removed = before - len(cache)
        if removed:
            print(
                f"清理过期缓存 {removed} 条(保留最近 {cache_max_age_days} 天)",
                file=sys.stderr,
            )

    save_json(cache_path, cache)

    # ------------------------------------------------------------ 输出 feed
    # 从缓存出，而不是只写"本次抓到的"。这样一次抓取失败、或者两次运行之间
    # 隔了一整夜，都不会让中间的文章永久消失。
    for name, ids in groups.items():
        idset = set(ids)
        entries = [
            v
            for v in cache.values()
            if v.get("channel_id") in idset and (v.get("orig_url") or no_content)
        ]
        entries.sort(key=lambda e: e.get("pub_time") or 0, reverse=True)
        entries = entries[:feed_max]

        slug = slugs[name]
        self_url = f"{base_url.rstrip('/')}/{slug}.atom" if base_url else ""
        xml = build_atom(f"语鲸 - {name}", self_url, entries)
        path = os.path.join(out, f"{slug}.atom")
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml)
        print(f"[{name}] {len(entries)} 篇 -> {path}", file=sys.stderr)

        del entries, xml
        gc.collect()

    if prune_feeds:
        prune_feed_files(out, slugs.values())

    pending = sum(1 for v in cache.values() if not v.get("orig_url") and not no_content)
    print(
        f"完成，新增详情 {new_details} 条，详情失败 {failed_details} 条，"
        f"待补链接 {pending} 条，缓存共 {len(cache)} 条",
        file=sys.stderr,
    )
    if type_hist:
        hist = ", ".join(f"{k}:{v}" for k, v in sorted(type_hist.items(), key=str))
        print(
            f"entry_type 分布 {hist}（当前只放行 {sorted(entry_types)}，"
            f"若有推送始终不出现，用 --entry-types 放行对应类型）",
            file=sys.stderr,
        )


def run_from_args(args):
    if args.list_channels:
        headers = build_headers()
        check_token_expiry()
        list_channels(headers)
        return

    entry_types = {int(x) for x in args.entry_types.split(",") if x.strip()}

    run(
        out=args.out,
        base_url=args.base_url,
        per_channel=args.per_channel,
        feed_max=args.feed_max,
        limit=args.limit,
        delay=args.delay,
        no_content=args.no_content,
        link_only=args.link_only,
        cache_path=args.cache,
        cache_max_age_days=args.cache_max_age_days,
        notify_days=args.notify_days,
        entry_types=entry_types,
        auto_groups=args.auto_groups,
        include_ungrouped=args.include_ungrouped,
        prune_feeds=args.prune_feeds,
    )


# ---------------------------------------------------------------- 命令行入口


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./feeds", help="输出目录")
    ap.add_argument("--base-url", default="", help="feed 的公开访问前缀")
    ap.add_argument(
        "--per-channel", type=int, default=10, help="每个公众号每轮最多拉多少条"
    )
    ap.add_argument(
        "--feed-max",
        type=int,
        default=120,
        help="每个 .atom 文件最多输出多少条(从缓存里按时间倒序取)",
    )
    ap.add_argument("--limit", type=int, default=10, help="每页条数")
    ap.add_argument("--delay", type=float, default=1.0, help="请求间隔秒")
    ap.add_argument("--no-content", action="store_true", help="不取详情(无链接/无正文)")
    ap.add_argument(
        "--link-only",
        action="store_true",
        help="取详情但不把全文塞进RSS，只保留标题/摘要/原文链接",
    )
    ap.add_argument("--cache", default="./lw_cache.json", help="详情缓存文件")
    ap.add_argument(
        "--cache-max-age-days",
        type=int,
        default=14,
        help="缓存条目保留天数(同时决定 feed 里能看到多久以前的文章)",
    )
    ap.add_argument(
        "--entry-types",
        default=",".join(str(t) for t in sorted(DEFAULT_ENTRY_TYPES)),
        help="放行的 entry_type，逗号分隔。7=文章",
    )
    ap.add_argument(
        "--no-auto-groups",
        dest="auto_groups",
        action="store_false",
        help="不从接口同步分组，改用代码里的 FALLBACK_GROUPS",
    )
    ap.add_argument(
        "--no-ungrouped",
        dest="include_ungrouped",
        action="store_false",
        help=f"不为未分组的公众号生成 {UNGROUPED_NAME}.atom",
    )
    ap.add_argument(
        "--no-prune",
        dest="prune_feeds",
        action="store_false",
        help="保留分组改名/删除后遗留的旧 .atom 文件",
    )
    ap.add_argument(
        "--notify-days",
        type=int,
        default=1,
        help="令牌剩余天数低于此值时通过 Server酱 推送微信提醒",
    )
    ap.add_argument("--list-channels", action="store_true", help="打印所有 channel_id")
    args = ap.parse_args()

    load_env_file()
    run_from_args(args)


if __name__ == "__main__":
    main()
