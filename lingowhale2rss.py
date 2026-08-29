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
本版针对"部分文章漏抓"做了四处结构性修改:

1. 逐个 channel 拉取，而不是把整组 channel_ids 混在一起拉前 N 条。
   旧写法下 yiyao 组 32 个号共享 30 条配额，日更号会把低频号挤出窗口。
2. Atom 从缓存出，而不是只写"本次抓到的"。缓存保留 14 天，因此单次抓取
   失败、或两次运行之间的时间窗口过长，都不会再造成文章永久丢失。
3. 详情抓取失败的条目不再写进 feed（旧版会输出一条没有 <link> 的条目，
   Miniflux 落库后即使下次成功也未必回填链接），改为跳过、下次重试。
4. entry_type 过滤可配置，并统计被过滤掉的类型，方便确认多图文次条、
   专题聚合等是不是被误杀。
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
# 把需要的类型号加进来即可（用 --entry-types 7,8 覆盖，无需改代码）。
DEFAULT_ENTRY_TYPES = {7}

# ---------------------------------------------------------------- 分组配置
# key = 输出的 feed 文件名(建议英文), value = channel_id 列表
# 用 --list-channels 可重新打印当前账号下的所有 channel_id
GROUPS = {
    "yiyao": [
        "682b68f7da6f685c6aed8e4f",  # 21新健康
        "6969972d784e4c64543c7510",  # Briinsight
        "67cc08b69a4297b6148b4b21",  # 医药笔记
        "67f72ad8cae6ac82234c91c7",  # CMAC发布
        "67cc08ae9a4297b6148b3da7",  # 药明康德
        "67cc08ae9a4297b6148b3eb8",  # E药经理人
        "67f61fd5291555e5370a23e5",  # 医药投资部落
        "6891f758da9a363d9d73ac31",  # 金玉良研
        "683d96ec7f29ff293410775c",  # 中国食品药品监管杂志
        "67ce97d19133eb94ea6e113a",  # 同写意
        "689fdf2050a99184e313384f",  # 新浪医药
        "6879f8342731550460eba1d1",  # 乐城先行区管理局发布
        "67cc08b69a4297b6148b4b20",  # 丁香园 Insight 数据库
        "69952d0409487f1311b19250",  # PV行者
        "6822f1230fad9d73f5cfc17e",  # BiG生物创新社
        "682c8918f38a13a2ad655977",  # 佰傲谷BioValley
        "67cc08ae9a4297b6148b3e8a",  # 医药健闻
        "67cc08ac9a4297b6148b3a4f",  # 深蓝观
        "6822ddc92a4055faa7032e61",  # 医药观澜
        "67cc08ae9a4297b6148b3e05",  # 医药魔方
        "67cc08a49a4297b6148b2df7",  # VIP说
        "6a50814c6818a65c9ce6cd2f",  # DIA资讯
        "6a5084e813e73afc68e07646",  # 制药台
        "6a508005393401f4b8118e6f",  # 奥来恩医药
        "6a5086b36818a65c9ce6dcef",  # 沪上临研人
        "6a3a1f95cae9209922735445",  # NOVOTECH诺为泰
        "6a40c6beed5e6ba8031e3056",  # 药政沙龙
        "6a393cf6cae920992270ea52",  # 有临医药
        "6a50829b393401f4b81195f9",  # Ainusen医药
        "6a5081146818a65c9ce6ccc4",  # 东药西毒
        "6a5080fb6818a65c9ce6cc7b",  # DTRIAL PHARMA
        "6a5085a0393401f4b8119eca",  # PV人儿
    ],
    "cro": [
        "69057be581154864e97fff1e",  # 泰格医药
        "67ce97d19133eb94ea6e113e",  # IQVIA艾昆纬
        "6a50824713e73afc68e06e9e",  # Caidya康缔亚
        "6a508269393401f4b8119563",  # 北京海金格医药
        "6a5085b26818a65c9ce6db1e",  # 普瑞盛GCPClinPlus
        "6a50872f393401f4b811a1f5",  # ICON爱恩康医学
        "6a584f912093ad6c4d121c3e",  # Parexel
        "6a5085f2393401f4b8119f3d",  # 昆拓医药研发
        "6a5080e16818a65c9ce6cc55",  # 富启睿Fortrea
    ],
    "zhuce": [
        "6814e26c9c69993601c767ce",  # 注册圈
        "6814e28a288f7f1429cfe82f",  # iReg
        "6a50852813e73afc68e076bd",  # 杨晴的注册研习社
        "6a50858b393401f4b8119ea8",  # RA-Li
        "6a5084b06818a65c9ce6d90e",  # 注册法规杂谈123
    ],
    "za": [
        "67cc08b49a4297b6148b47fc",  # 不坑老师
        "689c0fd9124d4c92decbe3f0",  # 知彼而知己
        "67cc08a19a4297b6148b299c",  # 腾讯研究院
        "6a5085d513e73afc68e078ca",  # 码海听潮
        "6a508282393401f4b81195d1",  # APP喵
    ],
    "qita": [
        "67cc08ae9a4297b6148b3e32",  # 研发客
        "67cc08b09a4297b6148b40c3",  # 生物技术小编
        "68a5f0e0a8e586f9f6ea1724",  # 药闻资讯
        "67cc08b39a4297b6148b4684",  # 药事纵横
        "6822f092f162e4a82732479f",  # 药智数据
        "6a5d66ff1466736b89488b6a",  # 药政work幻想家Annie.zhong
        "6a5d668bac9875af5a9be87d",  # Susan法研社
        "6a50853e13e73afc68e076e6",  # 文森特谈临研
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


def load_notify_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}


def save_notify_state(path, state):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)


# ---------------------------------------------------------------- 令牌检查


def check_token_expiry(notify_state_path=None, notify_days=1):
    """
    检查 LW_ACCESS_TOKEN / LW_AUTH_TOKEN 的剩余有效期。

    剩余天数跌破 notify_days 时推一条微信消息；用 notify_state_path 记录
    "这个 exp 值是否已经通知过"，避免一天跑 3 次调度就收到 3 条重复消息——
    只有换了新令牌(exp 变化)才会重新触发提醒。
    """
    state = load_notify_state(notify_state_path) if notify_state_path else {}
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
        save_notify_state(notify_state_path, state)


# ---------------------------------------------------------------- 拉取


def fetch_channel_feed(headers, channel_id, max_items, limit, delay, cutoff=0):
    """
    按 cursor 翻页拉【单个公众号】的文章列表。

    单号拉取是这版的核心改动：旧版把整组 channel_ids 一起丢给接口再取前 N 条，
    等于让同组所有号抢一个配额，日更号必然把低频号挤掉。

    cutoff: unix 时间戳。翻到比它更老的文章就停——反正缓存也只保留这么久，
    再往前翻纯属浪费请求额度。
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


def load_cache(path):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:  # noqa: BLE001
            pass
    return {}


def save_cache(path, cache):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    os.replace(tmp, path)


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
    data = post(SUBS_EP, headers, {"sort_type": SORT_TYPE})
    for s in data.get("user_subscribes") or []:
        if "subscription_group" in s:
            g = s["subscription_group"]
            print(f'\n# 分组: {g["name"]}')
            ids = [c["channel_id"] for c in (g.get("channels") or [])]
            for c in g.get("channels") or []:
                print(f'    {c["channel_id"]}  {c["name"]}')
            print(f'    -> "{g["name"]}": {json.dumps(ids)},')
        elif "subscription_channel" in s:
            c = s["subscription_channel"]
            print(f'    {c["channel_id"]}  {c["name"]}  (未分组)')


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
):
    entry_types = set(entry_types or DEFAULT_ENTRY_TYPES)
    headers = build_headers()

    notify_state_path = os.path.join(
        os.path.dirname(os.path.abspath(cache_path)), "lw_notify_state.json"
    )
    check_token_expiry(notify_state_path=notify_state_path, notify_days=notify_days)

    os.makedirs(out, exist_ok=True)
    cache = load_cache(cache_path)
    cutoff = time.time() - cache_max_age_days * 86400 if cache_max_age_days > 0 else 0

    # link_only/no_content 模式下不需要正文，把已缓存条目里的 html 直接清空。
    # 这一行是"自愈"：哪怕磁盘上的缓存文件是之前全文模式攒下的大文件，
    # 本次运行加载进内存后立刻瘦身，且下面 save_cache() 会把瘦身结果写回磁盘。
    if link_only or no_content:
        for v in cache.values():
            v["html"] = ""

    new_details = 0
    failed_details = 0
    type_hist = {}

    for name, channel_ids in GROUPS.items():
        print(f"[{name}] 拉取中… ({len(channel_ids)} 个号)", file=sys.stderr)
        group_new = 0

        for cid in channel_ids:
            try:
                raw = fetch_channel_feed(
                    headers, cid, per_channel, limit, delay, cutoff=cutoff
                )
            except Exception as e:  # noqa: BLE001
                # 单个号失败不该拖垮整组：本轮跳过，缓存里的旧条目照常出 feed，
                # 下一轮再补。
                print(f"  [!] 列表失败 {cid}: {e}", file=sys.stderr)
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
                    "title": it.get("title"),
                    "pub_time": it.get("pub_time"),
                    "channel": (it.get("channel") or {}).get("name", ""),
                    "abstract": (it.get("abstract") or "")
                    .replace("<hl>", "")
                    .replace("</hl>", ""),
                    "group": name,
                }

                row = cache.get(eid)
                if row is not None:
                    # 老条目：补齐列表侧字段（旧版缓存只存了正文相关的几项，
                    # 没有 title/group 就没法从缓存重建 feed）
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
                        group_new += 1
                        time.sleep(delay)
                    except Exception as e:  # noqa: BLE001
                        # 关键改动：拿不到 orig_url 就先不写进 feed。
                        # 旧版会输出一条没有 <link> 的条目，Miniflux 按 GUID 落库后
                        # 即使下轮补到链接也未必回填，肉眼看就是"这篇丢了"。
                        # 这里仍然写缓存（保留 title/pub_time），orig_url 为空，
                        # 输出阶段会跳过它，下一轮自动重试详情。
                        failed_details += 1
                        print(f"  [!] 详情失败 {eid}: {e}", file=sys.stderr)

                cache[eid] = row

            if kept:
                print(f"  {cid}: +{kept}", file=sys.stderr)
            time.sleep(delay)

            del raw

        print(f"[{name}] 本轮新增详情 {group_new} 条", file=sys.stderr)
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

    save_cache(cache_path, cache)

    # ------------------------------------------------------------ 输出 feed
    # 从缓存出，而不是只写"本次抓到的"。这样一次抓取失败、或者两次运行之间
    # 隔了一整夜，都不会让中间的文章永久消失。
    for name in GROUPS:
        entries = [
            v
            for v in cache.values()
            if v.get("group") == name and (v.get("orig_url") or no_content)
        ]
        entries.sort(key=lambda e: e.get("pub_time") or 0, reverse=True)
        entries = entries[:feed_max]

        self_url = f"{base_url.rstrip('/')}/{name}.atom" if base_url else ""
        xml = build_atom(f"语鲸 - {name}", self_url, entries)
        path = os.path.join(out, f"{name}.atom")
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml)
        print(f"[{name}] {len(entries)} 篇 -> {path}", file=sys.stderr)

        del entries, xml
        gc.collect()

    pending = sum(
        1 for v in cache.values() if not v.get("orig_url") and not no_content
    )
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
    )


# ---------------------------------------------------------------- 命令行入口


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./feeds", help="输出目录")
    ap.add_argument("--base-url", default="", help="feed 的公开访问前缀")
    ap.add_argument(
        "--per-channel",
        type=int,
        default=10,
        help="每个公众号每轮最多拉多少条(取代旧的 --max-items)",
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
