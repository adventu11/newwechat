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
import urllib.request
from datetime import datetime, timezone
from xml.sax.saxutils import escape

API = "https://api-public.lingowhale.com/api/lingowhale/v1/"
FEED_EP = API + "feed/subscription"
DETAIL_EP = API + "entry_detail/get"
SUBS_EP = API + "user_subscribe/list"

# 语鲸通用排序参数。注意: 缺失时接口不会报错，而是静默返回默认订阅，务必带上。
SORT_TYPE = 2

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


# ---------------------------------------------------------------- 令牌检查
def check_token_expiry():
    tok = os.environ.get("LW_ACCESS_TOKEN", "")
    parts = tok.split(".")
    if len(parts) != 3:
        return
    try:
        p = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(p))
    except Exception:  # noqa: BLE001
        return
    exp = claims.get("exp")
    if not exp:
        return
    days = (exp - time.time()) / 86400
    if days < 0:
        sys.exit("Access-Token 已过期，请重新登录语鲸后抓取新令牌")
    if days < 3:
        print(f"[!] Access-Token 仅剩 {days:.1f} 天，请尽快更新", file=sys.stderr)
    else:
        print(f"令牌剩余 {days:.1f} 天", file=sys.stderr)


# ---------------------------------------------------------------- 拉取
def fetch_feed(headers, channel_ids, max_items, limit, delay):
    """按 cursor 翻页拉文章列表"""
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
                "channel_ids": channel_ids,
            },
        )
        batch = data.get("feed_list") or []
        if not batch:
            break
        items.extend(batch)
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
                print(f'  {c["channel_id"]}  {c["name"]}')
            print(f'  -> "{g["name"]}": {json.dumps(ids)},')
        elif "subscription_channel" in s:
            c = s["subscription_channel"]
            print(f'  {c["channel_id"]}  {c["name"]}  (未分组)')


# ---------------------------------------------------------------- 核心流程
# 抽成独立函数，方便 supervisor.py 直接 import 调用，避免另起一个 Python
# 进程(subprocess)——两个解释器同时常驻在内存紧张的环境里会直接顶爆限额。
def run(
    out="./feeds",
    base_url="",
    max_items=50,
    limit=10,
    delay=1.0,
    no_content=False,
    link_only=False,
    cache_path="./lw_cache.json",
    cache_max_age_days=14,
):
    headers = build_headers()
    check_token_expiry()

    os.makedirs(out, exist_ok=True)
    cache = load_cache(cache_path)

    # link_only/no_content 模式下不需要正文，把已缓存条目里的 html 直接清空。
    # 这一行是"自愈"：哪怕磁盘上的缓存文件是之前全文模式攒下的大文件，
    # 本次运行加载进内存后立刻瘦身，且下面 save_cache() 会把瘦身结果写回磁盘。
    if link_only or no_content:
        for v in cache.values():
            v["html"] = ""

    new_details = 0

    for name, channel_ids in GROUPS.items():
        print(f"[{name}] 拉取中…", file=sys.stderr)
        raw = fetch_feed(headers, channel_ids, max_items, limit, delay)
        entries = []

        for it in raw:
            if it.get("entry_type") != 7:  # 7 = 文章，其它是专题聚合
                continue
            eid = it["entry_id"]
            row = {
                "entry_id": eid,
                "title": it.get("title"),
                "pub_time": it.get("pub_time"),
                "channel": (it.get("channel") or {}).get("name", ""),
                "abstract": (it.get("abstract") or "")
                .replace("<hl>", "")
                .replace("</hl>", ""),
            }

            if eid in cache:
                row.update(cache[eid])
                # 老条目可能是升级前缓存的，没有 pub_time，借这次机会补上，
                # 否则永远进不了下面的按时间清理逻辑，缓存会一直有增无减
                cache[eid].setdefault("pub_time", it.get("pub_time"))
            elif not no_content:
                try:
                    res = fetch_detail(headers, eid, it["entry_type"])
                    d = {
                        # 语鲸返回的是 http，换成 https 只是去掉一个明显的
                        # "机器访问"信号，不能根治环境异常校验，但没有副作用
                        "orig_url": res.get("orig_url", "").replace(
                            "http://mp.weixin.qq.com", "https://mp.weixin.qq.com", 1
                        ),
                        "author": (res.get("author") or {}).get("name", ""),
                        # link_only 模式下不需要正文，压根不存，
                        # 而不是存了再在输出阶段丢弃——减少的是缓存本身的体积
                        "html": "" if link_only else res.get("html", ""),
                        "pub_time": it.get("pub_time"),
                    }
                    cache[eid] = d
                    row.update(d)
                    new_details += 1
                    time.sleep(delay)
                except Exception as e:  # noqa: BLE001
                    print(f"  详情失败 {eid}: {e}", file=sys.stderr)

            entries.append(row)

        self_url = f"{base_url.rstrip('/')}/{name}.atom" if base_url else ""
        xml = build_atom(f"语鲸 - {name}", self_url, entries)
        path = os.path.join(out, f"{name}.atom")
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml)
        print(f"[{name}] {len(entries)} 篇 -> {path}", file=sys.stderr)

        # 组间显式回收：raw/entries 本轮已写盘，尽快归还内存池，
        # 而不是等垃圾回收器自己判断时机——在紧张的内存限额下这点很重要
        del raw, entries
        gc.collect()

    # 按发布时间清理过期缓存条目，防止 lw_cache.json 无限增长。
    # 这是这次 OOM 的根本原因：缓存从不清理，天数越多、常驻内存越高。
    if cache_max_age_days > 0:
        cutoff = time.time() - cache_max_age_days * 86400
        before = len(cache)
        cache = {
            k: v for k, v in cache.items() if (v.get("pub_time") or cutoff) >= cutoff
        }
        removed = before - len(cache)
        if removed:
            print(f"清理过期缓存 {removed} 条(保留最近 {cache_max_age_days} 天)", file=sys.stderr)

    save_cache(cache_path, cache)
    print(f"完成，新增详情 {new_details} 条，缓存共 {len(cache)} 条", file=sys.stderr)


def run_from_args(args):
    if args.list_channels:
        headers = build_headers()
        check_token_expiry()
        list_channels(headers)
        return
    run(
        out=args.out,
        base_url=args.base_url,
        max_items=args.max_items,
        limit=args.limit,
        delay=args.delay,
        no_content=args.no_content,
        link_only=args.link_only,
        cache_path=args.cache,
        cache_max_age_days=args.cache_max_age_days,
    )


# ---------------------------------------------------------------- 命令行入口
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./feeds", help="输出目录")
    ap.add_argument("--base-url", default="", help="feed 的公开访问前缀")
    ap.add_argument("--max-items", type=int, default=50, help="每个 feed 最多条数")
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
        help="缓存条目保留天数，超过则清理(0=不清理，不建议)",
    )
    ap.add_argument("--list-channels", action="store_true", help="打印所有 channel_id")
    args = ap.parse_args()

    load_env_file()
    run_from_args(args)


if __name__ == "__main__":
    main()
