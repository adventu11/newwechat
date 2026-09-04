#!/usr/bin/env python3
"""
report.py —— 在 lingowhale2rss 抓到的文章上加一层 AI 筛选/摘要，产出每日简报。

面向的读者角色是【临床试验项目经理】，判断标准写在 ROLE_PROMPT 里，
想调口味直接改那段文字，或者用 REPORT_ROLE_FILE 指向一个外部 txt 覆盖它。

两阶段设计（省钱 + 抗长文本）:
  阶段一 粗筛: 只把 标题 + 公众号 + 摘要 丢给模型，一次判 40 条，
              输出 keep / category / priority。绝大多数无关内容在这里被砍掉。
  阶段二 精读: 只对通过粗筛的文章取全文，每 4 篇一批做要点提炼。
              全文按 REPORT_MAX_CHARS 截断，避免长文顶爆上下文。
最后由 Python 按分类拼装报告——排版是确定性的，不让模型自由发挥版式。

产出四份:
  data/reports/YYYY-MM-DD.md         归档
  data/reports/YYYY-MM-DD-audit.md   粗筛审计: 135 篇候选逐条 保留/过滤 + 理由
  data/feeds/<REPORT_SLUG>.atom      Miniflux 订阅(保留最近 REPORT_KEEP 期)
  Server酱微信推送                    配了 SERVERCHAN_SENDKEY 才推

依赖: 只用标准库 + 一个 OpenAI 兼容的 chat/completions 接口。
      DeepSeek / 智谱 / 通义 / Kimi / OpenAI 都能直接用，改 LLM_BASE_URL 即可。

单独跑一次:
    LLM_API_KEY=sk-xxx python report.py --once --dry-run
"""

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from xml.sax.saxutils import escape

import lingowhale2rss as lw2r

# ---------------------------------------------------------------- 模型配置

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.deepseek.com/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "deepseek-chat")
LLM_API_KEY_ENV = "LLM_API_KEY"

# 分类顺序 = 报告里的章节顺序
CATEGORIES = ["法规发布", "会议通知", "直播通知", "行业资讯", "专业知识"]

ROLE_PROMPT = """你在为一位【临床试验项目经理（CTPM）】做信息筛选。她的日常工作是：
推进临床试验项目落地、管理 CRO/中心/供应商、把控进度质量成本、应对稽查核查、
跟踪法规与指导原则变化。

【保留】符合以下任一条的内容：
- 法规发布：NMPA/CDE/CFDI/ICH/GCP/伦理相关的法规、指导原则、征求意见稿、问答、政策解读
- 会议通知：行业会议、培训班、研讨会的通知（含时间、地点、报名信息）
- 直播通知：线上直播、公开课、webinar 的预告
- 行业资讯：影响临床试验执行的行业动态，如核查通报、CRO/SMO 行业变化、
  临床试验数据要求变化、多中心协作与备案流程变化
- 专业知识：临床试验运营与项目管理实务，如方案设计、入组与受试者招募、
  监查稽查、数据管理与 EDC、统计与 eCTD 递交、风险管理、供应商与预算管理

【过滤】以下内容一律不要：
- 纯资本市场内容：股价、融资、并购、IPO、财报、市值分析
- 早期研发科普：靶点机制、分子发现、临床前研究，与试验执行无关
- 产品营销软文、招聘广告、公司宣传稿
- 与临床试验执行无关的疾病科普、健康养生
- 标题党式的新闻聚合、日报周报式的资讯罗列
- 一句话快讯、没有实质信息量的短消息"""


def load_role_prompt():
    """允许用外部文件覆盖角色设定，改口味不用动代码、不用重新构建镜像。"""
    path = os.environ.get("REPORT_ROLE_FILE", "")
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            text = f.read().strip()
        if text:
            print(f"[报告] 使用外部角色设定 {path}", file=sys.stderr)
            return text
    return ROLE_PROMPT


# ---------------------------------------------------------------- LLM 调用


def llm_chat(messages, temperature=0.2, max_tokens=3000, retries=3, timeout=120):
    key = os.environ.get(LLM_API_KEY_ENV, "")
    if not key:
        raise RuntimeError(f"未设置 {LLM_API_KEY_ENV}")
    url = LLM_BASE_URL.rstrip("/") + "/chat/completions"
    body = json.dumps(
        {
            "model": LLM_MODEL,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
        "Accept-Encoding": "gzip",
    }

    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
            j = json.loads(raw.decode("utf-8"))
            return j["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "ignore")[:300]
            except Exception:  # noqa: BLE001
                pass
            last = f"HTTP {e.code} {detail}"
            if e.code in (401, 403):  # 密钥错了，重试多少次都一样
                break
        except Exception as e:  # noqa: BLE001
            last = repr(e)
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"LLM 调用失败: {last}")


def parse_json_loose(text):
    """模型偶尔会裹 ```json 或者前后带一句废话，这里尽量抠出 JSON 数组/对象。"""
    t = (text or "").strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.S)
    try:
        return json.loads(t)
    except Exception:  # noqa: BLE001
        pass
    for opener, closer in (("[", "]"), ("{", "}")):
        i, j = t.find(opener), t.rfind(closer)
        if i != -1 and j > i:
            try:
                return json.loads(t[i : j + 1])
            except Exception:  # noqa: BLE001
                continue
    raise ValueError(f"无法解析模型返回的 JSON: {t[:200]}")


# ---------------------------------------------------------------- 正文处理


class _Text(HTMLParser):
    SKIP = {"script", "style", "noscript"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.buf, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP:
            self.skip += 1
        elif tag in ("p", "br", "div", "li", "h1", "h2", "h3", "tr"):
            self.buf.append("\n")

    def handle_endtag(self, tag):
        if tag in self.SKIP and self.skip:
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip:
            self.buf.append(data)


def html_to_text(html, limit=3000):
    if not html:
        return ""
    p = _Text()
    try:
        p.feed(html)
    except Exception:  # noqa: BLE001
        pass
    text = "".join(p.buf)
    text = re.sub(r"[ \t\r\u00a0]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text).strip()
    return text[:limit]


def get_article_text(headers, item, limit):
    """
    优先用缓存里的正文；link_only 模式下缓存不存正文，就临时回源取一次。

    只对通过粗筛的十几篇取全文，所以即使回源也不会明显拉长运行时间，
    更不会像"全量存正文"那样把 lw_cache.json 撑大。
    """
    text = html_to_text(item.get("html") or "", limit)
    if text:
        return text
    if headers is None:
        return item.get("abstract") or ""
    try:
        res = lw2r.fetch_detail(headers, item["entry_id"], 7)
        return html_to_text(res.get("html") or "", limit) or (item.get("abstract") or "")
    except Exception as e:  # noqa: BLE001
        print(f"  [!] 取正文失败 {item['entry_id']}: {e}", file=sys.stderr)
        return item.get("abstract") or ""


# ---------------------------------------------------------------- 阶段一 粗筛


def screen(items, role_prompt, batch_size=40, audit=None, max_tokens=6000):
    """
    输入候选文章，返回 {entry_id: {category, priority, reason}}，未通过的不出现。

    audit: 传入一个 list，会原样记下每一条的完整判定(含 keep=false 和理由)，
    用于事后回看"AI 到底看没看到这篇、为什么筛掉"。不传就不记录，行为不变。

    max_tokens 是每批判定的输出预算。有些模型(尤其带"思考"过程的)会先输出
    大段推理文字再给 JSON，batch_size 越大、需要的预算越高——预算不够时输出
    在 JSON 写完前被截断，导致整批解析失败。批太大反而更容易全批失败，
    宁可把 batch_size 调小一点，也别一味加大 max_tokens。
    """
    kept = {}
    sys_msg = (
        role_prompt
        + "\n\n现在给你一批文章的标题和摘要。逐条判断是否值得她读。"
        + "\n只输出 JSON 数组，不要任何解释文字、不要 markdown 代码块。"
        + "\n每个元素: {\"i\": 序号, \"keep\": true/false, "
        + '"category": "' + "|".join(CATEGORIES) + '", '
        + '"priority": 1-3, "reason": "12字以内理由"}'
        + "\npriority: 1=必须今天看 2=值得看 3=有空再看。"
        + "\n宁缺毋滥：拿不准的、信息量不足的，一律 keep=false，并在 reason 里说明原因。"
    )

    for start in range(0, len(items), batch_size):
        chunk = items[start : start + batch_size]
        lines = []
        for i, it in enumerate(chunk):
            abstract = (it.get("abstract") or "")[:120]
            lines.append(
                f"{i}. 【{it.get('channel') or '未知'}】{it.get('title') or ''}"
                + (f" —— {abstract}" if abstract else "")
            )
        try:
            out = llm_chat(
                [
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": "\n".join(lines)},
                ],
                max_tokens=max_tokens,
            )
            arr = parse_json_loose(out)
        except Exception as e:  # noqa: BLE001
            # 粗筛失败就跳过这一批，而不是把整批当成通过——
            # 宁可这批今天不进报告，也不要放一堆无关内容进去。
            print(f"  [!] 粗筛失败(第 {start // batch_size + 1} 批): {e}", file=sys.stderr)
            if audit is not None:
                for it in chunk:
                    audit.append(
                        {
                            "entry_id": it["entry_id"],
                            "title": it.get("title") or "",
                            "channel": it.get("channel") or "",
                            "keep": None,  # None = 这批模型调用失败，没判到
                            "category": "",
                            "reason": "本批粗筛调用失败",
                        }
                    )
            continue

        by_idx = {}
        for r in arr if isinstance(arr, list) else []:
            try:
                idx = int(r.get("i"))
            except (TypeError, ValueError):
                continue
            if 0 <= idx < len(chunk):
                by_idx[idx] = r

        for i, it in enumerate(chunk):
            r = by_idx.get(i)
            if r is None:
                if audit is not None:
                    audit.append(
                        {
                            "entry_id": it["entry_id"],
                            "title": it.get("title") or "",
                            "channel": it.get("channel") or "",
                            "keep": None,  # 模型这批返回里漏判了这一条
                            "category": "",
                            "reason": "模型未返回判定",
                        }
                    )
                continue

            cat = r.get("category")
            if cat not in CATEGORIES:
                cat = "行业资讯"
            reason = (r.get("reason") or "")[:40]
            keep = bool(r.get("keep"))

            if audit is not None:
                audit.append(
                    {
                        "entry_id": it["entry_id"],
                        "title": it.get("title") or "",
                        "channel": it.get("channel") or "",
                        "keep": keep,
                        "category": cat,
                        "reason": reason,
                    }
                )
            if not keep:
                continue

            try:
                pri = max(1, min(3, int(r.get("priority", 2))))
            except (TypeError, ValueError):
                pri = 2
            kept[it["entry_id"]] = {"category": cat, "priority": pri, "reason": reason}

        print(
            f"  粗筛 {start + 1}-{start + len(chunk)}: 通过 "
            f"{sum(1 for it in chunk if it['entry_id'] in kept)} 条",
            file=sys.stderr,
        )
        time.sleep(1)
    return kept


def render_audit(date_str, audit, cands_total):
    """把粗筛全过程渲染成人可读的 markdown：每一篇 保留/过滤 + 理由。"""
    kept_n = sum(1 for a in audit if a["keep"] is True)
    dropped_n = sum(1 for a in audit if a["keep"] is False)
    unjudged_n = sum(1 for a in audit if a["keep"] is None)

    out = [
        f"# 粗筛审计 {date_str}",
        "",
        f"候选 {cands_total} 篇，模型判定 {len(audit)} 篇 "
        f"（保留 {kept_n} / 过滤 {dropped_n} / 未判定 {unjudged_n}）。",
        "",
        "## 保留",
        "",
    ]
    for a in audit:
        if a["keep"] is True:
            out.append(f"- ✅ 【{a['channel']}】{a['title']} — {a['category']} — {a['reason']}")
    out.append("")
    out.append("## 过滤")
    out.append("")
    for a in audit:
        if a["keep"] is False:
            out.append(f"- ❌ 【{a['channel']}】{a['title']} — {a['reason']}")
    if unjudged_n:
        out.append("")
        out.append("## 未判定(模型批次调用失败或漏判，未计入报告，也未计入过滤)")
        out.append("")
        for a in audit:
            if a["keep"] is None:
                out.append(f"- ⚠️ 【{a['channel']}】{a['title']} — {a['reason']}")
    return "\n".join(out)


# ---------------------------------------------------------------- 阶段二 精读


def digest(items, role_prompt, batch_size=4, max_tokens=5000):
    """对通过粗筛的文章做要点提炼，返回 {entry_id: {summary, points}}。

    max_tokens 同 screen()：批越大、原文越长，需要的输出预算越高，
    预算不够会导致整批解析失败——失败不丢文章(自动降级成用摘要)，
    但会丢失这批的要点提炼，报告质量打折。批量失败率明显偏高时，
    优先调小 batch_size，其次再考虑加大这个预算。
    """
    out = {}
    sys_msg = (
        role_prompt
        + "\n\n现在逐篇提炼要点，读者是上述项目经理，写给她看，别写成新闻稿。"
        + "\n只输出 JSON 数组，不要解释、不要 markdown 代码块。"
        + '\n每个元素: {"id": "文章id", "summary": "一句话结论，40字以内", '
        + '"points": ["要点1", "要点2"]}'
        + "\npoints 2-4 条，每条 30 字以内，只保留对她有行动价值的信息："
        + "生效时间、适用范围、报名截止、时间地点、具体做法、与旧规的差异。"
        + "\n会议/直播类必须写明时间和报名方式（原文没写就写“原文未提”）。"
        + "\n不要复述标题，不要写“本文介绍了”这类空话。"
    )

    for start in range(0, len(items), batch_size):
        chunk = items[start : start + batch_size]
        blocks = []
        for it in chunk:
            blocks.append(
                f"### id: {it['entry_id']}\n"
                f"标题: {it.get('title') or ''}\n"
                f"来源: {it.get('channel') or ''}\n"
                f"正文:\n{it.get('_text') or it.get('abstract') or ''}"
            )
        try:
            res = llm_chat(
                [
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": "\n\n".join(blocks)},
                ],
                max_tokens=max_tokens,
            )
            arr = parse_json_loose(res)
        except Exception as e:  # noqa: BLE001
            print(f"  [!] 精读失败(第 {start // batch_size + 1} 批): {e}", file=sys.stderr)
            arr = []

        got = {r.get("id"): r for r in arr if isinstance(r, dict)}
        for it in chunk:
            r = got.get(it["entry_id"])
            if r:
                pts = r.get("points")
                out[it["entry_id"]] = {
                    "summary": (r.get("summary") or "").strip(),
                    "points": [str(p).strip() for p in pts][:4]
                    if isinstance(pts, list)
                    else [],
                }
            else:
                # 精读失败的不丢弃：降级成摘要，至少标题和链接还在报告里
                out[it["entry_id"]] = {
                    "summary": (it.get("abstract") or "")[:60],
                    "points": [],
                }
        print(
            f"  精读 {start + 1}-{start + len(chunk)} 完成", file=sys.stderr
        )
        time.sleep(1)
    return out


# ---------------------------------------------------------------- 报告渲染


def build_sections(items):
    """按分类分组、组内按优先级和时间排序。排版由代码定，不交给模型。"""
    sections = []
    for cat in CATEGORIES:
        rows = [it for it in items if it["_cat"] == cat]
        rows.sort(key=lambda x: (x["_pri"], -(x.get("pub_time") or 0)))
        if rows:
            sections.append((cat, rows))
    return sections


def render_markdown(date_str, sections, stats):
    out = [f"# 临床试验日报 {date_str}", ""]
    if not sections:
        out.append("今日没有筛出相关内容。")
    for cat, rows in sections:
        out.append(f"## {cat}（{len(rows)}）")
        out.append("")
        for it in rows:
            star = "🔴 " if it["_pri"] == 1 else ""
            out.append(f"**{star}[{it.get('title') or '(无标题)'}]({it.get('orig_url')})**")
            out.append(f"<sub>{it.get('channel') or ''}</sub>")
            if it["_summary"]:
                out.append("")
                out.append(it["_summary"])
            for p in it["_points"]:
                out.append(f"- {p}")
            out.append("")
    out.append("---")
    out.append(
        f"<sub>候选 {stats['total']} 篇 → 入选 {stats['kept']} 篇，"
        f"由 {LLM_MODEL} 生成，仅供快速浏览，细节以原文为准。</sub>"
    )
    return "\n".join(out)


def render_html(date_str, sections, stats):
    out = [f"<h2>临床试验日报 {date_str}</h2>"]
    if not sections:
        out.append("<p>今日没有筛出相关内容。</p>")
    for cat, rows in sections:
        out.append(f"<h3>{escape(cat)}（{len(rows)}）</h3><ul>")
        for it in rows:
            star = "🔴 " if it["_pri"] == 1 else ""
            out.append(
                f'<li><a href="{escape(it.get("orig_url") or "")}">'
                f'{star}{escape(it.get("title") or "(无标题)")}</a>'
                f' <small>{escape(it.get("channel") or "")}</small>'
            )
            if it["_summary"]:
                out.append(f"<p>{escape(it['_summary'])}</p>")
            if it["_points"]:
                out.append("<ul>")
                out.extend(f"<li>{escape(p)}</li>" for p in it["_points"])
                out.append("</ul>")
            out.append("</li>")
        out.append("</ul>")
    out.append(
        f"<hr><small>候选 {stats['total']} 篇 → 入选 {stats['kept']} 篇，"
        f"由 {escape(LLM_MODEL)} 生成，细节以原文为准。</small>"
    )
    return "".join(out)


# ---------------------------------------------------------------- 主流程


def generate(
    cache_path="./data/lw_cache.json",
    out_dir="./data/feeds",
    reports_dir="./data/reports",
    base_url="",
    window_hours=26,
    max_articles=25,
    max_chars=3000,
    screen_batch=20,
    screen_max_tokens=6000,
    digest_batch=3,
    digest_max_tokens=5000,
    report_slug="daily-report",
    report_keep=30,
    push=True,
    dry_run=False,
    tz_offset=8,
):
    tz = timezone(timedelta(hours=tz_offset))
    now = datetime.now(tz)
    date_str = now.strftime("%Y-%m-%d")
    role_prompt = load_role_prompt()

    data_dir = os.path.dirname(os.path.abspath(cache_path))
    state_path = os.path.join(data_dir, "lw_report_state.json")
    state = lw2r.load_json(state_path, {"reported": {}})
    reported = state.get("reported", {})

    cache = lw2r.load_json(cache_path)
    if not cache:
        print("[报告] 缓存为空，跳过", file=sys.stderr)
        return None

    # 候选 = 窗口内 + 有链接 + 没在之前的报告里出现过
    # 窗口默认 26 小时(比 24 多 2 小时冗余)，重复由 reported 挡住，
    # 所以哪怕某天调度没跑成，第二天也只会补进真正没报过的文章。
    since = time.time() - window_hours * 3600
    cands = [
        v
        for k, v in cache.items()
        if v.get("orig_url")
        and (v.get("pub_time") or 0) >= since
        and k not in reported
    ]
    cands.sort(key=lambda x: -(x.get("pub_time") or 0))
    print(f"[报告] 候选 {len(cands)} 篇", file=sys.stderr)
    if not cands:
        return None

    picked = {}
    audit = []
    picked = screen(
        cands, role_prompt, batch_size=screen_batch, audit=audit, max_tokens=screen_max_tokens
    )
    kept = [it for it in cands if it["entry_id"] in picked]
    kept.sort(key=lambda x: (picked[x["entry_id"]]["priority"], -(x.get("pub_time") or 0)))
    if len(kept) > max_articles:
        print(f"[报告] 入选 {len(kept)} 篇，按优先级截到 {max_articles} 篇", file=sys.stderr)
        kept = kept[:max_articles]
    print(f"[报告] 粗筛通过 {len(kept)} 篇，开始精读", file=sys.stderr)

    headers = None
    if any(not it.get("html") for it in kept):
        try:
            headers = lw2r.build_headers()  # link_only 模式下要回源取正文
        except SystemExit:
            print("[报告] 无语鲸令牌，退化为用摘要生成", file=sys.stderr)

    for it in kept:
        it["_text"] = get_article_text(headers, it, max_chars)

    digests = digest(kept, role_prompt, batch_size=digest_batch, max_tokens=digest_max_tokens)

    for it in kept:
        meta = picked[it["entry_id"]]
        d = digests.get(it["entry_id"], {})
        it["_cat"] = meta["category"]
        it["_pri"] = meta["priority"]
        it["_summary"] = d.get("summary", "")
        it["_points"] = d.get("points", [])

    sections = build_sections(kept)
    stats = {"total": len(cands), "kept": len(kept)}
    md = render_markdown(date_str, sections, stats)
    html = render_html(date_str, sections, stats)

    if dry_run:
        print(md)
        return md

    # ---- 归档 markdown + 粗筛审计(每篇 保留/过滤 + 理由，回答"AI 到底看了哪些")
    os.makedirs(reports_dir, exist_ok=True)
    with open(os.path.join(reports_dir, f"{date_str}.md"), "w", encoding="utf-8") as f:
        f.write(md)
    audit_path = os.path.join(reports_dir, f"{date_str}-audit.md")
    with open(audit_path, "w", encoding="utf-8") as f:
        f.write(render_audit(date_str, audit, len(cands)))
    print(f"[报告] 粗筛审计已写入 {audit_path}", file=sys.stderr)

    # ---- 写成 Atom，Miniflux 里当成一个"日报"订阅源
    hist_path = os.path.join(data_dir, "lw_report_history.json")
    hist = lw2r.load_json(hist_path, [])
    if not isinstance(hist, list):
        hist = []
    hist = [h for h in hist if h.get("date") != date_str]
    hist.insert(
        0,
        {
            "date": date_str,
            "entry_id": f"report-{date_str}",
            "title": f"临床试验日报 {date_str}（{len(kept)} 条）",
            "pub_time": time.time(),
            "html": html,
            "channel": "AI 日报",
            "author": "lingowhale2rss",
            "orig_url": f"{base_url.rstrip('/')}/reports/{date_str}.md"
            if base_url
            else "",
        },
    )
    hist = hist[:report_keep]
    lw2r.save_json(hist_path, hist)

    os.makedirs(out_dir, exist_ok=True)
    self_url = f"{base_url.rstrip('/')}/{report_slug}.atom" if base_url else ""
    xml = lw2r.build_atom("临床试验日报", self_url, hist)
    with open(os.path.join(out_dir, f"{report_slug}.atom"), "w", encoding="utf-8") as f:
        f.write(xml)

    # ---- 记账：只记"模型真正判过的"（keep 是 True 或 False）
    # 粗筛某一批调用失败时，那批文章在 audit 里是 keep=None(未判定)，
    # 绝不能记进 reported——不然它们再也不会被拿去判一次，等于永久漏读。
    # 等下一轮 window_hours 窗口还覆盖得到，它们会自动重新进入候选。
    judged_ids = {a["entry_id"] for a in audit if a["keep"] is not None}
    unjudged = len(cands) - len(judged_ids)
    if unjudged:
        print(f"[报告] {unjudged} 篇因粗筛调用失败未被判定，将在下一轮重试", file=sys.stderr)
    ts = int(time.time())
    for it in cands:
        if it["entry_id"] in judged_ids:
            reported[it["entry_id"]] = ts
    cutoff = ts - 30 * 86400
    state["reported"] = {k: v for k, v in reported.items() if v >= cutoff}
    lw2r.save_json(state_path, state)

    if push:
        body = md if len(md) < 30000 else md[:30000] + "\n\n…（内容过长已截断）"
        lw2r.send_wechat_notify(f"临床试验日报 {date_str}（{len(kept)} 条）", body)

    print(f"[报告] 完成: {len(kept)} 条 -> {report_slug}.atom", file=sys.stderr)
    return md


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="./data/lw_cache.json")
    ap.add_argument("--out", default="./data/feeds")
    ap.add_argument("--reports-dir", default="./data/reports")
    ap.add_argument("--base-url", default="")
    ap.add_argument("--window-hours", type=int, default=26)
    ap.add_argument("--max-articles", type=int, default=25)
    ap.add_argument("--max-chars", type=int, default=3000, help="每篇正文喂给模型的上限")
    ap.add_argument(
        "--screen-batch",
        type=int,
        default=20,
        help="粗筛每批文章数。批越大越容易被模型输出长度截断导致整批解析失败，"
        "遇到'无法解析模型返回的JSON'报错就调小这个值",
    )
    ap.add_argument(
        "--screen-max-tokens",
        type=int,
        default=6000,
        help="粗筛每批的输出预算(tokens)。带推理过程的模型需要更大预算",
    )
    ap.add_argument(
        "--digest-batch",
        type=int,
        default=3,
        help="精读每批文章数。同样批越大越容易被截断解析失败",
    )
    ap.add_argument(
        "--digest-max-tokens", type=int, default=5000, help="精读每批的输出预算(tokens)"
    )
    ap.add_argument("--report-slug", default="daily-report")
    ap.add_argument("--no-push", dest="push", action="store_false", help="不推微信")
    ap.add_argument("--dry-run", action="store_true", help="只打印，不落盘不推送")
    ap.add_argument("--once", action="store_true", help="兼容参数，无实际作用")
    args = ap.parse_args()

    lw2r.load_env_file()
    generate(
        cache_path=args.cache,
        out_dir=args.out,
        reports_dir=args.reports_dir,
        base_url=args.base_url,
        window_hours=args.window_hours,
        max_articles=args.max_articles,
        max_chars=args.max_chars,
        screen_batch=args.screen_batch,
        screen_max_tokens=args.screen_max_tokens,
        digest_batch=args.digest_batch,
        digest_max_tokens=args.digest_max_tokens,
        report_slug=args.report_slug,
        push=args.push,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
