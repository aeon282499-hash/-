# -*- coding: utf-8 -*-
"""ir_digest.py — 📢開示: 当日＋前営業日の TDnet 開示のうち「数字で語れるもの」を PDF から抜いて data/ir_digest.json に書く
（2026-10-22 本人「これ毎日自動で読んでチンパンに出して」）。

やること:
  1) ir_builder が取った TDnet の行（時刻・コード・社名・表題・PDF）から、表題で種類を決める
       修正（業績予想の修正）／配当（配当予想の修正）／TOB／自社株買い／分割／特損・特益／子会社化・M&A／月次／短信／増資
  2) その PDF の冒頭（最大 MAX_PAGES ページ）を読み、種類ごとの数字を抜く
       修正   : 前回→今回の 営業利益（無ければ経常・純利益）と増減率・売上高。中間と通期の両方があれば通期
       配当   : 前回→今回の年間配当
       TOB    : 買付価格・期間・終値比（プレミアム）
       自社株 : 上限株数・発行済比・金額
       分割   : 1株→N株
       特損益 : 金額
       短信   : 営業利益と前年比・通期予想に対する進捗
  3) 値段（終値・前日比）と、既出なら当日の反応／引け後なら翌日の反応（ir_builder.reaction）を付ける
出力: data/ir_digest.json = {generated_at, days:[{date, n_all, n_kind, n_read, rows:[...]}]}
      row = {id,time,code,name,kind,dir(+1/0/-1),num,sub,title,pdf,intraday,close,r1,react}
画面: web/js/news.js（#/news・🚩初動の切替）。数字の行だけ・バナー無し（feedback_kabuai_numbers_only）。

元データは TDnet 直取り（brisk のデータは規約で転載不可＝載せない）。TDnet の一覧には「PR情報」（受注・採用・サービス停止などの自主発信）
が無いので、それらは出ない（2026-10-22 実測: brisk 272件のうち TDnet にあるのは 110件・無い 162件の大半が PR情報と英文・総会招集）。
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
JST = timezone(timedelta(hours=9))
OUT_PATH = HERE / "data" / "ir_digest.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"}
MAX_PAGES = 4
MAX_CHARS = 9000
DEADLINE_SEC = float(os.environ.get("KABUAI_IR_DIGEST_DEADLINE", "120"))
THREADS = 8
DAYS = 2                      # 当日＋前営業日
SESSION_END = "15:30"

logging.getLogger("pypdf").setLevel(logging.ERROR)

# ── 表題 → 種類 ─────────────────────────────────────────────────────
# 順に当てて最初に当たったもの。(種類, 当てる, 除く)
KIND_RULES: list[tuple[str, str, str]] = [
    ("TOB", r"公開買付|ＴＯＢ|TOB", r"結果|終了|期間の延長|撤回|買集め|準ずる|届出書|応募契約|意見表明の(変更|撤回)"),
    ("修正", r"業績予想.*(修正|変更)|業績(見通し|見込み).*(修正|変更)|上方修正|下方修正|予想との差異|差異に関する|(売上|利益).*予想.*(修正|変更)", r""),
    ("配当", r"配当予想.*(修正|変更)|増配|復配|減配|無配|記念配当|特別配当", r""),
    ("自社株", r"自己株式(の)?取得|自己株式立会外買付|自社株買い|自己株式の取得に係る", r"取得状況|取得終了|処分|消却|譲渡制限|報酬|結果"),
    ("分割", r"株式分割", r"基準日の設定|定款"),
    ("特損益", r"特別損失|特別利益|減損損失|減損の計上|売却益|売却損", r""),
    ("M&A", r"株式(の)?取得|子会社化|吸収合併|吸収分割|事業譲受|事業譲渡|買収|株式交換|経営統合", r"法定(事前|事後)|完了|報酬|取得状況|自己株式"),
    ("月次", r"月次|売上高速報|売上速報|既存店|月間売上", r""),
    ("短信", r"決算短信", r"訂正"),
    ("増資", r"第三者割当|公募|新株予約権|転換社債|ＣＢ|MSCB|売出し|株式の発行|資本業務提携", r"払込完了|行使(状況|完了)|大量転換|報酬|取得|消却|結果|ストック・?オプション|償還|減資|終了|中止"),
]
KIND_LABEL = {"TOB": "TOB", "修正": "業績修正", "配当": "配当", "自社株": "自社株買い", "分割": "分割", "特損益": "特損益",
              "M&A": "買収・提携", "月次": "月次", "短信": "決算", "増資": "増資・CB"}

_WS = re.compile(r"\s+")
_JA_SP = re.compile(r"(?<=[^\x20-\x7e]) | (?=[^\x20-\x7e])")


def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")


def normalize(text: str) -> str:
    """NFKC → 空白1つ → 日本語の間の空白を落とす（数字の表はそのまま）。"""
    t = nfkc(text)
    t = _WS.sub(" ", t).strip()
    return _JA_SP.sub("", t)


def kind_of(title: str) -> str | None:
    t = nfkc(title)
    for k, pat, ex in KIND_RULES:
        if re.search(pat, t) and not (ex and re.search(ex, t)):
            return k
    return None


# ── PDF ─────────────────────────────────────────────────────────────
def pdf_text(url: str, timeout: float = 25.0) -> tuple[str, str]:
    """(本文, エラー)。"""
    if not url:
        return "", "pdf無し"
    try:
        import requests
        from pypdf import PdfReader
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code != 200:
            return "", f"http{r.status_code}"
        if len(r.content) > 12 * 1024 * 1024:
            return "", "12MB超"
        reader = PdfReader(io.BytesIO(r.content))
        parts = []
        for page in reader.pages[:MAX_PAGES]:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        text = normalize("\n".join(parts))
        if len(text) < 20:
            return "", "画像PDF"
        if text[:400].count("/") > 40:          # フォントの対応表が無い PDF（「/0 /1 /0 /2」の羅列）
            return "", "読めないPDF"
        return text[:MAX_CHARS], ""
    except Exception as e:
        return "", f"{type(e).__name__}: {str(e)[:60]}"


# ── 数字の読み取り ──────────────────────────────────────────────────
NUM_TOK = re.compile(r"[△▲-]?\d[\d,]*(?:\.\d+)?(?:\s*[～〜~]\s*[△▲-]?\d[\d,]*(?:\.\d+)?)?|[―－—-](?=[\s―－—\-△▲(（]|$)")
_SKIP_AFTER = re.compile(r"^\s*[年月日株銭期回倍名社件カヶ]")     # 「2026年」「1株」「21銭」は表の数字ではない


def to_num(tok: str) -> float | None:
    """'△500' → -500、'27,000～30,000' → 上限、'―' → None。"""
    if tok is None:
        return None
    tok = tok.strip()
    if tok in ("―", "－", "—", "-", ""):
        return None
    if re.search(r"[～〜~]", tok):
        tok = re.split(r"[～〜~]", tok)[-1].strip()
    neg = tok.startswith(("△", "▲", "-"))
    tok = tok.lstrip("△▲-").replace(",", "")
    try:
        v = float(tok)
    except ValueError:
        return None
    return -v if neg else v


def nums_after(text: str, start: int, n: int, maxspan: int = 220) -> list[float | None]:
    """text[start:] の数字トークンを n 個（単位の語や「円 銭」の手前まで）。"""
    seg = text[start:start + maxspan]
    out: list[float | None] = []
    for m in NUM_TOK.finditer(seg):
        # 「2026年」「1株当たり」「48円21銭」の 21 は表の数字ではない
        if _SKIP_AFTER.match(seg[m.end():m.end() + 2]):
            continue
        out.append(to_num(m.group(0)))
        if len(out) >= n:
            break
    return out


def oku(v: float | None, unit: str = "百万円") -> str:
    if v is None:
        return "―"
    x = v / 100.0 if unit == "百万円" else v / 100000.0 if unit == "千円" else v
    if abs(x) >= 1000:
        return f"{x:,.0f}億"
    if abs(x) >= 10:
        return f"{x:,.0f}億"
    return f"{x:.1f}億"


def pct(v: float | None) -> str:
    if v is None:
        return ""
    return f"{'+' if v > 0 else ''}{v:.1f}%"


COLS = [("売上", r"売上高|売上収益|営業収益|経常収益|総収入|純営業収益"), ("営業", r"営業利益|事業利益|営業損益"),
        ("経常", r"経常利益|税引前利益|税引前(当期|中間|四半期)?利益|経常損益"), ("純", r"純利益|当期利益|中間利益|四半期利益")]


def _columns(head: str) -> list[str]:
    """表の見出し部分から列の並びを推定（売上/営業/経常/純 の出現順・重複は最初だけ）。"""
    found = []
    for lab, pat in COLS:
        m = re.search(pat, head)
        if m:
            found.append((m.start(), lab))
    found.sort()
    return [lab for _, lab in found]


def parse_revision(text: str) -> dict | None:
    """業績予想の修正表。複数あれば最後（通期が後ろに来る）。"""
    t = text
    heads = [m.start() for m in re.finditer(r"前回発表予想|前回予想|当初予想|前回公表予想", t)]
    nows = [m.start() for m in re.finditer(r"今回修正予想|今回予想|修正予想|今回発表予想", t)]
    if not heads or not nows:
        return None
    best = None
    for h in heads:
        n = next((x for x in nows if x > h), None)
        if n is None or n - h > 400:
            continue
        # 見出し（この表の直前 260字）から列の並び
        seg_head = t[max(0, h - 260):h]
        cols = _columns(seg_head)
        if not cols:
            continue
        unit = "千円" if "千円" in seg_head else "百万円"
        a = nums_after(t, h, len(cols))
        b = nums_after(t, n, len(cols))
        if len(a) < len(cols) or len(b) < len(cols):
            continue
        rm = re.search(r"増減率", t[n:n + 500])
        rate = nums_after(t, n + rm.end(), len(cols), 160) if rm else []
        # 期間: 表の直前の「(2026年4月1日~2027年3月31日)」の長さで決める（12ヶ月=通期・6=中間・9=第3四半期・3=第1四半期）。無ければ語で
        per_seg = t[max(0, h - 300):h]
        period = ""
        spans = re.findall(r"(\d{4})年\s*(\d{1,2})月\s*\d{1,2}日\s*[~〜～-]\s*(\d{4})年\s*(\d{1,2})月\s*\d{1,2}日", per_seg)
        if spans:
            y0, m0, y1, m1 = map(int, spans[-1])
            months = (y1 - y0) * 12 + (m1 - m0) + 1
            period = {12: "通期", 6: "中間", 9: "第3四半期", 3: "第1四半期"}.get(months, "")
        if not period:
            period = "通期" if re.search(r"通期", per_seg) else "中間" if re.search(r"中間|第２四半期|第2四半期", per_seg) else "第3四半期" if re.search(r"第３四半期|第3四半期", per_seg) else "第1四半期" if re.search(r"第１四半期|第1四半期", per_seg) else ""
        best = {"cols": cols, "a": a, "b": b, "rate": rate, "unit": unit, "period": period}
    if not best:
        return None
    cols = best["cols"]
    # 主役: 営業 → 経常 → 純 → 売上。営業が据え置きで経常・純だけ変わった時はそちら（1976 明星工業型）
    order = [c for c in ("営業", "経常", "純", "売上") if c in cols]
    main = next((c for c in order if best["a"][cols.index(c)] != best["b"][cols.index(c)]), order[0] if order else None)
    if main is None:
        return None
    i = cols.index(main)
    a, b = best["a"][i], best["b"][i]
    r = best["rate"][i] if i < len(best["rate"]) else None
    if r is None and a not in (None, 0) and b is not None:
        r = round((b / a - 1) * 100, 1) if a > 0 else None
    label = {"営業": "営業利益", "経常": "経常利益", "純": "純利益", "売上": "売上高"}[main]
    parts = [f"{label} {oku(a, best['unit'])}→{oku(b, best['unit'])} {pct(r)}".strip()]
    if main != "売上" and "売上" in cols:
        j = cols.index("売上")
        sa, sb = best["a"][j], best["b"][j]
        sr = best["rate"][j] if j < len(best["rate"]) else (round((sb / sa - 1) * 100, 1) if sa and sb and sa > 0 else None)
        parts.append(f"売上 {oku(sa, best['unit'])}→{oku(sb, best['unit'])} {pct(sr)}".strip())
    # 方向: 主役の増減（赤字→赤字縮小は +）
    d = 0
    if a is not None and b is not None:
        d = 1 if b > a else -1 if b < a else 0
    return {"num": "・".join(parts), "sub": best["period"], "dir": d}


def parse_dividend(text: str) -> dict | None:
    t = text
    m_now = re.search(r"今回修正予想|修正予想|今回予想", t)
    m_prev = re.search(r"前回予想|前回発表予想|前回公表予想", t)
    if not m_now:
        return None

    def annual(start: int) -> float | None:
        seg = t[start:start + 160]
        seg = re.split(r"当期実績|前期実績|前回予想|前回発表|増減|修正の理由", seg[1:])[0]
        vals: list[float] = []
        for m in re.finditer(r"(\d+(?:\.\d+)?)\s*円(?:\s*(\d+)\s*銭)?|(\d+\.\d{2})(?![\d])", seg):
            if m.group(3):
                vals.append(float(m.group(3)))
            else:
                vals.append(float(m.group(1)) + (int(m.group(2)) / 100 if m.group(2) else 0.0))
        return vals[-1] if vals else None
    nv = annual(m_now.start())
    pv = annual(m_prev.start()) if m_prev else None
    if nv is None:
        return None
    d = 0 if pv is None else (1 if nv > pv else -1 if nv < pv else 0)
    return {"num": f"年間配当 {pv:g}円→{nv:g}円" if pv is not None else f"年間配当 {nv:g}円", "sub": pct(round((nv / pv - 1) * 100, 1)) if pv else "", "dir": d}


def parse_tob(text: str, title: str, close: float | None) -> dict | None:
    t = text
    m = re.search(r"(普通株式)?[1１]株につき[、，]?\s*金?\s*([\d,]+)\s*円", t) or re.search(r"買付け?等?の価格[^\d]{0,60}?([\d,]+)\s*円", t)
    if not m:
        return None
    price = to_num(m.group(m.lastindex))
    per = re.search(r"買付け?等?の期間[^\d]{0,12}(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日.{0,24}?(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", t)
    sub = f"{per.group(2)}/{per.group(3)}〜{per.group(5)}/{per.group(6)}" if per else ""
    target = bool(re.search(r"当社株|当社の株", nfkc(title)))
    prem = ""
    if target and close and price:
        prem = f" 終値比 {pct(round((price / close - 1) * 100, 1))}"
    return {"num": f"TOB価格 {price:,.0f}円{prem}", "sub": sub, "dir": 1 if target else 0}


def parse_buyback(text: str) -> dict | None:
    t = text
    sh = re.search(r"取得(し得る|する|対象)株式の(総数|数)[^\d]{0,30}([\d,]+)\s*株", t)
    rt = re.search(r"割合[^\d]{0,12}([\d.]+)\s*[%％]", t) or re.search(r"発行済株式総数[^\d]{0,40}?([\d.]+)\s*[%％]", t)
    amt = re.search(r"取得価額の総額[^\d]{0,30}([\d,]+)\s*(百万円|億円|千円|円)", t)
    if not sh and not amt:
        return None
    parts = []
    if sh:
        parts.append(f"上限 {to_num(sh.group(3)):,.0f}株" + (f"（{rt.group(1)}%）" if rt else ""))
    if amt:
        v, u = to_num(amt.group(1)), amt.group(2)
        parts.append(oku(v, "百万円") if u == "百万円" else f"{v:g}億" if u == "億円" else oku(v, "千円") if u == "千円" else oku(v / 1e6, "百万円"))
    return {"num": "・".join(parts), "sub": "", "dir": 1}


def parse_split(text: str) -> dict | None:
    m = re.search(r"[1１]株につき\s*([\d.]+)\s*株", text)
    if not m:
        return None
    return {"num": f"1株→{to_num(m.group(1)):g}株", "sub": "", "dir": 1}


def parse_special(text: str, title: str) -> dict | None:
    t = text
    m = re.search(r"(特別損失|特別利益|減損損失|売却益|売却損)[^。]{0,120}?([\d,]+)\s*(百万円|億円|千円)", t)
    if not m:
        return None
    lab, v, u = m.group(1), to_num(m.group(2)), m.group(3)
    s = f"{v:g}億" if u == "億円" else oku(v, u)
    neg = lab in ("特別損失", "減損損失", "売却損")
    return {"num": f"{lab} {s}", "sub": "", "dir": -1 if neg else 1}


def parse_tanshin(text: str) -> dict | None:
    """決算短信のサマリー（1ページ目）: 当期の 営業利益と前年比、通期予想に対する進捗。"""
    t = text
    m = re.search(r"経営成績", t)
    if not m:
        return None
    seg = t[m.start():m.start() + 700]
    cols = _columns(seg[:200])
    unit = "千円" if "千円" in seg[:300] else "百万円"
    pm = re.search(r"\d{4}年\s*\d{1,2}月期(?:第[1-4１-４]四半期|中間期)?", seg)
    if not pm:
        return None
    period = nfkc(pm.group(0)).replace(" ", "")
    vals = nums_after(seg, pm.end(), len(cols) * 2, 260)
    if len(vals) < 2:
        return None
    main = next((c for c in ("営業", "経常", "純") if c in cols), None)
    if not main:
        return None
    i = cols.index(main) * 2
    if i + 1 >= len(vals):
        return None
    v, r = vals[i], vals[i + 1]
    label = {"営業": "営業利益", "経常": "経常利益", "純": "純利益"}[main]
    num = f"{label} {oku(v, unit)} 前年比{pct(r)}" if r is not None else f"{label} {oku(v, unit)}"
    # 通期予想に対する進捗
    sub = ""
    fm = re.search(r"業績予想", t)
    if fm and v is not None:
        fseg = t[fm.start():fm.start() + 900]
        zm = re.search(r"通期", fseg)
        if zm:
            fcols = _columns(fseg[:220]) or cols
            fv = nums_after(fseg, zm.end(), len(fcols) * 2, 260)
            if main in fcols:
                j = fcols.index(main) * 2
                if j < len(fv) and fv[j]:
                    sub = f"通期予想 {oku(fv[j], unit)}・進捗{v / fv[j] * 100:.0f}%"
    d = 1 if (r is not None and r > 0) or (r is None and v and v > 0) else -1 if r is not None and r < 0 else 0
    return {"num": num, "sub": f"{period}" + (f"・{sub}" if sub else ""), "dir": d}


def parse_monthly(text: str) -> dict | None:
    t = text
    m = re.search(r"既存店[^\d△▲]{0,40}?([△▲]?\d+(?:\.\d+)?)\s*[%％]", t) or re.search(r"(売上高?|全店)[^\d△▲]{0,30}?前年(同月)?比[^\d△▲]{0,12}([△▲]?\d+(?:\.\d+)?)\s*[%％]", t)
    if not m:
        return None
    v = to_num(m.group(m.lastindex))
    if v is None:
        return None
    lab = "既存店" if "既存店" in m.group(0) else "売上"
    return {"num": f"{lab} 前年比 {pct(v)}", "sub": "", "dir": 1 if v > 0 else -1 if v < 0 else 0}


def parse_ma(text: str) -> dict | None:
    t = text
    amt = re.search(r"取得価額[^\d]{0,60}?([\d,]+)\s*(百万円|億円|千円)", t)
    sales = re.search(r"売上高[^\d]{0,40}?([\d,]+)\s*(百万円|億円|千円)", t)
    parts = []
    if amt:
        v, u = to_num(amt.group(1)), amt.group(2)
        parts.append("取得 " + (f"{v:g}億" if u == "億円" else oku(v, u)))
    if sales:
        v, u = to_num(sales.group(1)), sales.group(2)
        parts.append("相手の売上 " + (f"{v:g}億" if u == "億円" else oku(v, u)))
    if not parts:
        return None
    return {"num": "・".join(parts), "sub": "", "dir": 0}


def parse_equity(text: str) -> dict | None:
    t = text
    m = re.search(r"希薄化[^\d]{0,80}?([\d.]+)\s*[%％]", t) or re.search(r"発行済株式総数に対する[^\d]{0,30}?([\d.]+)\s*[%％]", t)
    amt = re.search(r"(調達|発行)(する)?(資金|価額|総額)[^\d]{0,40}?([\d,]+)\s*(百万円|億円|千円|円)", t)
    parts = []
    if amt:
        v, u = to_num(amt.group(4)), amt.group(5)
        parts.append("調達 " + (f"{v:g}億" if u == "億円" else oku(v, u) if u != "円" else oku(v / 1e6)))
    if m:
        parts.append(f"希薄化 {m.group(1)}%")
    if not parts:
        return None
    return {"num": "・".join(parts), "sub": "", "dir": -1}


def extract(kind: str, text: str, title: str, close: float | None) -> dict:
    try:
        if kind == "修正":
            return parse_revision(text) or {}
        if kind == "配当":
            return parse_dividend(text) or {}
        if kind == "TOB":
            return parse_tob(text, title, close) or {}
        if kind == "自社株":
            return parse_buyback(text) or {}
        if kind == "分割":
            return parse_split(text) or {}
        if kind == "特損益":
            return parse_special(text, title) or {}
        if kind == "短信":
            return parse_tanshin(text) or {}
        if kind == "月次":
            return parse_monthly(text) or {}
        if kind == "M&A":
            return parse_ma(text) or {}
        if kind == "増資":
            return parse_equity(text) or {}
    except Exception:
        return {}
    return {}


def title_dir(kind: str, title: str) -> int:
    t = nfkc(title)
    if re.search(r"上方修正|増配|復配|記念配当|特別配当|特別利益|売却益", t):
        return 1
    if re.search(r"下方修正|減配|無配|特別損失|減損|売却損", t):
        return -1
    return {"TOB": 1, "自社株": 1, "分割": 1, "増資": -1}.get(kind, 0)


# ── 本体 ─────────────────────────────────────────────────────────────
def build_digest(ir_rows: list[dict], data: dict, deadline_sec: float = DEADLINE_SEC) -> dict:
    """ir_rows: ir_builder の rows_all（date,time,code,name,title,pdf,intraday,close,react を持つ）。"""
    t0 = time.monotonic()
    days = sorted({r["date"] for r in ir_rows}, reverse=True)[:DAYS]
    cand = []
    for r in ir_rows:
        if r["date"] not in days:
            continue
        k = kind_of(r["title"])
        if k:
            cand.append((k, r))
    cand.sort(key=lambda kr: (kr[1]["date"], kr[1]["time"]), reverse=True)

    def work(kr):
        k, r = kr
        if time.monotonic() - t0 > deadline_sec:
            return k, r, "", "期限"
        text, err = pdf_text(r.get("pdf") or "")
        return k, r, text, err
    rows_out: dict[str, list[dict]] = {d: [] for d in days}
    n_read = 0
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        for k, r, text, err in ex.map(work, cand):
            close = r.get("close")
            ex_ = extract(k, text, r["title"], close) if text else {}
            if text:
                n_read += 1
            d = ex_.get("dir")
            if d is None or d == 0:
                d = title_dir(k, r["title"]) if d is None else (title_dir(k, r["title"]) or 0)
            react = r.get("react") or {}
            df = None
            r1 = None
            try:
                import ir_builder
                df = ir_builder._px(data, r["code"])
                if df is not None and len(df) >= 2:
                    r1 = round((float(df["Close"].iloc[-1]) / float(df["Close"].iloc[-2]) - 1) * 100, 2)
            except Exception:
                pass
            rows_out[r["date"]].append({
                "id": r["id"], "time": r["time"], "code": r["code"], "name": r["name"], "kind": k, "label": KIND_LABEL.get(k, k),
                "dir": int(d), "num": ex_.get("num") or "", "sub": ex_.get("sub") or "", "title": r["title"][:120], "pdf": r.get("pdf"),
                "intraday": bool(r.get("intraday")), "close": close, "r1": r1,
                "react": {"day_chg": react.get("day_chg"), "next_gap": react.get("next_gap"), "next_chg": react.get("next_chg"), "next_date": react.get("next_date")},
                "err": err or "",
            })
    out_days = []
    for d in days:
        rows = sorted(rows_out[d], key=lambda x: (x["time"], x["code"]), reverse=True)
        # 同じ会社・同じ種類で数字の無い行（補足説明資料など）は、数字のある行があれば落とす
        has_num = {(r["code"], r["kind"]) for r in rows if r["num"]}
        rows = [r for r in rows if r["num"] or (r["code"], r["kind"]) not in has_num]
        out_days.append({"date": d, "n_all": sum(1 for r in ir_rows if r["date"] == d), "n_kind": len(rows), "n_read": sum(1 for r in rows if r["num"]), "rows": rows})
    meta = {"generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"), "days": out_days, "n_cand": len(cand), "n_read": n_read,
            "elapsed_s": round(time.monotonic() - t0), "source": "TDnet 適時開示（PR情報は含まない）"}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(meta, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"  [digest] 📢開示: {len(cand)}件のうち本文{n_read}・数字{sum(d['n_read'] for d in out_days)} {meta['elapsed_s']}s → {OUT_PATH.name}")
    return meta


if __name__ == "__main__":
    # 手元テスト: python -X utf8 ir_digest.py [YYYYMMDD] → TDnet を直接取って data/ir_digest.json を書く（値段・反応は無し）
    import sys
    import ir_builder
    ymd = sys.argv[1] if len(sys.argv) > 1 else datetime.now(JST).strftime("%Y%m%d")
    rows = ir_builder.fetch_tdnet_day(ymd) or []
    d = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
    ir_rows = [{**r, "date": d, "intraday": bool(r["time"]) and r["time"] < SESSION_END, "close": None, "react": {}} for r in rows if re.match(r"^[0-9][0-9A-Z]{3}$", r["code"])]
    m = build_digest(ir_rows, {})
    for day in m["days"]:
        for r in day["rows"]:
            print(f"{r['time']} {r['code']} {r['name'][:10]:<10} [{r['label']}] {'↑' if r['dir'] > 0 else '↓' if r['dir'] < 0 else '→'} {r['num']} {r['sub']} | {r['title'][:40]} {r['err']}")
