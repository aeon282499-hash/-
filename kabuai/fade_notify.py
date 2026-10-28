"""fade_notify.py — 友達の「売りフェード戦略」の銘柄選定だけを Discord に通知する（発注機能なし）。

元になった検証: fade_handover_20261023.zip / backtest/fade_rank_bt.py（2017-01〜2026-08・9.6年）。
このモジュールはその候補条件・採点・順位・除外を同じ式で再現し、翌営業日の候補を出すだけ。
売買の判断・発注は本人の手動（kabu API / 立花 API の注文コードは一切書かない）。

候補（基準日 D＝直近の営業日の日足）:
  終値 100 円以上・売買代金 3 億円以上・前日比(ret1=AdjC/前日AdjC−1) +8% 以上・普通株（S17≠99・5桁目=0）
採点: ret1 / vol20(日次リターン20日σ) / dev25(25日線乖離) / rng((高−安)/前日終値) の
      同日候補内の百分位(rank pct)の平均×4（最大4）→ score 降順で全体順位 rank_all（同点は ret1 降順）
除外: 決算起因（D 当日の開示、または D−1 の 15 時以降の開示＝J-Quants /fins/summary）
      日証金の申込停止（J-Quants /markets/margin-alert RestrictedByJSF）。注意喚起(PrecautionByJSF)は表示のみ
      ※ fade_rank_bt.py は順位を付けた後に除外行を落とす＝1位が除外なら「ルールAなし」（2位を繰り上げない）
ルールA: 全体 1 位 かつ 前日比 +17% 以上 の 1 本（除外に当たらない）
参考:   全体 2〜5 位（ルールAの 1 本が売れないときの予備）

通知（Discord・環境変数 DISCORD_WEBHOOK_FADE_URL。URL はコード/ログに出さない）:
  evening（毎営業日 19:00）: 翌営業日の候補（コード/銘柄名/市場/終値/前日比/売買代金/順位/スコア/翌日の値幅上限/
                             制度信用で売れるか(貸借銘柄か)/日証金の規制）＋執行の一文
  score  （翌営業日 15:40）: 候補の 寄値→引値 の紙の損益（% と 50 万円換算の円）。売ったかどうかに関係なく毎日貯める

台帳: kabuai/fade_notify_ledger.json（plans[取引日] / scores[取引日]）

使い方:
  python -X utf8 kabuai/fade_notify.py --mode evening               # 今日の日足 → 翌営業日の計画
  python -X utf8 kabuai/fade_notify.py --mode score                 # 今日の寄値→引値 で採点
  python -X utf8 kabuai/fade_notify.py --mode auto                  # JST 17時前=score / 以降=evening（cron 用）
  python -X utf8 kabuai/fade_notify.py --mode evening --date 2026-10-23 --dry-run   # リプレイ（送らず表示）

環境変数:
  JQUANTS_API_KEY            必須（親の .env も読む。FADE_NOTIFY_DOTENV=<path> で別の .env を指せる）
  DISCORD_WEBHOOK_FADE_URL   通知先。未設定なら標準出力だけ（非致命）
  FADE_NOTIFY_BARS_PKL       日足を API でなくこの pkl（_bars_daily_5y_1024.pkl 形式）から読む（リプレイ・PC検証用）
  FADE_NOTIFY_CACHE_DIR      日足の日別 JSON キャッシュ（任意）
  FADE_NOTIFY_LOOKBACK       日足の取得営業日数（既定 45 ＝ 25日線＋20日σに十分）
  JQUANTS_VERIFY_SSL=false   PC の AV SSL 傍受対策
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

JST = timezone(timedelta(hours=9))
HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
JQUANTS_BASE = "https://api.jquants.com/v2"
LEDGER = HERE / "fade_notify_ledger.json"
JSF_LIST_URL = "http://www.taisyaku.jp/data/seigenichiran.csv"   # 日証金 制限措置等一覧（当日・cp932）

# ── 候補条件（fade_rank_bt.py と同じ） ──
PRICE_MIN = 100.0
VA_MIN = 3e8
RET_MIN = 0.08
STRONG_RET = 0.17
SIZE_YEN = 500_000
SCORE_COLS = ("ret1", "vol20", "dev25", "rng")
TOP_SHOW = 5
# 日証金の「注意喚起」も除外するか。依頼文は「除外：…日証金の申込停止・注意喚起」＝両方除外。
# 友達の fade_trade_v1.16 は申込停止だけ除外・注意喚起は表示のみ（日計りなら逆日歩は付かない）。False でそちらに揃う。
EXCLUDE_JSF_WARN = os.getenv("FADE_NOTIFY_EXCLUDE_JSF_WARN", "1") not in ("0", "false", "False")
LOOKBACK = int(os.getenv("FADE_NOTIFY_LOOKBACK", "45"))

RULE_LINE = ("翌朝、寄り成行で空売り → 約定したら即、引け成行で返済予約。損切り・利確なし。"
             "1本50万円まで。8:58の気配がストップ高張り付きなら見送り")
BT_LINE = ("検証(2017-01〜2026-08・9.6年・fade_rank_bt.py): ルールA 1,549件 勝率67% 平均+3.62% PF2.14(料0.3%) 年+240万@50万"
           "／寄成全量約定・滑りなし・現存銘柄のみ・在庫/売建規制は未反映＝実際はこれより悪い")


def log(msg: str) -> None:
    print(f"[fade_notify] {msg}", flush=True)


# ───────────────────────── 営業日 ─────────────────────────
_CAL: dict | None = None


def _calendar() -> dict:
    global _CAL
    if _CAL is None:
        p = PARENT / "market_calendar.csv"
        _CAL = {}
        if p.exists():
            df = pd.read_csv(p, dtype={"Date": str})
            _CAL = dict(zip(df["Date"], df["HolDiv"].astype(int)))
    return _CAL


def is_trading_day(d: date) -> bool:
    cal = _calendar()
    ds = d.strftime("%Y-%m-%d")
    if ds in cal:
        return cal[ds] in (1, 2)          # 1=営業日 2=半日 3=祝日取引(現物休場) 0=休場
    import jpholiday
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def prev_trading_day(d: date) -> date:
    cur = d - timedelta(days=1)
    for _ in range(20):
        if is_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    return cur


def next_trading_day(d: date) -> date:
    cur = d + timedelta(days=1)
    for _ in range(20):
        if is_trading_day(cur):
            return cur
        cur += timedelta(days=1)
    return cur


def trading_days_upto(d: date, n: int) -> list[str]:
    """d を含む直近 n 営業日（昇順・YYYY-MM-DD）。"""
    out = []
    cur = d
    while len(out) < n:
        if is_trading_day(cur):
            out.append(cur.strftime("%Y-%m-%d"))
        cur -= timedelta(days=1)
    return out[::-1]


# ───────────────────────── J-Quants ─────────────────────────
def _jq_key() -> str:
    key = (os.getenv("JQUANTS_API_KEY", "") or "").strip()
    if not key:
        raise ValueError("JQUANTS_API_KEY が未設定")
    return key


def _jq_get(path: str, params: dict | None = None) -> dict:
    import requests
    verify = os.getenv("JQUANTS_VERIFY_SSL", "true").lower() not in ("0", "false", "no")
    for attempt in range(4):
        r = requests.get(f"{JQUANTS_BASE}{path}", headers={"x-api-key": _jq_key()},
                         params=params or {}, timeout=(10, 60), verify=verify)
        if r.status_code == 429:
            log("429 → 60秒待機")
            time.sleep(60)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"J-Quants {path}: 429 が続く")


def _jq_pages(path: str, params: dict) -> list[dict]:
    out: list[dict] = []
    pk = None
    while True:
        p = dict(params)
        if pk:
            p["pagination_key"] = pk
        d = _jq_get(path, p)
        out.extend(d.get("data", []))
        pk = d.get("pagination_key")
        if not pk:
            return out
        time.sleep(1.1)


def is_common(code) -> bool:
    c = str(code).strip()
    return len(c) < 5 or c[4] == "0"


def code4(code) -> str:
    c = str(code).strip()
    return c[:4] if len(c) == 5 and c.endswith("0") else c


def load_master() -> pd.DataFrame:
    """/equities/master → DataFrame[Code, CoName, MktNm, S17]（全上場・ETF 含む）。"""
    m = _jq_get("/equities/master")
    df = pd.DataFrame(m.get("data", []))
    keep = [c for c in ("Code", "CoName", "MktNm", "S17", "Mrgn") if c in df.columns]
    return df[keep].astype(str)


def fetch_bars_api(days: list[str]) -> pd.DataFrame:
    """/equities/bars/daily を日付ごとに取得 → DataFrame[Date, Code, O, H, L, C, Va, AdjFactor]。"""
    cache_dir = os.getenv("FADE_NOTIFY_CACHE_DIR", "").strip()
    recs: list[dict] = []
    for i, ds in enumerate(days):
        cp = Path(cache_dir) / f"daily_{ds}.json" if cache_dir else None
        if cp and cp.exists():
            rows = json.loads(cp.read_text(encoding="utf-8"))
        else:
            try:
                rows = _jq_pages("/equities/bars/daily", {"date": ds})
            except Exception as e:  # noqa: BLE001
                log(f"{ds} 取得失敗: {e}")
                rows = []
            if cp and rows:
                cp.parent.mkdir(parents=True, exist_ok=True)
                cp.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            time.sleep(1.1)
        recs.extend(rows)
        if (i + 1) % 10 == 0:
            log(f"日足 {i + 1}/{len(days)} 日")
    if not recs:
        raise RuntimeError("J-Quants から日足 0 件")
    df = pd.DataFrame(recs)
    return _normalize_bars(df)


def _normalize_bars(df: pd.DataFrame) -> pd.DataFrame:
    need = ["Date", "Code", "O", "H", "L", "C", "Va"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise RuntimeError(f"日足のカラム不足: {miss}")
    if "AdjFactor" not in df.columns:
        df["AdjFactor"] = 1.0
    out = df[need + ["AdjFactor"]].copy()
    out["Date"] = pd.to_datetime(out["Date"])
    out["Code"] = out["Code"].astype(str)
    for c in ("O", "H", "L", "C", "Va", "AdjFactor"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["AdjFactor"] = out["AdjFactor"].fillna(1.0)
    return out.drop_duplicates(["Date", "Code"]).sort_values(["Code", "Date"]).reset_index(drop=True)


def load_bars_pkl(path: str, days: list[str]) -> pd.DataFrame:
    df = pd.read_pickle(path)
    df = df[df["Date"].isin(pd.to_datetime(days))]
    return _normalize_bars(df)


def load_bars(asof: date) -> pd.DataFrame:
    days = trading_days_upto(asof, LOOKBACK)
    pkl = os.getenv("FADE_NOTIFY_BARS_PKL", "").strip()
    if pkl:
        log(f"日足を pkl から読む: {Path(pkl).name}")
        return load_bars_pkl(pkl, days)
    log(f"日足 {days[0]} 〜 {days[-1]}（{len(days)} 営業日）を J-Quants から取得")
    return fetch_bars_api(days)


EARN_TITLE_RE = re.compile(r"決算短信|業績予想|配当予想|業績.{0,6}修正|予想.{0,6}修正")


def _tdnet_earn_codes(asof: date, prev: date) -> set[str] | None:
    """TDnet の一覧（表題）から決算系の開示を拾う。D 当日全部 ＋ D−1 の 15 時以降。{code5}。取れなければ None。
    /fins/summary の当日分が 19 時にまだ無い日の保険（fade_rank_bt.py の定義は fins/summary）。"""
    try:
        sys.path.insert(0, str(HERE))
        from ir_builder import fetch_tdnet_day
    except Exception as e:  # noqa: BLE001
        log(f"TDnet モジュール読込失敗: {e}")
        return None
    s: set[str] = set()
    for d, after15 in ((asof, False), (prev, True)):
        rows = fetch_tdnet_day(d.strftime("%Y%m%d"))
        if rows is None:
            return None
        for r in rows:
            if not EARN_TITLE_RE.search(r.get("title", "")):
                continue
            if after15 and not (str(r.get("time", "00:00"))[:2].isdigit() and int(str(r.get("time"))[:2]) >= 15):
                continue
            s.add(str(r.get("code5", "")))
    return s


def fetch_earn_codes(asof: date) -> set[str] | None:
    """決算起因の判定: D 当日の開示 ＋ D−1 の 15 時以降の開示。{code5}。
    主: J-Quants /fins/summary（fade_rank_bt.py と同じ定義）／ 補: TDnet 表題（当日分が未公開の時の保険・和集合）。
    両方取れなければ None（除外せず・通知に⚠️）。"""
    prev = prev_trading_day(asof)
    s: set[str] | None = None
    try:
        cur = _jq_pages("/fins/summary", {"date": asof.strftime("%Y-%m-%d")})
        pre = _jq_pages("/fins/summary", {"date": prev.strftime("%Y-%m-%d")})
        s = {str(r.get("Code", "")) for r in cur}
        n_pre = 0
        for r in pre:
            hh = str(r.get("DiscTime") or "")[:2]
            if hh.isdigit() and int(hh) >= 15:
                s.add(str(r.get("Code", "")))
                n_pre += 1
        log(f"決算開示(J-Quants): {asof} {len(cur)}件 / {prev} 15時以降 {n_pre}件")
    except Exception as e:  # noqa: BLE001
        log(f"決算開示(J-Quants)の取得失敗: {e}")
    t = _tdnet_earn_codes(asof, prev)
    if t is not None:
        log(f"決算開示(TDnet表題): {len(t)}件")
        s = (s or set()) | t
    return s


def fetch_iss_map() -> dict:
    """{code4: IssType}（"2"=貸借銘柄＝制度信用で空売り可）。直近公表週の /markets/margin-interest。"""
    try:
        cur = datetime.now(JST).date() - timedelta(days=4)
        for _ in range(14):
            if cur.weekday() < 5:
                rows = _jq_pages("/markets/margin-interest", {"date": cur.strftime("%Y-%m-%d")})
                if rows:
                    log(f"貸借区分: {cur} 公表分 {len(rows)}件")
                    return {code4(r.get("Code", "")): str(r.get("IssType", "")) for r in rows if is_common(r.get("Code", ""))}
            cur -= timedelta(days=1)
    except Exception as e:  # noqa: BLE001
        log(f"貸借区分の取得失敗: {e}")
    return {}


def fetch_alert_map() -> dict:
    """{code4: {jsf_stop, jsf_warn, tse_reg, daily_pub}} 直近公表日の /markets/margin-alert。"""
    try:
        cur = datetime.now(JST).date()
        for _ in range(8):
            if cur.weekday() < 5:
                rows = _jq_pages("/markets/margin-alert", {"date": cur.strftime("%Y-%m-%d")})
                if rows:
                    out = {}
                    for r in rows:
                        if not is_common(r.get("Code", "")):
                            continue
                        pr = r.get("PubReason") or {}
                        out[code4(r.get("Code", ""))] = {
                            "jsf_stop": pr.get("RestrictedByJSF") == "1",
                            "jsf_warn": pr.get("PrecautionByJSF") == "1",
                            "tse_reg": pr.get("Restricted") == "1",
                            "daily_pub": pr.get("DailyPublication") == "1",
                        }
                    log(f"規制銘柄: {cur} 公表分 {len(out)}銘柄（売り禁 {sum(v['jsf_stop'] for v in out.values())}）")
                    return out
            cur -= timedelta(days=1)
    except Exception as e:  # noqa: BLE001
        log(f"規制銘柄の取得失敗: {e}")
    return {}


def fetch_jsf_list() -> dict:
    """日証金 制限措置等一覧（当日 CSV）→ {code4: 措置の文字列}。取れなければ {}（表示の補助。判定の正は margin-alert）。"""
    try:
        req = urllib.request.Request(JSF_LIST_URL, headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=15).read().decode("cp932", errors="replace")
    except Exception as e:  # noqa: BLE001
        log(f"日証金一覧の取得失敗: {e}")
        return {}
    out: dict = {}
    for line in raw.splitlines():
        cells = [c.strip('" \t') for c in line.split(",")]
        code = next((c for c in cells if re.fullmatch(r"\d{4}|\d{3}[A-Z]", c)), None)
        if not code:
            continue
        txt = " ".join(c for c in cells if c and c != code and not re.fullmatch(r"[\d/.\-:]+", c))
        out[code] = (out.get(code, "") + " " + txt).strip()[:80]
    log(f"日証金一覧: {len(out)}銘柄")
    return out


# ───────────────────────── 値幅制限 ─────────────────────────
def band(p: float) -> float:
    """東証の制限値幅（基準値段 p → 値幅）。fade_rank_bt.py の band() と同じ表。"""
    for lim, w in ((100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500),
                   (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000),
                   (50000, 7000), (70000, 10000), (100000, 15000), (150000, 30000), (200000, 40000),
                   (300000, 50000), (500000, 70000), (700000, 100000), (1000000, 150000)):
        if p < lim:
            return float(w)
    return p * 0.15


# ───────────────────────── 候補・採点（fade_rank_bt.py と同じ式） ─────────────────────────
def add_features(b: pd.DataFrame) -> pd.DataFrame:
    """銘柄ごとに AdjC・ret1・vol20・ma25・dev25・prevC・rng を付ける。b は Code/Date 昇順。"""
    # 出来ずの日（J-Quants は四本値 null の行を返す）は落とす。残すと ret1 が NaN になり 20日σ/25日線が
    # 以後20〜25日 NaN のまま＝百分位の分母から外れて順位が狂う（2026-05-28 6977 で実測・友達の表は値あり）
    b = b.dropna(subset=["C"]).sort_values(["Code", "Date"]).reset_index(drop=True)
    # 調整後終値: AdjC_t = C_t × Π_{s>t} AdjFactor_s（分割日の係数はその日以降の価格に対して、前の価格を直す）
    af = b["AdjFactor"].astype(float)
    cum = af.iloc[::-1].groupby(b["Code"].iloc[::-1]).cumprod().iloc[::-1]
    b["AdjC"] = b["C"] * cum / af
    g = b.groupby("Code", sort=False)
    b["ret1"] = g["AdjC"].pct_change(fill_method=None)
    b["vol20"] = g["ret1"].transform(lambda s: s.rolling(20).std())
    b["ma25"] = g["AdjC"].transform(lambda s: s.rolling(25).mean())
    b["dev25"] = b["AdjC"] / b["ma25"] - 1
    b["prevC"] = g["C"].shift(1)
    b["rng"] = (b["H"] - b["L"]) / b["prevC"]
    return b


def score_rank(cand: pd.DataFrame) -> pd.DataFrame:
    """同日候補内の百分位の平均×4 → score、降順で rank_all（同点は ret1 降順）。"""
    cand = cand.copy()
    if cand.empty:
        cand["score"] = []
        cand["rank_all"] = []
        return cand
    cols = [c for c in SCORE_COLS if c in cand.columns]
    pct = cand[cols].rank(pct=True)
    cand["score"] = pct.mean(axis=1, skipna=True) * 4
    # 同点の順: fade_rank_bt.py は Code 昇順に並んだ表に rank(method="first") ＝ 同点はコード昇順が上。
    # （友達の fade_trade_v1.16 の夜の計画は ret1 降順で同点を切る＝BT と違う。ここは BT に合わせる。
    #   同点は百分位が 1/n 刻みなので候補が少ない日に起きる＝2026-05〜08 の 65 日で 2 日）
    if "Code" in cand.columns:
        cand = cand.sort_values(["score", "Code"], ascending=[False, True], kind="mergesort")
    else:
        cand = cand.sort_index().sort_values("score", ascending=False, kind="mergesort")
    cand["rank_all"] = np.arange(1, len(cand) + 1)
    return cand


def candidates_for_day(bars: pd.DataFrame, asof, universe: set[str] | None) -> pd.DataFrame:
    """基準日 asof の候補（特徴量・score・rank_all・UL1 付き）。universe は 5桁 Code の集合（None=全部）。"""
    b = bars
    if universe is not None:
        b = b[b["Code"].isin(universe)]
    b = b[b["Code"].map(is_common)]
    f = add_features(b)
    last = f[f["Date"] == pd.Timestamp(asof)]
    cand = last[(last["C"] >= PRICE_MIN) & (last["Va"] >= VA_MIN) & (last["ret1"] >= RET_MIN)].copy()
    cand = score_rank(cand)
    cand["UL1"] = cand["C"] + cand["C"].map(band)        # 翌日の値幅上限 = D 終値 + 値幅
    cand["strong"] = cand["ret1"] >= STRONG_RET
    return cand


# ───────────────────────── 計画（evening） ─────────────────────────
def build_plan(asof: date, trade_day: date, cand: pd.DataFrame, master: pd.DataFrame,
               earn: set[str] | None, iss: dict, alert: dict, jsf_list: dict) -> dict:
    names = dict(zip(master["Code"], master["CoName"])) if not master.empty else {}
    mkts = dict(zip(master["Code"], master["MktNm"])) if not master.empty else {}
    rows = []
    if "Code" in cand.columns:
        cand = cand.set_index("Code")
    for c5, r in cand.head(TOP_SHOW + 3).iterrows():
        c4 = code4(c5)
        al = alert.get(c4) or {}
        reasons = []
        if earn is not None and (c5 in earn):
            reasons.append("決算起因")
        if al.get("jsf_stop"):
            reasons.append("日証金停止")
        jsf_txt = jsf_list.get(c4, "")
        if "停止" in jsf_txt and "日証金停止" not in reasons:
            reasons.append("日証金停止")
        if EXCLUDE_JSF_WARN and (al.get("jsf_warn") or "注意喚起" in jsf_txt):
            reasons.append("日証金注意喚起")
        it = iss.get(c4)
        rows.append(dict(
            code=c4, code5=str(c5), name=str(names.get(str(c5), ""))[:12], mkt=str(mkts.get(str(c5), ""))[:6],
            close=float(r["C"]), ret1=float(r["ret1"]), va=float(r["Va"]), rank_all=int(r["rank_all"]),
            score=round(float(r["score"]), 2), ul1=float(r["UL1"]), strong=bool(r["strong"]),
            vol20=None if pd.isna(r["vol20"]) else round(float(r["vol20"]), 4),
            dev25=None if pd.isna(r["dev25"]) else round(float(r["dev25"]), 4),
            rng=None if pd.isna(r["rng"]) else round(float(r["rng"]), 4),
            seido="○" if it == "2" else ("×" if it else "?"),
            jsf_stop=bool(al.get("jsf_stop")), jsf_warn=bool(al.get("jsf_warn")), tse_reg=bool(al.get("tse_reg")),
            daily_pub=bool(al.get("daily_pub")), jsf_txt=jsf_txt[:40], reasons=reasons,
        ))
    rule_a = None
    rule_a_why = ""
    top = next((x for x in rows if x["rank_all"] == 1), None)
    if top is None:
        rule_a_why = "候補なし"
    elif not top["strong"]:
        rule_a_why = f"1位 {top['code']} は前日比{top['ret1']*100:+.2f}%（+17%未満）"
    elif top["reasons"]:
        rule_a_why = f"1位 {top['code']} は除外（{'・'.join(top['reasons'])}）"
    else:
        rule_a = top
    return dict(
        asof=asof.isoformat(), trade_day=trade_day.isoformat(), made=datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
        n_cand=int(len(cand)), rule_a=rule_a, rule_a_why=rule_a_why,
        rows=[x for x in rows if x["rank_all"] <= TOP_SHOW],
        earn_checked=earn is not None, iss_checked=bool(iss), alert_checked=bool(alert), sent=False,
    )


def _fmt_row(x: dict, head: str) -> str:
    seido = {"○": "制度信用○(貸借銘柄)", "×": "制度信用×(信用銘柄＝一般信用の在庫次第)", "?": "貸借区分なし"}[x["seido"]]
    reg = []
    if x["jsf_stop"]:
        reg.append("🚫日証金 申込停止")
    if x["jsf_warn"]:
        reg.append("⚠️日証金 注意喚起")
    if x["tse_reg"]:
        reg.append("取引所規制(増担保等)")
    if x["daily_pub"]:
        reg.append("日々公表")
    if x["jsf_txt"] and not (x["jsf_stop"] or x["jsf_warn"]):
        reg.append(f"日証金一覧: {x['jsf_txt']}")
    reg_s = "・".join(reg) if reg else "日証金の規制なし"
    ex = f"\n　除外: {'・'.join(x['reasons'])}" if x["reasons"] else ""
    return (f"{head} `{x['code']}` {x['name']}（{x['mkt']}）\n"
            f"　終値 {x['close']:,.0f}円　前日比 {x['ret1']*100:+.1f}%　代金 {x['va']/1e8:,.1f}億\n"
            f"　全体 {x['rank_all']}位　スコア {x['score']:.2f}/4　翌日の値幅上限 {x['ul1']:,.0f}円\n"
            f"　{seido}　{reg_s}{ex}")


def plan_payload(plan: dict) -> dict:
    td = datetime.fromisoformat(plan["trade_day"])
    asof = datetime.fromisoformat(plan["asof"])
    lines = [f"基準日 {asof:%m/%d} の日足・候補 {plan['n_cand']}銘柄（終値100円以上・代金3億以上・前日比+8%以上）"]
    if not plan["earn_checked"]:
        lines.append("⚠️ 決算開示が取れず「決算起因」の除外は未確認")
    if not plan["iss_checked"]:
        lines.append("⚠️ 貸借区分が取れず「制度信用で売れるか」は未確認")
    if not plan["alert_checked"]:
        lines.append("⚠️ 規制銘柄情報が取れず「日証金の規制」は未確認")
    lines.append("")
    if plan["rule_a"]:
        lines.append("**【ルールA】全体1位 かつ 前日比+17%以上 の1本**")
        lines.append(_fmt_row(plan["rule_a"], "🩳"))
    else:
        lines.append(f"**【ルールA】該当なし**（{plan['rule_a_why']}）")
        top = next((x for x in plan["rows"] if x["rank_all"] == 1), None)
        if top:
            lines.append(_fmt_row(top, "1位"))
    others = [x for x in plan["rows"] if 2 <= x["rank_all"] <= TOP_SHOW]
    if others:
        lines.append("")
        lines.append("**【参考】全体2〜5位（ルールAの1本が売れないときの予備）**")
        for x in others:
            lines.append(_fmt_row(x, f"{x['rank_all']}位"))
    lines.append("")
    lines.append(f"📌 {RULE_LINE}")
    lines.append(f"-# {BT_LINE}")
    lines.append("-# 銘柄の選定だけ。発注はしない。投資判断は自己責任。")
    title = f"🩳 {td:%m/%d(%a)} 売りフェード（友達版）候補" + ("" if plan["rule_a"] else "・ルールAなし")
    desc = "\n".join(lines)
    return {"embeds": [{"title": title, "description": desc[:4000], "color": 0xFF8787 if plan["rule_a"] else 0x868E96}]}


# ───────────────────────── 本人ch 向け 1行（2026-10-29 本人「やって」） ─────────────────────────
# 20年(2001〜2026)の検証で 友達の①は 寄成 PF1.92 より 前日終値+2%の当日中指値 PF2.21 のほうが良かった
# （寄りで約定48%・日中に届いて約定30%・届かず22%。日中は1ティック抜けで約定とする厳しめでも PF2.19）。
# 本人の売りフェードch(既定 DISCORD_WEBHOOK_DAY_URL)へ、銘柄と値段だけ送る。友達chの文面は変えない。
OWN_LIMIT_PCT = 0.02
OWN_WEBHOOK_ENV = os.getenv("FADE_NOTIFY_OWN_ENV", "DISCORD_WEBHOOK_OWN_FADE_URL")   # 10/29 本人提示の新ch（旧 DAY_URL は .env 側が404）


def _tick(p: float) -> float:
    return 1.0 if p <= 3000 else 5.0 if p <= 5000 else 10.0 if p <= 30000 else 50.0 if p <= 50000 else 100.0


OWN_RANKS = 3   # 2026-10-29 本人「1〜3位にして」（20年: 1〜3位 PF2.14・4位からは負け年あり）


NEW1_MIN, NEW1_LIMIT_PCT, NEW1_RANKS = 0.17, 0.03, 3


def new1_plan(cand, master, iss, alert, jsf_list) -> dict | None:
    """新①（2026-10-29 本人「17%でやって」）: 貸借銘柄(IssType=2)で前日比+17%以上を 前日比の大きい順に1〜3位 →
    前日終値+3%の当日中指値で空売り→引成。売り禁・決算は除外しない（売り禁は一般信用の在庫が要ると表示）。
    J-Q 5年(1本50万): 1,003本 PF1.96 +1,102万 最大DD27万・1位PF2.04/2位1.64/3位2.30・前半1.69/後半2.08。"""
    if not iss or cand.empty:
        return None
    c = cand.reset_index() if "Code" not in cand.columns else cand
    sub = c[c["Code"].map(lambda x: iss.get(code4(x)) == "2") & (c["ret1"] >= NEW1_MIN)]
    sub = sub.sort_values(["ret1", "Code"], ascending=[False, True]).head(NEW1_RANKS)
    names = dict(zip(master["Code"], master["CoName"])) if not master.empty else {}
    rows = []
    for i, r in enumerate(sub.itertuples(), 1):
        c4 = code4(r.Code); al = alert.get(c4) or {}; jt = jsf_list.get(c4, "")
        rows.append(dict(rank=i, code=c4, name=str(names.get(str(r.Code), ""))[:12], close=float(r.C), ret1=float(r.ret1),
                         jsf_stop=bool(al.get("jsf_stop")) or "停止" in jt, jsf_warn=bool(al.get("jsf_warn")) or "注意喚起" in jt))
    return dict(rows=rows, n_loan17=int(len(c[c["Code"].map(lambda x: iss.get(code4(x)) == "2") & (c["ret1"] >= NEW1_MIN)])))


def _new1_lines(npl: dict | None, dup: set[str]) -> tuple[list[str], int]:
    if npl is None:
        return ["【新①】貸借区分が取れず判定できません"], 0
    if not npl["rows"]:
        return ["【新①】なし（貸借で前日比+17%以上が無い）"], 0
    out, n = [f"【新①】貸借・前日比+17%以上の大きい順（{npl['n_loan17']}銘柄中）"], 0
    for x in npl["rows"]:
        if x["code"] in dup:
            out.append(f"{x['rank']}位 `{x['code']}` {x['name']}（上と同じ銘柄・重ねない）")
            continue
        t = _tick(x["close"]); lim = float(np.ceil(x["close"] * (1 + NEW1_LIMIT_PCT) / t) * t)
        sh = int(SIZE_YEN / lim // 100 * 100)
        tag = "🚫売り禁（一般信用の在庫が要る）" if x["jsf_stop"] else ("制度信用○・⚠️注意喚起" if x["jsf_warn"] else "制度信用○")
        out.append(f"{x['rank']}位 `{x['code']}` {x['name']}　前日終値 ¥{x['close']:,.0f}（{x['ret1']*100:+.1f}%）\n"
                   f"　🔴 当日中指値 ¥{lim:,.0f}（+{NEW1_LIMIT_PCT*100:.0f}%）で空売り → 引成で買戻し\n"
                   f"　50万: {sh:,}株" + ("（100株で50万超）" if sh == 0 else "") + f"　{tag}")
        n += 1
    return out, n


def loan_plan(asof, trade_day, cand, master, earn, iss, alert, jsf_list) -> dict | None:
    """貸借版（2026-10-29 本人「貸借版も通知して」）: 候補を貸借銘柄(IssType=2)だけにして、その中で4指標の順位を付け直す。
    1位 かつ +17%以上 かつ 除外なし → 前日終値+2%の当日中指値。J-Q 5年(貸借だけ): 468本 PF1.97 +638万@50万
    （今の①の再現 632本 PF1.80 +548万 より上・売り禁でない日 +263万 vs +189万）。"""
    if not iss or cand.empty:
        return None
    c = cand.reset_index() if "Code" not in cand.columns else cand
    sub = c[c["Code"].map(lambda x: iss.get(code4(x)) == "2")].drop(columns=["score", "rank_all"], errors="ignore")
    if sub.empty:
        return dict(rule_a=None, rule_a_why="貸借の候補なし", top=None)
    p = build_plan(asof, trade_day, score_rank(sub), master, earn, iss, alert, jsf_list)
    top = next((x for x in p["rows"] if x["rank_all"] == 1), None)
    return dict(rule_a=p["rule_a"], rule_a_why=p["rule_a_why"].replace("1位", "貸借1位"), top=top, n_cand=int(len(sub)))


def _loan_lines(lp: dict | None) -> list[str]:
    if lp is None:
        return ["【貸借版】貸借区分が取れず判定できません"]
    a = lp["rule_a"]
    if not a:
        return [f"【貸借版】なし（{lp['rule_a_why']}）"]
    lim = float(np.ceil(a["close"] * (1 + OWN_LIMIT_PCT) / _tick(a["close"])) * _tick(a["close"]))
    sh = int(SIZE_YEN / lim // 100 * 100)
    warn = "　⚠️日証金 注意喚起" if a["jsf_warn"] else ""
    return ["【貸借版】制度信用で売れる銘柄の中の1位",
            f"`{a['code']}` {a['name']}　前日終値 ¥{a['close']:,.0f}\n"
            f"　🔴 当日中指値 ¥{lim:,.0f}（+{OWN_LIMIT_PCT*100:.0f}%）で空売り → 引成で買戻し\n"
            f"　50万: {sh:,}株" + ("（100株で50万超）" if sh == 0 else "") + f"　制度信用○{warn}"]


TOP3_LIMIT_PCT = 0.03


def top3_plan(cand, master, iss, alert, jsf_list, earn) -> dict:
    """毎日の3本（2026-10-29 本人「とにかく毎日儲かる順に3つ」）: 候補(終値100円以上・代金3億以上・前日比+8%以上)を
    前日比の大きい順に3本 → 前日終値+3%の当日中指値で空売り→引成・各50万。貸借/売り禁/決算は表示だけ(除外しない)。
    検証: J-Q 5年 2,344本 PF2.07 +3,638万 DD52万(1位2.37/2位2.09/3位1.69)・立花20年 26/26年プラス・各時代1〜3位ともPF1.46以上。"""
    c = cand.reset_index() if "Code" not in cand.columns else cand
    c = c[c["C"].map(lambda p: np.ceil(p * (1 + TOP3_LIMIT_PCT) / _tick(p)) * _tick(p) * 100 <= SIZE_YEN)]   # 50万で100株買える銘柄だけ(20年: PF1.93・26/26年)
    c = c.sort_values(["ret1", "Code"], ascending=[False, True]).head(3)
    names = dict(zip(master["Code"], master["CoName"])) if not master.empty else {}
    rows = []
    for i, r in enumerate(c.itertuples(), 1):
        c4 = code4(r.Code); al = alert.get(c4) or {}; jt = jsf_list.get(c4, "")
        rows.append(dict(rank=i, code=c4, name=str(names.get(str(r.Code), ""))[:12], close=float(r.C), ret1=float(r.ret1),
                         seido="○" if iss.get(c4) == "2" else ("×" if iss.get(c4) else "?"),
                         jsf_stop=bool(al.get("jsf_stop")) or "停止" in jt, jsf_warn=bool(al.get("jsf_warn")) or "注意喚起" in jt,
                         earn=bool(earn is not None and str(r.Code) in earn)))
    return dict(rows=rows, n_cand=int(len(cand)))


def own_payload(plan: dict) -> dict:
    """本人ch: 毎日の3本だけ（前日比の大きい順・+3%指値・各50万）。"""
    td = datetime.fromisoformat(plan["trade_day"])
    tp = plan.get("top3") or {"rows": []}
    lines = []
    for x in tp["rows"]:
        t = _tick(x["close"]); lim = float(np.ceil(x["close"] * (1 + TOP3_LIMIT_PCT) / t) * t)
        sh = int(SIZE_YEN / lim // 100 * 100)
        tags = [{"○": "制度信用○", "×": "制度信用×（一般信用の在庫次第）", "?": "貸借区分なし"}[x["seido"]]]
        if x["jsf_stop"]: tags.append("🚫売り禁（一般信用の在庫が要る）")
        elif x["jsf_warn"]: tags.append("⚠️注意喚起")
        if x["earn"]: tags.append("📄決算がらみ")
        lines.append(f"{x['rank']}位 `{x['code']}` {x['name']}　前日終値 ¥{x['close']:,.0f}（{x['ret1']*100:+.1f}%）\n"
                     f"　🔴 当日中指値 ¥{lim:,.0f}（+{TOP3_LIMIT_PCT*100:.0f}%）で空売り → 引成で買戻し\n"
                     f"　50万: {sh:,}株" + ("（100株で50万超）" if sh == 0 else "") + "　" + "・".join(tags))
    if not lines:
        lines.append("候補なし（前日比+8%以上・代金3億以上が無い）")
    else:
        lines.append(f"\n-# 寄りがS高張り付き（寄値＝値幅上限）なら取消。1本50万・最大{len(tp['rows']) * 50}万")
    return {"embeds": [{"title": f"🩳 {td:%m/%d(%a)} 今日の3本（前日比の大きい順・指値+{TOP3_LIMIT_PCT*100:.0f}%）",
                        "description": "\n".join(lines), "color": 0xFF8787 if tp["rows"] else 0x868E96}]}


def _own_payload_friend_new1(plan: dict) -> dict:   # 旧: 友達版1〜3位＋新①（10/29 毎日の3本に置き換え・数字の比較用に残す）
    """全体1〜3位のうち 前日比+17%以上 を1本50万で（除外に当たる順位は見送りと表示・繰り上げない）＋ 貸借版の1本。"""
    td = datetime.fromisoformat(plan["trade_day"])
    lines, n_go = [], 0
    for x in sorted(plan["rows"], key=lambda r: r["rank_all"]):
        if x["rank_all"] > OWN_RANKS:
            break
        if not x["strong"]:
            lines.append(f"{x['rank_all']}位 `{x['code']}` {x['name']}　前日比{x['ret1']*100:+.2f}%（+17%未満）→ 見送り")
            continue
        if x["reasons"]:
            lines.append(f"{x['rank_all']}位 `{x['code']}` {x['name']}　見送り（{'・'.join(x['reasons'])}）")
            continue
        lim = float(np.ceil(x["close"] * (1 + OWN_LIMIT_PCT) / _tick(x["close"])) * _tick(x["close"]))
        sh = int(SIZE_YEN / lim // 100 * 100)
        seido = {"○": "制度信用○", "×": "制度信用×（一般信用の在庫次第）", "?": "貸借区分なし"}[x["seido"]]
        warn = "　⚠️日証金 注意喚起" if x["jsf_warn"] else ""
        lines.append(f"{x['rank_all']}位 `{x['code']}` {x['name']}　前日終値 ¥{x['close']:,.0f}\n"
                     f"　🔴 当日中指値 ¥{lim:,.0f}（+{OWN_LIMIT_PCT*100:.0f}%）で空売り → 引成で買戻し\n"
                     f"　50万: {sh:,}株" + ("（100株で50万超）" if sh == 0 else "") + f"　{seido}{warn}")
        n_go += 1
    if not lines:
        lines.append("候補なし")
    if not n_go:
        lines.append("→ 撃つ玉なし")
    # 新①（上の1〜3位と同じ銘柄なら重ねて撃たない）
    go = {x["code"] for x in plan["rows"] if x["rank_all"] <= OWN_RANKS and x["strong"] and not x["reasons"]}
    lines.append("")
    nl, n_loan = _new1_lines(plan.get("new1"), go)
    lines += nl
    n_all = n_go + n_loan
    if n_all:
        lines.append(f"\n-# 寄りがS高張り付き（寄値＝値幅上限）なら取消。1本50万・最大{n_all * 50}万")
    return {"embeds": [{"title": f"🩳 {td:%m/%d(%a)} 友達版 {n_go}本＋新① {n_loan}本",
                        "description": "\n".join(lines), "color": 0xFF8787 if n_all else 0x868E96}]}


def post_own(plan: dict, dry_run: bool) -> bool:
    hook = os.getenv(OWN_WEBHOOK_ENV, "").strip()
    payload = own_payload(plan)
    if dry_run or not hook:
        print(f"---- 本人ch（{OWN_WEBHOOK_ENV}・dry-run / 未設定なら送らない）----")
        for e in payload["embeds"]:
            print(f"## {e['title']}\n{e['description']}\n")
        return False
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(hook, data=body, headers={"Content-Type": "application/json", "User-Agent": "kabuai-fade-notify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            log(f"本人ch 送信OK status={r.status}")
            return True
    except Exception as e:  # noqa: BLE001  URL はログに出さない
        log(f"本人ch 送信失敗: {type(e).__name__}")
        return False


# ───────────────────────── 採点（score） ─────────────────────────
def fetch_day_bars_for(codes5: list[str], d: date) -> dict:
    """取引日 d の {code5: (O, C, H)}。pkl が指定されていればそこから、無ければ API の日付一括。"""
    pkl = os.getenv("FADE_NOTIFY_BARS_PKL", "").strip()
    if pkl:
        df = pd.read_pickle(pkl)
        df = df[(df["Date"] == pd.Timestamp(d)) & (df["Code"].astype(str).isin(codes5))]
        return {str(r.Code): (float(r.O), float(r.C), float(r.H)) for r in df.itertuples()}
    rows = _jq_pages("/equities/bars/daily", {"date": d.strftime("%Y-%m-%d")})
    out = {}
    want = set(codes5)
    for r in rows:
        c = str(r.get("Code", ""))
        if c in want:
            try:
                out[c] = (float(r["O"]), float(r["C"]), float(r["H"]))
            except (KeyError, TypeError, ValueError):
                continue
    return out


def build_score(plan: dict, bars: dict) -> dict:
    items = []
    for x in plan["rows"]:
        oc = bars.get(x["code5"])
        rec = dict(code=x["code"], name=x["name"], rank_all=x["rank_all"], rule_a=bool(plan["rule_a"] and plan["rule_a"]["code"] == x["code"]),
                   reasons=x["reasons"], seido=x["seido"], jsf_stop=x["jsf_stop"])
        if not oc:
            rec.update(status="日足なし", open=None, close=None, ret=None, pnl=None)
        else:
            o, c, h = oc
            pinned = o >= x["ul1"] - 1e-9
            ret = (o - c) / o if o else None
            rec.update(open=o, close=c, high=h, pinned=pinned, ret=ret,
                       pnl=None if ret is None else round(SIZE_YEN * ret),
                       status="張り付き見送り" if pinned else "採点")
        items.append(rec)
    return dict(trade_day=plan["trade_day"], asof=plan["asof"], made=datetime.now(JST).strftime("%Y-%m-%d %H:%M"), items=items, sent=False)


def score_payload(score: dict, ledger: dict) -> dict:
    td = datetime.fromisoformat(score["trade_day"])
    lines = [f"寄値→引値 の紙採点（空売り・50万円換算・料/滑りなし・売ったかどうかは問わない）", ""]
    for it in score["items"]:
        head = "🩳ルールA" if it["rule_a"] else f"{it['rank_all']}位"
        if it.get("ret") is None:
            lines.append(f"{head} `{it['code']}` {it['name']}: {it['status']}")
            continue
        flag = " ⛔張り付き(寄値=上限)＝見送り扱い" if it.get("pinned") else ""
        ex = f"（除外: {'・'.join(it['reasons'])}）" if it["reasons"] else ""
        lines.append(f"{head} `{it['code']}` {it['name']}{ex}: 寄 {it['open']:,.0f} → 引 {it['close']:,.0f}　"
                     f"**{it['ret']*100:+.2f}%**　{it['pnl']:+,}円{flag}")
    # 通算（ルールA・張り付きを除く）
    a = []
    for d, s in sorted(ledger.get("scores", {}).items()):
        for it in s.get("items", []):
            if it.get("rule_a") and it.get("ret") is not None and not it.get("pinned") and not it.get("reasons"):
                a.append(it["ret"])
    if a:
        arr = np.array(a)
        w = arr[arr > 0].sum()
        l = -arr[arr < 0].sum()
        pf = (w / l) if l > 0 else float("inf")
        lines += ["", f"通算（ルールA・張り付き除く）{len(arr)}本: 勝率 {(arr > 0).mean()*100:.0f}%　平均 {arr.mean()*100:+.2f}%　"
                      f"合計 {int(SIZE_YEN*arr.sum()):+,}円　PF {pf:.2f}"]
    lines.append("-# 紙の採点。実際の約定・プレミアム料・在庫は反映していない。")
    return {"embeds": [{"title": f"📝 {td:%m/%d(%a)} 売りフェード（友達版）採点", "description": "\n".join(lines)[:4000], "color": 0x4DABF7}]}


# ───────────────────────── Discord / 台帳 ─────────────────────────
def post_discord(payload: dict, dry_run: bool) -> bool:
    hook = os.getenv("DISCORD_WEBHOOK_FADE_URL", "").strip()
    if dry_run or not hook:
        print("---- 送信内容（dry-run / webhook未設定）----")
        for e in payload.get("embeds", []):
            print(f"## {e['title']}\n{e['description']}\n")
        return False
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(hook, data=body, headers={"Content-Type": "application/json",
                                                          "User-Agent": "kabuai-fade-notify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            log(f"discord 送信OK status={r.status}")
            return True
    except urllib.error.HTTPError as e:
        log(f"discord 送信失敗 status={e.code}")     # URL はログに出さない
        return False
    except Exception as e:  # noqa: BLE001
        log(f"discord 送信失敗: {type(e).__name__}")
        return False


def load_ledger() -> dict:
    if LEDGER.exists():
        return json.loads(LEDGER.read_text(encoding="utf-8"))
    return {"plans": {}, "scores": {}}


def save_ledger(led: dict) -> None:
    tmp = LEDGER.with_suffix(".tmp")
    tmp.write_text(json.dumps(led, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, LEDGER)


# ───────────────────────── モード ─────────────────────────
def run_evening(day: date, dry_run: bool, force: bool) -> int:
    """day = 基準日 D（その日の日足で翌営業日の計画）。"""
    if not is_trading_day(day) and not force:
        log(f"{day} は休場日 → 何もしない")
        return 0
    trade_day = next_trading_day(day)
    led = load_ledger()
    prev = led["plans"].get(trade_day.isoformat())
    if prev and prev.get("sent") and not force and not dry_run:
        log(f"{trade_day} の計画は送信済み → skip")
        return 0
    bars = load_bars(day)
    last = bars["Date"].max().date()
    if last != day:
        msg = f"🚨 日足が {last:%m/%d} までしか無く {day:%m/%d} 分がありません（J-Quants 未公開か取得失敗）。{trade_day:%m/%d} の計画は作りません"
        log(msg)
        if not dry_run:
            post_discord({"content": msg}, dry_run)
        return 1
    try:
        master = load_master()
    except Exception as e:  # noqa: BLE001
        log(f"master 取得失敗（市場区分・社名なしで続行）: {e}")
        master = pd.DataFrame(columns=["Code", "CoName", "MktNm", "S17"])
    universe = set(master.loc[master["S17"] != "99", "Code"]) if not master.empty else None
    cand = candidates_for_day(bars, day, universe)
    log(f"候補 {len(cand)} 銘柄")
    earn = fetch_earn_codes(day)
    iss = fetch_iss_map()
    alert = fetch_alert_map()
    jsf_list = fetch_jsf_list()
    plan = build_plan(day, trade_day, cand, master, earn, iss, alert, jsf_list)
    plan["new1"] = new1_plan(cand, master, iss, alert, jsf_list)
    plan["top3"] = top3_plan(cand, master, iss, alert, jsf_list, earn)
    payload = plan_payload(plan)
    sent = post_discord(payload, dry_run)
    plan["sent"] = bool(sent)
    plan["own_sent"] = post_own(plan, dry_run)
    if not dry_run:
        led["plans"][trade_day.isoformat()] = plan
        save_ledger(led)
    return 0


def run_score(day: date, dry_run: bool, force: bool) -> int:
    """day = 取引日（寄値→引値 を採点）。"""
    led = load_ledger()
    plan = led["plans"].get(day.isoformat())
    if not plan:
        log(f"{day} の計画が台帳に無い → 採点なし")
        return 0
    prev = led["scores"].get(day.isoformat())
    if prev and prev.get("sent") and not force and not dry_run:
        log(f"{day} の採点は送信済み → skip")
        return 0
    codes5 = [x["code5"] for x in plan["rows"]]
    try:
        bars = fetch_day_bars_for(codes5, day)
    except Exception as e:  # noqa: BLE001
        log(f"当日日足の取得失敗: {e}")
        bars = {}
    if not bars:
        # 15:40 は J-Quants の当日日足がまだ無いのが普通。通知はせず、19:00 の evening の頭（catch_up）で採点する
        log(f"{day:%m/%d} の日足がまだ無い → 19:00 の evening でまとめて採点")
        return 1
    score = build_score(plan, bars)
    if not dry_run:
        led["scores"][day.isoformat()] = score      # 通算に今日を含めてから本文を作る
    payload = score_payload(score, led if not dry_run else {**led, "scores": {**led.get("scores", {}), day.isoformat(): score}})
    sent = post_discord(payload, dry_run)
    score["sent"] = bool(sent)
    if not dry_run:
        led["scores"][day.isoformat()] = score
        save_ledger(led)
    return 0


def catch_up(day: date, dry_run: bool) -> None:
    """取引日が day 以前で、まだ採点を送っていない計画をまとめて採点する（2026-10-29: 15:40 は日足が未公開で毎回空振りしていた）。"""
    led = load_ledger()
    todo = [d for d, p in sorted(led["plans"].items())
            if d <= day.isoformat() and not (led["scores"].get(d) or {}).get("sent")]
    for d in todo:
        log(f"未採点の {d} を採点")
        run_score(date.fromisoformat(d), dry_run, force=False)


def main() -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.getenv("FADE_NOTIFY_DOTENV", "") or (PARENT / ".env"))   # ワークツリーから動かす時は本体の .env を指す
    except Exception:  # noqa: BLE001
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["evening", "score", "auto", "catchup"], default="auto")
    ap.add_argument("--date", help="evening: 基準日 D（既定=JST今日） / score: 取引日（既定=JST今日）")
    ap.add_argument("--dry-run", action="store_true", help="Discord に送らず表示だけ・台帳も書かない")
    ap.add_argument("--force", action="store_true", help="休場日ガード・送信済みガードを無視")
    a = ap.parse_args()
    now = datetime.now(JST)
    day = datetime.strptime(a.date, "%Y-%m-%d").date() if a.date else now.date()
    mode = a.mode
    if mode == "auto":
        mode = "score" if now.hour < 17 else "evening"
    log(f"mode={mode} date={day} dry_run={a.dry_run}")
    if mode == "evening":
        catch_up(day, a.dry_run)          # 今日の取引日の分と、取りこぼした日の採点を先に
        return run_evening(day, a.dry_run, a.force)
    if mode == "catchup":
        catch_up(day, a.dry_run)
        return 0
    return run_score(day, a.dry_run, a.force)


if __name__ == "__main__":
    sys.exit(main())
