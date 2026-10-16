"""screen_only.py — 「銘柄選別だけ」の Discord 通知（売買判断・自動発注は一切しない）。

候補リストを出すだけ。売買の可否・サイズ・発注は本人の裁量（このスクリプトは注文APIを一切触らない）。

通知の種類:
  ① 08:30 今日の候補（押し目買い用）  5日で−5%以上下げ・株価1,000〜20,000円・前日代金5億円以上・下落率順・上位30
  ② 08:30 ショート候補（参考）        5日で+5%以上上げ・同条件・上昇率順・上位15
  ③ 09:35 ①の絞り込み                 × 見送り: 9:30までの高値が前日終値比+3%以上 / 9:15時点で始値比+1%以上
                                      ◎ 注目:   9:05時点で前日終値比−3%以下   残りは ○（×が優先）
  ④ 毎回末尾に押し目ルールの一文

データ:
  日足   = J-Quants V2 /equities/bars/daily（AdjC・Va=売買代金）。前日終値基準・75日線は終値SMA75。
  5分足  = yfinance（Yahoo）。実測(2026-10-16): ラベルは足の開始時刻・09:00-09:05の足は欠落・配信は約15〜20分遅延。
           → 「9:30までの高値」= 当日始値(日足Open) と 09:05〜09:25ラベル足の高値の最大
             「9:15時点」      = 09:10ラベル足の終値（=9:15の価格）
             「9:05時点」      = 09:05ラベル足の始値（=9:00足の終値に相当。9:00足自体は Yahoo に無い）
           ③は 09:25ラベル足が出揃うまで待ってから判定する（SCREEN_WAIT_MAX_MIN・既定25分）。

環境変数:
  JQUANTS_API_KEY             必須
  DISCORD_WEBHOOK_SCREEN_URL  通知先（未設定なら送らず標準出力のみ）。URLはログに出さない。
  MIN_TURNOVER_OKU            前日売買代金の下限（億円・既定5）
  MAX_MCAP_OKU                時価総額の上限（億円・既定1000=1000億以上は除外・0で無制限）。売りフェードと同じ土俵感
  SCREEN_LOOKBACK             日足の取得営業日数（既定85・75日線＋余裕）
  SCREEN_CACHE_DIR            日足の日別キャッシュ置き場（リプレイ用・任意）
  SCREEN_YF_INSECURE=1        PCのAV SSL傍受対策（curl_cffi verify=False）。CIでは不要。
  JQUANTS_VERIFY_SSL=false    同上（J-Quants側）

使い方:
  python kabuai/screen_only.py --mode morning                 # ①②（当日）
  python kabuai/screen_only.py --mode update                  # ③（当日）
  python kabuai/screen_only.py --mode auto                    # JSTの時刻で morning/update を自動選択（cron用）
  python kabuai/screen_only.py --mode morning --date 2026-10-14 --dry-run   # 過去日リプレイ（送信せず表示）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

JST = timezone(timedelta(hours=9))
HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
JQUANTS_BASE = "https://api.jquants.com/v2"

MIN_TURNOVER_OKU = float(os.getenv("MIN_TURNOVER_OKU", "5"))
MAX_MCAP_OKU = float(os.getenv("MAX_MCAP_OKU", "1000"))   # 0=無制限。超大型(キオクシア/SBG等)をデイトレ土俵から外す
# 💰タグ: 2026-07-17〜10-15 の60日で①候補に押し目ルールを機械的に当てた再生(RESULTS_screen_rule_60d.md)で
# 唯一前半/後半ともプラスだった帯＝前日代金30〜100億(n=88・+0.46%/件・PF1.98)。隣の10〜30億は最悪(PF0.75)で
# 単調でないため噪音の可能性あり＝「勝てる」とは書かない。全体は−0.04%/件・PF0.94・9〜10月はマイナス。
TAG_TOV_LO, TAG_TOV_HI = float(os.getenv("TAG_TOV_LO", "30")), float(os.getenv("TAG_TOV_HI", "100"))
PRICE_MIN, PRICE_MAX = 1_000, 20_000
DIP_PCT, SURGE_PCT = -5.0, 5.0
TOP_DIP, TOP_SURGE = 30, 15
MA_DAYS, MA_BAND = 75, 2.0
LOOKBACK = int(os.getenv("SCREEN_LOOKBACK", "85"))
WAIT_MAX_MIN = float(os.getenv("SCREEN_WAIT_MAX_MIN", "25"))
WAIT_POLL_SEC = 60

RULE_LINE = ("押し目ルール：5分足25MA上向き → 25MAより上の小さい陰線 → 高値+1ティックに逆指値買い／"
             "利確+1.5%（最低+1%）・損切り−3%・14:45手仕舞い")
DISCLAIMER = "※銘柄の選別だけ。売買の判断・発注はしない。投資判断は自己責任。"


# ───────────────────────── 営業日 ─────────────────────────
def _calendar() -> dict:
    p = PARENT / "market_calendar.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p, dtype={"Date": str})
    return dict(zip(df["Date"], df["HolDiv"].astype(int)))


def is_trading_day(d) -> bool:
    cal = _calendar()
    ds = d.strftime("%Y-%m-%d")
    if ds in cal:
        # HolDiv: 1=営業日・2=半日立会・3=祝日取引日(先物のみ・現物は休場)・0=休場
        return cal[ds] in (1, 2)
    import jpholiday
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def trading_days_before(d, n: int) -> list:
    """d より前（d を含まない）の営業日 n 日を昇順で。"""
    out = []
    cur = d - timedelta(days=1)
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
    r = requests.get(f"{JQUANTS_BASE}{path}", headers={"x-api-key": _jq_key()},
                     params=params or {}, timeout=60, verify=verify)
    r.raise_for_status()
    return r.json()


def _is_common(code) -> bool:
    c = str(code).strip()
    return len(c) < 5 or c[4] == "0"


def load_names() -> dict:
    """プライム/スタンダード/グロースの普通株 code4 → 社名（＝東証の全銘柄ユニバース）。"""
    names = {}
    m = _jq_get("/equities/master")
    for it in m.get("data", []):
        mkt = it.get("MktNm", "")
        if not any(k in mkt for k in ("プライム", "スタンダード", "グロース")):
            continue
        if not _is_common(it.get("Code", "")):
            continue
        names[str(it["Code"])[:4]] = it.get("CoName", "")
    return names


def _fetch_day(ds: str) -> list:
    cache_dir = os.getenv("SCREEN_CACHE_DIR", "").strip()
    cp = Path(cache_dir) / f"daily_{ds}.json" if cache_dir else None
    if cp and cp.exists():
        return json.loads(cp.read_text(encoding="utf-8"))
    recs = []
    pk = None
    while True:
        params = {"date": ds}
        if pk:
            params["pagination_key"] = pk
        try:
            d = _jq_get("/equities/bars/daily", params)
        except Exception as e:
            if "429" in str(e):
                print("  [jq] 429 → 60秒待機")
                time.sleep(60)
                continue
            print(f"  [jq] {ds} 取得失敗: {e}")
            break
        recs.extend(d.get("data", []))
        pk = d.get("pagination_key")
        if not pk:
            break
        time.sleep(1.2)
    time.sleep(1.2)
    if cp and recs:
        cp.parent.mkdir(parents=True, exist_ok=True)
        cp.write_text(json.dumps(recs, ensure_ascii=False), encoding="utf-8")
    return recs


def load_daily(days: list) -> pd.DataFrame:
    """code4 × 日付の (close, turnover) ロング形式。"""
    recs = []
    print(f"[jq] 日足 {days[0]}〜{days[-1]}（{len(days)}営業日）取得 …")
    for i, ds in enumerate(days):
        recs.extend(_fetch_day(ds))
        if (i + 1) % 20 == 0:
            print(f"  [jq] {i + 1}/{len(days)}")
    if not recs:
        raise RuntimeError("J-Quants から 0 レコード")
    df = pd.DataFrame(recs)
    df = df[df["Code"].map(_is_common)].copy()
    df["code"] = df["Code"].astype(str).str[:4]
    df["close"] = pd.to_numeric(df["AdjC"], errors="coerce")
    df["turnover"] = pd.to_numeric(df["Va"], errors="coerce") if "Va" in df.columns else float("nan")
    # MktCap は百万円単位（トヨタ 43,945,507 ≒ 43.9兆円で確認・2026-10-16）
    df["mcap"] = pd.to_numeric(df["MktCap"], errors="coerce") if "MktCap" in df.columns else float("nan")
    df = df.dropna(subset=["close"])[["code", "Date", "close", "turnover", "mcap"]]
    return df.sort_values(["code", "Date"])


# ───────────────────────── 選別 ─────────────────────────
def _ma_pos(close: float, ma) -> str:
    if ma is None or pd.isna(ma):
        return "—"
    dev = (close / ma - 1) * 100
    if abs(dev) <= MA_BAND:
        return "±2%以内"
    return "上" if dev > 0 else "下"


def screen(daily: pd.DataFrame, names: dict):
    data_date = str(daily["Date"].max())
    rows = []
    for code, g in daily.groupby("code", sort=False):
        if code not in names:
            continue
        if str(g["Date"].iloc[-1]) != data_date:
            continue  # 前日に出来ていない（上場廃止・売買停止）
        c = g["close"].to_numpy()
        if len(c) < 6:
            continue
        close = float(c[-1])
        r5 = (close / float(c[-6]) - 1) * 100
        tov = g["turnover"].iloc[-1]
        tov_oku = float(tov) / 1e8 if pd.notna(tov) else None
        ma = float(c[-MA_DAYS:].mean()) if len(c) >= MA_DAYS else None
        mc = g["mcap"].iloc[-1]
        mcap_oku = float(mc) / 100 if pd.notna(mc) else None   # 百万円 → 億円
        rows.append({"code": code, "name": names.get(code, ""), "close": close, "r5": r5,
                     "tov_oku": tov_oku, "mcap_oku": mcap_oku, "ma_pos": _ma_pos(close, ma)})

    def base_ok(r: dict) -> bool:
        if MAX_MCAP_OKU > 0 and r["mcap_oku"] is not None and r["mcap_oku"] >= MAX_MCAP_OKU:
            return False
        return (PRICE_MIN <= r["close"] <= PRICE_MAX
                and r["tov_oku"] is not None and r["tov_oku"] >= MIN_TURNOVER_OKU)

    dips = sorted([r for r in rows if base_ok(r) and r["r5"] <= DIP_PCT], key=lambda r: r["r5"])[:TOP_DIP]
    surges = sorted([r for r in rows if base_ok(r) and r["r5"] >= SURGE_PCT], key=lambda r: -r["r5"])[:TOP_SURGE]
    print(f"[screen] 母集団 {len(rows)} 本・押し目候補 {len(dips)}・ショート候補 {len(surges)}（前日 {data_date}）")
    return dips, surges, data_date


def fmt_row(r: dict) -> str:
    mc = f" 時総{r['mcap_oku']:,.0f}億" if r.get("mcap_oku") is not None else ""
    tag = "💰" if (r.get("tov_oku") is not None and TAG_TOV_LO <= r["tov_oku"] < TAG_TOV_HI) else "　"
    return (f"{tag}`{r['code']}` {r['name'][:10]} ¥{r['close']:,.0f} "
            f"5日{r['r5']:+.1f}% 代金{r['tov_oku']:.1f}億{mc} 75日線:{r['ma_pos']}")


# ───────────────────────── 5分足（③） ─────────────────────────
def _yf_session():
    if os.getenv("SCREEN_YF_INSECURE", "") == "1":
        from curl_cffi import requests as cr
        return cr.Session(impersonate="chrome", verify=False)
    return None


def fetch_intraday(codes: list, day):
    """当日の 5分足 {code: df(JST index)} と 当日始値 {code: open}（日足Open）。"""
    import warnings
    warnings.filterwarnings("ignore")
    import yfinance as yf
    tickers = [c + ".T" for c in codes]
    start, end = day.strftime("%Y-%m-%d"), (day + timedelta(days=1)).strftime("%Y-%m-%d")
    kw = dict(tickers=tickers, start=start, end=end, group_by="ticker", auto_adjust=False,
              progress=False, threads=False, session=_yf_session())
    bars = {}
    opens = {}
    try:
        df = yf.download(interval="5m", **kw)
    except Exception as e:
        print(f"[yf] 5分足取得失敗: {e}")
        df = None
    if df is not None and len(df):
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        df = df.tz_convert("Asia/Tokyo")
        for c, t in zip(codes, tickers):
            try:
                sub = df[t] if len(tickers) > 1 else df
                sub = sub.dropna(subset=["Close"])
            except Exception:
                continue
            if len(sub):
                bars[c] = sub
    try:
        dd = yf.download(interval="1d", **kw)
        if dd is not None and len(dd):
            for c, t in zip(codes, tickers):
                try:
                    sub = dd[t] if len(tickers) > 1 else dd
                    o = sub["Open"].dropna()
                    if len(o):
                        opens[c] = float(o.iloc[0])
                except Exception:
                    pass
    except Exception as e:
        print(f"[yf] 日足Open取得失敗: {e}")
    return bars, opens


def _n_with_0925(bars: dict) -> int:
    return sum(1 for b in bars.values() if len(b) and (b.index.strftime("%H:%M") >= "09:25").any())


def _bars_complete(bars: dict, n_total: int) -> bool:
    """09:25ラベル足（9:25-9:30）が出揃ったか（分足のある銘柄の8割以上）。"""
    have = [b for b in bars.values() if len(b)]
    if not have or len(have) < max(1, int(n_total * 0.5)):
        return False
    return _n_with_0925(bars) >= len(have) * 0.8


def refine(dips: list, day, live: bool):
    codes = [r["code"] for r in dips]
    note = ""
    deadline = datetime.now(JST) + timedelta(minutes=WAIT_MAX_MIN)
    while True:
        bars, opens = fetch_intraday(codes, day)
        if _bars_complete(bars, len(codes)) or not live:
            break
        if datetime.now(JST) >= deadline:
            note = "⚠️ 9:30までの足が出揃う前に締め切り（Yahooの遅延）。揃った分で判定。"
            break
        print(f"[yf] 09:25足 {_n_with_0925(bars)}/{len(bars)} 本 … {WAIT_POLL_SEC}秒待って再取得")
        time.sleep(WAIT_POLL_SEC)

    out = []
    for r in dips:
        b = bars.get(r["code"])
        x = dict(r, mark="○", why="", open=None, h0930=None, p0905=None, p0915=None)
        if b is None or not len(b):
            x["why"] = "分足なし"
            out.append(x)
            continue
        lab = b.index.strftime("%H:%M")
        prev = r["close"]
        o = opens.get(r["code"], float(b["Open"].iloc[0]))
        m = b[lab <= "09:25"]
        h0930 = max(float(m["High"].max()) if len(m) else o, o)
        first = b[lab == "09:05"]
        p0905 = float(first["Open"].iloc[0]) if len(first) else float(b["Close"].iloc[0])
        b0910 = b[lab <= "09:10"]
        p0915 = float(b0910["Close"].iloc[-1]) if len(b0910) else None
        x.update(open=o, h0930=h0930, p0905=p0905, p0915=p0915)
        why = []
        if h0930 / prev - 1 >= 0.03:
            why.append(f"9:30高値{(h0930 / prev - 1) * 100:+.1f}%")
        if p0915 is not None and p0915 / o - 1 >= 0.01:
            why.append(f"9:15始値比{(p0915 / o - 1) * 100:+.1f}%")
        if why:
            x["mark"], x["why"] = "×", "・".join(why)
        elif p0905 / prev - 1 <= -0.03:
            x["mark"], x["why"] = "◎", f"9:05 {(p0905 / prev - 1) * 100:+.1f}%"
        out.append(x)
    return out, note


def fmt_refined(x: dict) -> str:
    prev = x["close"]
    if x["open"] is None:
        return f"{x['mark']} `{x['code']}` {x['name'][:10]} 前¥{prev:,.0f}（分足なし・○扱い）"
    g = (x["open"] / prev - 1) * 100
    p05 = f"{(x['p0905'] / prev - 1) * 100:+.1f}%" if x["p0905"] else "—"
    p15 = f"{(x['p0915'] / x['open'] - 1) * 100:+.1f}%" if x["p0915"] else "—"
    h = f"{(x['h0930'] / prev - 1) * 100:+.1f}%"
    return (f"{x['mark']} `{x['code']}` {x['name'][:10]} 前¥{prev:,.0f} 寄{g:+.1f}% "
            f"9:05 {p05} / 9:15始値比 {p15} / 9:30高 {h} 75日線:{x['ma_pos']}")


# ───────────────────────── Discord ─────────────────────────
def post_discord(payload: dict, dry_run: bool) -> None:
    hook = os.getenv("DISCORD_WEBHOOK_SCREEN_URL", "").strip()
    if dry_run or not hook:
        print("---- 送信内容（dry-run / webhook未設定）----")
        for e in payload.get("embeds", []):
            print(f"## {e['title']}\n{e['description']}\n")
        return
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(hook, data=body, headers={"Content-Type": "application/json",
                                                          "User-Agent": "kabuai-screen/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print(f"[discord] 送信OK status={r.status}")
    except urllib.error.HTTPError as e:
        print(f"[discord] 送信失敗 status={e.code}")  # URLは出さない
        raise SystemExit(1)


def _embed(title: str, body: str, color: int) -> dict:
    desc = body + "\n\n" + RULE_LINE + "\n" + DISCLAIMER
    return {"title": title, "description": desc[:4000], "color": color}


def morning_payload(dips, surges, day, data_date) -> dict:
    hdr = (f"条件: 5日(前日÷6営業日前)で−{abs(DIP_PCT):.0f}%以下・前日終値{PRICE_MIN:,}〜{PRICE_MAX:,}円・"
           f"前日代金{MIN_TURNOVER_OKU:g}億以上"
           + (f"・時価総額{MAX_MCAP_OKU:,.0f}億未満" if MAX_MCAP_OKU > 0 else "")
           + f"・下落率順・上位{TOP_DIP}\n前日={data_date} 終値基準\n"
           f"💰=前日代金{TAG_TOV_LO:g}〜{TAG_TOV_HI:g}億（60日の機械再生で唯一プラスの帯・n=88・噪音の可能性あり。全体は−0.04%/件）\n\n")
    d_body = hdr + ("\n".join(fmt_row(r) for r in dips) if dips else "該当なし")
    s_hdr = f"条件: 5日で+{SURGE_PCT:.0f}%以上・同じ株価/代金条件・上昇率順・上位{TOP_SURGE}\n\n"
    s_body = s_hdr + ("\n".join(fmt_row(r) for r in surges) if surges else "該当なし")
    ds = day.strftime("%m/%d")
    return {"embeds": [
        _embed(f"🔎 {ds} 今日の候補（押し目買い用）{len(dips)}本 — 銘柄選別のみ", d_body, 0x4DABF7),
        _embed(f"🩳 {ds} ショート候補（参考）{len(surges)}本", s_body, 0xFF8787),
    ]}


def update_payload(refined, day, data_date, note: str) -> dict:
    hot = [x for x in refined if x["mark"] == "◎"]
    mid = [x for x in refined if x["mark"] == "○"]
    skip = [x for x in refined if x["mark"] == "×"]
    lines = [f"◎注目 {len(hot)}本（9:05時点で前日比−3%以下）"] + [fmt_refined(x) for x in hot]
    lines += ["", f"○ {len(mid)}本"] + [fmt_refined(x) for x in mid]
    lines += ["", f"×見送り {len(skip)}本（9:30までの高値+3%以上 / 9:15で始値比+1%以上）"]
    if note:
        lines.insert(0, note + "\n")
    body = f"前日={data_date} 終値基準・5分足はYahoo(約20分遅延)\n\n" + "\n".join(lines)
    title = f"🔄 {day.strftime('%m/%d')} 9:35更新 押し目候補の絞り込み（◎{len(hot)}/○{len(mid)}/×{len(skip)}）"
    return {"embeds": [_embed(title, body, 0x69DB7C)]}


# ───────────────────────── main ─────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["morning", "update", "auto"], default="auto")
    ap.add_argument("--date", help="リプレイ日 YYYY-MM-DD（省略=JST今日）")
    ap.add_argument("--dry-run", action="store_true", help="Discordに送らず表示だけ")
    ap.add_argument("--force", action="store_true", help="休場日ガードを無視")
    a = ap.parse_args()

    now = datetime.now(JST)
    day = datetime.strptime(a.date, "%Y-%m-%d").date() if a.date else now.date()
    live = not a.date or day == now.date()
    mode = a.mode
    if mode == "auto":
        mode = "morning" if now.hour < 9 or (now.hour == 9 and now.minute < 10) else "update"
    print(f"[screen_only] mode={mode} day={day} live={live} dry_run={a.dry_run} MIN_TURNOVER_OKU={MIN_TURNOVER_OKU:g}")

    if not is_trading_day(day) and not a.force:
        print(f"{day} は休場日 → 通知なし")
        return 0

    names = load_names()
    daily = load_daily(trading_days_before(day, LOOKBACK))
    dips, surges, data_date = screen(daily, names)
    if mode == "morning":
        post_discord(morning_payload(dips, surges, day, data_date), a.dry_run)
        return 0
    if not dips:
        print("①候補なし → ③は送らない")
        return 0
    refined, note = refine(dips, day, live)
    post_discord(update_payload(refined, day, data_date, note), a.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
