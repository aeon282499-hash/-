"""🌙 キオクシア打法（引け成行で買い → 翌朝の寄り成行で売る）の紙運用・通知（2026-10-08）

ルール（本人の⑤）: 15:20 時点で 前日比プラス(現在値 ≥ 前日終値) かつ 始値 < 前日終値×1.03 なら今夜買い。
  陽線(現在値 > 始値)は「本命」として印を付けるだけで、陰線でも⑤を満たせば買う。
  ディフェンシブ化（10/8 検証・全期間161夜: 累計619万→588万 / DD −116万→−33万 / 最悪夜 −11.5%→−4.1%）:
    株数 = 基準金額 × (3% ÷ 直近10日の平均値幅率) を 0.3〜1.5倍に制限 → 100株単位（ボラ連動）。
    参考列として 固定300株 と 日経ETF(1321)等金額空売りヘッジ も紙で並走記録する。
  見送り: kioxia_overnight_skip.txt に書いた日付（決算日など・1行1日 YYYY-MM-DD）。
発注は一切しない。通知は DISCORD_WEBHOOK_KIOXIA_URL（無ければ DISCORD_WEBHOOK_GOKUJO_URL＝俺専用の極上ch）。

使い方:
  python kioxia_overnight.py --judge            # 平日15:20（Windowsタスク KioxiaOvernightJudge）
  python kioxia_overnight.py --settle           # 平日 9:12（Windowsタスク KioxiaOvernightSettle）前夜の紙を翌寄りで決済
  python kioxia_overnight.py --judge --dry      # 通知もファイル更新もしない
  python kioxia_overnight.py --replay 2026-04-08 2026-10-08   # J-Quants日足で本人のレポートと同じ条件を再計算（確定終値判定）
  python kioxia_overnight.py --test             # ロジックの自己テスト

価格ソース: live_flow/minutes/YYYY-MM-DD.jsonl（LiveFlow の毎分断面 s["285A"] = [現在値, 始値, 高値, 安値, 出来高, 代金, VWAP, 前日終値]）
  → 無ければ立花APIで直接取得 → それも無理なら通知にエラーを出す。
  日足(ATR・再計算・決済の予備) は J-Quants v2 /equities/bars/daily（AdjO/AdjH/AdjL/AdjC）。
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:
    pass

CODE = "285A"            # キオクシアHD
HEDGE = "1321"           # 日経225連動型上場投信（ヘッジの参考列）
JQ_CODE = {"285A": "285A0", "1321": "13210"}
GAP_MAX = 1.03           # 始値 < 前日終値×1.03
ATR_N, ATR_BASE, SZ_MIN, SZ_MAX = 10, 0.03, 0.3, 1.5
UNIT = 100
BASE_YEN = int(os.environ.get("KIOXIA_BASE_YEN", "5370000"))   # ボラ連動の基準金額（=300株相当・紙）
FIXED_SHARES = int(os.environ.get("KIOXIA_FIXED_SHARES", "100"))   # 本人の実弾は固定株数（10/9〜 100株）
WEBHOOK_ENVS = ("DISCORD_WEBHOOK_KIOXIA_URL", "DISCORD_WEBHOOK_GOKUJO_URL")
STATE = ROOT / "kioxia_overnight_state.json"
LOG = ROOT / "kioxia_overnight_log.csv"
SKIP = ROOT / "kioxia_overnight_skip.txt"
CACHE = ROOT / "_kioxia_daily_cache.pkl"
MINUTES = ROOT / "live_flow" / "minutes"
COLS = ["date", "ts", "signal", "reason", "prev_close", "open", "last", "yosen", "pos", "atr_pct", "sz", "shares_vol", "shares_fixed",
        "entry", "hedge_px", "hedge_shares", "exit_date", "exit_open", "hedge_exit", "ov_pct", "pnl_fixed", "pnl_vol", "pnl_hedge",
        "official_close", "pnl_official_vol"]


def log(msg: str) -> None:
    print(f"[kioxia {datetime.now():%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------- 通知
def send(text: str, dry: bool = False) -> bool:
    if dry:
        print("---- (dry) Discord ----\n" + text + "\n----", flush=True)
        return False
    url = ""
    for k in WEBHOOK_ENVS:
        url = os.environ.get(k, "").strip()
        if url:
            break
    if not url:
        log("webhook未設定・通知なし")
        return False
    try:
        import requests
        r = requests.post(url, json={"content": text[:1990]}, timeout=10)
        return r.status_code in (200, 204)
    except Exception as e:  # noqa: BLE001
        log(f"Discord送信失敗: {e}")
        return False


# ---------------------------------------------------------------- 日足（J-Quants）
def jq_daily(code: str, start: str, end: str):
    """J-Quants v2 日足を DataFrame(index=Date, cols O/H/L/C 調整後) で返す。キャッシュは当日1回だけ更新。"""
    import pandas as pd
    import requests
    import urllib3
    urllib3.disable_warnings()
    key = os.environ.get("JQUANTS_API_KEY", "").strip()
    cache = {}
    if CACHE.exists():
        try:
            cache = pd.read_pickle(CACHE)
        except Exception:
            cache = {}
    ck = f"{code}:{start}:{end}:{datetime.now():%Y-%m-%d}"
    if ck in cache:
        return cache[ck]
    rows, p = [], {"code": JQ_CODE.get(code, code), "from": start, "to": end}
    while True:
        r = requests.get("https://api.jquants.com/v2/equities/bars/daily", headers={"x-api-key": key}, params=p, timeout=(10, 60), verify=False)
        r.raise_for_status()
        j = r.json()
        rows += j.get("data", [])
        if "pagination_key" in j:
            p["pagination_key"] = j["pagination_key"]
        else:
            break
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date").sort_index()[["AdjO", "AdjH", "AdjL", "AdjC"]].rename(columns={"AdjO": "O", "AdjH": "H", "AdjL": "L", "AdjC": "C"}).astype(float)
    cache = {k: v for k, v in cache.items() if k.endswith(f"{datetime.now():%Y-%m-%d}")}
    cache[ck] = df
    try:
        pd.to_pickle(cache, CACHE)
    except Exception:
        pass
    return df


def atr_pct(df, upto: str) -> float | None:
    """直近 ATR_N 日（upto より前の確定日足）の (高値−安値)÷終値 の平均。"""
    x = df[df.index < upto].tail(ATR_N)
    if len(x) < ATR_N:
        return None
    return float(((x["H"] - x["L"]) / x["C"]).mean())


def size_mult(atr: float | None) -> float:
    if not atr or atr <= 0:
        return 1.0
    return float(min(SZ_MAX, max(SZ_MIN, ATR_BASE / atr)))


def shares_for(yen: float, px: float) -> int:
    n = int(yen / px / UNIT) * UNIT
    return max(UNIT, n)


# ---------------------------------------------------------------- 場中の値（LiveFlow断面 → 立花API）
def quote_from_minutes(code: str, day: str, first_open: bool = False) -> dict | None:
    f = MINUTES / f"{day}.jsonl"
    if not f.exists():
        return None
    lines = f.read_text(encoding="utf-8").splitlines()
    seq = lines if first_open else reversed(lines)
    for ln in seq:
        try:
            r = json.loads(ln)
            v = r.get("s", {}).get(code)
        except Exception:
            continue
        if not v or len(v) < 8:
            continue
        last, op, hi, lo, _vo, _va, _vw, pc = v[:8]
        if first_open:
            if op is None:
                continue
            return {"ts": r["ts"], "open": float(op), "last": float(last) if last else None, "prev_close": float(pc) if pc else None}
        if last is None or pc is None:
            continue
        return {"ts": r["ts"], "last": float(last), "open": float(op) if op else None, "high": float(hi) if hi else None,
                "low": float(lo) if lo else None, "prev_close": float(pc)}
    return None


def quote_from_api(code: str) -> dict | None:
    try:
        from tachibana import TachibanaClient
        c = TachibanaClient()
        c.login()
        try:
            rows = c.market_price([code], ("pDPP", "pDOP", "pDHP", "pDLP", "pPRP"))
        finally:
            try:
                c.logout()
            except Exception:
                pass
        if not rows:
            return None
        r = rows[0]
        f = lambda k: float(r[k]) if r.get(k) not in (None, "") else None  # noqa: E731
        return {"ts": f"{datetime.now():%Y-%m-%d %H:%M:%S}", "last": f("pDPP"), "open": f("pDOP"), "high": f("pDHP"), "low": f("pDLP"), "prev_close": f("pPRP")}
    except Exception as e:  # noqa: BLE001
        log(f"立花API取得失敗: {e}")
        return None


def get_quote(code: str, day: str) -> dict | None:
    q = quote_from_minutes(code, day)
    if q and q.get("last") and q.get("prev_close"):
        return q
    return quote_from_api(code)


# ---------------------------------------------------------------- 判定
def judge_rule(prev_close: float, open_: float | None, last: float) -> tuple[bool, str, bool]:
    """⑤: 前日比≥0 かつ 始値<前日終値×1.03。戻り: (買うか, 理由, 陽線か)"""
    if open_ is None:
        return False, "始値なし", False
    up = last >= prev_close
    gap_ok = open_ < prev_close * GAP_MAX
    yosen = last > open_
    if up and gap_ok:
        return True, "⑤ 前日比プラス・始値+3%未満" + ("・陽線=本命" if yosen else "・陰線"), yosen
    why = []
    if not up:
        why.append(f"前日比マイナス {last / prev_close - 1:+.2%}")
    if not gap_ok:
        why.append(f"始値が前日終値比 {open_ / prev_close - 1:+.2%} ≥ +3%")
    return False, " / ".join(why), yosen


def skip_dates() -> set[str]:
    if not SKIP.exists():
        return set()
    out = set()
    for ln in SKIP.read_text(encoding="utf-8").splitlines():
        s = ln.strip().split("#")[0].strip()
        if len(s) == 10:
            out.add(s)
    return out


def read_log() -> list[dict]:
    if not LOG.exists():
        return []
    with open(LOG, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_log(rows: list[dict]) -> None:
    with open(LOG, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in COLS})


def stats_text(rows: list[dict]) -> str:
    done = [r for r in rows if r.get("signal") == "BUY" and r.get("pnl_vol") not in ("", None)]
    if not done:
        return "紙の実績: まだ決済なし"
    pv = [float(r["pnl_vol"]) for r in done]
    pf = [float(r["pnl_fixed"]) for r in done]
    ph = [float(r["pnl_hedge"]) for r in done if r.get("pnl_hedge") not in ("", None)]
    win = sum(1 for x in pf if x > 0)
    cum, peak, dd = 0.0, 0.0, 0.0
    for x in pf:
        cum += x
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    last20 = sum(pf[-20:])
    s = (f"紙の実績 {len(pf)}夜 勝率{win / len(pf):.0%}  固定株数 {sum(pf) / 1e4:+.1f}万(DD {dd / 1e4:.1f}万・直近20夜 {last20 / 1e4:+.1f}万)"
         f" ／ ボラ連動 {sum(pv) / 1e4:+.1f}万")
    if ph:
        s += f" ／ +日経ヘッジ {sum(ph) / 1e4:+.1f}万"
    if len(pv) >= 20 and last20 < 0:
        s += "\n⚠️ 直近20夜の合計がマイナス＝停止ラインの目安"
    return s


def judge(now: datetime, dry: bool) -> int:
    day = f"{now:%Y-%m-%d}"
    if day in skip_dates():
        send(f"🌙 キオクシア打法 {now:%m/%d}: ⛔ 見送り（kioxia_overnight_skip.txt の指定日）", dry)
        return 0
    q = get_quote(CODE, day)
    if not q:
        if not (MINUTES / f"{day}.jsonl").exists():
            log("今日の分足断面なし＝休場の可能性・無言で終了")
            return 0
        send(f"🌙 キオクシア打法 {now:%m/%d}: ❌ 値が取れない（LiveFlow断面・立花APIとも）＝判定不能", dry)
        return 1
    buy, reason, yosen = judge_rule(q["prev_close"], q.get("open"), q["last"])
    pos = None
    if q.get("high") and q.get("low") and q["high"] > q["low"]:
        pos = (q["last"] - q["low"]) / (q["high"] - q["low"])
    # ATR と株数
    start = (now - timedelta(days=40)).strftime("%Y-%m-%d")
    atr = None
    try:
        df = jq_daily(CODE, start, day)
        atr = atr_pct(df, day)
    except Exception as e:  # noqa: BLE001
        log(f"J-Quants日足失敗（ATRなし→1.0倍）: {e}")
    sz = size_mult(atr)
    sh_vol = shares_for(BASE_YEN * sz, q["last"])
    hq = get_quote(HEDGE, day) if buy else None
    hedge_px = hq["last"] if hq and hq.get("last") else None
    hedge_sh = int(sh_vol * q["last"] / hedge_px) if hedge_px else None
    chg = q["last"] / q["prev_close"] - 1
    gap = (q["open"] / q["prev_close"] - 1) if q.get("open") else float("nan")
    head = f"🌙 キオクシア打法 {now:%m/%d} {q['ts'][11:16]}時点"
    body = (f"前日終値 {q['prev_close']:,.0f} → 始値 {q.get('open') or 0:,.0f}({gap:+.1%}) → 現在 {q['last']:,.0f}({chg:+.1%}) "
            f"{'陽線' if yosen else '陰線'}" + (f"・レンジ位置{pos:.0%}" if pos is not None else ""))
    if buy:
        msg = (f"{head}\n✅ **今夜買い（引け成行）→ 明朝 寄り成行で全株売り**\n{body}\n"
               f"株数 **{FIXED_SHARES}株（固定）** ≈{FIXED_SHARES * q['last'] / 1e4:,.0f}万円"
               f" ／ 参考: ボラ連動なら{sh_vol:,}株（直近{ATR_N}日の値幅率 {atr * 100 if atr else 0:.1f}% → {sz:.2f}倍）\n{reason}")
        if hedge_px:
            msg += f"\n参考ヘッジ: 1321 を {hedge_sh:,}口 空売り（{hedge_px:,.0f}円・等金額）"
        msg += "\n⚠️ 1夜で−12%の窓はあり得る。株数は固定・翌朝は必ず全部売る"
    else:
        msg = f"{head}\n⚪ 今夜は見送り: {reason}\n{body}"
    rows = read_log()
    msg += "\n" + stats_text(rows)
    send(msg, dry)
    if dry:
        return 0
    rows = [r for r in rows if r.get("date") != day]
    rows.append({"date": day, "ts": q["ts"], "signal": "BUY" if buy else "SKIP", "reason": reason, "prev_close": q["prev_close"], "open": q.get("open") or "",
                 "last": q["last"], "yosen": int(yosen), "pos": f"{pos:.3f}" if pos is not None else "", "atr_pct": f"{atr * 100:.3f}" if atr else "",
                 "sz": f"{sz:.3f}", "shares_vol": sh_vol if buy else "", "shares_fixed": FIXED_SHARES if buy else "", "entry": q["last"] if buy else "",
                 "hedge_px": hedge_px or "", "hedge_shares": hedge_sh or ""})
    write_log(rows)
    STATE.write_text(json.dumps({"date": day, "signal": "BUY" if buy else "SKIP", "entry": q["last"], "shares_vol": sh_vol, "hedge_px": hedge_px,
                                 "hedge_shares": hedge_sh, "status": "open" if buy else "none", "ts": q["ts"]}, ensure_ascii=False, indent=1), encoding="utf-8")
    log(f"判定 {'BUY' if buy else 'SKIP'}: {reason}")
    return 0


# ---------------------------------------------------------------- 決済（翌朝の寄り）
def settle(now: datetime, dry: bool) -> int:
    rows = read_log()
    pend = [r for r in rows if r.get("signal") == "BUY" and r.get("exit_open") in ("", None) and r["date"] < f"{now:%Y-%m-%d}"]
    if not pend:
        log("決済待ちなし")
        return 0
    day = f"{now:%Y-%m-%d}"
    q = quote_from_minutes(CODE, day, first_open=True)
    if not q:
        # 予備: J-Quants の当日日足（夕方以降に取れる）
        try:
            df = jq_daily(CODE, day, day)
            if not df.empty:
                q = {"ts": day + " 15:30:00", "open": float(df["O"].iloc[-1]), "prev_close": None}
        except Exception as e:  # noqa: BLE001
            log(f"J-Quants予備も失敗: {e}")
    if not q:
        q = quote_from_api(CODE)
    if not q or not q.get("open"):
        send(f"🏁 キオクシア打法 {now:%m/%d}: ❌ 寄り値が取れない＝決済未記録（後で --settle を再実行）", dry)
        return 1
    hq = quote_from_minutes(HEDGE, day, first_open=True) or {}
    hedge_exit = hq.get("open")
    out = []
    for r in pend:
        ent = float(r["entry"]); ex = float(q["open"])
        ov = ex / ent - 1
        r["exit_date"], r["exit_open"], r["ov_pct"] = day, ex, f"{ov * 100:.3f}"
        r["pnl_fixed"] = round((ex - ent) * int(r["shares_fixed"]))
        r["pnl_vol"] = round((ex - ent) * int(r["shares_vol"]))
        if hedge_exit and r.get("hedge_px") and r.get("hedge_shares"):
            r["hedge_exit"] = hedge_exit
            r["pnl_hedge"] = round(float(r["pnl_vol"]) - (float(hedge_exit) - float(r["hedge_px"])) * int(r["hedge_shares"]))
        # 本人レポート方式（確定終値で買った扱い）
        try:
            df = jq_daily(CODE, r["date"], r["date"])
            if not df.empty:
                oc = float(df["C"].iloc[-1])
                r["official_close"] = oc
                r["pnl_official_vol"] = round((ex - oc) * int(r["shares_vol"]))
        except Exception:
            pass
        out.append(r)
    if not dry:
        write_log(rows)
        st = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
        st["status"] = "settled"; st["exit_open"] = q["open"]; st["exit_date"] = day
        STATE.write_text(json.dumps(st, ensure_ascii=False, indent=1), encoding="utf-8")
    r = out[-1]
    pv = float(r["pnl_vol"]); pf_ = float(r["pnl_fixed"]); ov = float(r["ov_pct"])
    emoji = "🟢" if pf_ > 0 else ("🔴" if pf_ < 0 else "⚪")
    msg = (f"🏁 キオクシア打法 {r['date'][5:]}夜 → {now:%m/%d}寄り {emoji} **{pf_ / 1e4:+.1f}万円**（{ov:+.2f}%・{int(r['shares_fixed'])}株固定）\n"
           f"買 {float(r['entry']):,.0f} → 寄 {float(r['exit_open']):,.0f} ／ ボラ連動({int(r['shares_vol']):,}株)なら {pv / 1e4:+.1f}万")
    if r.get("pnl_hedge") not in ("", None):
        msg += f" ／ +日経ヘッジなら {float(r['pnl_hedge']) / 1e4:+.1f}万"
    if r.get("official_close") not in ("", None):
        oc_ = float(r["official_close"])
        msg += f"\n（確定終値 {oc_:,.0f} で買った扱いなら {(float(r['exit_open']) - oc_) * int(r['shares_fixed']) / 1e4:+.1f}万＝15:20判定との差）"
    msg += "\n" + stats_text(rows)
    send(msg, dry)
    log(f"決済 {r['date']} → {day}: {pv:+,.0f}円")
    return 0


# ---------------------------------------------------------------- 再計算（本人レポート方式・確定終値で判定）
def replay(start: str, end: str) -> int:
    import pandas as pd
    df = jq_daily(CODE, (pd.Timestamp(start) - pd.Timedelta(days=30)).strftime("%Y-%m-%d"), end)
    d = df.copy(); d["pc"] = d["C"].shift(1); d["no"] = d["O"].shift(-1)
    d["atr"] = ((d["H"] - d["L"]) / d["C"]).rolling(ATR_N).mean().shift(1)
    d = d.loc[start:end].dropna(subset=["pc", "no"])
    n = w = 0; pf = pv = 0.0; cum = peak = dd = 0.0
    for day, r in d.iterrows():
        buy, reason, _ = judge_rule(r["pc"], r["O"], r["C"])
        if not buy:
            continue
        sz = size_mult(r["atr"] if r["atr"] == r["atr"] else None)
        sh = shares_for(BASE_YEN * sz, r["C"])
        p_fixed = (r["no"] - r["C"]) * FIXED_SHARES; p_vol = (r["no"] - r["C"]) * sh
        n += 1; w += p_fixed > 0; pf += p_fixed; pv += p_vol
        cum += p_vol; peak = max(peak, cum); dd = min(dd, cum - peak)
    print(f"{start}〜{end} ⑤: {n}回 勝率{w / max(n, 1):.1%} 固定{FIXED_SHARES}株 {pf:+,.0f}円 ／ ボラ連動({BASE_YEN:,}円基準) {pv:+,.0f}円 DD {dd:,.0f}円")
    return 0


# ---------------------------------------------------------------- 自己テスト
def test() -> int:
    assert judge_rule(100, 101, 100)[0] is True          # 同値・始値+1% → 買い
    assert judge_rule(100, 102.9, 105)[0] is True
    assert judge_rule(100, 103.0, 105)[0] is False       # +3%ちょうどは対象外
    assert judge_rule(100, 99, 99.9)[0] is False         # 前日比マイナス
    assert judge_rule(100, 101, 100.5)[2] is False       # 陰線（本命ではないが買い）
    assert judge_rule(100, None, 100)[0] is False
    assert abs(size_mult(0.03) - 1.0) < 1e-9 and abs(size_mult(0.06) - 0.5) < 1e-9
    assert size_mult(0.20) == SZ_MIN and size_mult(0.001) == SZ_MAX and size_mult(None) == 1.0
    assert shares_for(5_370_000, 17_900) == 300 and shares_for(10, 17_900) == 100
    assert quote_from_minutes("285A", "1900-01-01") is None
    print("kioxia_overnight: self-test OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--judge", action="store_true"); ap.add_argument("--settle", action="store_true")
    ap.add_argument("--replay", nargs=2, metavar=("START", "END")); ap.add_argument("--test", action="store_true")
    ap.add_argument("--dry", action="store_true"); ap.add_argument("--date", help="判定/決済の日付を指定（再生用 YYYY-MM-DD）")
    a = ap.parse_args()
    now = datetime.strptime(a.date, "%Y-%m-%d").replace(hour=15, minute=20) if a.date else datetime.now()
    if a.test:
        return test()
    if a.replay:
        return replay(*a.replay)
    if a.judge:
        return judge(now, a.dry)
    if a.settle:
        return settle(now, a.dry)
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
