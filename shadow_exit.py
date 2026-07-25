# -*- coding: utf-8 -*-
"""shadow_exit.py — 出口ボラ正規化の「紙並走」台帳（2026-07-25・本人指示=案2）。

■ 何をするものか
  本番の損切りは今まで通り一律 -3%（配信も帳簿も発注も一切変えない）。
  そのすぐ横で「もし損切りを ATR%×2.0（下限2.0%）にしていたら、この玉はどうなっていたか」
  だけを別台帳に記録し続ける。数ヶ月ぶん貯めてから、実績と突き合わせて採否を決める。

■ 根拠（10年BT・2026-07-25）
  損切りを銘柄の値動きの大きさに比例させると、大100万で10年 +234.5万 → +345.3万、
  最悪3年 -116.2万 → -78.1万。勝ち7/10年。同じ平均幅の「固定%」では逆に現行より悪化する
  ＝効いているのは幅ではなく「銘柄ごとに変えること」自体。検証は _bt_atr_exit_*.py 群。
  ただし2025年は現行の方が良かった（+104.9 vs +87.9万）ため、実物を見てから決める。

■ 非破壊の担保
  ・本ファイルは positions*.json / today_signals*.json を **読むだけ**。書くのは shadow_exit_*.json のみ。
  ・main.py 側は末尾で try/except に包んで呼ぶだけ（例外は握り潰す＝朝の配信を絶対に止めない）。
  ・Discord配信・発注指示・実帳簿には一切関与しない。

■ 限界（結果を読むときの注意）
  記録するのは「本番が実際に採用した玉」だけ。出口が変われば決済日がズレ、空く枠も変わるので
  本来は選ぶ銘柄自体が変わる（10年BTでは2026年で134件→130件）。この台帳が測れるのは
  “同じ玉を違う損切りで持ったらどうだったか”＝**出口単独の効果**であって、BTの差分とは一致しない。

実行:
  python -X utf8 shadow_exit.py --report     # 実績との突き合わせを表示
  python -X utf8 shadow_exit.py --backfill   # 既存 positions から過去分を再構成（初回のみ）
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pandas as pd

# ── 検証で確定したパラメータ（BT: _bt_atr_exit_kt.py の最終形）───────────────
ATR_MULT      = 2.0     # 損切り幅 = ATR% × これ
STOP_FLOOR    = 2.0     # 下限%（これ未満は板の値幅の中でBTが約定を再現できない）
STOP_CEIL     = 15.0    # 暴走防止のサニティ上限（入口ATRキャップ3.0%なので実質6%が最大）
TAKE_PROFIT   = 5.0     # 利確は現行のまま（BTで「TP据置が最良」と確定）
MAX_HOLD      = 3
ATR_PERIOD    = 14
LIVE_STOP     = 3.0     # 比較対象＝本番の一律損切り

TIER_FILES = {
    "main":  ("today_signals.json",        "positions.json",        1_000_000, "大資金"),
    "mid":   ("today_signals_mid.json",    "positions_mid.json",      500_000, "中資金"),
    "small": ("today_signals_small.json",  "positions_small.json",    300_000, "小資金"),
}


def ledger_path(key: str) -> str:
    return f"shadow_exit_{key}.json"


def load_ledger(key: str) -> list[dict]:
    p = ledger_path(key)
    if not os.path.exists(p):
        return []
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_ledger(key: str, rows: list[dict]) -> None:
    with open(ledger_path(key), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def atr_pct_at(df: pd.DataFrame, upto: str) -> float | None:
    """シグナル日(=エントリー前日)までのデータで ATR(14)/終値×100 を返す。screener.calc_atr と同式。"""
    if df is None or df.empty:
        return None
    d = df[df.index.strftime("%Y-%m-%d") <= upto]
    if len(d) < ATR_PERIOD + 2:
        return None
    high, low, close = d["High"], d["Low"], d["Close"]
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(ATR_PERIOD).mean().iloc[-1])
    last = float(close.iloc[-1])
    if not (atr > 0) or not (last > 0):
        return None
    return round(atr / last * 100, 3)


def shadow_stop_pct(atr_pct: float | None) -> float:
    """損切り幅%。ATRが取れない場合は本番と同じ3.0%にフォールバック（＝差分ゼロで安全側）。"""
    if atr_pct is None:
        return LIVE_STOP
    return round(min(max(ATR_MULT * atr_pct, STOP_FLOOR), STOP_CEIL), 2)


def _prev_trading_row_date(df: pd.DataFrame, entry_date_str: str) -> str | None:
    """エントリー日の直前の営業日（＝シグナル日）を価格データから引く。"""
    d = df[df.index.strftime("%Y-%m-%d") < entry_date_str]
    return d.index[-1].strftime("%Y-%m-%d") if len(d) else None


def record_signals(key: str, today: date, all_data: dict) -> int:
    """本番が今朝採用したBUYシグナルを影台帳に取り込む（同一(ticker,signal_date)は再登録しない）。"""
    sig_file = TIER_FILES[key][0]
    if not os.path.exists(sig_file):
        return 0
    with open(sig_file, encoding="utf-8") as f:
        payload = json.load(f)
    if payload.get("date") != today.strftime("%Y-%m-%d"):
        return 0                                  # 今日のファイルでなければ何もしない

    rows = load_ledger(key)
    seen = {(r["ticker"], r["signal_date"]) for r in rows}
    added = 0
    for s in payload.get("signals", []):
        if s.get("direction") != "BUY":           # SELLは対象外（年26件で検出力なし）
            continue
        tk = s["ticker"]
        sig_date = today.strftime("%Y-%m-%d")
        if (tk, sig_date) in seen:
            continue
        df = all_data.get(tk)
        atr = atr_pct_at(df, sig_date) if df is not None else None
        rows.append({
            "signal_date": sig_date,
            "entry_date":  sig_date,              # 当日寄り付きエントリー（本番と同じ）
            "ticker":      tk,
            "name":        s.get("name", tk),
            "prev_close":  s.get("prev_close", 0),
            "limit_price": s.get("limit_price"),
            "atr_pct":     atr,
            "stop_pct":    shadow_stop_pct(atr),
            "live_stop":   LIVE_STOP,
            "entry_open":  None,
            "status":      "pending",
            "hold_days":   0,
            "pnl_pct":     None,
            "exit_type":   None,
            "exit_date":   None,
        })
        added += 1
    if added:
        save_ledger(key, rows)
    return added


def update_ledger(key: str, today: date, all_data: dict) -> tuple[int, int]:
    """影台帳の未決済を前日までのデータで進める。tracker.update_positions と同じ判定順・同じ約定前提で、
    損切り幅だけ銘柄ごとの stop_pct を使う。戻り値=(決済数, 失効数)。"""
    from screener import calc_rsi

    rows = load_ledger(key)
    today_str = today.strftime("%Y-%m-%d")
    closed = expired = 0

    for pos in rows:
        if pos["status"] in ("closed", "expired"):
            continue
        df = all_data.get(pos["ticker"])
        if df is None or df.empty:
            continue
        entry_date_str = pos["entry_date"]

        if pos["status"] == "pending":
            er = df[df.index.strftime("%Y-%m-%d") == entry_date_str]
            if er.empty:
                continue                          # まだエントリー日が来ていない
            eo = float(er["Open"].iloc[0])
            lp = pos.get("limit_price")
            if lp and eo > lp:                    # 寄指不成立（本番と同じ扱い）
                pos.update(entry_open=eo, status="expired", exit_type="NOFILL",
                           exit_date=entry_date_str)
                expired += 1
                continue
            pos.update(entry_open=eo, status="open", hold_days=0)

        eo = pos["entry_open"]
        if not eo or eo <= 0:
            continue
        stop_price = eo * (1 - pos["stop_pct"] / 100)
        tp_price   = eo * (1 + TAKE_PROFIT / 100)

        post = df[(df.index.strftime("%Y-%m-%d") >= entry_date_str) &
                  (df.index.strftime("%Y-%m-%d") < today_str)]
        pos["hold_days"] = 0
        for dt_idx, row in post.iterrows():
            pos["hold_days"] += 1
            d_str = dt_idx.strftime("%Y-%m-%d")
            lo, hi, cl = float(row["Low"]), float(row["High"]), float(row["Close"])
            if lo <= stop_price:                                  # STOP優先（本番と同順）
                pos.update(pnl_pct=-pos["stop_pct"], exit_type="STOP",
                           exit_date=d_str, status="closed")
                closed += 1
                break
            if hi >= tp_price:
                pos.update(pnl_pct=+TAKE_PROFIT, exit_type="TP",
                           exit_date=d_str, status="closed")
                closed += 1
                break
            rsi_now = calc_rsi(df[df.index <= dt_idx]["Close"].dropna())
            rsi_exit = rsi_now is not None and rsi_now >= 50
            if rsi_exit or pos["hold_days"] >= MAX_HOLD:
                pos.update(pnl_pct=round((cl - eo) / eo * 100, 3),
                           exit_type="RSI" if rsi_exit else "MAXHOLD",
                           exit_date=d_str, status="closed")
                closed += 1
                break

    save_ledger(key, rows)
    return closed, expired


def run_shadow(tiers, today: date, get_data) -> None:
    """main.py 末尾から呼ぶ唯一の入口。例外は呼び出し側で握り潰される前提。"""
    keys = [t["key"] for t in tiers if t["key"] in TIER_FILES]
    if not keys:
        return
    if not any(os.path.exists(TIER_FILES[k][0]) or load_ledger(k) for k in keys):
        return
    all_data = get_data() if callable(get_data) else get_data
    if not all_data:
        print("[shadow] 価格データなし → スキップ")
        return
    for k in keys:
        c, e = update_ledger(k, today, all_data)
        a = record_signals(k, today, all_data)
        print(f"[shadow-{TIER_FILES[k][3]}] 新規{a}件 / 影決済{c}件 / 影失効{e}件")


# ────────────────────────────── レポート ──────────────────────────────
def _live_closed(key: str) -> dict:
    """本番帳簿の決済済みを {(ticker, signal_date): pos} で返す。"""
    path = TIER_FILES[key][1]
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    return {(p["ticker"], p["signal_date"]): p for p in rows
            if p.get("direction", "BUY") == "BUY" and p["status"] in ("closed", "expired")}


def report() -> None:
    print("=" * 96)
    print("出口ボラ正規化 紙並走レポート  ─ 本番(一律-3%) vs 影(ATR%×2.0・下限2.0%)")
    print("  ※記録は本番が実際に採用した玉のみ。銘柄選定の違い(枠の空き方)は含まない＝出口単独の効果")
    print("=" * 96)
    grand = 0.0
    for key, (_, _, size, label) in TIER_FILES.items():
        rows = load_ledger(key)
        if not rows:
            continue
        live = _live_closed(key)
        pairs = []
        for r in rows:
            k = (r["ticker"], r["signal_date"])
            lp = live.get(k)
            if lp is None or r["status"] not in ("closed", "expired"):
                continue
            lv = 0.0 if lp["status"] == "expired" else (lp.get("pnl_pct") or 0.0)
            sv = 0.0 if r["status"] == "expired" else (r.get("pnl_pct") or 0.0)
            pairs.append((r, lp, lv, sv))
        openn = sum(1 for r in rows if r["status"] in ("pending", "open"))
        if not pairs:
            print(f"\n  ---- {label} ---- 突き合わせ可能な決済 0件 / 影で保有中 {openn}件")
            continue
        dl = sum(p[2] for p in pairs) / 100 * size
        dsh = sum(p[3] for p in pairs) / 100 * size
        grand += dsh - dl
        print(f"\n  ---- {label}（1件{size//10_000}万）----")
        print(f"    決済 {len(pairs)}件 / 影で保有中 {openn}件")
        print(f"    本番 {sum(p[2] for p in pairs):+7.2f}% = {dl/10_000:+7.2f}万 "
              f"（勝率{sum(1 for p in pairs if p[2] > 0)/len(pairs)*100:4.1f}%）")
        print(f"    影   {sum(p[3] for p in pairs):+7.2f}% = {dsh/10_000:+7.2f}万 "
              f"（勝率{sum(1 for p in pairs if p[3] > 0)/len(pairs)*100:4.1f}%）")
        print(f"    差   {dsh/10_000 - dl/10_000:+7.2f}万")
        diff = sorted([p for p in pairs if abs(p[3] - p[2]) > 0.01],
                      key=lambda p: p[3] - p[2], reverse=True)
        if diff:
            print(f"    判定が割れた玉 {len(diff)}件:")
            for r, lp, lv, sv in diff[:10]:
                print(f"      {r['signal_date']} {r['name'][:14]:<14s} 影の損切り幅{r['stop_pct']:.1f}% | "
                      f"本番 {lv:+6.2f}%({lp.get('exit_type')}) → 影 {sv:+6.2f}%({r['exit_type']}) "
                      f"差{(sv-lv)/100*size/10_000:+.2f}万")
    print(f"\n  === 3階層合計の差: {grand/10_000:+.2f}万 ===")
    print("  ※判断は数ヶ月ぶん貯めてから。10年BTの根拠は memory/project_trading_signal.md 参照")


def backfill(days: int = 120) -> None:
    """既存 positions*.json の過去BUYから影台帳を再構成（初回のみ・以後は毎朝の増分）。"""
    from screener import batch_download_jquants, _jquants_id_token, RSI_WARMUP_CAL_DAYS
    today = date.today()
    start = (today - timedelta(days=days + RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
    print(f"[backfill] {start}〜{today} の価格データ取得中...")
    all_data = batch_download_jquants(_jquants_id_token(), start=start, end=today.strftime("%Y-%m-%d"))
    if not all_data:
        print("[backfill] データ取得失敗")
        return
    cutoff = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    for key, (_, pos_file, _, label) in TIER_FILES.items():
        if not os.path.exists(pos_file):
            continue
        with open(pos_file, encoding="utf-8") as f:
            live = json.load(f)
        rows = load_ledger(key)
        seen = {(r["ticker"], r["signal_date"]) for r in rows}
        added = 0
        for p in live:
            if p.get("direction", "BUY") != "BUY" or p["signal_date"] < cutoff:
                continue
            k = (p["ticker"], p["signal_date"])
            if k in seen:
                continue
            df = all_data.get(p["ticker"])
            sd = _prev_trading_row_date(df, p["entry_date"]) if df is not None else None
            atr = atr_pct_at(df, sd) if (df is not None and sd) else None
            rows.append({
                "signal_date": p["signal_date"], "entry_date": p["entry_date"],
                "ticker": p["ticker"], "name": p.get("name", p["ticker"]),
                "prev_close": p.get("prev_close", 0), "limit_price": p.get("limit_price"),
                "atr_pct": atr, "stop_pct": shadow_stop_pct(atr), "live_stop": LIVE_STOP,
                "entry_open": None, "status": "pending", "hold_days": 0,
                "pnl_pct": None, "exit_type": None, "exit_date": None,
            })
            added += 1
        save_ledger(key, rows)
        c, e = update_ledger(key, today, all_data)
        print(f"[backfill-{label}] 取込{added}件 → 影決済{c}件 / 影失効{e}件")


if __name__ == "__main__":
    import sys

    from dotenv import load_dotenv   # 単体実行時のみ（main.py経由では既に読み込み済み）
    load_dotenv()

    if "--backfill" in sys.argv:
        n = 120
        for a in sys.argv:
            if a.startswith("--days="):
                n = int(a.split("=")[1])
        backfill(n)
    report()
