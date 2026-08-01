# -*- coding: utf-8 -*-
"""_audit_swing_halt.py — スイング本番だけにある「売買停止」露出の定量測定（2026-08-02）。

BTは実在する足しか取引しないので、以下2つはBTが構造的に測れない本番専用の穴:
  F1: run_screener に銘柄単位の鮮度ガードが無い
      → 売買停止中の銘柄が「停止前の古い足」のままRSI/乖離を満たし続け、
        停止明けの暴発ギャップに突っ込むBUYを配信し得る（フェードは2026-07-22に修正済みの穴）
  F2: tracker.update_positions はエントリー日の足が無いと pending のまま
      → エントリー日に停止した銘柄は失効機構が無く永久ゾンビ（フェードはEXPIRE_DAYS=14あり）

直す前に頻度を測る（本人指示「修正する前に必ずバックテスト」）。
ついでに: 優先株汚染(8/2発見)が10年候補プール(_bt10y_candidates_margin.csv)に何行あるかも数える。

実行: python -X utf8 _audit_swing_halt.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

TOV_MIN = 2e9

print("[load] キャッシュ読込...", flush=True)
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))
nm = dict(old["name_map"]); nm.update(new["name_map"])


def merge(tk):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        return None
    d = pd.concat(dfs).sort_index()
    return d[~d.index.duplicated(keep="last")]


tickers = sorted(set(old["all_data"]) | set(new["all_data"]))
frames = {}
date_count: dict = {}
for tk in tickers:
    df = merge(tk)
    if df is None or len(df) < 40:
        continue
    frames[tk] = df
    for d in df.index:
        date_count[d] = date_count.get(d, 0) + 1

# 市場カレンダー = 1,000銘柄以上に足がある日
cal = sorted(d for d, c in date_count.items() if c >= 1000)
cal_pos = {d: i for i, d in enumerate(cal)}
print(f"[cal] 営業日 {len(cal)}日 ({cal[0].date()}〜{cal[-1].date()}) / 銘柄 {len(frames)}")


def indicators(df: pd.DataFrame):
    """凍結時点の本番指標（screener.pyのcalc_*と同じ式・round(2)まで再現）。"""
    c = df["Close"].astype(float)
    if len(c) < 30:
        return None
    delta = c.diff()
    ag = delta.clip(lower=0).ewm(alpha=1 / 14, min_periods=14).mean()
    al = (-delta.clip(upper=0)).ewm(alpha=1 / 14, min_periods=14).mean()
    rs = ag / al.replace(0, np.nan)
    rsi = round(float((100 - 100 / (1 + rs)).iloc[-1]), 2)
    ma = float(c.rolling(25).mean().iloc[-1])
    if not np.isfinite(ma) or ma == 0:
        return None
    dev = round((float(c.iloc[-1]) - ma) / ma * 100, 2)
    h, lo = df["High"].astype(float), df["Low"].astype(float)
    pc = c.shift(1)
    tr = pd.concat([h - lo, (h - pc).abs(), (lo - pc).abs()], axis=1).max(axis=1)
    atr14 = float(tr.rolling(14).mean().iloc[-1])
    atr_prev = float(tr.rolling(14).mean().iloc[-2]) if len(tr) > 15 else np.nan
    rr = (round((float(h.iloc[-2]) - float(lo.iloc[-2])) / atr_prev, 2)
          if np.isfinite(atr_prev) and atr_prev > 0 else None)
    v = df["Volume"].astype(float).dropna()
    vr = None
    if len(v) >= 22:
        avg = float(v.iloc[-22:-2].mean())
        if avg > 0:
            vr = round(float(v.iloc[-2]) / avg, 2)
    tov = float(c.iloc[-2]) * float(v.iloc[-2]) if len(c) >= 2 else 0.0
    day_chg = ((float(c.iloc[-1]) - float(c.iloc[-2])) / float(c.iloc[-2]) * 100
               if len(c) >= 2 and float(c.iloc[-2]) > 0 else 0.0)
    atr_pct = atr14 / float(c.iloc[-1]) * 100 if float(c.iloc[-1]) > 0 else 99.0
    return dict(rsi=rsi, dev=dev, rr=rr, vr=vr, tov=tov, atr_pct=atr_pct, day_chg=day_chg,
                px=float(c.iloc[-1]))


# ── F1: 停止ギャップ列挙と凍結指標のBUY/SELL通過判定 ─────────────────────
gap_eps, buy_hits, sell_hits = 0, [], []
for tk, df in frames.items():
    idx = [d for d in df.index if d in cal_pos]
    if len(idx) < 30:
        continue
    positions = [cal_pos[d] for d in idx]
    for k in range(1, len(positions)):
        gap = positions[k] - positions[k - 1] - 1
        if gap <= 0:
            continue
        gap_eps += 1
        frozen = df[df.index <= idx[k - 1]]
        ind = indicators(frozen)
        if ind is None or ind["tov"] < TOV_MIN:
            continue
        # BUY: RSI≤45 × 乖離≤-1.5 × (rr≥1.5 or vr≥2.0) × ATR%≤3.0
        if (ind["rsi"] <= 45 and ind["dev"] <= -1.5 and ind["atr_pct"] <= 3.0
                and ((ind["rr"] or 0) >= 1.5 or (ind["vr"] or 0) >= 2.0)):
            buy_hits.append((str(idx[k - 1].date()), tk, gap, ind))
        # SELL: 前日比≥3 × RSI≥60 × 乖離≥4 × ATR%≤2.5
        if (ind["day_chg"] >= 3.0 and ind["rsi"] >= 60 and ind["dev"] >= 4.0
                and ind["atr_pct"] <= 2.5
                and ((ind["rr"] or 0) >= 1.5 or (ind["vr"] or 0) >= 2.0)):
            sell_hits.append((str(idx[k - 1].date()), tk, gap, ind))

print(f"\n■ F1 鮮度ガード欠落の露出（10年・代金20億以上のみ判定）")
print(f"  停止ギャップ全体: {gap_eps}回")
print(f"  凍結指標がBUY条件を満たす停止: {len(buy_hits)}回"
      f"（露出延べ{sum(g for _, _, g, _ in buy_hits)}営業日）")
for d, tk, g, ind in buy_hits:
    print(f"    {d} {tk} {nm.get(tk, '?')} 停止{g}日 RSI{ind['rsi']} 乖離{ind['dev']} "
          f"代金{ind['tov']/1e8:.0f}億")
print(f"  凍結指標がSELL条件を満たす停止: {len(sell_hits)}回")
for d, tk, g, ind in sell_hits:
    print(f"    {d} {tk} {nm.get(tk, '?')} 停止{g}日 RSI{ind['rsi']} 乖離{ind['dev']}")

# ── F2: BTの採用玉でエントリー日に足が無い（ゾンビpending化する）件数 ──────
C = pd.read_csv("_bt10y_candidates_margin.csv", parse_dates=["entry"])
zombie = 0
no_frame = 0
for r in C.itertuples():
    df = frames.get(r.ticker)
    if df is None:
        no_frame += 1
        continue
    if r.entry not in df.index:
        zombie += 1
        print(f"  [F2] {r.entry.date()} {r.ticker} エントリー日に足なし")
print(f"\n■ F2 エントリー日停止（候補{len(C)}件中）: {zombie}件 / 照合不能{no_frame}件")

# ── 優先株汚染の10年候補プールへの波及 ─────────────────────────────────
bad = ["2593.T", "5076.T", "7550.T", "9201.T", "9202.T", "9434.T"]
sub = C[C.ticker.isin(bad)]
print(f"\n■ 優先株汚染6銘柄の候補行（旧キャッシュ由来のプール）: {len(sub)}件")
if len(sub):
    print(sub.groupby("ticker").size().to_string())
    print(f"  年別: {sub.groupby('year').size().to_dict()}")
