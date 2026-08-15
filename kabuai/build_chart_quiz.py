# -*- coding: utf-8 -*-
"""build_chart_quiz.py — 📈チャート演習の実データ生成（2026-08-15・本人「テクニカルでこれは買いか売りかをやって欲しかった」）。

過去10年の急騰候補プール(_fade_pool_v5_100.pkl)から100ケースを抽出し、
シグナル日までの30本のローソク足＋当日の指標を出題データにする。
- 出題時は銘柄名・日付を伏せる（記憶バイアス防止）→ 回答後に開示
- 「正解」はシステム判定との一致（GO=売り / NOGO=見送り）＝プロセスの採点
- 実際の翌日結果（寄→引%と70万建ての円）は正誤と別に必ず表示＝
  「正しい判断でも負ける日はある」を体で覚える設計
- 買いは全ケースで不正解（急騰翌日の順張りデイ買いはPF0.60/11年連続マイナス）

出力: kabuai/web/quiz_charts.json（静的・約100KB）
実行: リポジトリルートで python -X utf8 kabuai/build_chart_quiz.py
"""
from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POOL = os.path.join(ROOT, "_fade_pool_v5_100.pkl")
CACHE = os.path.join(ROOT, "jquants_cache.pkl")
OUT = os.path.join(ROOT, "kabuai", "web", "quiz_charts.json")

BARS = 30          # 出題チャートの本数（シグナル日を最終足に含む）
CAPITAL = 700_000  # 損益円の換算（現行の1玉サイズ）
RNG = np.random.default_rng(42)   # 再現可能な抽出

P = pd.read_pickle(POOL)
print(f"[pool] {len(P)}行")

# 現行ルール（daytrade_paper.pyのGO条件と同一・値がさは70万連動の7,000円で固定し
# サイズ由来のNOGOを教材から除外＝ルールの本質だけを問う）
pass_atr = P.atr >= 5.0
pass_dev = P.dev >= 12.0
pass_gain = P.gain >= 7.0
pass_vr = P.vr < 6.0
pass_rng = P.rng > 5.0
pass_tov = P.tov >= 3e8
pass_base = (P.vol_avg >= 100_000) & (P.px <= 7_000) & pass_tov
n_pass = pass_atr & pass_dev & pass_gain & pass_vr & pass_rng

go = P[pass_base & n_pass]
print(f"[go] {len(go)}玉 勝率{(go.pnl > 0).mean()*100:.1f}%")

def fail_only(mask_fail, *others):
    m = pass_base & mask_fail
    for o in others:
        m &= o
    return P[m]

nogo_sets = [
    ("atr", fail_only(P.atr < 5.0, pass_dev, pass_gain, pass_vr, pass_rng),
     "ATR{atr:.1f}% < 5%（動かない玉は急騰しても垂れない＝撃たない）"),
    ("dev", fail_only(P.dev < 12.0, pass_atr, pass_gain, pass_vr, pass_rng),
     "25MA乖離{dev:.1f}% < 12%（伸び切っていない＝反動が弱い）"),
    ("gain", fail_only((P.gain >= 5.0) & (P.gain < 7.0), pass_atr, pass_dev, pass_vr, pass_rng),
     "前日+{gain:.1f}% < +7%（過熱が足りない＝この帯の期待値は薄い）"),
    ("vr", fail_only(P.vr >= 6.0, pass_atr, pass_dev, pass_gain, pass_rng),
     "出来高{vr:.0f}倍 ≥ 6倍（祭りになりすぎ＝6-12倍帯はPF0.81の負け帯）"),
    ("sticky", fail_only(P.rng <= 5.0, pass_atr, pass_dev, pass_gain, pass_vr),
     "日中レンジ{rng:.1f}% ≤ 5%（S高張り付き＝翌日も踏み上げが止まらない危険玉）"),
]

def sample(df, n):
    if len(df) <= n:
        return df
    return df.iloc[RNG.choice(len(df), size=n, replace=False)]

# 各バケツをtarget+8で多めに引いておき、価格キャッシュで足が取れない行をスキップしても
# 合計がぴったり100になるまで詰める（2026-08-15: 99止まり→100丁度へ・本人「チャート100問」）
buckets = [
    (go[go.pnl > 0], 30, "SELL", ""),
    (go[go.pnl <= 0], 20, "SELL", ""),
] + [(df, 10, "PASS", why) for key, df, why in nogo_sets]
rows = [(list(sample(df, n + 8).itertuples()), n, sys_v, why) for df, n, sys_v, why in buckets]
print(f"[cases] 目標 GO50 + NOGO50 = 100（各バケツ+8の予備つき）")

# キャッシュは {all_data: {ticker: df}, name_map, ...} 構造。10年ローリング窓のため
# 古いシグナル日は 2016-2021 スナップにフォールバックする。
_c1 = pd.read_pickle(CACHE)
_c2 = pd.read_pickle(os.path.join(ROOT, "jquants_cache_2016_2021.pkl"))
cache = _c1["all_data"]
cache_old = _c2.get("all_data", {}) if isinstance(_c2, dict) else {}
names = {**(_c2.get("name_map", {}) if isinstance(_c2, dict) else {}), **_c1.get("name_map", {})}
print(f"[cache] 現行{len(cache)}銘柄 / 旧{len(cache_old)}銘柄 / 名前{len(names)}件")


def bars_upto(ticker, sig):
    ts = pd.Timestamp(sig)
    for src in (cache, cache_old):
        df = src.get(ticker)
        if df is None or df.empty:
            continue
        upto = df[df.index <= ts]
        if len(upto) >= BARS:
            return upto
    return None

cases = []
skip = 0


def build_case(r, sys_v, why_tpl):
    upto = bars_upto(r.ticker, r.sig)
    if upto is None:
        return None
    w = upto.tail(BARS)
    if abs(float(w["Close"].iloc[-1]) - float(r.px)) / r.px > 0.02:
        return None        # キャッシュとプールの終値が合わない行は使わない（分割等）
    bars = [[round(float(a), 1) for a in t]
            for t in zip(w["Open"], w["High"], w["Low"], w["Close"])]
    vols = [int(v) for v in w["Volume"].fillna(0)]
    sh = max(100, int(CAPITAL / r.px / 100) * 100)
    yen = int(round(r.pnl / 100 * sh * r.o1))
    why = why_tpl.format(atr=r.atr, dev=r.dev, gain=r.gain, rng=r.rng, vr=r.vr) if why_tpl else \
        f"前日+{r.gain:.1f}%×ATR{r.atr:.1f}%×乖離{r.dev:.1f}%×出来高{r.vr:.1f}倍×レンジ{r.rng:.1f}%＝全条件クリア"
    return {
        "bars": bars, "vols": vols,
        "meta": {"gain": round(float(r.gain), 1), "atr": round(float(r.atr), 1),
                 "dev": round(float(r.dev), 1), "vr": round(float(r.vr), 1),
                 "rng": round(float(r.rng), 1), "tov_oku": round(float(r.tov) / 1e8, 1)},
        "sys": sys_v, "why": why,
        "out": {"pct": round(float(r.pnl), 2), "yen": yen,
                "o1": round(float(r.o1), 1), "c1": round(float(r.c1), 1)},
        "reveal": {"code": r.ticker.replace(".T", ""), "date": r.sig,
                   "name": names.get(r.ticker, "")},
    }


for cand_rows, target, sys_v, why_tpl in rows:
    made = 0
    for r in cand_rows:
        if made >= target:
            break
        c = build_case(r, sys_v, why_tpl)
        if c is None:
            skip += 1
            continue
        cases.append(c)
        made += 1
    if made < target:
        print(f"[warn] バケツ不足 {sys_v}: {made}/{target}")

RNG.shuffle(cases)
for i, c in enumerate(cases):
    c["id"] = i + 1
out = {"version": "2026-08-15", "capital": CAPITAL, "n": len(cases), "cases": cases}
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
kb = os.path.getsize(OUT) / 1024
n_go = sum(1 for c in cases if c["sys"] == "SELL")
print(f"[out] {OUT}: {len(cases)}ケース (GO{n_go}/NOGO{len(cases)-n_go}) {kb:.0f}KB skip={skip}")
