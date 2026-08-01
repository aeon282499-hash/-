# -*- coding: utf-8 -*-
"""_audit_fade_rankpool.py — 本番の順位付けとBTの順位付けの母集団ズレを実測（2026-08-01）。

疑い①: 本番 daily_top_fades は pick_score（乖離+ATRの順位平均）を
  「候補プール全体（gain≥5%・ATR/乖離の下限未達も含む）」で計算してから GO を先頭に並べる。
  一方BT（_bt_fade_recheck / _bt_fade_sticky 等）は「GO条件で絞った後」に順位を付ける。
  2軸の順位平均は母集団に依存するので、GOが3本以上いる日に「どの2本を撃つか」が
  食い違い得る（da0ae89監査の同点/丸めと同じクラスの、コードに書いていない定義差）。

疑い②: GO閾値（前日+7%）の判定に round(gain,2) を渡している。
  raw 6.995〜7.000% が 7.00% に化けて通る（ATR/乖離は da0ae89 で raw 判定に直したのに
  gain だけ直し漏れ）。

実行: python -X utf8 _audit_fade_rankpool.py

【検証結果 2026-08-01】どちらも実害を確認し本番を修正（コミット参照）:
  ①順位母集団: 修正前は撃つ2本がBTと食い違う日が42日/1,191日(3.5%)・金額差+10.8万/10年。
    本番を「GO玉の中だけで順位付け」に修正後、食い違いは2日まで減少。残る2日
    (2017-12-01/2022-03-18)は**BT側**の pct rank 浮動小数点で本来同点(mix=0.6)が
    1e-16だけ割れて同点処理(ticker昇順)が発動しないアーティファクト＝本番(整数順位)が正しい。
  ②丸めgain: raw+6.998%がGO化した実害は10年2件・計-3.3万。raw判定に修正。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

CAP = 500_000
D = pd.read_pickle("_fade_pool_v5.pkl")

# 本番の候補プール＝gain≥5(プール床) × rng>5(張り付き) × vr<6。ATR/乖離/gain7は「後ろに回す」だけ。
POOL = D[(D.rng > 5.0) & (D.vr < 6.0)].copy()
POOL["go"] = (POOL.gain >= 7.0) & (POOL.atr >= 5.0) & (POOL.dev >= 12.0)
POOL["go_rounded"] = (POOL.gain.round(2) >= 7.0) & (POOL.atr >= 5.0) & (POOL.dev >= 12.0)


def yen(d):
    sh = (CAP / d.px // 100 * 100).astype(int)
    return (d.pnl / 100 * sh * d.o1).where(sh > 0, 0.0)


# ── 本番方式: 全候補で順位→GO先頭 ────────────────────────────────────────
def picks_prod(g: pd.DataFrame, go_col: str) -> pd.DataFrame:
    n = len(g)
    rd = {t: i for i, t in enumerate(
        g.sort_values(["dev", "ticker"], ascending=[False, True]).ticker)}
    ra = {t: i for i, t in enumerate(
        g.sort_values(["atr", "ticker"], ascending=[False, True]).ticker)}
    s = g.copy()
    s["score"] = [round((rd[t] + ra[t]) / 2 / max(n, 1), 4) for t in s.ticker]
    s = s.sort_values(["score", "ticker"])
    return s[s[go_col]].head(2)


# ── BT方式: GOで絞ってから順位 ───────────────────────────────────────────
def picks_bt(g: pd.DataFrame, go_col: str) -> pd.DataFrame:
    s = g[g[go_col]].copy()
    if s.empty:
        return s
    r = None
    for c in ("dev", "atr"):
        x = s[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    s["mix"] = r / 2
    return s.sort_values(["mix", "ticker"], kind="stable").head(2)


rows_p, rows_b, diff_days = [], [], []
for sig, g in POOL.groupby("sig", sort=True):
    if not g.go.any():
        continue
    p = picks_prod(g, "go"); b = picks_bt(g, "go")
    rows_p.append(p); rows_b.append(b)
    if set(p.ticker) != set(b.ticker):
        diff_days.append((sig, len(g[g.go]), sorted(p.ticker), sorted(b.ticker)))

P = pd.concat(rows_p); B = pd.concat(rows_b)
P["yen"] = yen(P); B["yen"] = yen(B)

print("=" * 110)
print("① 順位付けの母集団: 本番方式(全候補で順位→GO先頭) vs BT方式(GOで絞って順位)・10年・上位2本・50万")
print("=" * 110)
for lab, X in (("本番方式", P), ("BT方式", B)):
    w = (X.pnl > 0).mean() * 100
    gp = X.pnl[X.pnl > 0].sum(); gl = -X.pnl[X.pnl <= 0].sum()
    print(f"  {lab}: {len(X)}件 勝率{w:.1f}% PF{gp/gl:.2f} 10年計{X.yen.sum():+,.0f}円")
print(f"  撃つ2本が食い違う日: {len(diff_days)}日 / {P.sig.nunique()}日 "
      f"({len(diff_days)/P.sig.nunique()*100:.1f}%)")
if diff_days:
    print(f"  金額差(本番-BT): {P.yen.sum()-B.yen.sum():+,.0f}円")
    for sig, ngo, pp, bb in diff_days[:8]:
        print(f"    {sig} GO{ngo}本: 本番{pp} / BT{bb}")
    if len(diff_days) > 8:
        print(f"    …他{len(diff_days)-8}日")

print()
print("=" * 110)
print("② round(gain,2)判定: raw 6.995〜7.000% が GO に化ける玉")
print("=" * 110)
leak = POOL[POOL.go_rounded & ~POOL.go]
print(f"  該当候補: {len(leak)}件（10年）")
if len(leak):
    # そのうち実際に上位2本に選ばれて撃たれる玉
    shot = []
    for sig, g in POOL.groupby("sig", sort=True):
        if not g.go_rounded.any():
            continue
        p = picks_prod(g, "go_rounded")
        shot.append(p[p.gain < 7.0])
    S = pd.concat(shot) if shot else pd.DataFrame()
    if len(S):
        S["yen"] = yen(S)
        print(f"  うち実際に撃たれる玉: {len(S)}件 計{S.yen.sum():+,.0f}円")
        for r in S.itertuples():
            print(f"    {r.ent} {r.ticker} raw+{r.gain:.4f}% → 表示+{round(r.gain,2):.2f}%")
    else:
        print("  うち実際に撃たれる玉: 0件")
