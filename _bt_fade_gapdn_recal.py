# -*- coding: utf-8 -*-
"""_bt_fade_gapdn_recal.py — プレミアム料分岐定数 FADE_EDGE_PCT_GAPDN/MAIN の新土台再導出（2026-08-15）。

発端: 配信の円/株バンド(6円/12円)の元定数 0.254%/0.509% は 7月末・50万×2本・300円フロア・
値がさ5千円の土台で測った値。現行土台(100万×1番のみ)ではA14ログの逆算で下寄り枝が
+0.80%/玉と3倍リッチに見える＝線が保守的すぎる疑い。玉単位で測り直す。

判定の枠組み（発注時に寄り位置は未知＝注文種別の選択問題）:
  成行 ≥ 寄指 ⇔ プレミアム総額 ≤ E[円|下寄り]   … T1(成行OK上限)
  寄指 ≥ 見送り ⇔ プレミアム総額 ≤ E[円|上寄り]  … T2(見送り線)
頑健性: 前半/後半・上位3玉除去・年別符号・中央値。
実行: python -X utf8 _bt_fade_gapdn_recal.py
"""
from __future__ import annotations
import numpy as np
import pandas as pd

SIZE = 1_000_000
P = pd.read_pickle("_fade_pool_v5_100.pkl")
print(f"[pool] {len(P)}行 columns={list(P.columns)}", flush=True)

# ── ベースライン再現（_bt_fade_rebase100.py run() のデフォルトと同一）──
d = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
      & (P.tov >= 3e8) & (P.rng > 5.0) & (P.vol_avg >= 100_000)].copy()
r = None
for col in ("dev", "atr"):
    x = d.groupby("sig")[col].rank(ascending=False, pct=True)
    r = x if r is None else r + x
d["mix"] = r / 2
d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
d["rk"] = d.groupby("sig").cumcount() + 1
d = d[d.rk <= 1].copy()
d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
d = d[d.sh > 0].copy()
d["yen"] = d.pnl / 100 * d.sh * d.o1
print(f"[base] {len(d)}玉 10年{d.yen.sum():+,.0f}円 勝率{(d.pnl > 0).mean()*100:.1f}%"
      f"（rebase100ログ: 1222玉 +13,760,260円 59.9% と一致するはず）", flush=True)

d["half"] = np.where(d.y <= 2021, "前半16-21", "後半22-26")


def branch(lab, m):
    b = d[m]
    if not len(b):
        print(f"  {lab}: 0玉"); return
    yen = b.yen
    top3 = yen.nlargest(3).sum()
    yr = b.groupby("y").yen.sum()
    pos_years = int((yr > 0).sum())
    halves = b.groupby("half").yen.agg(["sum", "count", "mean"])
    print(f"  {lab}: {len(b)}玉({len(b)/len(d)*100:.0f}%) 平均{yen.mean():+,.0f}円/玉 "
          f"中央値{yen.median():+,.0f}円 PF{yen[yen>0].sum()/max(1,-yen[yen<0].sum()):.2f} "
          f"合計{yen.sum():+,.0f}円")
    print(f"      上位3玉除去後の平均{(yen.sum()-top3)/max(1,len(b)-3):+,.0f}円/玉 "
          f"陽性{pos_years}/{yr.size}年")
    for h, rrow in halves.iterrows():
        print(f"      {h}: {rrow['count']:.0f}玉 平均{rrow['mean']:+,.0f}円/玉 計{rrow['sum']:+,.0f}円")


print("\n== 寄りギャップ別の枝の実力（現行土台・1番のみ）==")
branch("上寄り(gu>=0)      ", d.gu >= 0)
branch("下寄り(gu<0) 全体  ", d.gu < 0)
branch("  浅い下寄り(-1〜0%)", (d.gu < 0) & (d.gu >= -1))
branch("  深い下寄り(<-1%)  ", d.gu < -1)

t1 = d.loc[d.gu < 0, "yen"].mean()
t2 = d.loc[d.gu >= 0, "yen"].mean()
t1_r = (d.loc[d.gu < 0, "yen"].sum() - d.loc[d.gu < 0, "yen"].nlargest(3).sum()) \
    / max(1, (d.gu < 0).sum() - 3)
t2_r = (d.loc[d.gu >= 0, "yen"].sum() - d.loc[d.gu >= 0, "yen"].nlargest(3).sum()) \
    / max(1, (d.gu >= 0).sum() - 3)
print(f"\n== 分岐定数の候補 ==")
print(f"  T1(成行OK上限)=下寄り枝平均: {t1:+,.0f}円 = {t1/SIZE*100:.3f}%  "
      f"(上位3玉除去{t1_r:+,.0f}円={t1_r/SIZE*100:.3f}%)")
print(f"  T2(見送り線)=上寄り枝平均:  {t2:+,.0f}円 = {t2/SIZE*100:.3f}%  "
      f"(上位3玉除去{t2_r:+,.0f}円={t2_r/SIZE*100:.3f}%)")
print(f"  旧定数: GAPDN 0.254%(2,540円) / MAIN 0.509%(5,090円)")
for px, lab in ((2165, "アスタリスク2,165円=400株"), (1000, "1,000円=1,000株"), (300, "300円=3,300株")):
    sh = int(SIZE / px // 100 * 100)
    print(f"  {lab}: 保守側(上位3除去)なら 成行OK〜{int(min(t1, t1_r)//sh)}円/株・"
          f"見送り線{int(min(t2, t2_r)//sh)}円/株")
