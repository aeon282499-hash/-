# -*- coding: utf-8 -*-
"""_bt_fade_sticky.py — 張り付き除外(レンジ≤5%)の閾値を初めて両方向に振る（2026-08-01）。

7/31時点の「まだ測れていない穴」＝プールが rng>5% で作られていたため
緩める方向(3-5%帯)が測定不能だった。_fade_pool_v5.pkl（張り付き除外なし・
監査修正済みの本番準拠プール）で閾値0〜7%を総当たりする。

ルールは本番 daily_top_fades と同一:
  前日+7%以上 × 貸借○ × ATR5%以上 × 25MA乖離12%以上 × 出来高比6倍未満 ×
  代金3億以上 → 乖離+ATRの順位平均で上位2本（同点ticker昇順）→ 寄成→引成 ／ 1玉50万

追加の監査（緩めた時に増える玉が「現実に建てられて返せる玉」か）:
  o1_is_limit = 翌日の寄りが値幅制限上限（寄りでの空売り自体は板寄せで成立するが過熱の極み）
  o1_locked   = 寄りS高のままh1==l1（終日ロック＝引成買戻し不能→翌日持ち越し事故）
  c1_at_limit = 引けが値幅制限上限（引けS高＝引成買戻し不能の恐れ→ユニチカ型）

実行: python -X utf8 _bt_fade_sticky.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

CAP = 500_000
_LIM = [(100,30),(200,50),(500,80),(700,100),(1000,150),(1500,300),(2000,400),(3000,500),
        (5000,700),(7000,1000),(10000,1500),(15000,3000),(20000,4000),(30000,5000),(50000,7000),(70000,10000)]


def _lim_up(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return base + w
    return base + 10000.0


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


D = pd.read_pickle("_fade_pool_v5.pkl")
D["c1_at_limit"] = D.apply(lambda r: bool(r.c1 >= _lim_up(r.px) - 1e-9), axis=1)

BASE = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) \
       & (D.tov >= 3e8) & (D.vol_avg >= 100_000)


def run(rng_min: float | None):
    d = D[BASE & ((D.rng > rng_min) if rng_min is not None else True)].copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 2].copy()
    d["sh"] = (CAP / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    d["ym"] = d.ent.str[:7]
    return d


def summarize(d, label):
    yy = d.groupby("y").yen.sum()
    ym = d.groupby("ym").yen.sum()
    h1 = d[d.y <= 2021].yen.sum(); h2 = d[d.y >= 2022].yen.sum()
    n_year = max(d.y.max() - d.y.min() + 1, 1)
    # コスト感応（往復%を1玉の建玉金額に対して）
    nets = {}
    for cost in (0.15, 0.20, 0.30):
        nets[cost] = (d.yen - cost / 100 * d.sh * d.o1).sum()
    return (f"  {label:<14}{len(d):>6}{d.sig.nunique():>7}{(d.pnl>0).mean()*100:>7.1f}%"
            f"{pf(d.pnl):>6.2f}{d.yen.sum():>+13,.0f}{d.yen.sum()/n_year:>+11,.0f}"
            f"{h1:>+12,.0f}{h2:>+12,.0f}"
            f"{int((yy>0).sum()):>4}/{yy.shape[0]:<2}{yy.min():>+11,.0f}{ym.min():>+11,.0f}"
            f"{nets[0.20]:>+13,.0f}")


print("=" * 132)
print("① 張り付き閾値スイープ（keep rng > X%・X=現行5・「なし」=除外撤廃・1玉50万・上位2本・gross円）")
print("=" * 132)
print(f"  {'閾値':<14}{'件数':>6}{'撃つ日':>7}{'勝率':>8}{'PF':>6}{'10年計':>13}{'年平均':>11}"
      f"{'前半16-21':>12}{'後半22-26':>12}{'勝年':>5}{'最悪年':>11}{'最悪月':>11}{'net0.2%':>13}")
runs = {}
for x in (None, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0):
    lab = "なし(全部)" if x is None else f"rng>{x:.0f}%" + ("（現行）" if x == 5.0 else "")
    runs[x] = run(x)
    print(summarize(runs[x], lab))

# ── ② 緩めた時に「増える/入れ替わる」玉の解剖 ───────────────────────────
cur = runs[5.0]; loose = runs[None]
key = lambda d: set(zip(d.sig, d.ticker))
added = loose[~loose.set_index(["sig", "ticker"]).index.isin(cur.set_index(["sig", "ticker"]).index)]
dropped = cur[~cur.set_index(["sig", "ticker"]).index.isin(loose.set_index(["sig", "ticker"]).index)]
print("\n" + "=" * 132)
print("② 除外を撤廃すると何が起きるか（現行との差分）")
print("=" * 132)
print(f"  増える玉 {len(added)}件: 勝率{(added.pnl>0).mean()*100:.1f}% PF{pf(added.pnl):.2f} "
      f"計{added.yen.sum():+,.0f}円 / うちrng≤5%の張り付き玉 {int((added.rng<=5).sum())}件")
print(f"  押し出されて消える玉 {len(dropped)}件: 勝率{(dropped.pnl>0).mean()*100:.1f}% "
      f"PF{pf(dropped.pnl):.2f} 計{dropped.yen.sum():+,.0f}円")
g = added[added.rng <= 5]
if len(g):
    print(f"\n  ■ 張り付き玉（rng≤5%）だけの成績: {len(g)}件 勝率{(g.pnl>0).mean()*100:.1f}% "
          f"PF{pf(g.pnl):.2f} 計{g.yen.sum():+,.0f}円")
    yg = g.groupby("y").yen.sum()
    print("  年別: " + " ".join(f"{y}:{v:+,.0f}" for y, v in yg.items()))
    print(f"  約定不能リスク: 寄りが値幅上限 {int(g.o1_is_limit.sum())}件"
          f" / 終日ロック(買戻し不能) {int(g.o1_locked.sum())}件"
          f" / 引けS高(引成不成立の恐れ) {int(g.c1_at_limit.sum())}件")
    top_t = g.groupby("ticker").yen.sum().sort_values()
    print("  ワースト銘柄: " + " ".join(f"{t}:{v:+,.0f}" for t, v in top_t.head(3).items()))
    print("  ベスト銘柄:   " + " ".join(f"{t}:{v:+,.0f}" for t, v in top_t.tail(3).items()))
    w = g.sort_values("yen")
    print("  ワースト5件:")
    for r_ in w.head(5).itertuples():
        print(f"    {r_.ent} {r_.ticker} 前日+{r_.gain:.1f}% rng{r_.rng:.1f}% "
              f"寄{r_.o1:,.0f}→引{r_.c1:,.0f} {r_.yen:+,.0f}円"
              f"{' [終日ロック]' if r_.o1_locked else ''}{' [引けS高]' if r_.c1_at_limit else ''}")
    # まぐれ依存チェック: 上位3日/3銘柄を除く
    by_day = g.groupby("ent").yen.sum().sort_values(ascending=False)
    by_tk = g.groupby("ticker").yen.sum().sort_values(ascending=False)
    print(f"  上位3日を除く: {g[~g.ent.isin(by_day.head(3).index)].yen.sum():+,.0f}円"
          f" / 上位3銘柄を除く: {g[~g.ticker.isin(by_tk.head(3).index)].yen.sum():+,.0f}円")

# ── ③ 現行picksの約定不能リスク（比較の基準線）──────────────────────────
print("\n" + "=" * 132)
print("③ 参考: 現行(rng>5%)の同リスク率")
print("=" * 132)
print(f"  {len(cur)}件中: 寄り値幅上限 {int(cur.o1_is_limit.sum())}件({cur.o1_is_limit.mean()*100:.1f}%)"
      f" / 終日ロック {int(cur.o1_locked.sum())}件({cur.o1_locked.mean()*100:.2f}%)"
      f" / 引けS高 {int(cur.c1_at_limit.sum())}件({cur.c1_at_limit.mean()*100:.2f}%)")
