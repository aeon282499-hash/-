# -*- coding: utf-8 -*-
"""_bt_fade_shortpos.py — 空売り残高報告×売りフェード（2026-08-03・本番無変更）。

新データ: /markets/short-sale-report（大口0.5%以上・報告者別・as-of合算）。
仮説（決算と逆向き）: 前日+7%急騰した玉に大口空売り残高が厚い＝急騰の正体がショート
スクイーズで翌日も踏み上げが続く危険玉→フェードの最悪テール(-26%級)の事前識別になるか。
※業種別空売り比率(bb0299d)・信用倍率/売り長(週次)は棄却済み。これは個別銘柄の大口残高＝別データ。

判定バー: 両期間改善 × 高原 × 上位3日/3銘柄除去 × 機構の説明。
実行: python -X utf8 _bt_fade_shortpos.py（要 _short_positions_10y.pkl + _short_positions_fade.pkl）
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

CAP = 500_000
D = pd.read_pickle("_fade_pool_v5.pkl")

SP: dict = pickle.load(open("_short_positions_10y.pkl", "rb"))
SP.update(pickle.load(open("_short_positions_fade.pkl", "rb")))

# ── as-of 大口空売り残高%（sig日時点・報告者別最新の合算・0.5%未満=0扱い）──
D = D.sort_values(["ticker", "sig"]).reset_index(drop=True)
short_so = np.zeros(len(D))
for tk, idxs in D.groupby("ticker").groups.items():
    rep = SP.get(str(tk).split(".")[0][:4])
    if rep is None or rep.empty:
        continue
    rep = rep.sort_values("DiscDate")
    rd = rep["DiscDate"].to_numpy(); rn = rep["SSName"].to_numpy()
    rv = rep["ShrtPosToSO"].to_numpy(dtype=float)
    j = 0
    cur: dict = {}
    for i in idxs:
        d0 = D.at[i, "sig"]
        while j < len(rd) and rd[j] <= d0:
            cur[rn[j]] = rv[j] if np.isfinite(rv[j]) else 0.0
            j += 1
        short_so[i] = sum(v for v in cur.values() if v >= 0.005) * 100
D["short_so"] = short_so
D = D.sort_values(["sig", "ticker"]).reset_index(drop=True)

BASE = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) \
       & (D.tov >= 3e8) & (D.rng > 5.0) & (D.vol_avg >= 100_000)
pool = D[BASE & np.isfinite(D.pnl)]
print(f"[pool] GO条件通過 {len(pool):,}件 / 残高あり {(pool.short_so>0).mean()*100:.1f}%")

# ── ① 素の層別（候補レベル・pnl=寄り売り→引け買戻し%・両期間）──
BINS = [(-0.01, 0.0001, "残高なし"), (0.0001, 1.0, "0〜1%"), (1.0, 2.0, "1〜2%"),
        (2.0, 3.0, "2〜3%"), (3.0, 99.0, "3%超")]
print("\n① 素の層別（GO条件通過の候補レベル）")
print(f"  {'帯':<10}{'期間':<10}{'n':>6}{'平均pnl':>9}{'勝率':>7}{'PF':>6}{'p1':>8}{'最悪':>8}{'踏上-8%率':>10}")
for lo, hi, lab in BINS:
    for per, mp in (("2016-21", pool.y <= 2021), ("2022-26", pool.y >= 2022)):
        m = (pool.short_so > lo) & (pool.short_so <= hi) & mp
        if m.sum() < 10:
            continue
        pp = pool.loc[m, "pnl"]
        gp = pp[pp > 0].sum(); gl = -pp[pp <= 0].sum()
        print(f"  {lab:<10}{per:<10}{m.sum():>6}{pp.mean():>+9.3f}{(pp>0).mean()*100:>6.1f}%"
              f"{gp/gl if gl else float('inf'):>6.2f}{pp.quantile(0.01):>+8.2f}{pp.min():>+8.2f}"
              f"{(pp<-8).mean()*100:>9.1f}%")

# ── 選定シム（現行: 乖離+ATR順位平均・上位2本・50万/玉・GO内順位）──
def run(extra_mask=None):
    m = BASE if extra_mask is None else (BASE & extra_mask)
    d = D[m & np.isfinite(D.pnl)].copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= 2].copy()
    d["sh"] = (CAP / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0]
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def s(R, lab):
    yy = R.groupby("y").yen.sum().reindex(range(2016, 2027), fill_value=0.0)
    ym = R.assign(ym=R.ent.str[:7]).groupby("ym").yen.sum()
    gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    by_day = R.groupby("ent").yen.sum().sort_values()
    by_tk = R.groupby("ticker").yen.sum().sort_values()
    ex_day = R[~R.ent.isin(by_day.tail(3).index)].yen.sum()
    ex_tk = R[~R.ticker.isin(by_tk.tail(3).index)].yen.sum()
    print(f"  {lab:<26}{len(R):>6}{(R.pnl>0).mean()*100:>6.1f}%{gp/gl:>6.2f}{R.yen.sum():>+13,.0f}"
          f"{yy[yy.index<=2021].sum():>+11,.0f}{yy[yy.index>=2022].sum():>+12,.0f}"
          f"{int((yy>0).sum()):>4}/11{yy.min():>+11,.0f}{ym.min():>+11,.0f}"
          f"{R.yen.min():>+10,.0f}")


HDR = (f"  {'構成':<26}{'件数':>6}{'勝率':>7}{'PF':>6}{'10年計':>13}{'前半':>11}"
       f"{'後半':>12}{'勝年':>5}{'最悪年':>11}{'最悪月':>11}{'最悪1玉':>10}")

print("\n② 選定シム（現行 vs 残高除外の各閾値・上位3日/3銘柄除去は別掲）")
print(HDR)
s(run(), "現行（残高不問）")
for thr, lab in ((3.0, "残高3%超をNO-GO"), (2.0, "残高2%超をNO-GO"),
                 (1.0, "残高1%超をNO-GO"), (0.0001, "残高ありは全部NO-GO")):
    s(run(D.short_so <= thr), lab)
s(run(D.short_so > 0.0001), "【対照】残高ありだけ撃つ")

# 上位除去チェック（現行と最有力候補のみ・bt-4検査）
print("\n③ 参考: 撃たれた玉の残高帯別（現行picks内・どの帯が痛いか）")
R0 = run()
R0["band"] = pd.cut(R0.short_so, [-0.01, 0.0001, 1, 2, 3, 99],
                    labels=["なし", "0-1%", "1-2%", "2-3%", "3%+"])
t = R0.groupby("band").agg(n=("pnl", "size"), 勝率=("pnl", lambda x: f"{(x>0).mean()*100:.1f}%"),
                           平均=("pnl", "mean"), 計円=("yen", "sum"), 最悪=("pnl", "min"))
print(t.round(3).to_string())
