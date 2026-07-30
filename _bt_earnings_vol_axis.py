# -*- coding: utf-8 -*-
"""_bt_earnings_vol_axis.py — まだ触っていない2軸（2026-07-31・本番無変更）。

本人「まだ試してないところあるでしょ」。あった。

これまで測った軸は全部「どの銘柄が上がるか（＝方向）」を当てにいくものだった。
今日の結論はそれが全滅（順位に情報ゼロ・勝率は49-51%で動かない）。
だが本システムのエッジは方向でなく **非対称** にある:
    勝ち平均 +4.96% / 負け平均 -3.73%（勝率49%でもPF1.3になる理由）
非対称が正体なら、**振れ幅そのものを大きくすれば非対称も拡大するはず**。
方向を当てる話ではないので、これまでの全滅とは筋が違う。

軸① 決算ボラ: その銘柄の「過去の決算翌寄りギャップの絶対値」の中央値。
     価格ATR（＝入口ボラ正規化として棄却済み）とは別物で、決算という
     イベントに対してその銘柄がどれだけ動く体質かを直接測る。
     ※点in時間: その晩より前の決算だけを使う。

軸② 決算集中度: その晩に何社が発表するか。集中日（8月上旬など）と閑散日で
     歪みの質が違うのでは。日の選別は一度も測っていない
     （測ったのは「市場の決算反応ゲート」であって集中度ではない）。

実行: python -X utf8 _bt_earnings_vol_axis.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

import _bt_earnings_fundamentals as B

RAW = pd.read_csv("_earnings_events_rich2.csv")      # 全イベント（絞り込み前）
E = B.E.copy()
SLOTS = 8
DD_BUDGET = 2_400_000


# ── 軸①: 決算ボラ（点in時間）──────────────────────────────
# 全イベント表から銘柄ごとの過去ギャップ絶対値の中央値を、その時点までで作る
RAW = RAW.sort_values(["ticker", "d0"])
g = RAW.groupby("ticker")["gap"]
RAW["egap_vol"] = (g.transform(lambda s: s.abs().shift(1).expanding(min_periods=3).median()))
RAW["egap_n"] = g.transform(lambda s: s.abs().shift(1).expanding(min_periods=1).count())
E = E.merge(RAW[["ticker", "d0", "egap_vol", "egap_n"]], on=["ticker", "d0"], how="left")

# ── 軸②: 決算集中度 ────────────────────────────────────
nightly = RAW.groupby("d0").size().rename("n_all")
E = E.merge(nightly, left_on="d0", right_index=True, how="left")
E["n_cand"] = E.groupby("d0")["ticker"].transform("size")

E = E.sort_values(["d0", "rsi"]).reset_index(drop=True)
print(f"[データ] 候補{len(E):,}件 / 決算ボラ付与{E.egap_vol.notna().mean()*100:.0f}%"
      f" / 決算ボラ中央値{E.egap_vol.median():.2f}%")


def run(mask, slots=SLOTS, order=("d0", "rsi")):
    P = B.sim(E[mask].sort_values(list(order)), slots=slots)
    if not len(P):
        return None
    y = P["pnl"] * 1_000_000 / 100
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = (P.groupby("year")["pnl"].sum() * 1e4).reindex(range(2016, 2027), fill_value=0)
    win = P.pnl[P.pnl > 0]
    los = P.pnl[P.pnl <= 0]
    return dict(n=len(P), tot=float(y.sum()), dd=dd, ratio=float(y.sum()) / abs(dd),
                norm=float(y.sum()) * DD_BUDGET / abs(dd),
                wr=(P.pnl > 0).mean() * 100, w=win.mean(), l=los.mean(),
                pos=int((yr > 0).sum()), e1=float(yr[yr.index <= 2021].sum()),
                e2=float(yr[yr.index >= 2022].sum()))


HDR = (f"  {'設定':<28}{'件数':>6}{'10年計':>11}{'最大DD':>10}{'比':>6}{'勝率':>7}"
       f"{'勝ち平均':>8}{'負け平均':>8}{'前半':>9}{'後半':>10}{'同DD換算':>11}")


def line(tag, r, b=None):
    if r is None:
        print(f"  {tag:<28}  —")
        return
    print(f"  {tag:<28}{r['n']:>6}{r['tot']/1e4:>10,.0f}万{r['dd']/1e4:>9,.0f}万{r['ratio']:>6.2f}"
          f"{r['wr']:>6.1f}%{r['w']:>+8.2f}{r['l']:>+8.2f}{r['e1']/1e4:>8,.0f}万{r['e2']/1e4:>9,.0f}万"
          f"{r['norm']/1e4:>10,.0f}万")


ALL = pd.Series(True, index=E.index)
BASE = run(ALL)
print("\n" + "=" * 128)
print("軸① 決算ボラ＝その銘柄が「決算でどれだけ動く体質か」（過去の決算ギャップ絶対値の中央値・点in時間）")
print("=" * 128)
print(HDR)
line("現行（全部）", BASE)
q = E["egap_vol"].quantile([.2, .4, .6, .8]).values
print(f"  ※五分位の境目: {q.round(2)}")
for lo, hi, tag in [(None, q[0], "最も動かない20%だけ"), (q[0], q[1], "20-40%"),
                    (q[1], q[2], "40-60%"), (q[2], q[3], "60-80%"),
                    (q[3], None, "最も動く20%だけ")]:
    m = E["egap_vol"].notna()
    if lo is not None:
        m &= E["egap_vol"] >= lo
    if hi is not None:
        m &= E["egap_vol"] < hi
    line(tag, run(m), BASE)
print("  --- 足切り（欠測はフェイルオープン）---")
for thr in (3.0, 4.0, 5.0, 6.0, 7.0):
    line(f"決算ボラ>={thr:.0f}%だけ買う", run(E["egap_vol"].isna() | (E["egap_vol"] >= thr)), BASE)
for thr in (5.0, 4.0):
    line(f"決算ボラ<{thr:.0f}%だけ買う", run(E["egap_vol"].isna() | (E["egap_vol"] < thr)), BASE)

print("\n" + "=" * 128)
print("軸② 決算集中度＝その晩に何社が発表するか（日の選別は未検証）")
print("=" * 128)
print(HDR)
line("現行（全部）", BASE)
qn = E["n_all"].quantile([.25, .5, .75]).values
print(f"  ※その晩の全発表社数の四分位: {qn.round(0)}")
for lo, hi, tag in [(None, qn[0], "閑散な晩だけ"), (qn[0], qn[1], "やや閑散"),
                    (qn[1], qn[2], "やや集中"), (qn[2], None, "決算集中日だけ")]:
    m = pd.Series(True, index=E.index)
    if lo is not None:
        m &= E["n_all"] >= lo
    if hi is not None:
        m &= E["n_all"] < hi
    line(tag, run(m), BASE)

print("\n" + "=" * 128)
print("③ 1晩に何枚まで建てるか（現行は上限なし＝集中日に8枚まとめて入る）")
print("=" * 128)
print(HDR)


def run_nightcap(cap: int):
    A = E.sort_values(["d0", "rsi"])
    days = sorted(A["d0"].unique())
    di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, gg in A.groupby("d0", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        nn = 0
        for r in gg.itertuples():
            if len(busy) >= SLOTS or nn >= cap:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            if r.gap > B.PEAD_THR and np.isfinite(r.r5):
                pnl, span = r.r5, B.PEAD_DAYS
            else:
                pnl, span = r.gap, 1
            busy.append(i + span)
            held[r.ticker] = i + span
            nn += 1
            out.append(dict(year=r.year, pnl=pnl))
    P = pd.DataFrame(out)
    y = P["pnl"] * 1_000_000 / 100
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = (P.groupby("year")["pnl"].sum() * 1e4).reindex(range(2016, 2027), fill_value=0)
    win, los = P.pnl[P.pnl > 0], P.pnl[P.pnl <= 0]
    return dict(n=len(P), tot=float(y.sum()), dd=dd, ratio=float(y.sum()) / abs(dd),
                norm=float(y.sum()) * DD_BUDGET / abs(dd), wr=(P.pnl > 0).mean() * 100,
                w=win.mean(), l=los.mean(), pos=int((yr > 0).sum()),
                e1=float(yr[yr.index <= 2021].sum()), e2=float(yr[yr.index >= 2022].sum()))


line("現行（1晩の上限なし）", BASE)
for cap in (2, 3, 4, 5, 6):
    line(f"1晩{cap}枚まで", run_nightcap(cap), BASE)

print("\n[読み方] «同DD換算» が現行(760万)を明確に超え、かつ前半/後半とも崩れないものだけが候補。")
