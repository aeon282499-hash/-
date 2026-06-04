"""bt_signal_exit.py — 買いシグナルの「出口の目安」をBTで確定する。

エッジのある買い系シグナル(accum/strong_accum/reversal)の発動日終値でエントリーした
と仮定し、出口ポリシー= 時間決済 T営業日 × 損切り SL%（利確はせず利を引っ張る）を
グリッドで当てて、発動日基準の実現リターンを年別に集計する。損切りは翌日以降の安値
(Low)が entry×(1−SL) に触れたらその水準で約定したとみなす（日足ストップ注文の近似）。

狙い: テーマトラッカーBTの「約8日保有・利を引っ張る・損切り-12%」が、本アプリの
モメンタムシグナルのentryでも頑健か検証し、表示用の固定ポリシーを1つ選ぶ。

実行: python bt_signal_exit.py
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd

import signals as sig
from signal_track import _rolling
from momentum import MIN_HISTORY

TMAX = 20
BUY = ["strong_accum", "accum", "reversal"]   # 得意保有ゲートが立つ＝エッジのある買い系
DEFS = {k: fn for (k, _l, _e, _d, _s, fn) in sig.SIGNAL_DEFS}
T_GRID = [5, 8, 10, 13, 15, 20]
SL_GRID = [8, 10, 12, 15, 100]                # 100 = 実質ノーストップ（比較用）


def collect():
    c = pickle.load(open(Path(__file__).resolve().parent.parent / "jquants_cache.pkl", "rb"))
    data = c["all_data"]
    ent = {k: {"price": [], "low": [], "close": [], "year": []} for k in BUY}
    nn = 0
    for tk, df in data.items():
        if df is None or len(df) < MIN_HISTORY + TMAX + 1:
            continue
        df = df.sort_index()
        R = _rolling(df)
        idx = R.index
        close = R["close"].to_numpy(dtype=float)
        low = df["Low"].astype(float).to_numpy()
        mom = R["momentum"].to_numpy(dtype=float)
        vr = R["vr"].to_numpy(dtype=float); power = R["power"].to_numpy(dtype=float)
        r1 = R["r1"].to_numpy(dtype=float); r5 = R["r5"].to_numpy(dtype=float)
        r20 = R["r20"].to_numpy(dtype=float); rsi = R["rsi"].to_numpy(dtype=float)
        n = len(close)
        nn += 1
        for p in range(MIN_HISTORY - 1, n - 1 - TMAX):
            if (np.isnan(mom[p]) or np.isnan(vr[p]) or np.isnan(power[p]) or np.isnan(rsi[p])
                    or np.isnan(r5[p]) or np.isnan(r1[p]) or np.isnan(r20[p])):
                continue
            ep = close[p]
            if ep <= 0 or np.isnan(ep):
                continue
            row = {"momentum": float(mom[p]), "vr": float(vr[p]), "power": float(power[p]),
                   "r1": float(r1[p]), "r5": float(r5[p]), "r20": float(r20[p]),
                   "rsi": float(rsi[p]), "mom_hist": mom[p - 4:p + 1].tolist()}
            for k in BUY:
                try:
                    if DEFS[k](row):
                        ent[k]["price"].append(ep)
                        ent[k]["low"].append(low[p + 1:p + 1 + TMAX])
                        ent[k]["close"].append(close[p + 1:p + 1 + TMAX])
                        ent[k]["year"].append(idx[p].year)
                except Exception:
                    pass
    A = {}
    for k in BUY:
        A[k] = {
            "price": np.asarray(ent[k]["price"], float),
            "low": np.vstack(ent[k]["low"]) if ent[k]["low"] else np.empty((0, TMAX)),
            "close": np.vstack(ent[k]["close"]) if ent[k]["close"] else np.empty((0, TMAX)),
            "year": np.asarray(ent[k]["year"], int),
        }
    print(f"集約: {nn}銘柄  " + " ".join(f"{k}:{A[k]['price'].size:,}" for k in BUY))
    return A


def sim(P, lowm, closem, T, SL):
    """各エントリーの実現リターン（時間決済T×損切りSL%・利確なし）。"""
    if P.size == 0:
        return np.empty(0)
    stop = P * (1.0 - SL / 100.0)
    hit = (lowm[:, :T] <= stop[:, None]).any(axis=1)
    timed = closem[:, T - 1] / P - 1.0
    return np.where(hit, -SL / 100.0, timed)


def summarize(ret, year):
    if ret.size == 0:
        return None
    win = (ret > 0).mean() * 100
    avg = ret.mean() * 100
    g = ret[ret > 0]; l = ret[ret < 0]
    avgwin = g.mean() * 100 if g.size else 0.0
    avgloss = l.mean() * 100 if l.size else 0.0
    rr = (avgwin / abs(avgloss)) if avgloss < 0 else float("inf")
    yrs = sorted(set(year.tolist()))
    yr_avg = []
    for y in yrs:
        m = year == y
        yr_avg.append(f"{y}:{ret[m].mean()*100:+.1f}%" if m.sum() >= 30 else f"{y}:-")
    pos_years = sum(1 for y in yrs if (year == y).sum() >= 30 and ret[year == y].mean() > 0)
    tot_years = sum(1 for y in yrs if (year == y).sum() >= 30)
    return dict(win=win, avg=avg, avgwin=avgwin, avgloss=avgloss, rr=rr,
                n=ret.size, yr=yr_avg, pos=pos_years, tot=tot_years)


def main():
    A = collect()

    # プール（買い系3つを合算）で頑健なポリシーを探す
    print("\n========== プール(strong_accum+accum+reversal) ==========")
    print(f"{'policy':>14s}  {'n':>8s}  勝率   平均   平均益  平均損   RR   陽性年")
    cand = []
    for T in T_GRID:
        for SL in SL_GRID:
            rets = []; years = []
            for k in BUY:
                rets.append(sim(A[k]["price"], A[k]["low"], A[k]["close"], T, SL))
                years.append(A[k]["year"])
            ret = np.concatenate(rets); yr = np.concatenate(years)
            s = summarize(ret, yr)
            cand.append(((T, SL), s))
            sllab = "なし" if SL >= 100 else f"-{SL}%"
            print(f"   T{T:>2d}/SL{sllab:>4s}  {s['n']:>8,d}  {s['win']:4.1f}% {s['avg']:+5.2f}% "
                  f"{s['avgwin']:+5.2f}% {s['avgloss']:+5.2f}% {s['rr']:4.2f}  {s['pos']}/{s['tot']}")

    # ── 採用ポリシー: 保有8日 / 損切り-12% / 利確なし(利を引っ張る) ──
    # 選定理由: (1) 勝率はT8〜10/SL-12%帯が最高(約49%)。平均とRRはTに対し機械的に
    #   単調増なので「RR最大=T20/SL-8%」を採ると勝率45%まで落ちる罠(=横軸を伸ばすほど
    #   見かけが良くなるだけ)。(2) 全買い系で平均プラス・陽性年5/6(唯一の負け年2021は
    #   データ開始年で全ポリシー共通＝構造的)。(3) テーマトラッカーBTで既に検証・運用中の
    #   先験ポリシー(約8日・利伸ばし・SL-12%)と一致し、本アプリのentryでも頑健と確認できた。
    POLICY = (8, 12)
    T, SL = POLICY
    s = summarize(np.concatenate([sim(A[k]["price"], A[k]["low"], A[k]["close"], T, SL) for k in BUY]),
                  np.concatenate([A[k]["year"] for k in BUY]))

    print(f"\n★ 採用ポリシー: 保有{T}日 / 損切り-{SL}% / 利確なし  "
          f"平均{s['avg']:+.2f}% 勝率{s['win']:.1f}% RR{s['rr']:.2f} 陽性年{s['pos']}/{s['tot']}")
    print("  年別平均: " + " ".join(s["yr"]))

    # 参考: 近傍ポリシーの年別内訳（頑健性の確認用）
    for (pt, psl) in [(10, 12), (8, 15), (20, 8)]:
        rr = np.concatenate([sim(A[k]["price"], A[k]["low"], A[k]["close"], pt, psl) for k in BUY])
        yy = np.concatenate([A[k]["year"] for k in BUY])
        ps = summarize(rr, yy)
        print(f"  参考 保有{pt}日/SL-{psl}%: 平均{ps['avg']:+.2f}% 勝率{ps['win']:.1f}% RR{ps['rr']:.2f} "
              f"陽性年{ps['pos']}/{ps['tot']}")

    # 採用ポリシーを各シグナルへ適用した実現統計（表示候補）
    print(f"\n========== 固定ポリシー 保有{T}日/損切り-{SL}% をシグナル別に ==========")
    print(f"{'signal':>13s}  {'n':>8s}  勝率   平均   平均益  平均損   RR")
    for k in BUY:
        ret = sim(A[k]["price"], A[k]["low"], A[k]["close"], T, SL)
        ss = summarize(ret, A[k]["year"])
        if ss:
            print(f"   {k:>10s}  {ss['n']:>8,d}  {ss['win']:4.1f}% {ss['avg']:+5.2f}% "
                  f"{ss['avgwin']:+5.2f}% {ss['avgloss']:+5.2f}% {ss['rr']:4.2f}")


if __name__ == "__main__":
    main()
