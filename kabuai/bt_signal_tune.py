"""bt_signal_tune.py — 弱シグナル(加速/昇格/押し目/話題集中)の閾値グリッド検証。

signal_track._rolling を再利用して全銘柄・全営業日の指標系列を一度だけ作り、
各営業日の特徴量(momentum/vr/power/r1/r5/r20/rsi/accel/histmin)と 5/10/20営業日後の
順方向リターン・暦年を巨大配列に集約する。あとは numpy のブール mask で任意の
閾値変種を高速評価し、年別の勝率/平均でロバストネスを見る。

狙い: 「期待」ラベルなのに期待値マイナスの弱シグナルを、勝率>=52%＆平均プラスかつ
年をまたいで頑健な部分領域へ絞れるか検証する。見つからなければ正直にスタンス格下げ。

実行: python bt_signal_tune.py
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from signal_track import _rolling
from momentum import MIN_HISTORY

HMAX = 20


def collect():
    c = pickle.load(open(Path(__file__).resolve().parent.parent / "jquants_cache.pkl", "rb"))
    data = c["all_data"]
    cols = {k: [] for k in
            ("mom", "vr", "power", "r1", "r5", "r20", "rsi", "accel", "histmin",
             "f5", "f10", "f20", "year")}
    nn = 0
    for tk, df in data.items():
        if df is None or len(df) < MIN_HISTORY + HMAX + 1:
            continue
        R = _rolling(df)
        idx = R.index
        close = R["close"].to_numpy(dtype=float)
        mom = R["momentum"].to_numpy(dtype=float)
        accel = mom - np.concatenate([np.full(4, np.nan), mom[:-4]])
        histmin = pd.Series(mom).rolling(5).min().to_numpy()
        n = len(close)
        p0, p1 = MIN_HISTORY - 1, n - 1 - HMAX
        if p1 < p0:
            continue
        P = np.arange(p0, p1 + 1)
        ep = close[P]
        with np.errstate(invalid="ignore", divide="ignore"):
            f5 = close[P + 5] / ep - 1.0
            f10 = close[P + 10] / ep - 1.0
            f20 = close[P + 20] / ep - 1.0
        years = np.array([d.year for d in idx[P]])
        cols["mom"].append(mom[P]); cols["vr"].append(R["vr"].to_numpy(float)[P])
        cols["power"].append(R["power"].to_numpy(float)[P]); cols["r1"].append(R["r1"].to_numpy(float)[P])
        cols["r5"].append(R["r5"].to_numpy(float)[P]); cols["r20"].append(R["r20"].to_numpy(float)[P])
        cols["rsi"].append(R["rsi"].to_numpy(float)[P]); cols["accel"].append(accel[P])
        cols["histmin"].append(histmin[P])
        cols["f5"].append(f5); cols["f10"].append(f10); cols["f20"].append(f20)
        cols["year"].append(years)
        nn += 1
    A = {k: np.concatenate(v) for k, v in cols.items()}
    # 全特徴量・全fwdが有限な行だけ残す
    good = np.ones(len(A["mom"]), bool)
    for k in ("mom", "vr", "power", "r1", "r5", "r20", "rsi", "accel", "histmin", "f5", "f10", "f20"):
        good &= np.isfinite(A[k])
    A = {k: v[good] for k, v in A.items()}
    print(f"集約: {nn}銘柄 / {len(A['mom']):,}サンプル / 年 {A['year'].min()}〜{A['year'].max()}")
    return A


def evalmask(A, mask, label):
    years = sorted(set(A["year"][mask].tolist()))
    out = [f"\n■ {label}  (n={int(mask.sum()):,})"]
    for h in ("f5", "f10", "f20"):
        fwd = A[h][mask]
        if fwd.size == 0:
            out.append(f"   {h}: -")
            continue
        win = (fwd > 0).mean() * 100
        avg = fwd.mean() * 100
        med = np.median(fwd) * 100
        # 年別勝率
        yr = []
        for y in years:
            m2 = mask & (A["year"] == y)
            fy = A[h][m2]
            yr.append(f"{y}:{(fy>0).mean()*100:.0f}%" if fy.size >= 30 else f"{y}:-")
        flag = "  <<<" if (win >= 52 and avg > 0) else ""
        out.append(f"   {h[1:]:>3s}d  勝{win:5.1f}% 平均{avg:+5.2f}% 中{med:+5.2f}%  [{' '.join(yr)}]{flag}")
    print("\n".join(out))


def main():
    A = collect()

    print("\n========== ACCEL（加速）==========")
    base = (A["accel"] >= 15.0) & (A["mom"] >= 55)
    evalmask(A, base, "現行: accel>=15 & mom>=55")
    for at in (15, 20, 25):
        for mm in (55, 60):
            for rmax in (70, 75, 100):
                for vmin in (1.0, 1.3):
                    m = (A["accel"] >= at) & (A["mom"] >= mm) & (A["rsi"] < rmax) & (A["vr"] >= vmin)
                    if m.sum() < 300:
                        continue
                    evalmask(A, m, f"accel>={at} mom>={mm} rsi<{rmax} vr>={vmin}")

    print("\n========== PROMOTE（昇格）==========")
    bp = ((A["histmin"] < 60) & (A["mom"] >= 60)) | ((A["histmin"] < 80) & (A["mom"] >= 80))
    evalmask(A, bp, "現行: 60跨ぎ or 80跨ぎ")
    m60 = (A["histmin"] < 60) & (A["mom"] >= 60) & (A["mom"] < 80)
    evalmask(A, m60, "60跨ぎのみ(80未満)")
    m60r = m60 & (A["rsi"] < 70)
    evalmask(A, m60r, "60跨ぎ & rsi<70")
    m60v = m60 & (A["vr"] >= 1.3)
    evalmask(A, m60v, "60跨ぎ & vr>=1.3")

    print("\n========== DIP（押し目）==========")
    bd = (A["r20"] >= 15) & (A["r1"] < 0) & (A["mom"] >= 45) & (A["rsi"] >= 40) & (A["rsi"] <= 65)
    evalmask(A, bd, "現行: r20>=15 r1<0 mom>=45 40<=rsi<=65")
    for t20 in (15, 20, 25):
        for mm in (45, 55):
            for rlo in (40, 45):
                m = (A["r20"] >= t20) & (A["r1"] < 0) & (A["mom"] >= mm) & (A["rsi"] >= rlo) & (A["rsi"] <= 65)
                if m.sum() < 300:
                    continue
                evalmask(A, m, f"r20>={t20} mom>={mm} {rlo}<=rsi<=65")

    print("\n========== BUZZ（話題集中）==========")
    bb = (A["vr"] >= 2.5) & (A["power"] >= 5.0) & (np.abs(A["r1"]) >= 4.0)
    evalmask(A, bb, "現行: vr>=2.5 power>=5 |r1|>=4")
    bu = (A["vr"] >= 2.5) & (A["power"] >= 5.0) & (A["r1"] >= 4.0)
    evalmask(A, bu, "上方ブレイクのみ r1>=+4")
    bu2 = bu & (A["r5"] < 10)
    evalmask(A, bu2, "上方 & 直近5日まだ伸び切ってない(r5<10)")
    bd2 = (A["vr"] >= 2.5) & (A["power"] >= 5.0) & (A["r1"] <= -4.0)
    evalmask(A, bd2, "下方スパイクのみ r1<=-4（反発狙い検証）")


if __name__ == "__main__":
    main()
