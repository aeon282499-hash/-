"""bt_dip_tune.py — strong_dip(強押し目)に本当にエッジがあるか検証し、正直化する。

strong_dip は現状 stance=期待 だが、長期トラックでは10/20日リターンがマイナス・
勝率<50%・標本も薄い。そこで「上昇トレンド中の押し目を買う」テーゼに salvage できる
キャリブレーションがあるかを、押し目の広い母集団に対しゲートをスイープして確認する。
出口は確定ポリシー= 保有8日 × 損切り-12% × 利確なし(利を伸ばす)で評価。
エッジが出なければ stance を実績に合わせて正直化する(reversal で確立した手順)。

母集団(superset): r20>=10(ある程度の上昇) & r5<0 & r1<0(直近2日の押し) &
mom>=40 & 30<=rsi<=65。この中で strong_dip(r20>=25 & mom>=50 & 35<=rsi<=60)他を当てる。
実行: python bt_dip_tune.py
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np

from signal_track import _rolling, EXIT_T, EXIT_SL
from momentum import MIN_HISTORY

TMAX = 20


def collect():
    c = pickle.load(open(Path(__file__).resolve().parent.parent / "jquants_cache.pkl", "rb"))
    data = c["all_data"]
    cols = {k: [] for k in ("price", "vr", "power", "r5", "r1", "r20", "rsi", "mom", "year")}
    lows, closes = [], []
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
            # 押し目の広い母集団に限定して集める(全営業日だと巨大なため)
            if not (r20[p] >= 10 and r5[p] < 0 and r1[p] < 0 and mom[p] >= 40 and 30 <= rsi[p] <= 65):
                continue
            cols["price"].append(ep); cols["vr"].append(vr[p]); cols["power"].append(power[p])
            cols["r5"].append(r5[p]); cols["r1"].append(r1[p]); cols["r20"].append(r20[p])
            cols["rsi"].append(rsi[p]); cols["mom"].append(mom[p]); cols["year"].append(idx[p].year)
            lows.append(low[p + 1:p + 1 + TMAX]); closes.append(close[p + 1:p + 1 + TMAX])
    A = {k: np.asarray(v, float) for k, v in cols.items()}
    A["year"] = A["year"].astype(int)
    A["low"] = np.vstack(lows); A["close_fwd"] = np.vstack(closes)
    print(f"集約: {nn}銘柄  押し目母集団(r20>=10&r5<0&r1<0&mom>=40&rsi30-65): {A['price'].size:,}")
    return A


def exit_ret(A, mask=None):
    P = A["price"]; lowm = A["low"]; closem = A["close_fwd"]
    if mask is not None:
        P, lowm, closem = P[mask], lowm[mask], closem[mask]
    if P.size == 0:
        return np.empty(0)
    stop = P * (1.0 - EXIT_SL / 100.0)
    hit = (lowm[:, :EXIT_T] <= stop[:, None]).any(axis=1)
    timed = closem[:, EXIT_T - 1] / P - 1.0
    return np.where(hit, -EXIT_SL / 100.0, timed)


def stat(ret, year):
    if ret.size == 0:
        return None
    win = (ret > 0).mean() * 100
    g = ret[ret > 0]; l = ret[ret < 0]
    aw = g.mean() * 100 if g.size else 0.0
    al = l.mean() * 100 if l.size else 0.0
    rr = aw / abs(al) if al < 0 else float("inf")
    yrs = [y for y in sorted(set(year.tolist())) if (year == y).sum() >= 30]
    pos = sum(1 for y in yrs if ret[year == y].mean() > 0)
    return dict(n=ret.size, win=win, avg=ret.mean() * 100, aw=aw, al=al, rr=rr, pos=pos, tot=len(yrs))


def show(tag, A, mask, base_n):
    ret = exit_ret(A, mask)
    year = A["year"] if mask is None else A["year"][mask]
    s = stat(ret, year)
    if not s:
        print(f"  {tag:<36s} 標本不足"); return None
    frac = 100.0 * s["n"] / base_n
    print(f"  {tag:<36s} n{s['n']:>7,d}({frac:4.1f}%) 勝率{s['win']:4.1f}% "
          f"平均{s['avg']:+5.2f}% 益{s['aw']:+5.2f}% 損{s['al']:+5.2f}% RR{s['rr']:4.2f} "
          f"陽性年{s['pos']}/{s['tot']}")
    return s


def main():
    A = collect()
    yr = A["year"]; bn = A["price"].size

    print(f"\n========== 押し目 出口ポリシー(保有{EXIT_T}日/損切り-{EXIT_SL}%) ==========")
    show("母集団baseline(広い押し目)", A, None, bn)
    # 現行 strong_dip の正確な定義
    sd = (A["r20"] >= 25) & (A["mom"] >= 50) & (A["rsi"] >= 35) & (A["rsi"] <= 60)
    base = show("現行strong_dip(r20>=25&mom>=50&rsi35-60)", A, sd, bn)

    print("\n-- 上昇強度 r20 単独(押し目母集団内) --")
    for v in (15, 25, 35, 50):
        show(f"r20>={v}", A, A["r20"] >= v, bn)
    print("-- 勢い mom 単独 --")
    for v in (45, 50, 55, 60):
        show(f"mom>={v}", A, A["mom"] >= v, bn)
    print("-- 押しの深さ r5 単独 --")
    for v in (0, -2, -5, -8):
        show(f"r5<{v}", A, A["r5"] < v, bn)
    print("-- 出来高 vr / power 単独 --")
    for v in (1.2, 1.5):
        show(f"vr>={v}", A, A["vr"] >= v, bn)
    for v in (0.0, 2.0):
        show(f"power>={v}", A, A["power"] >= v, bn)

    print("\n-- strong_dip 近傍の再キャリブレーション候補 --")
    cands = [
        ("r20>=25 & mom>=55 & rsi35-60", (A["r20"] >= 25) & (A["mom"] >= 55) & (A["rsi"] >= 35) & (A["rsi"] <= 60)),
        ("r20>=35 & mom>=55", (A["r20"] >= 35) & (A["mom"] >= 55)),
        ("r20>=25 & mom>=50 & vr>=1.3", (A["r20"] >= 25) & (A["mom"] >= 50) & (A["vr"] >= 1.3)),
        ("r20>=25 & mom>=50 & r5>-5(浅い押し)", (A["r20"] >= 25) & (A["mom"] >= 50) & (A["r5"] > -5)),
        ("r20>=25 & mom>=50 & r5<-5(深い押し)", (A["r20"] >= 25) & (A["mom"] >= 50) & (A["r5"] < -5)),
        ("r20>=50 & mom>=55", (A["r20"] >= 50) & (A["mom"] >= 55)),
        ("r20>=25 & mom>=50 & rsi>=45(浅い調整)", (A["r20"] >= 25) & (A["mom"] >= 50) & (A["rsi"] >= 45)),
    ]
    for tag, m in cands:
        show(tag, A, m, bn)

    # 現行strong_dipの年別内訳
    print("\n-- 現行strong_dip 年別内訳 --")
    ret = exit_ret(A, sd); y = A["year"][sd]
    cells = []
    for yy in sorted(set(y.tolist())):
        mm = y == yy
        cells.append(f"{yy}:{ret[mm].mean()*100:+.1f}%(n{mm.sum()})" if mm.sum() >= 30 else f"{yy}:n{mm.sum()}-")
    print("  " + " ".join(cells))

    if base:
        print(f"\n判定基準: 採用には『勝率>=52% & 平均>0 & 陽性年=全年 & 標本が年あたり実用十分』。"
              f"  現行strong_dip 勝率{base['win']:.1f}% 平均{base['avg']:+.2f}% RR{base['rr']:.2f} "
              f"陽性年{base['pos']}/{base['tot']} → これを満たすか、満たす近傍があるか。")


if __name__ == "__main__":
    main()
