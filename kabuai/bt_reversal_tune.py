"""bt_reversal_tune.py — reversal の上位ティア「strong_reversal」が成立するか検証。

reversal(下落トレンドからの反転初動: r20<0 & r5>0 & r1>0 & rsi<55)は買い系で
有効だが3シグナル中もっとも質が薄い(出口8日/SL-12%で勝率51%/RR1.22)。そこで
reversal発火サンプルに追加ゲート(出来高vr/power・推進r5/r1・余地rsi)を当て、
材料的に勝率/RRが上がり・年別に頑健・標本が十分残る「強い反転」部分集合が
あるかをグリッドで探す。なければ正直に見送る(誇張した上位ティアは作らない)。

出口は確定ポリシー= 保有8日 × 損切り-12% × 利確なし(利を伸ばす)で評価する。
実行: python bt_reversal_tune.py
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np

import signals as sig
from signal_track import _rolling, EXIT_T, EXIT_SL
from momentum import MIN_HISTORY

TMAX = 20
REV = sig.SIGNAL_DEFS[[k for (k, *_ ) in sig.SIGNAL_DEFS].index("reversal")][5]


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
            row = {"momentum": float(mom[p]), "vr": float(vr[p]), "power": float(power[p]),
                   "r1": float(r1[p]), "r5": float(r5[p]), "r20": float(r20[p]),
                   "rsi": float(rsi[p]), "mom_hist": mom[p - 4:p + 1].tolist()}
            try:
                if not REV(row):
                    continue
            except Exception:
                continue
            cols["price"].append(ep); cols["vr"].append(vr[p]); cols["power"].append(power[p])
            cols["r5"].append(r5[p]); cols["r1"].append(r1[p]); cols["r20"].append(r20[p])
            cols["rsi"].append(rsi[p]); cols["mom"].append(mom[p]); cols["year"].append(idx[p].year)
            lows.append(low[p + 1:p + 1 + TMAX]); closes.append(close[p + 1:p + 1 + TMAX])
    A = {k: np.asarray(v, float) for k, v in cols.items()}
    A["year"] = A["year"].astype(int)
    A["low"] = np.vstack(lows); A["close_fwd"] = np.vstack(closes)
    print(f"集約: {nn}銘柄  reversal発火: {A['price'].size:,}")
    return A


def exit_ret(A, mask=None):
    """確定出口ポリシー(保有EXIT_T×損切りEXIT_SL%・利確なし)の実現リターン。"""
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
    return dict(n=ret.size, win=win, avg=ret.mean() * 100, aw=aw, al=al, rr=rr,
                pos=pos, tot=len(yrs))


def show(tag, A, mask, base_n):
    ret = exit_ret(A, mask)
    year = A["year"] if mask is None else A["year"][mask]
    s = stat(ret, year)
    if not s:
        print(f"  {tag:<34s} 標本不足"); return None
    frac = 100.0 * s["n"] / base_n
    print(f"  {tag:<34s} n{s['n']:>7,d}({frac:4.1f}%) 勝率{s['win']:4.1f}% "
          f"平均{s['avg']:+5.2f}% 益{s['aw']:+5.2f}% 損{s['al']:+5.2f}% RR{s['rr']:4.2f} "
          f"陽性年{s['pos']}/{s['tot']}")
    return s


def main():
    A = collect()
    yr = A["year"]; base_n = A["price"].size

    print(f"\n========== reversal 出口ポリシー(保有{EXIT_T}日/損切り-{EXIT_SL}%) ==========")
    base = show("baseline(全reversal)", A, None, base_n)

    # 1次元スイープ: 各ゲート単独で勝率/RRがどう動くか
    print("\n-- 出来高/勢いゲート単独 --")
    for v in (1.2, 1.5, 2.0):
        show(f"vr>={v}", A, A["vr"] >= v, base_n)
    for v in (2.0, 3.0, 4.0):
        show(f"power>={v}", A, A["power"] >= v, base_n)
    print("-- 推進力ゲート単独 --")
    for v in (2.0, 4.0, 6.0):
        show(f"r5>={v}", A, A["r5"] >= v, base_n)
    for v in (1.0, 2.0, 3.0):
        show(f"r1>={v}", A, A["r1"] >= v, base_n)
    print("-- 余地ゲート単独 --")
    for v in (50, 45, 40):
        show(f"rsi<{v}", A, A["rsi"] < v, base_n)
    for v in (-5, -10, -15):
        show(f"r20<{v}(深い押し)", A, A["r20"] < v, base_n)

    # 2-3次元の組合せ候補(出来高で本物の買いを確認しつつ深すぎない反発)
    print("\n-- 組合せ候補(strong_reversal案) --")
    combos = [
        ("vr>=1.5 & power>=2", (A["vr"] >= 1.5) & (A["power"] >= 2.0)),
        ("vr>=1.5 & r5>=4", (A["vr"] >= 1.5) & (A["r5"] >= 4.0)),
        ("vr>=2 & power>=3", (A["vr"] >= 2.0) & (A["power"] >= 3.0)),
        ("vr>=1.5 & power>=2 & rsi<50", (A["vr"] >= 1.5) & (A["power"] >= 2.0) & (A["rsi"] < 50)),
        ("vr>=1.5 & r5>=4 & rsi<50", (A["vr"] >= 1.5) & (A["r5"] >= 4.0) & (A["rsi"] < 50)),
        ("vr>=2 & power>=3 & r5>=4", (A["vr"] >= 2.0) & (A["power"] >= 3.0) & (A["r5"] >= 4.0)),
        ("vr>=2 & power>=4 & r5>=5", (A["vr"] >= 2.0) & (A["power"] >= 4.0) & (A["r5"] >= 5.0)),
    ]
    for tag, m in combos:
        show(tag, A, m, base_n)

    print("\n-- r5>=4 を軸にした絞り込み(頑健性重視) --")
    r5g = A["r5"] >= 4.0
    extra = [
        ("r5>=4 & r20<-10", r5g & (A["r20"] < -10)),
        ("r5>=4 & rsi<45", r5g & (A["rsi"] < 45)),
        ("r5>=4 & power>=2", r5g & (A["power"] >= 2.0)),
        ("r5>=6 & r20<-10", (A["r5"] >= 6.0) & (A["r20"] < -10)),
    ]
    for tag, m in extra:
        show(tag, A, m, base_n)

    # 採用候補の年別内訳(構造的に2021が弱いだけかを確認)
    print("\n-- 年別内訳 --")
    for tag, m in [("baseline", None), ("r5>=4", r5g), ("r5>=4 & r20<-10", r5g & (A["r20"] < -10))]:
        ret = exit_ret(A, m); y = A["year"] if m is None else A["year"][m]
        cells = []
        for yy in sorted(set(y.tolist())):
            mm = y == yy
            cells.append(f"{yy}:{ret[mm].mean()*100:+.1f}%({mm.sum()//1000}k)" if mm.sum() >= 30 else f"{yy}:-")
        print(f"  {tag:<18s} " + " ".join(cells))

    if base:
        print(f"\n基準: baseline 勝率{base['win']:.1f}% RR{base['rr']:.2f} 陽性年{base['pos']}/{base['tot']}。"
              "  採用は『勝率を+3pt超 or RRを+0.15超 改善 & 陽性年=全年 & 発火率>=15%』が目安。")


if __name__ == "__main__":
    main()
