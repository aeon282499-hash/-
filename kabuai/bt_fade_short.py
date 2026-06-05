"""bt_fade_short.py — 過熱銘柄を「ショート/回避」して優位性が出るかを正直に検証する。

bt_momentum_core.py の所見＝指数は順方向(ロング)の買いランキングではなく、むしろ最上位
グレードSほど将来弱い(fwd10で S-2.65%/勝率39.7% vs D+0.58%/51.4%)という逆相関だった。
signals.py でも accel/promote/buzz は「将来やるなら fade/ショート側で再設計」と明記済み。
そこで本スクリプトは過熱の定義(グレードS・高RSI・buzz・パラボリック等)を **ショート視点**
で評価する。ロングの確定出口を鏡写しにした出口で実現リターンを測る:

  エントリー: 過熱シグナル発動日の終値で空売り
  ショート出口(ロング policy の鏡): 保有EXIT_T(8)営業日の時間決済。ただし翌日以降の
    高値が entry×(1+EXIT_SL/100=+12%) に触れたら踏み上げ損切り(=-12%)。利確なし(下げを伸ばす)。
  short_ret = 踏み上げ時 -EXIT_SL/100、時間決済時 -(close[p+8]/entry - 1)

判定は買い系と同じゲート: 勝率>=52% & 平均>0 & 年別がほぼ全年プラス。
さらに「回避(AVOID)」視点として、同じ母集団のロング出口リターンも併記し
「買うと負ける」かを確認する。2021-2026の日本株は基調上昇のため、ショートは上昇年に
逆風を受けやすい——年別で正直に出す。エッジが出なければ「不採用」と明記する。

実行: python bt_fade_short.py

── 結果(2026-06-05・437万標本2021-2026): ショート不採用／回避(AVOID)は確認 ──
ベースライン(8日/±12%): 全銘柄ショート 勝率46.4%・平均-0.37%・陽性年1/6(=基調上昇の逆風)、
全銘柄ロング 勝率50.7%・平均+0.43%・陽性年5/6。過熱をショート: グレードS(mom>=80)勝率42.4%
/平均+0.75%/RR1.52/陽性年4/5、極端(mom>=90)平均+1.80%/RR1.85だが陽性年3/5。いずれも勝率
40%級の低勝率・高RR(まれな暴落で稼ぐ裾)型で、頻度は全銘柄で年130-300件と僅少。rsi>=80単独
/buzz/パラボリックはショートでも平均≒0(エッジ無)。→ 52%勝率ゲート未達・2023(強い上昇年)が負・
借株/逆日歩/踏み上げ未モデル＝『ショート信号』としては不採用(脆い裾トレード/カーブフィット回避)。
一方ロング(AVOID視点): 同じ過熱母集団を買うと勝率28-40%(全体50.7%比で大幅劣後)・平均≒0〜マイナス・
fwd10は グレードS-2.65%/mom>=90 -5.37%/最過熱-4.25% と明確に負。→『最過熱は買い妙味が薄い
(買わない方がよい)』は実績で確認＝既存の指数注意文を数値で裏づけ。新たなショート商品は作らず、
回避メッセージの定量化(例: グレードS=過去fwd10≒-2.6%/勝率約40%)に留めるのが正直。
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np

from signal_track import _rolling, EXIT_T, EXIT_SL
from momentum import MIN_HISTORY

TMAX = 11  # 前方は p+EXIT_T(8) と参考の fwd10 を取れれば十分


def collect():
    c = pickle.load(open(Path(__file__).resolve().parent.parent / "jquants_cache.pkl", "rb"))
    data = c["all_data"]
    cols = {k: [] for k in ("mom", "rsi", "vr", "power", "r5", "r20", "r1",
                            "year", "short_ret", "long_ret", "fwd10")}
    nn = 0
    sl = EXIT_SL / 100.0
    for tk, df in data.items():
        if df is None or len(df) < MIN_HISTORY + TMAX + 1:
            continue
        df = df.sort_index()
        R = _rolling(df)
        idx = R.index
        close = R["close"].to_numpy(dtype=float)
        high = df["High"].astype(float).to_numpy()
        low = df["Low"].astype(float).to_numpy()
        mom = R["momentum"].to_numpy(dtype=float)
        rsi = R["rsi"].to_numpy(dtype=float)
        vr = R["vr"].to_numpy(dtype=float)
        power = R["power"].to_numpy(dtype=float)
        r5 = R["r5"].to_numpy(dtype=float)
        r20 = R["r20"].to_numpy(dtype=float)
        r1 = R["r1"].to_numpy(dtype=float)
        n = len(close)
        nn += 1
        for p in range(MIN_HISTORY - 1, n - 1 - TMAX):
            ep = close[p]
            if ep <= 0 or np.isnan(ep):
                continue
            if (np.isnan(mom[p]) or np.isnan(rsi[p]) or np.isnan(vr[p]) or np.isnan(power[p])
                    or np.isnan(r5[p]) or np.isnan(r20[p]) or np.isnan(r1[p])):
                continue
            xc = close[p + EXIT_T]
            if np.isnan(xc):
                continue
            # ショート出口（ロング policy の鏡）: 踏み上げ(高値が+12%)で損切り、なければ時間決済
            up_stop = ep * (1.0 + sl)
            if (high[p + 1: p + 1 + EXIT_T] >= up_stop).any():
                sret = -sl
            else:
                sret = -(xc / ep - 1.0)
            # ロング出口（既存の確定policy）＝AVOID視点の裏返し
            dn_stop = ep * (1.0 - sl)
            if (low[p + 1: p + 1 + EXIT_T] <= dn_stop).any():
                lret = -sl
            else:
                lret = xc / ep - 1.0
            f10 = close[p + 10] / ep - 1.0
            cols["mom"].append(mom[p]); cols["rsi"].append(rsi[p]); cols["vr"].append(vr[p])
            cols["power"].append(power[p]); cols["r5"].append(r5[p]); cols["r20"].append(r20[p])
            cols["r1"].append(r1[p]); cols["year"].append(idx[p].year)
            cols["short_ret"].append(sret); cols["long_ret"].append(lret); cols["fwd10"].append(f10)
    A = {k: np.asarray(v, dtype=np.float32) for k, v in cols.items()}
    A["year"] = A["year"].astype(int)
    print(f"集約: {nn}銘柄  全標本 {A['mom'].size:,}  営業日年 {sorted(set(A['year'].tolist()))}")
    return A


def stat(ret, year):
    if ret.size == 0:
        return None
    ret = ret.astype(float)
    win = (ret > 0).mean() * 100
    g = ret[ret > 0]; l = ret[ret < 0]
    aw = g.mean() * 100 if g.size else 0.0
    al = l.mean() * 100 if l.size else 0.0
    rr = aw / abs(al) if al < 0 else float("inf")
    yrs = [y for y in sorted(set(year.tolist())) if (year == y).sum() >= 50]
    pos = sum(1 for y in yrs if ret[year == y].mean() > 0)
    yc = " ".join(f"{y}:{ret[year==y].mean()*100:+.1f}" for y in yrs)
    return dict(n=ret.size, win=win, avg=ret.mean() * 100, aw=aw, al=al, rr=rr,
                pos=pos, tot=len(yrs), yc=yc)


def show(tag, A, mask, base_n, col="short_ret", years=False):
    ret = A[col] if mask is None else A[col][mask]
    yr = A["year"] if mask is None else A["year"][mask]
    s = stat(ret, yr)
    if not s:
        print(f"  {tag:<38s} 標本不足"); return None
    frac = 100.0 * s["n"] / base_n
    print(f"  {tag:<38s} n{s['n']:>8,d}({frac:4.1f}%) 勝率{s['win']:4.1f}% "
          f"平均{s['avg']:+5.2f}% 益{s['aw']:+5.2f}% 損{s['al']:+5.2f}% RR{s['rr']:4.2f} "
          f"陽性年{s['pos']}/{s['tot']}")
    if years:
        print(f"        年別: {s['yc']}")
    return s


def main():
    A = collect()
    bn = A["mom"].size

    print(f"\n========== 0) ベースライン（全標本・出口{EXIT_T}日/±{EXIT_SL}%） ==========")
    show("全銘柄ショート(=地合いに逆らう基準)", A, None, bn, "short_ret", years=True)
    show("全銘柄ロング(=普通に買う基準)", A, None, bn, "long_ret", years=True)

    # 過熱/fade 候補シグナル（masks）
    mom = A["mom"]; rsi = A["rsi"]; vr = A["vr"]; power = A["power"]
    r5 = A["r5"]; r1 = A["r1"]; r20 = A["r20"]
    cands = [
        ("グレードS (mom>=80)", mom >= 80),
        ("極端 (mom>=90)", mom >= 90),
        ("過熱S (mom>=80 & rsi>=75)", (mom >= 80) & (rsi >= 75)),
        ("買われすぎ (rsi>=80)", rsi >= 80),
        ("buzz (vr>=2.5 & power>=5 & |r1|>=4)", (vr >= 2.5) & (power >= 5) & (np.abs(r1) >= 4)),
        ("パラボリック (r5>=15 & rsi>=70)", (r5 >= 15) & (rsi >= 70)),
        ("S延伸 (mom>=80 & r5>=10)", (mom >= 80) & (r5 >= 10)),
        ("最過熱 (mom>=80 & rsi>=80 & r5>=10)", (mom >= 80) & (rsi >= 80) & (r5 >= 10)),
        ("急騰一服 (r5>=20 & r1<0)", (r5 >= 20) & (r1 < 0)),
    ]

    print(f"\n========== 1) 過熱シグナルを【ショート】したら（出口{EXIT_T}日/踏み上げ+{EXIT_SL}%損切り） ==========")
    for tag, m in cands:
        show(tag, A, m, bn, "short_ret", years=True)

    print(f"\n========== 2) 同じ母集団を【ロング】したら（=買うと負けるか・AVOID視点） ==========")
    for tag, m in cands:
        show(tag, A, m, bn, "long_ret")

    print(f"\n========== 3) 参考: 過熱シグナルの fwd10 単純平均（ロング・出口なし） ==========")
    for tag, m in cands:
        ret = A["fwd10"][m]
        if ret.size:
            print(f"  {tag:<38s} n{ret.size:>8,d} fwd10平均{ret.mean()*100:+5.2f}% 勝率{(ret>0).mean()*100:4.1f}%")

    print("\n判定基準: ショート採用は『勝率>=52% & 平均>0 & 年別ほぼ全年プラス & 実用十分な頻度』。"
          "\n回避(買わない)採用は『同母集団のロングが明確にベースライン未満（負 or 大幅劣後）』。"
          "\nどちらも満たさなければ正直に不採用と記す（カーブフィット回避）。")


if __name__ == "__main__":
    main()
