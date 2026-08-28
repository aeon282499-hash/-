# -*- coding: utf-8 -*-
"""_bt_fade_shortbal.py — フェード×信用売残（本人「売り残が多いと買い戻しがあるから難しい？」・2026-08-28）。
_margin_10y_full.pkl（週末信用残・LongVol/ShrtVol）を sig 日時点で「公表済み」の最新週（Date+4日<=sig）で結合。
指標: 売残回転=ShrtVol/平均出来高(日)、買残回転=LongVol/平均出来高、貸借倍率=Long/Shrt。
五分位の件あたり%（①のみ）＋フィルタ（再ランク）＋サイズ傾斜（①平均100万正規化）。100/50土台・10年グロス。
"""
import pickle, numpy as np, pandas as pd
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
MAR = pickle.load(open("_margin_10y_full.pkl", "rb"))
def attach(df):
    out = {"shrt": [], "lng": []}
    cache = {}
    for tk, sig in zip(df.ticker, df.sig):
        m = MAR.get(tk[:4])
        if m is None or m.empty:
            out["shrt"].append(np.nan); out["lng"].append(np.nan); continue
        if tk not in cache:
            cache[tk] = (m.index + pd.Timedelta(days=4)).strftime("%Y-%m-%d").to_numpy(), m
        avail, mm = cache[tk]
        i = np.searchsorted(avail, sig, side="right") - 1
        if i < 0:
            out["shrt"].append(np.nan); out["lng"].append(np.nan); continue
        out["shrt"].append(mm.ShrtVol.iloc[i]); out["lng"].append(mm.LongVol.iloc[i])
    df = df.copy(); df["shrt"] = out["shrt"]; df["lng"] = out["lng"]
    df["s_turn"] = df.shrt / df.vol_avg          # 売残回転（日）
    df["l_turn"] = df.lng / df.vol_avg
    df["ratio"] = df.lng / df.shrt.replace(0, np.nan)   # 貸借倍率
    return df
P2 = attach(P)
print(f"結合率: {P2.shrt.notna().mean()*100:.1f}%  (n={len(P2)})")
d0 = select(P2); base = evaluate(d0)
r1 = d0[(d0.rk == 1) & d0.shrt.notna()].copy()
for col, lab in (("s_turn", "売残回転(日)"), ("l_turn", "買残回転(日)"), ("ratio", "貸借倍率(買残/売残)"), ("shrt", "売残株数")):
    x = r1[r1[col].notna() & np.isfinite(r1[col])].copy()
    x["q"] = pd.qcut(x[col].rank(method="first"), 5, labels=False)
    g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda v: (v > 0).mean()),
                           lo=(col, "min"), hi=(col, "max"))
    print(f"\n① {lab} 五分位（件あたり%）"); print(g.round(2).to_string())
print("\n" + hdr); show("現行 100/50", base)
print("■ フィルタ（再ランク）")
for lab, mask in {
    "売残回転<=1日": P2.s_turn <= 1, "売残回転<=2日": P2.s_turn <= 2, "売残回転>=0.5日": P2.s_turn >= 0.5,
    "売残回転>=1日": P2.s_turn >= 1, "貸借倍率>=1(買残>売残)": P2.ratio >= 1, "貸借倍率>=3": P2.ratio >= 3,
    "貸借倍率<=1(売残>買残)": P2.ratio <= 1, "買残回転>=1日": P2.l_turn >= 1, "買残回転<=3日": P2.l_turn <= 3,
    "信用残データあり": P2.shrt.notna(),
}.items():
    show(lab, evaluate(select(P2[mask.fillna(False)])))
print("■ サイズ傾斜（①平均100万に正規化）")
def norm(raw_fn):
    def f(d):
        raw = raw_fn(d); m = raw[d.rk == 1].mean(); return np.where(d.rk == 1, raw * S1 / m, S2)
    return f
show("売残回転<=0.5→1.3 / >=2→0.7", evaluate(d0, norm(lambda d: np.where(d.s_turn.fillna(1) <= 0.5, 1.3*S1, np.where(d.s_turn.fillna(1) >= 2, 0.7*S1, S1)))))
show("売残回転>=2→1.3 / <=0.5→0.7(逆)", evaluate(d0, norm(lambda d: np.where(d.s_turn.fillna(1) >= 2, 1.3*S1, np.where(d.s_turn.fillna(1) <= 0.5, 0.7*S1, S1)))))
show("貸借倍率>=3→1.3 / <=1→0.7", evaluate(d0, norm(lambda d: np.where(d.ratio.fillna(2) >= 3, 1.3*S1, np.where(d.ratio.fillna(2) <= 1, 0.7*S1, S1)))))
