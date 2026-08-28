# -*- coding: utf-8 -*-
"""_bt_fade_volume.py — 出来高軸の集中検証（2026-08-29未明・本人「出来高とか」）。100/50土台・10年グロス。
vr=急騰日出来高/平均出来高, vol_avg=平均出来高(株), tov=急騰日売買代金, avg_tov=平均代金(vol_avg×px)。"""
import numpy as np, pandas as pd
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
P2 = P.copy(); P2["avg_tov"] = P2.vol_avg * P2.px; P2["tov_ratio"] = P2.tov / P2.avg_tov
d0 = select(P2); base = evaluate(d0)
r1 = d0[d0.rk == 1].copy()
for col, lab in (("vr", "出来高比(急騰日/平均)"), ("vol_avg", "平均出来高(株)"), ("tov", "急騰日代金(円)"), ("avg_tov", "平均代金(円)"), ("tov_ratio", "代金比(急騰日/平均)")):
    x = r1[np.isfinite(r1[col])].copy(); x["q"] = pd.qcut(x[col].rank(method="first"), 5, labels=False)
    g = x.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda v: (v > 0).mean()), lo=(col, "min"), hi=(col, "max"))
    print(f"\n① {lab} 五分位（件あたり%）"); print(g.round(2).to_string())
print("\n" + hdr); show("現行 100/50", base)
print("■ フィルタ（再ランク）")
F = {
  "出来高比 vr 1-2": (P2.vr >= 1) & (P2.vr < 2), "出来高比 vr>=2": P2.vr >= 2, "出来高比 vr>=3": P2.vr >= 3,
  "出来高比 vr<4": P2.vr < 4, "出来高比 vr<5": P2.vr < 5, "出来高比 vr<8(緩和)": (P2.vr < 8),
  "出来高比 上限撤廃(緩和)": P2.px > 0,
  "平均出来高>=30万株": P2.vol_avg >= 3e5, "平均出来高>=100万株": P2.vol_avg >= 1e6, "平均出来高<=300万株": P2.vol_avg <= 3e6,
  "平均出来高>=5万株(緩和)": P2.vol_avg >= 5e4,
  "平均代金>=1億": P2.avg_tov >= 1e8, "平均代金>=3億": P2.avg_tov >= 3e8, "平均代金<=30億": P2.avg_tov <= 30e8,
  "急騰日代金>=2億(緩和)": P2.tov >= 2e8, "急騰日代金>=1億(緩和)": P2.tov >= 1e8,
  "代金比>=2": P2.tov_ratio >= 2, "代金比<=5": P2.tov_ratio <= 5, "代金比 2-6": (P2.tov_ratio >= 2) & (P2.tov_ratio <= 6),
}
BASE_NOVR = (D.gain >= 7.0) & (D.atr >= 5.0) & (D.dev >= 12.0) & (D.tov >= 3e8) & (D.vol_avg >= 100_000) & (D.rng > 5.0) & (D.px * 100 <= CAP)
BASE_NOTOV = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) & (D.vol_avg >= 100_000) & (D.rng > 5.0) & (D.px * 100 <= CAP)
BASE_NOVOL = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) & (D.tov >= 3e8) & (D.rng > 5.0) & (D.px * 100 <= CAP)
for k, mask in F.items():
    if "緩和" in k:
        if "vr" in k or "上限撤廃" in k:
            pool = D[BASE_NOVR & (D.vr < 8 if "8" in k else True)]
        elif "代金" in k:
            pool = D[BASE_NOTOV & (D.tov >= (2e8 if "2億" in k else 1e8))]
        else:
            pool = D[BASE_NOVOL & (D.vol_avg >= 5e4)]
        pool = pool.copy(); pool["avg_tov"] = pool.vol_avg * pool.px; pool["tov_ratio"] = pool.tov / pool.avg_tov
        show(k, evaluate(select(pool)))
    else:
        show(k, evaluate(select(P2[mask])))
print("■ サイズ傾斜（①平均100万正規化）")
def norm(raw_fn):
    def f(d):
        raw = raw_fn(d); m = raw[d.rk == 1].mean(); return np.where(d.rk == 1, raw * S1 / m, S2)
    return f
show("出来高比 vr>=3→1.3 / <1.5→0.7", evaluate(d0, norm(lambda d: np.where(d.vr >= 3, 1.3*S1, np.where(d.vr < 1.5, 0.7*S1, S1)))))
show("出来高比 vr<1.5→1.3 / >=3→0.7 (逆)", evaluate(d0, norm(lambda d: np.where(d.vr < 1.5, 1.3*S1, np.where(d.vr >= 3, 0.7*S1, S1)))))
show("平均代金>=10億→1.3 / <2億→0.7", evaluate(d0, norm(lambda d: np.where(d.avg_tov >= 10e8, 1.3*S1, np.where(d.avg_tov < 2e8, 0.7*S1, S1)))))
show("平均代金<2億→1.3 / >=10億→0.7 (逆)", evaluate(d0, norm(lambda d: np.where(d.avg_tov < 2e8, 1.3*S1, np.where(d.avg_tov >= 10e8, 0.7*S1, S1)))))
show("代金比>=4→1.3 / <2→0.7", evaluate(d0, norm(lambda d: np.where(d.tov_ratio >= 4, 1.3*S1, np.where(d.tov_ratio < 2, 0.7*S1, S1)))))
