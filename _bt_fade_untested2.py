# -*- coding: utf-8 -*-
"""_bt_fade_untested2.py — 未検証軸 第2弾（2026-08-29未明・本人「未検証まだあるでしょ」）。
①候補本数(その日のBASE通過数=相場の過熱度) ②#3を小玉で追加 ③負け月/負け週後のサイズ縮小(レジーム)
④決算シーズン月 ⑤#1と#2が同業種の日。100/50土台・10年グロス。"""
import numpy as np, pandas as pd
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
nb = P.groupby("sig").size().rename("nb")
P2 = P.join(nb, on="sig")
d0 = select(P2); base = evaluate(d0)
print(hdr); show("現行 100/50", base)

print("■ ① 候補本数（その日にBASEを通った銘柄数＝過熱度）")
r1 = d0[d0.rk == 1].copy(); r1["q"] = pd.qcut(r1.nb.rank(method="first"), 5, labels=False)
g = r1.groupby("q").agg(n=("pnl", "size"), avg=("pnl", "mean"), win=("pnl", lambda v: (v > 0).mean()), lo=("nb", "min"), hi=("nb", "max"))
print("  ①の候補本数五分位（件あたり%）"); print(g.round(2).to_string())
for lab, mask in {"候補>=2本の日だけ": P2.nb >= 2, "候補>=3本の日だけ": P2.nb >= 3, "候補<=5本の日だけ": P2.nb <= 5, "候補<=10本の日だけ": P2.nb <= 10}.items():
    show(lab, evaluate(select(P2[mask])))
def norm(raw_fn):
    def f(d):
        raw = raw_fn(d); m = raw[d.rk == 1].mean(); return np.where(d.rk == 1, raw * S1 / m, S2)
    return f
show("候補>=5→1.3 / 1本→0.7 (過熱に厚く)", evaluate(d0, norm(lambda d: np.where(d.nb >= 5, 1.3*S1, np.where(d.nb <= 1, 0.7*S1, S1)))))
show("候補1本→1.3 / >=5→0.7 (閑散に厚く)", evaluate(d0, norm(lambda d: np.where(d.nb <= 1, 1.3*S1, np.where(d.nb >= 5, 0.7*S1, S1)))))

print("■ ② #3 を小玉で追加（①100/②50は固定）")
def select3(pool):
    d = pool.copy(); r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True); r = x if r is None else r + x
    d["mix"] = r / 2; d = d.sort_values(["sig", "mix", "ticker"], kind="stable"); d["rk"] = d.groupby("sig").cumcount() + 1
    return d[d.rk <= 3].copy()
d3 = select3(P2)
for s3 in (300_000, 500_000):
    def f(d, s3=s3):
        cap = np.where(d.rk == 1, S1, np.where(d.rk == 2, S2, s3)); return cap
    m = evaluate(d3, f)
    show(f"#3 を{s3//10000}万で追加(資金{(S1+S2+s3)//10000}万)", m)
r3 = d3[d3.rk == 3]; print(f"  #3単独: n={len(r3)} 平均{r3.pnl.mean():+.2f}% 勝率{(r3.pnl>0).mean()*100:.0f}% PF{pf(r3.pnl):.2f}")

print("■ ③ レジーム縮小（前月/前週のシステム損益で①のサイズを変える・平均100万正規化）")
day = evaluate.__globals__  # noqa
def regime(kind, lose_m, win_m):
    def f(d):
        t = d[d.rk == 1].copy()
        cap0 = np.where(t.rk == 1, S1, S2); sh = (cap0 / t.px // 100 * 100).astype(int)
        t["yen"] = t.pnl / 100 * sh * t.o1
        key = t.ent.str[:7] if kind == "month" else pd.to_datetime(t.ent).dt.strftime("%G-W%V")
        agg = t.groupby(key).yen.sum().sort_index()
        prev = agg.shift(1)
        mult = prev.apply(lambda v: S1 if pd.isna(v) else (lose_m*S1 if v < 0 else win_m*S1))
        m = key.map(mult).fillna(S1)
        out = pd.Series(S2, index=d.index, dtype=float); out.loc[t.index] = m.values
        return out.to_numpy()
    return norm(f)
show("前月マイナス→0.7 / プラス→1.3", evaluate(d0, regime("month", 0.7, 1.3)))
show("前月マイナス→1.3 / プラス→0.7 (逆張り)", evaluate(d0, regime("month", 1.3, 0.7)))
show("前週マイナス→0.7 / プラス→1.3", evaluate(d0, regime("week", 0.7, 1.3)))
show("前週マイナス→1.3 / プラス→0.7 (逆張り)", evaluate(d0, regime("week", 1.3, 0.7)))

print("■ ④ 決算シーズン月（2/5/8/11月）")
mo = d0.ent.str[5:7].astype(int)
show("決算月(2,5,8,11)除外", evaluate(select(P2[~P2.ent.str[5:7].astype(int).isin([2, 5, 8, 11])])))
show("決算月だけ", evaluate(select(P2[P2.ent.str[5:7].astype(int).isin([2, 5, 8, 11])])))
r1 = d0[d0.rk == 1]; mm = r1.groupby(r1.ent.str[5:7]).pnl.agg(["size", "mean"]); print("  ①の月別 件あたり%:", " ".join(f"{m}月:{v:+.2f}" for m, v in mm["mean"].items()))

print("■ ⑤ #1と#2が同業種の日")
sec = d0.pivot_table(index="sig", columns="rk", values="sector", aggfunc="first")
same = sec[(sec[1] == sec[2])].index
d_same = d0[d0.sig.isin(same)]; d_diff = d0[~d0.sig.isin(same)]
for lab, dd_ in (("同業種の日", d_same), ("別業種の日", d_diff)):
    r2 = dd_[dd_.rk == 2]; print(f"  {lab}: 日数{dd_.sig.nunique()} #2平均{r2.pnl.mean():+.2f}% 勝率{(r2.pnl>0).mean()*100:.0f}%  #1平均{dd_[dd_.rk==1].pnl.mean():+.2f}%")
