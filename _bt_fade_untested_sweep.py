# -*- coding: utf-8 -*-
"""_bt_fade_untested_sweep.py — 「まだ試してない軸」の一括スイープ（2026-08-28夜・本人依頼）。
土台=現行選定(BASE)×上位2×①100万/②50万。フィルタ系は候補プールに掛けてから再ランク（繰り上げ込み）。
サイズ系は選定不変で配分だけ変える。10年グロス。
"""
import numpy as np, pandas as pd
CAP = 1_000_000
D = pd.read_pickle("_fade_pool_v5_100.pkl")
BASE = (D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0) \
    & (D.tov >= 3e8) & (D.vol_avg >= 100_000) & (D.rng > 5.0) & (D.px * 100 <= CAP)
P = D[BASE].copy()
S1, S2 = 1_000_000, 500_000

def select(pool):
    d = pool.copy()
    r = None
    for c in ("dev", "atr"):
        x = d.groupby("sig")[c].rank(ascending=False, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    return d[d.rk <= 2].copy()

def pf(x):
    n = -x[x <= 0].sum(); return x[x > 0].sum() / n if n else float("inf")

def evaluate(d, size_fn=None):
    if size_fn is None:
        cap = np.where(d.rk == 1, S1, S2)
    else:
        cap = size_fn(d)
    sh = (cap / d.px // 100 * 100).astype(int)
    sh = np.where((d.rk == 2) & (d.px * 100 > S2), 0, sh)
    yen = pd.Series(d.pnl.to_numpy() / 100 * sh * d.o1.to_numpy(), index=d.index)
    t = pd.DataFrame({"y": d.y, "ym": d.ent.str[:7], "ent": d.ent, "yen": yen}); t = t[t.yen != 0]
    yy = t.groupby("y").yen.sum(); mo = t.groupby("ym").yen.sum(); day = t.groupby("ent").yen.sum()
    eq = day.sort_index().cumsum(); dd = (eq - eq.cummax()).min()
    return dict(n=len(t), tot=t.yen.sum(), pf=pf(t.yen), h1=yy[yy.index <= 2021].sum(), h2=yy[yy.index >= 2022].sum(),
                wy=int((yy > 0).sum()), ny=len(yy), wm=mo.min(), m20=int((mo < -2e5).sum()), dd=dd, y26=yy.get(2026, 0))

base = evaluate(select(P))
def show(label, m):
    d = m["tot"] - base["tot"]
    print(f"  {label:<34}{m['n']:>5}{m['tot']/1e4:>+9.0f}万{d/1e4:>+7.0f}{m['pf']:>6.2f}{m['h1']/1e4:>+8.0f}{m['h2']/1e4:>+8.0f}"
          f"{m['wy']:>4}/{m['ny']}{m['wm']/1e4:>+7.0f}{m['m20']:>5}{m['dd']/1e4:>+7.0f}{m['y26']/1e4:>+7.0f}")
hdr = f"  {'軸':<34}{'玉':>5}{'10年':>10}{'差':>7}{'PF':>6}{'前半':>8}{'後半':>8}{'勝年':>7}{'最悪月':>7}{'-20万':>5}{'DD':>7}{'2026':>7}"
print("=" * 118); print("■ 基準 = 現行選定 × ①100万/②50万"); print(hdr); show("現行 100/50", base)

print("=" * 118); print("■ A. フィルタ系（候補に掛けて再ランク＝繰り上げ込み）"); print(hdr)
F = {
  "上昇日数 up_days<=2":       P.up_days <= 2,
  "上昇日数 up_days<=3":       P.up_days <= 3,
  "上昇日数 up_days>=2":       P.up_days >= 2,
  "2日累積 gain2>=10":         P.gain2 >= 10,
  "2日累積 gain2<=20":         P.gain2 <= 20,
  "前日陽線実体 body>=50":     P.body >= 50,
  "前日陽線実体 body<=50":     P.body <= 50,
  "5日乖離 dev5>=15":          P.dev5 >= 15,
  "5日乖離 dev5<=25":          P.dev5 <= 25,
  "出来高比 vr<=3":            P.vr <= 3,
  "出来高比 vr>=1.5":          P.vr >= 1.5,
  "前日比 gain 7-15%":         P.gain <= 15,
  "前日比 gain>=10":           P.gain >= 10,
  "ATR 5-10%":                 P.atr <= 10,
  "ATR>=7":                    P.atr >= 7,
  "乖離 dev>=20":              P.dev >= 20,
  "乖離 dev<=40":              P.dev <= 40,
  "代金 tov>=5億":             P.tov >= 5e8,
  "代金 tov>=10億":            P.tov >= 10e8,
  "日経25MA以下 nk_below":     P.nk_below == True,
  "日経25MA以上":              P.nk_below == False,
  "日経前日 nk_chg<0":         P.nk_chg < 0,
  "日経前日 nk_chg>=0":        P.nk_chg >= 0,
  "月曜除外":                  P.dow != 0,
  "金曜除外":                  P.dow != 4,
  "決算5日内除外 !earn5":      ~P.earn5.astype(bool),
  "前日S高張り付き除外":       ~P.o1_is_limit.astype(bool) if "o1_is_limit" in P else P.px > 0,
  "株価>=300円":               P.px >= 300,
  "株価>=500円":               P.px >= 500,
  "株価<=3000円":              P.px <= 3000,
  "業種:不動産/建設除外":      ~P.sector.isin(["不動産業", "建設業"]),
}
for k, mask in F.items():
    show(k, evaluate(select(P[mask])))

print("=" * 118); print("■ B. サイズ系（選定不変・①の玉サイズだけ可変・②50万固定）"); print(hdr)
d0 = select(P)
def atr_size(target):
    def f(d):
        s1 = np.clip(S1 * target / d.atr, 0.5 * S1, 1.5 * S1)
        return np.where(d.rk == 1, s1, S2)
    return f
for tgt in (6, 7, 8, 10):
    show(f"ATR正規化 ①=100万×{tgt}/ATR (0.5-1.5倍)", evaluate(d0, atr_size(tgt)))
def dev_size(lo, hi):
    def f(d):
        s1 = np.where(d.dev >= hi, 1.3 * S1, np.where(d.dev <= lo, 0.7 * S1, S1))
        return np.where(d.rk == 1, s1, S2)
    return f
show("乖離傾斜 ① dev>=25→130万/<=15→70万", evaluate(d0, dev_size(15, 25)))
def gain_size(lo, hi):
    def f(d):
        s1 = np.where(d.gain >= hi, 1.3 * S1, np.where(d.gain <= lo, 0.7 * S1, S1))
        return np.where(d.rk == 1, s1, S2)
    return f
show("前日比傾斜 ① gain>=15→130万/<=9→70万", evaluate(d0, gain_size(9, 15)))
def tov_size(d):
    s1 = np.where(d.tov >= 10e8, 1.3 * S1, np.where(d.tov < 5e8, 0.7 * S1, S1))
    return np.where(d.rk == 1, s1, S2)
show("代金傾斜 ① >=10億→130万/<5億→70万", evaluate(d0, tov_size))
def nk_size(d):
    s1 = np.where(d.nk_below, 1.3 * S1, 0.7 * S1)
    return np.where(d.rk == 1, s1, S2)
show("地合い傾斜 ① 日経25MA以下→130万/以上→70万", evaluate(d0, nk_size))
def streak_size(d):
    # 前営業日の①が負け→翌日①を70万、勝ち→130万（連敗縮小の逆張り検証）
    r1 = d[d.rk == 1].sort_values("ent")
    prev = r1.pnl.shift(1)
    m = pd.Series(S1, index=d.index, dtype=float)
    m.loc[r1.index] = np.where(prev > 0, 1.3 * S1, np.where(prev <= 0, 0.7 * S1, S1))
    return np.where(d.rk == 1, m, S2)
show("前回勝ち→130万/負け→70万(連勝乗せ)", evaluate(d0, streak_size))
def streak_size2(d):
    r1 = d[d.rk == 1].sort_values("ent")
    prev = r1.pnl.shift(1)
    m = pd.Series(S1, index=d.index, dtype=float)
    m.loc[r1.index] = np.where(prev > 0, 0.7 * S1, np.where(prev <= 0, 1.3 * S1, S1))
    return np.where(d.rk == 1, m, S2)
show("前回負け→130万/勝ち→70万(連敗逆張り)", evaluate(d0, streak_size2))
