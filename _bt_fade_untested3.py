# -*- coding: utf-8 -*-
"""_bt_fade_untested3.py — 未検証 第3弾（2026-08-29未明）。100/50土台・10年グロス。
A 日中損切り(h1が寄値×(1+X%)に触れたらその水準で買戻し=悲観約定・ギャップ無視の楽観は無し)
B 追加ショート(h1が寄値×(1+X%)に触れたら同サイズ追加・引成)
C 常連(同銘柄が直近20/60日にBASE通過あり) D 同銘柄の前回フェード結果 E 3傾斜の重ね掛け"""
import numpy as np, pandas as pd
exec(open("_bt_fade_untested_sweep.py", encoding="utf-8").read().split('print("=" * 118); print("■ 基準')[0])
d0 = select(P); base = evaluate(d0)
print(hdr); show("現行 100/50（引成のみ）", base)

def eval_pnl(d, pnl, label):
    dd = d.copy(); dd["pnl"] = pnl; show(label, evaluate(dd))

print("■ A 日中損切り（寄値×(1+X%)に高値が触れたら、その水準で買戻し）")
for x in (3, 5, 7, 10, 15):
    hit = d0.h1 >= d0.o1 * (1 + x / 100)
    pnl = np.where(hit, -x, d0.pnl)
    eval_pnl(d0, pnl, f"損切り +{x}%（発動{hit.mean()*100:.0f}%）")
print("■ B 追加ショート（寄値×(1+X%)に触れたら①と同サイズを追加・引成）: 合成損益%=(pnl + pnl_add)/2 をサイズ2倍で")
for x in (2, 3, 5):
    hit = d0.h1 >= d0.o1 * (1 + x / 100)
    add_pnl = (d0.o1 * (1 + x / 100) - d0.c1) / (d0.o1 * (1 + x / 100)) * 100   # 追加分の%
    # 追加した玉は同サイズ→円ベースで2玉分。evaluateはサイズ固定なので、pnlを「1玉換算」に合成: 発動時 pnl_total = pnl + add_pnl（2玉分の円を1玉サイズで表現）
    pnl = np.where(hit, d0.pnl + add_pnl, d0.pnl)
    eval_pnl(d0, pnl, f"追加 +{x}%（発動{hit.mean()*100:.0f}%・発動玉は資金2倍）")
    add_only = add_pnl[hit]
    print(f"     追加玉単独: n={int(hit.sum())} 平均{add_only.mean():+.2f}% 勝率{(add_only>0).mean()*100:.0f}% PF{pf(pd.Series(add_only.to_numpy())):.2f}")

print("■ C 常連（同銘柄が直近N日以内にBASE通過＝急騰の常連）")
allc = D[BASE][["ticker", "sig"]].copy(); allc["sigd"] = pd.to_datetime(allc.sig)
allc = allc.sort_values(["ticker", "sigd"])
def prior_count(days):
    out = pd.Series(0, index=d0.index)
    grp = {t: g.sigd.to_numpy() for t, g in allc.groupby("ticker")}
    for i, (t, s) in enumerate(zip(d0.ticker, pd.to_datetime(d0.sig))):
        arr = grp.get(t)
        if arr is None: continue
        lo = s - pd.Timedelta(days=days)
        out.iloc[i] = int(((arr >= np.datetime64(lo)) & (arr < np.datetime64(s))).sum())
    return out
for days in (20, 60):
    pc = prior_count(days); d0[f"pc{days}"] = pc
    r1 = d0[d0.rk == 1]
    for k in (0, 1, 2):
        sub = r1[(r1[f"pc{days}"] == k) if k < 2 else (r1[f"pc{days}"] >= 2)]
        print(f"   直近{days}日の急騰歴 {'>=2' if k == 2 else k}回: n={len(sub)} 平均{sub.pnl.mean():+.2f}% 勝率{(sub.pnl>0).mean()*100:.0f}%")
P3 = P.copy(); P3["_i"] = range(len(P3))
for days in (20, 60):
    pcP = pd.Series(0, index=P3.index)
    grp = {t: g.sigd.to_numpy() for t, g in allc.groupby("ticker")}
    vals = []
    for t, s in zip(P3.ticker, pd.to_datetime(P3.sig)):
        arr = grp.get(t); lo = s - pd.Timedelta(days=days)
        vals.append(0 if arr is None else int(((arr >= np.datetime64(lo)) & (arr < np.datetime64(s))).sum()))
    P3[f"pc{days}"] = vals
show("常連除外（20日内に急騰歴あり）", evaluate(select(P3[P3.pc20 == 0])))
show("常連除外（60日内に急騰歴あり）", evaluate(select(P3[P3.pc60 == 0])))
show("常連だけ（60日内に急騰歴あり）", evaluate(select(P3[P3.pc60 >= 1])))

print("■ D 同銘柄の前回フェード結果（①のみ・前回が勝ち/負け）")
r1 = d0[d0.rk == 1].sort_values("ent").copy()
r1["prev_pnl"] = r1.groupby("ticker").pnl.shift(1)
for lab, m in (("前回なし(初見)", r1.prev_pnl.isna()), ("前回勝ち", r1.prev_pnl > 0), ("前回負け", r1.prev_pnl <= 0)):
    sub = r1[m]; print(f"   {lab}: n={len(sub)} 平均{sub.pnl.mean():+.2f}% 勝率{(sub.pnl>0).mean()*100:.0f}%")

print("■ E 3傾斜の重ね掛け（①平均100万正規化・上限1.5倍/下限0.5倍）")
def norm(raw_fn):
    def f(d):
        raw = raw_fn(d); m = raw[d.rk == 1].mean(); return np.where(d.rk == 1, raw * S1 / m, S2)
    return f
def wk_mult(d):
    t = d[d.rk == 1].copy(); sh = (S1 / t.px // 100 * 100).astype(int); t["yen"] = t.pnl / 100 * sh * t.o1
    key = pd.to_datetime(t.ent).dt.strftime("%G-W%V"); agg = t.groupby(key).yen.sum().sort_index(); prev = agg.shift(1)
    mult = prev.apply(lambda v: 1.0 if pd.isna(v) else (0.7 if v < 0 else 1.3))
    m = pd.Series(1.0, index=d.index); m.loc[t.index] = key.map(mult).fillna(1.0).values; return m.to_numpy()
dev_m = lambda d: np.where(d.dev >= 20, 1.3, np.where(d.dev <= 15, 0.7, 1.0))
vr_m = lambda d: np.where(d.vr >= 3, 1.3, np.where(d.vr < 1.5, 0.7, 1.0))
show("乖離のみ", evaluate(d0, norm(lambda d: S1 * dev_m(d))))
show("出来高比のみ", evaluate(d0, norm(lambda d: S1 * vr_m(d))))
show("前週レジームのみ", evaluate(d0, norm(lambda d: S1 * wk_mult(d))))
show("乖離×出来高比", evaluate(d0, norm(lambda d: S1 * np.clip(dev_m(d) * vr_m(d), 0.5, 1.5))))
show("乖離×前週", evaluate(d0, norm(lambda d: S1 * np.clip(dev_m(d) * wk_mult(d), 0.5, 1.5))))
show("出来高比×前週", evaluate(d0, norm(lambda d: S1 * np.clip(vr_m(d) * wk_mult(d), 0.5, 1.5))))
show("3本重ね（clip0.5-1.5）", evaluate(d0, norm(lambda d: S1 * np.clip(dev_m(d) * vr_m(d) * wk_mult(d), 0.5, 1.5))))
show("3本重ね（clip0.7-1.3）", evaluate(d0, norm(lambda d: S1 * np.clip(dev_m(d) * vr_m(d) * wk_mult(d), 0.7, 1.3))))
