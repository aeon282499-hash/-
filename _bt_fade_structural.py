# -*- coding: utf-8 -*-
"""_bt_fade_structural.py — 売りフェード：フィルタでない「構造」の軸（2026-07-31）。

今日ここまでで振った軸は約60通り。**候補を削るフィルタ系は9軸試して全滅**したので、
残っているのは種類の違う話だけ。ここで測るのは:
  ①保有期間  引成で閉じずに2〜5日持つ（＝そもそも日計りが最適かの検証）
  ②建玉サイズ  全部50万固定をやめ、スコアに応じて張り分ける
  ③その日の候補数  「過熱銘柄が大量に出た日」と「1つしか出ない日」で質が違うか
  ④信用残  買残回転/信用倍率（スイングでは買残回転フィルタが効いた・フェードは未検証）
  ⑤業種  33業種のどれで効くか（業種capは棄却済みだが業種そのものは未検証）

⚠️多重比較: 今日60通り振った後なので、**名目上の「両期間改善」はもう弱い証拠**。
採用の敷居を上げる: 両期間改善 かつ 近傍が高原 かつ 機構が説明でき かつ **単独で年+5万以上**。

出力: _fade_pool_v3.pkl（先の終値c2-c5＋信用残を追加）
実行: python -X utf8 _bt_fade_structural.py
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

SIZE = 500_000
YEARS = list(range(2016, 2027))

print("[build] プールに先の終値と信用残を追加...", flush=True)
P = pd.read_pickle("_fade_pool_v2.pkl")
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))


def merge(tk):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        return None
    d = pd.concat(dfs).sort_index()
    return d[~d.index.duplicated(keep="last")]


MG = pickle.load(open("_margin_10y_full.pkl", "rb"))
rows = []
for tk, g in P.groupby("ticker"):
    df = merge(tk)
    if df is None:
        continue
    c = df["Close"].astype(float).to_numpy()
    v = df["Volume"].astype(float).to_numpy()
    tovs = pd.Series(c * v).rolling(20).mean().to_numpy()
    pos = {d.strftime("%Y-%m-%d"): i for i, d in enumerate(df.index)}
    # 信用残（週次）: シグナル日以前の最新レコードを引く
    m = MG.get(str(tk)[:4])
    if m is not None and len(m):
        m = m.sort_index()
        midx = np.array([d.strftime("%Y-%m-%d") for d in m.index])
        lv, sv = m["LongVol"].to_numpy(float), m["ShrtVol"].to_numpy(float)
    else:
        midx = None
    for r in g.itertuples():
        i = pos.get(r.sig)
        if i is None:
            continue
        rec = {"sig": r.sig, "ticker": tk}
        for k in range(2, 6):                     # entry(=i+1)から k 日目の終値
            j = i + k
            rec[f"c{k}"] = float(c[j]) if j < len(c) else np.nan
        if midx is not None:
            p = np.searchsorted(midx, r.sig, side="right") - 1
            if p >= 0 and tovs[i] > 0:
                rec["days_cover"] = lv[p] * c[i] / tovs[i]     # 買残の回転日数
                rec["mratio"] = lv[p] / sv[p] if sv[p] > 0 else np.nan   # 信用倍率
        rows.append(rec)
X = pd.DataFrame(rows)
D = P.merge(X, on=["sig", "ticker"], how="left")
D["ym"] = D.ent.str[:7]
D.to_pickle("_fade_pool_v3.pkl")
print(f"[build] {len(D):,}件 / 信用残が付いた割合 {D.days_cover.notna().mean()*100:.0f}%"
      f" / c5が取れた割合 {D.c5.notna().mean()*100:.0f}%\n", flush=True)


def pick(d, n=2):
    d = d[(d.gain >= 7.0) & (d.vr < 6.0) & (d.atr >= 5.0) & (d.dev >= 12.0)
          & (d.tov >= 3e8) & (d.rng > 5.0) & (d.vol_avg >= 100_000)].copy()
    r = None
    for col in ("dev", "atr"):
        r = (d.groupby("sig")[col].rank(ascending=False, pct=True) if r is None
             else r + d.groupby("sig")[col].rank(ascending=False, pct=True))
    d["mix"] = r / 2
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d["ncand"] = d.groupby("sig")["ticker"].transform("size")
    d = d[d.rk <= n].copy()
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    return d[d.sh > 0].copy()


B = pick(D)
B["yen"] = B.pnl / 100 * B.sh * B.o1


def st(d, col="yen", pcol="pnl"):
    yr = d.groupby("y")[col].sum().reindex(YEARS, fill_value=0)
    mm = d.groupby("ym")[col].sum()
    p = d[pcol]; loss = -p[p < 0].sum()
    return dict(n=len(d), wr=(p > 0).mean() * 100, pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d[col].sum(), avg=d[col].sum() / 11, win=int((yr > 0).sum()),
                wm=mm.min(), a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


BASE = st(B)
HDR = (f"  {'設定':<28}{'玉数':>6}{'勝率':>7}{'PF':>6}{'年平均':>12}{'勝ち':>6}{'最悪月':>11}"
       f"{'前半':>12}{'後半':>12}{'判定':>12}")


def row(lab, s):
    ok = s["a"] > BASE["a"] and s["b"] > BASE["b"] and s["win"] >= BASE["win"]
    big = s["avg"] - BASE["avg"] >= 50_000
    mk = ("★採用候補" if ok and big else ("両期間◯だが小" if ok else ("片側" if s["tot"] > BASE["tot"] else "")))
    print(f"  {lab:<28}{s['n']:>6}{s['wr']:>6.1f}%{s['pf']:>6.2f}{s['avg']:>+11,.0f}円{s['win']:>4}/11"
          f"{s['wm']:>+10,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>14}")


def sec(t):
    print("\n" + "=" * 130); print(t); print("=" * 130); print(HDR); row("現行(引成・50万固定)", BASE)


sec("① 保有期間 ─ 引成で閉じず何日か持つ（空売りなので 寄り→N日目の終値）")
for k in range(2, 6):
    x = B[B[f"c{k}"].notna()].copy()
    x["pnl2"] = (x.o1 - x[f"c{k}"]) / x.o1 * 100
    x["yen2"] = x.pnl2 / 100 * x.sh * x.o1
    row(f"{k}日目の引けまで持つ", st(x, "yen2", "pnl2"))

sec("② 建玉サイズ ─ スコア上位に厚く張る（合計の資金は同じ）")
for lab, w in (("1番70万/2番30万", {1: 700_000, 2: 300_000}),
               ("1番80万/2番20万", {1: 800_000, 2: 200_000}),
               ("1番100万/2番0(=1本)", {1: 1_000_000, 2: 0})):
    x = B.copy()
    x["sh2"] = [int(w[r] / p // 100 * 100) for r, p in zip(x.rk, x.px)]
    x = x[x.sh2 > 0]
    x["yen2"] = x.pnl / 100 * x.sh2 * x.o1
    row(lab, st(x, "yen2"))

sec("③ その日の候補数（過熱銘柄が大量に出た日 vs 1つしか出ない日）")
for lo, hi, lab in ((1, 1, "候補1本の日だけ"), (2, 3, "候補2-3本の日"), (4, 8, "候補4-8本の日"),
                    (9, 999, "候補9本以上の日"), (1, 3, "候補3本以下だけ撃つ"), (4, 999, "候補4本以上だけ撃つ")):
    row(lab, st(B[(B.ncand >= lo) & (B.ncand <= hi)]))

sec("④ 信用残 ─ 買残の回転日数（スイングでは>0.8日を除外して効いた）")
M = B[B.days_cover.notna()]
print(f"  ※信用残が付いた玉 {len(M)}/{len(B)}件で比較")
row("信用残あり玉のみ(基準)", st(M))
for v in (0.3, 0.5, 0.8, 1.5):
    row(f"買残回転 {v}日以下だけ", st(M[M.days_cover <= v]))
for v in (0.5, 0.8):
    row(f"買残回転 {v}日超だけ", st(M[M.days_cover > v]))

sec("⑤ 信用倍率（買残/売残・低いほど売り長＝踏み上げ済み）")
R = B[B.mratio.notna()]
for lo, hi, lab in ((0, 1, "売り長(倍率<1)"), (1, 3, "倍率1-3"), (3, 10, "倍率3-10"),
                    (10, 1e9, "倍率10+(売残ほぼ無)"), (0, 3, "倍率3以下だけ撃つ"), (3, 1e9, "倍率3超だけ撃つ")):
    row(lab, st(R[(R.mratio >= lo) & (R.mratio < hi)]))

print("\n" + "=" * 130)
print("⑥ 業種別（33業種・玉数が多い順に上位15）")
print("=" * 130)
print(f"  {'業種':<22}{'玉数':>6}{'勝率':>7}{'PF':>6}{'10年計':>13}{'前半':>12}{'後半':>12}")
g = B[B.sector != ""].groupby("sector")
for s_, x in sorted(g, key=lambda kv: -len(kv[1]))[:15]:
    p = x.pnl; loss = -p[p < 0].sum()
    print(f"  {s_:<22}{len(x):>6}{(p>0).mean()*100:>6.1f}%{(p[p>0].sum()/loss if loss else 9.99):>6.2f}"
          f"{x.yen.sum():>+12,.0f}円{x[x.y<=2021].yen.sum():>+11,.0f}円{x[x.y>=2022].yen.sum():>+11,.0f}円")
