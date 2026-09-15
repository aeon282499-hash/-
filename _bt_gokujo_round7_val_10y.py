# -*- coding: utf-8 -*-
"""_bt_gokujo_round7_val_10y.py — 極上ラウンド7（2026-09-15 本人「J-Qの新データで売買シグナルをさらに儲かるように」）
J-Quants /equities/valuation（2026-09-14提供開始・_valuation_10y.pkl・2016-09-15〜）の公式値で、買い選定の残り軸を測る:
  A 時価総額（三分位・絶対帯）  B PBR  C PER/赤字  D ROE・予想ROE  E 会社予想EPS成長  F 並び順に時価総額を混ぜる
土台＝round6と同一（極上 新ルール: DAY1CUT＋2日目勝ち乗せ・1×150万・10年=2016-08〜）。判定＝土台超え(16-20/21-26/PF/最悪年)＋同率ランダム除外の99%点超え。
※10年しか無いデータなので「10年で勝てれば本物」は幻（9/5実証）＝採用は次の26年代理検証(売買代金順位の帯)と併せて判断。
実行: python -X utf8 _bt_gokujo_round7_val_10y.py > _log_gokujo_round7_val_10y.txt
"""
import numpy as np, pandas as pd, json, time
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read()
head = src.split("# ── A 決算日近接 ──")[0]
exec(head)

t1 = time.time()
V = pd.read_pickle("_valuation_10y.pkl")
V["code4"] = V.Code.astype(str).str[:4]
V = V.sort_values(["code4", "Date"])
cand = pd.DataFrame({"i": np.arange(n), "code4": C.ticker.astype(str).str[:4].to_numpy(), "entd": entd.to_numpy()})
cand = cand[GV[cand.i.to_numpy()]].sort_values("entd")
J = pd.merge_asof(cand, V[["code4", "Date", "EPS", "FwdEPS", "BPS", "ROE", "FwdROE", "PER", "FwdPER", "PBR", "MktCap"]],
                  left_on="entd", right_on="Date", by="code4", allow_exact_matches=False, tolerance=pd.Timedelta(days=10))
J = J.set_index("i")
def col(name):
    x = np.full(n, np.nan); x[J.index.to_numpy()] = J[name].to_numpy(dtype=float); return x
MCAP, PBR, PER, FPER, ROE, FROE, EPS, FEPS = (col(c) for c in ("MktCap", "PBR", "PER", "FwdPER", "ROE", "FwdROE", "EPS", "FwdEPS"))
MCAP = MCAP * 1e6   # J-Quants の MktCap は百万円単位（1301=27,938 → 279億円）→ 円へ
known = GV & np.isfinite(MCAP)
print(f"\n[valuation] 結合 {known.sum()}/{GV.sum()}件 ({time.time()-t1:.0f}s)・時価総額 中央値{np.nanmedian(MCAP[known])/1e8:,.0f}億 PBR中央値{np.nanmedian(PBR[known]):.2f}", flush=True)

def tercile_block(nm, x, low_good_label="下位1/3", extra=()):
    ok = GV & np.isfinite(x); q1, q2 = np.nanquantile(x[ok], [1/3, 2/3])
    lo, mid, hi = ok & (x <= q1), ok & (x > q1) & (x <= q2), ok & (x > q2)
    print(f"\n■ {nm}: 既知{ok.sum()}件 三分位境界 {q1:,.3g}/{q2:,.3g}", flush=True)
    pm("下位1/3", lo); pm("中位", mid); pm("上位1/3", hi); pm("不明", GV & ~ok)
    tests = [(f"{nm} 上位1/3を除外", G & ~hi), (f"{nm} 下位1/3を除外", G & ~lo), (f"{nm} 中位のみ", G & ~lo & ~hi),
             (f"{nm} 上位1/3のみ", G & (hi | ~ok)), (f"{nm} 下位1/3のみ", G & (lo | ~ok))] + list(extra)
    for lab, m in tests:
        frac = 1 - (GV & m).sum() / GV.sum()
        nf, sd = noise_floor(G, frac) if 0 < frac < 0.9 else (None, 0)
        line10(lab, m, B10, nf); print(f"      （除外率{frac*100:.0f}%・同率ランダム除外の99%点 {'-' if nf is None else round(nf)}万 sd{sd:.0f}）", flush=True)

# A 時価総額
okm = GV & np.isfinite(MCAP)
tercile_block("時価総額", MCAP, extra=(
    ("時価総額<300億を除外", G & ~(okm & (MCAP < 3e10))), ("時価総額<1000億を除外", G & ~(okm & (MCAP < 1e11))),
    ("時価総額>1兆を除外", G & ~(okm & (MCAP > 1e12))), ("時価総額>3000億を除外", G & ~(okm & (MCAP > 3e11))),
    ("300〜3000億のみ", G & (okm & (MCAP >= 3e10) & (MCAP <= 3e11)))))
# B PBR
okp = GV & np.isfinite(PBR) & (PBR > 0)
tercile_block("PBR", np.where(PBR > 0, PBR, np.nan), extra=(("PBR≤1を除外", G & ~(okp & (PBR <= 1))), ("PBR≥3を除外", G & ~(okp & (PBR >= 3))), ("PBR≤2のみ(9/2の疑惑再検)", G & (okp & (PBR <= 2)))))
# C PER / 赤字
loss = GV & np.isfinite(EPS) & (EPS <= 0)
print(f"\n■ 赤字(EPS≤0) {loss.sum()}件 / 黒字 {(GV & np.isfinite(EPS) & (EPS > 0)).sum()}件", flush=True); pm("赤字", loss); pm("黒字", GV & np.isfinite(EPS) & (EPS > 0))
for lab, m in (("赤字を除外", G & ~loss), ("赤字のみ", G & (loss | ~np.isfinite(EPS)))):
    frac = 1 - (GV & m).sum() / GV.sum(); nf, sd = noise_floor(G, frac) if 0 < frac < 0.9 else (None, 0)
    line10(lab, m, B10, nf); print(f"      （除外率{frac*100:.0f}%・ランダム99%点 {'-' if nf is None else round(nf)}万 sd{sd:.0f}）", flush=True)
tercile_block("PER(黒字のみ)", np.where(PER > 0, PER, np.nan))
tercile_block("予想PER(黒字のみ)", np.where(FPER > 0, FPER, np.nan))
# D ROE
tercile_block("ROE", ROE, extra=(("ROE≤0を除外", G & ~(GV & np.isfinite(ROE) & (ROE <= 0))),))
tercile_block("予想ROE", FROE)
# E 会社予想EPS成長
g = np.where(np.isfinite(EPS) & np.isfinite(FEPS) & (EPS > 0), FEPS / EPS - 1, np.nan)
tercile_block("予想EPS成長(FwdEPS/EPS-1)", g, extra=(("減益予想(成長<0)を除外", G & ~(GV & np.isfinite(g) & (g < 0))),))
# F 並び順（SCOREに時価総額順位を混ぜる）
print("\n■ F 並び順に時価総額を混ぜる（土台の玉数は変えない）", flush=True)
for w in (0.3, 0.6):
    for lab, key in ((f"小型優先 w{w}", SCORE + w * rk(MCAP, low_good=True)), (f"大型優先 w{w}", SCORE + w * rk(MCAP, low_good=False)),
                     (f"低PBR優先 w{w}", SCORE + w * rk(np.where(PBR > 0, PBR, np.nan), low_good=True)), (f"高ROE優先 w{w}", SCORE + w * rk(ROE, low_good=False))):
        R = with_addon(run_idx(G & V10, key, PNL1, EXO1)); r = stat10(R)
        j = " ★" if (r["h1"] > B10["h1"] and r["h2"] > B10["h2"] and r["pf"] >= B10["pf"] and r["worst"] >= B10["worst"] - 1) else ""
        print(f"  {lab:<34} 玉{r['n']:>4} 勝率{r['win']:>5.1f}% PF{r['pf']:>5.2f} 10年{r['tot']:>+6,.0f}万 | 16-20{r['h1']:>+5,.0f} 21-26{r['h2']:>+5,.0f} | 勝年{r['wy']}/{r['ny']} 最悪年{r['worst']:>+4,.0f}" + j, flush=True)
print(f"\n[done] {time.time()-t0:.0f}s")
