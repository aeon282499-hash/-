# -*- coding: utf-8 -*-
"""_bt_2026_newrules_0919.py — 本人「このルールで今年運営してたらどうなってるの？」（2026-09-19）
 A フェード 2026年（立花26年プール・〜2026-09-01）: 現行 vs 大型(時価総額≥1000億)除外 の月別・外れた大型玉の一覧
 B フェード 紙台帳（positions_day_paper.json・2026年の確定玉）に時価総額を付けて「大型を外していたら」
 C 極上 2026年: 1×150乗せなし（新ルール） vs 1×150＋乗せ／1×200＋乗せ（旧）の月別
実行: python -X utf8 _bt_2026_newrules_0919.py > _log_2026_newrules_0919.txt"""
from __future__ import annotations
import json, pickle, sys, time
import numpy as np, pandas as pd
sys.path.insert(0, ".")
from _bt_fade_26y_lib import load, base_mask, rank, settle, stats, S1, S2, pf

t0 = time.time()
D = load("20y"); P = D[base_mask(D)].copy().reset_index(drop=True); P["sig"] = P.sig.astype(str); P["ent"] = P.ent.astype(str); n = len(P)
SIGD = pd.to_datetime(P.sig); Y26 = (P.y == 2026).to_numpy()
VAL = pickle.load(open("_valuation_10y.pkl", "rb")); VC = VAL["cols"]; VD = VAL["data"]; VB = np.datetime64(VAL["base"])
sday = (SIGD.to_numpy().astype("datetime64[D]") - VB).astype(float); tick4 = P.ticker.astype(str).str[:4].to_numpy()
j = VC.index("MktCap"); MCAP = np.full(n, np.nan)
for i in np.where(P.y.to_numpy() >= 2017)[0]:
    m = VD.get(tick4[i])
    if m is None or len(m) == 0: continue
    k = np.searchsorted(m[:, 0], sday[i], side="left") - 1
    if k >= 0 and sday[i] - m[k, 0] <= 10: MCAP[i] = m[k, j] * 1e6
P["mcap"] = MCAP
P26 = P[Y26].copy(); fin = np.isfinite(P26.mcap.to_numpy()); big = fin & (P26.mcap.to_numpy() >= 1e11)
def run26(mask=None):
    Q = P26 if mask is None else P26[mask]; return settle(rank(Q), (S1, S2), 2)
b_old = run26(); b_new = run26(~big)
def summ(b):
    s = stats(b); mo = b.groupby("ym").yen.sum() / 1e4; day = b.groupby("ent").yen.sum().sort_index(); cum = day.cumsum(); dd = (cum - cum.cummax()).min() / 1e4
    return s, mo, dd
print(f"■ A フェード 2026年（立花プール 2026-01-05〜{P26.sig.max()}・寄成→引成・①100/②50）", flush=True)
for lab, b in (("現行（大型も撃つ）", b_old), ("新ルール（≥1000億は撃たない）", b_new)):
    s, mo, dd = summ(b)
    print(f"  {lab:<22} 玉{s['n']:>4} 勝率{s['win%']:.1f}% PF{s['PF']:.2f} 合計{s['total']/1e4:>+6,.0f}万 最悪月{s['worst_month']/1e4:+.0f} 最悪日{s['worst_day']/1e4:+.0f} DD{dd:+.0f} | 月別 " + " ".join(f"{k[5:]}月{v:+.0f}" for k, v in mo.items()))
# 外れた大型玉・繰り上がった玉
old_keys = set(zip(b_old.sig, b_old.ticker)); new_keys = set(zip(b_new.sig, b_new.ticker))
dropped = b_old[[k not in new_keys for k in zip(b_old.sig, b_old.ticker)]]; added = b_new[[k not in old_keys for k in zip(b_new.sig, b_new.ticker)]]
print(f"\n  外れた大型玉 {len(dropped)}本: 合計{dropped.yen.sum()/1e4:+,.0f}万・勝率{(dropped.pnl>0).mean()*100:.0f}%")
for _, r in dropped.sort_values("sig").iterrows():
    print(f"    {r.sig} #{r.rk} {r.ticker} 時価総額{r.mcap/1e8:,.0f}億 {r.pnl:+.2f}% {r.yen/1e4:+.1f}万")
print(f"  繰り上がった玉 {len(added)}本: 合計{added.yen.sum()/1e4:+,.0f}万・勝率{(added.pnl>0).mean()*100:.0f}%")
for _, r in added.sort_values("sig").iterrows():
    print(f"    {r.sig} #{r.rk} {r.ticker} 時価総額{(r.mcap/1e8 if np.isfinite(r.mcap) else float('nan')):,.0f}億 {r.pnl:+.2f}% {r.yen/1e4:+.1f}万")
# サイズ変更玉（順位が動いて①⇄②）
both = b_old.merge(b_new, on=["sig", "ticker"], suffixes=("_o", "_n")); moved = both[both.rk_o != both.rk_n]
print(f"  順位が動いた玉 {len(moved)}本（①⇄②で株数が変わる）: 円の差{(moved.yen_n - moved.yen_o).sum()/1e4:+,.0f}万")

# ── B 紙台帳（実運用と同じ判定・2026年）に時価総額を付ける ──
print("\n■ B 紙台帳 2026年（positions_day_paper.json・確定玉）で「大型を外していたら」", flush=True)
try:
    import daytrade_paper as dp
    from datetime import date
    bk = json.load(open("positions_day_paper.json", encoding="utf-8")); rows = bk.get("positions") if isinstance(bk, dict) else bk
    done = [p for p in rows if p.get("status") == "closed" and str(p.get("signal_date", "")).startswith("2026") and p.get("direction") == "SELL"]
    tok = dp._jq_token(); cache = {}
    def mcap_of(p):
        bd = p.get("basis_date") or p.get("signal_date")
        if bd not in cache:
            y, m_, d_ = (int(x) for x in bd.split("-")); cache[bd] = dp.fetch_mcap_map(tok, date(y, m_, d_))
        return cache[bd].get(p["ticker"][:4])
    tot = 0; tot_new = 0; big_rows = []
    for p in done:
        yen = p.get("pnl_yen") or 0; mc = mcap_of(p); p["_mc"] = mc
        tot += yen
        if mc is not None and mc >= 1000: big_rows.append(p)
        else: tot_new += yen
    print(f"  確定{len(done)}玉 合計{tot/1e4:+,.1f}万 → 大型{len(big_rows)}玉({sum((p.get('pnl_yen') or 0) for p in big_rows)/1e4:+,.1f}万)を外すと {tot_new/1e4:+,.1f}万（繰り上がり玉は帳簿に無いので未計上）")
    for p in sorted(big_rows, key=lambda x: x["signal_date"]):
        print(f"    {p['signal_date']} #{p.get('rank')} {p['name']}({p['ticker'][:4]}) 時価総額{p['_mc']:,}億 {p.get('pnl_pct', 0):+.2f}% {(p.get('pnl_yen') or 0)/1e4:+.1f}万")
    mo_b = {}
    for p in done:
        mo_b[p["signal_date"][:7]] = mo_b.get(p["signal_date"][:7], 0) + (p.get("pnl_yen") or 0)
    print("  月別（帳簿・現行）: " + " ".join(f"{ym}:{v/1e4:+.1f}万" for ym, v in sorted(mo_b.items())))
    mo_n = {}
    for p in done:
        if not (p.get("_mc") is not None and p["_mc"] >= 1000):
            mo_n[p["signal_date"][:7]] = mo_n.get(p["signal_date"][:7], 0) + (p.get("pnl_yen") or 0)
    print("  月別（帳簿・大型除外）: " + " ".join(f"{ym}:{v/1e4:+.1f}万" for ym, v in sorted(mo_n.items())))
except Exception as e:
    print(f"  帳簿の集計失敗: {e}")

# ── C 極上 2026年 ──
print("\n■ C 極上 2026年（立花プール・新ルール土台）", flush=True)
import re
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read(); exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))
for lab, kw in (("新ルール 1×150 乗せなし", dict(slots=1, size=1_500_000, addon_frac=0.0)), ("旧 1×150＋同額乗せ(9/14〜9/18)", dict(slots=1, size=1_500_000, addon_frac=1.0)),
                ("旧 1×200＋同額乗せ(9/19朝)", dict(slots=1, size=2_000_000, addon_frac=1.0)), ("参考 2×150 乗せなし(現金100万後)", dict(slots=2, size=1_500_000, addon_frac=0.0))):
    R = run_gen(G, SCORE, **kw); R = R[R.y == 2026]
    mo = R.assign(ym=DATEv[R.i.to_numpy()].strftime("%m")).groupby("ym").yen.sum() / 1e4
    print(f"  {lab:<28} 玉{len(R):>3} 勝率{(R.pnl>0).mean()*100:.1f}% 合計{R.yen.sum()/1e4:>+5,.0f}万 最悪玉{R.yen.min()/1e4:+.1f}万 | 月別 " + " ".join(f"{k}月{v:+.0f}" for k, v in mo.items()))
print(f"\n[done] {time.time()-t0:.0f}s")
