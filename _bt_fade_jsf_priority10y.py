# -*- coding: utf-8 -*-
"""_bt_fade_jsf_priority10y.py — フェードの並び順に「売り禁/注意喚起 優先」を混ぜる案を10年で測る（2026-09-03）。

発端: 日証金の日次データ（2023-04〜）で「制限措置あり優先」が合算+574万→+637万・両期間プラス（_bt_fade_jsf_rank.py）。
ただし3.3年＝1相場なので、同じ情報（J-Quants 信用規制フラグ・_margin_alert_bal_10y.pkl・as-of 7暦日）で
10年に延ばし、本来の採用バー（17-21/22-26 両期間改善 × 上位3除去でも残る × 最悪月悪化なし）で判定する。
評価は 1番×100万＋2番×50万（8/31〜の実運用）の合算。参考に 1番のみ も併記。
実行: python -X utf8 _bt_fade_jsf_priority10y.py > _log_fade_jsf_priority10y.txt
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE1, SIZE2 = 1_000_000, 500_000
FLAGS = ["Restricted", "DailyPublication", "Monitoring", "RestrictedByJSF", "PrecautionByJSF"]
P = pd.read_pickle("_fade_pool_v5_100.pkl")
G = P[(P.gain >= 7.0) & (P.vr < 6.0) & (P.atr >= 5.0) & (P.dev >= 12.0)
      & (P.tov >= 3e8) & (P.rng > 5.0) & (P.vol_avg >= 100_000)].copy()
G["ym"] = G.ent.str[:7]

A = pd.read_pickle("_margin_alert_bal_10y.pkl")
A = A[A.Code.astype(str).str.len() == 5]
A = A[A.Code.astype(str).str[-1] == "0"]
A["code4"] = A.Code.astype(str).str[:4]
A = A.sort_values(["code4", "PubDate"])
by_code = {c: g.reset_index(drop=True) for c, g in A.groupby("code4")}
flg = {c: np.zeros(len(G)) for c in FLAGS}
on = np.zeros(len(G), dtype=bool)
for i, (cd, d_sig) in enumerate(zip(G.ticker.str.replace(".T", "", regex=False), G.sig)):
    g = by_code.get(cd)
    if g is None:
        continue
    pos = g.PubDate.searchsorted(d_sig, side="right") - 1
    if pos < 0:
        continue
    r = g.iloc[pos]
    if (pd.Timestamp(d_sig) - pd.Timestamp(r.PubDate)).days > 7:
        continue
    on[i] = True
    for c in FLAGS:
        v = r[c]
        flg[c][i] = float(v) if not isinstance(v, str) else 0.0
G["al_on"] = on
for c in FLAGS:
    G["al_" + c] = flg[c]
G["jsf"] = (G.al_RestrictedByJSF == 1).astype(float)          # 売り禁（申込停止/制限）
G["jsf_or_prec"] = ((G.al_RestrictedByJSF == 1) | (G.al_PrecautionByJSF == 1)).astype(float)   # ＋注意喚起
G["any_alert"] = G.al_on.astype(float)
print(f"[join] 候補{len(G):,} 期間{G.sig.min()}〜{G.sig.max()} 売り禁率{G.jsf.mean()*100:.0f}% 売り禁or注意喚起{G.jsf_or_prec.mean()*100:.0f}% リスト載り{G.any_alert.mean()*100:.0f}%")


def pct_desc(d, col):
    return d.groupby("sig")[col].rank(ascending=False, pct=True)


def rank_by(d, key_fn):
    d = d.copy(); d["key"] = key_fn(d)
    d = d.sort_values(["sig", "key", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    return d


def settle(d, size):
    d = d.copy(); d["sh"] = (size / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy(); d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def pf(x):
    n = abs(x[x <= 0].sum())
    return x[x > 0].sum() / n if n else float("inf")


def evaluate(label, key_fn):
    R = rank_by(G, key_fn)
    b1 = settle(R[R.rk == 1], SIZE1); b2 = settle(R[R.rk == 2], SIZE2); both = pd.concat([b1, b2])
    yy = both.groupby("y").yen.sum(); ym = both.groupby("ym").yen.sum()
    a = both[both.y <= 2021].yen.sum(); b = both[both.y >= 2022].yen.sum()
    top3 = both.nlargest(3, "yen").yen.sum()
    y1 = b1.groupby("y").yen.sum()
    print(f"  {label:<36} 合算{both.yen.sum():>+13,.0f} 前半17-21{a:>+12,.0f} 後半22-26{b:>+12,.0f} 勝ち年{int((yy>0).sum())}/{yy.index.nunique()}"
          f" 最悪月{ym.min():>+9,.0f} 上位3除去{both.yen.sum()-top3:>+13,.0f} | 1番{len(b1)}玉 PF{pf(b1.pnl):.2f} {b1.yen.sum():>+13,.0f} 売り禁率{b1.jsf.mean()*100:>3.0f}%"
          f" | 2番{len(b2)}玉 PF{pf(b2.pnl):.2f} {b2.yen.sum():>+11,.0f}")
    print(f"      年別(合算,万): { {int(k): round(v/1e4) for k, v in yy.items()} }")
    return both


cur = lambda d: (pct_desc(d, "dev") + pct_desc(d, "atr")) / 2
print("=" * 200)
print("並び順の比較 10年（1番×100万＋2番×50万）")
print("=" * 200)
B = evaluate("現行: 乖離×ATR", cur)
evaluate("売り禁(RestrictedByJSF)優先 → 現行", lambda d: (1 - d.jsf) * 10 + cur(d))
evaluate("売り禁 or 注意喚起 優先 → 現行", lambda d: (1 - d.jsf_or_prec) * 10 + cur(d))
evaluate("リスト載り(何らかの規制)優先 → 現行", lambda d: (1 - d.any_alert) * 10 + cur(d))
evaluate("現行 - 0.5×売り禁(タイブレーク寄り)", lambda d: cur(d) - 0.5 * d.jsf)
evaluate("現行 - 0.25×売り禁", lambda d: cur(d) - 0.25 * d.jsf)
evaluate("現行 - 0.5×(売り禁 or 注意喚起)", lambda d: cur(d) - 0.5 * d.jsf_or_prec)
evaluate("[参考] 売り禁を後回し(現行+0.5×売り禁)", lambda d: cur(d) + 0.5 * d.jsf)

print("\n[層別・現行1番] 売り禁 / 注意喚起のみ / 規制なし")
R = rank_by(G, cur); b1 = settle(R[R.rk == 1], SIZE1)
for lab, m in (("売り禁", b1.jsf == 1), ("注意喚起のみ", (b1.jsf == 0) & (b1.al_PrecautionByJSF == 1)), ("リスト外", b1.al_on == 0), ("載りだが売り禁/注意喚起以外", (b1.al_on) & (b1.jsf_or_prec == 0))):
    s = b1[m]
    if len(s):
        print(f"  {lab:<22} {len(s):>5}玉 勝率{(s.pnl>0).mean()*100:>5.1f}% PF{pf(s.pnl):>5.2f} 平均{s.pnl.mean():>+6.2f}% 合計{s.yen.sum():>+13,.0f} 前半{s[s.y<=2021].yen.sum():>+12,.0f} 後半{s[s.y>=2022].yen.sum():>+12,.0f}")
