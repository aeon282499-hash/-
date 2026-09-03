# -*- coding: utf-8 -*-
"""_bt_new_rules_26y_screen.py — 26年日足で「新規ルール候補」を候補レベルでスクリーニング（2026-09-03）。

本人「データ揃ったけど新しく儲かりそうなルールない？」。作法: まず候補レベル（選定前）で4期間の平均%/勝率/PF を見て、
4期間すべてプラスのものだけを次段（枠シム・OOS）へ。コスト: 日計り往復0.1%、オーバーナイト0.1%を差し引く。
A. 急騰日(+7%・代金3億・現行GO条件)の分解: 引け→翌寄り(オーバーナイト) / 翌日寄→引(現行フェード) / 翌日引→翌々日引 / 翌々日寄→引
B. 「引け買い→翌寄り売り」= 急騰日のオーバーナイト・ロング（現行フェードの前半戦を取る案）
C. 月替わり効果: 1321(日経ETF) 月末前営業日引け買い→翌月3営業日目引け売り
D. 暴落後の反発: 5日で-15%以上×代金3億→翌寄り買い→5日後引け
E. 高値ブレイク: 終値が過去120日高値更新×出来高2倍×代金3億→翌寄り買い→10日後引け
F. 急騰の2日目ショート: +7%の翌日も陽線(寄→引で+3%以上)なら、翌々日寄り売り→引け
実行: python -X utf8 _bt_new_rules_26y_screen.py > _log_new_rules_26y_screen.txt
"""
from __future__ import annotations

import pickle

import numpy as np
import pandas as pd

H = pickle.load(open("tachibana_history.pkl", "rb"))["all_data"]
nm = dict(pickle.load(open("jquants_cache.pkl", "rb"))["name_map"])
COST_DAY, COST_ON = 0.10, 0.10
ERAS = ((2001, 2008, "01-08"), (2009, 2016, "09-16"), (2017, 2021, "17-21"), (2022, 2026, "22-26"))


def era_table(df, col, label, cost):
    d = df.dropna(subset=[col]).copy(); d["r"] = d[col] - cost
    print(f"\n[{label}] n={len(d):,}  コスト{cost}%差引")
    rows = []
    for lo, hi, e in ERAS:
        s = d[(d.y >= lo) & (d.y <= hi)].r
        if len(s) == 0:
            continue
        gp = s[s > 0].sum(); gl = -s[s <= 0].sum()
        rows.append({"era": e, "n": len(s), "avg%": round(s.mean(), 3), "win%": round((s > 0).mean() * 100, 1), "PF": round(gp / gl if gl else np.inf, 2), "中央値%": round(s.median(), 3)})
    t = pd.DataFrame(rows); print(t.to_string(index=False))
    ok = int((t["avg%"] > 0).sum()); print(f"  → 4期間プラス: {ok}/{len(t)}" + ("  ★候補" if ok == len(t) and len(t) == 4 else ""))
    return t


rows = []
for tk, df in H.items():
    if nm.get(tk) is None or len(df) < 200:
        continue
    df = df.dropna(subset=["Close"])
    o = df["Open"].to_numpy(float); c = df["Close"].to_numpy(float); h = df["High"].to_numpy(float); l = df["Low"].to_numpy(float); v = df["Volume"].to_numpy(float)
    n = len(c); idx = df.index
    cs, vs, hs, ls = map(pd.Series, (c, v, h, l))
    tov = (cs * vs).rolling(20).median().to_numpy(); vma = vs.shift(1).rolling(20).mean().to_numpy()
    pc = cs.shift(1); tr = pd.concat([hs - ls, (hs - pc).abs(), (ls - pc).abs()], axis=1).max(axis=1); atr = (tr.rolling(14).mean() / cs * 100).to_numpy()
    ma25 = cs.rolling(25).mean().to_numpy(); hi120 = hs.shift(1).rolling(120).max().to_numpy()
    ret1 = np.full(n, np.nan); ret1[1:] = (c[1:] / c[:-1] - 1) * 100
    ret5 = np.full(n, np.nan); ret5[5:] = (c[5:] / c[:-5] - 1) * 100
    y = idx.year.to_numpy()
    for t in range(30, n - 12):
        if not (c[t] > 0 and o[t + 1] > 0 and np.isfinite(tov[t]) and tov[t] >= 3e8 and np.isfinite(vma[t]) and vma[t] >= 1e5):
            continue
        rec = None
        # A/B/F: 急騰日
        if ret1[t] >= 7 and atr[t] >= 5 and (c[t] / ma25[t] - 1) * 100 >= 12 and v[t] / vma[t] < 6 and (h[t] - l[t]) / c[t] * 100 > 5 and c[t] * 100 <= 1e6:
            rec = {"kind": "spike", "y": y[t + 1], "ticker": tk,
                   "overnight": (o[t + 1] / c[t] - 1) * 100, "d1_oc_short": (o[t + 1] - c[t + 1]) / o[t + 1] * 100,
                   "d1c_d2c_short": (c[t + 1] - c[t + 2]) / c[t + 1] * 100, "d2_oc_short": (o[t + 2] - c[t + 2]) / o[t + 2] * 100 if o[t + 2] > 0 else np.nan,
                   "d1_up3": (c[t + 1] / o[t + 1] - 1) * 100 >= 3}
            rows.append(rec)
        # D: 暴落後の反発
        if ret5[t] <= -15 and c[t] * 100 <= 1e6:
            rows.append({"kind": "crash", "y": y[t + 1], "ticker": tk, "buy_o1_c5": (c[t + 5] / o[t + 1] - 1) * 100, "buy_o1_c1": (c[t + 1] / o[t + 1] - 1) * 100})
        # E: 高値ブレイク
        if np.isfinite(hi120[t]) and c[t] > hi120[t] and v[t] / vma[t] >= 2 and c[t] * 100 <= 1e6:
            rows.append({"kind": "breakout", "y": y[t + 1], "ticker": tk, "buy_o1_c10": (c[t + 10] / o[t + 1] - 1) * 100, "buy_o1_c3": (c[t + 3] / o[t + 1] - 1) * 100})
D = pd.DataFrame(rows)
S = D[D.kind == "spike"]
print("=" * 100); print("A. 急騰日（現行フェードGO条件）の前後分解  ※26年・候補レベル・現存銘柄のみ"); print("=" * 100)
era_table(S, "overnight", "引け→翌寄り（ロングなら＋）", COST_ON)
era_table(S, "d1_oc_short", "翌日 寄→引 ショート＝現行フェード", COST_DAY)
era_table(S, "d1c_d2c_short", "翌日引け→翌々日引け ショート（持ち越し）", COST_ON)
era_table(S, "d2_oc_short", "翌々日 寄→引 ショート", COST_DAY)
print("\n" + "=" * 100); print("F. 急騰の翌日も強かった玉（寄→引+3%以上）の翌々日 寄→引 ショート"); print("=" * 100)
era_table(S[S.d1_up3], "d2_oc_short", "翌日陽線+3%後の翌々日ショート", COST_DAY)
era_table(S[~S.d1_up3], "d2_oc_short", "（対照）翌日陰線後の翌々日ショート", COST_DAY)
print("\n" + "=" * 100); print("D. 5日で-15%以上の暴落後 翌寄り買い"); print("=" * 100)
Cr = D[D.kind == "crash"]
era_table(Cr, "buy_o1_c1", "翌日引けまで", COST_DAY)
era_table(Cr, "buy_o1_c5", "5日後引けまで", COST_ON)
print("\n" + "=" * 100); print("E. 120日高値ブレイク×出来高2倍 翌寄り買い"); print("=" * 100)
Br = D[D.kind == "breakout"]
era_table(Br, "buy_o1_c3", "3日後引けまで", COST_ON)
era_table(Br, "buy_o1_c10", "10日後引けまで", COST_ON)
print("\n" + "=" * 100); print("C. 月替わり効果（1321 日経225ETF）: 月末前営業日引け買い→翌月3営業日目引け売り"); print("=" * 100)
etf = H.get("1321.T")
if etf is not None and len(etf):
    e = etf.dropna(subset=["Close"]); c = e["Close"]; ym = c.index.to_period("M")
    rows2 = []
    months = sorted(set(ym))
    for i in range(1, len(months) - 1):
        cur_ = c[ym == months[i]]; nxt = c[ym == months[i + 1]]
        if len(cur_) < 3 or len(nxt) < 3:
            continue
        rows2.append({"y": months[i + 1].year, "r": (nxt.iloc[2] / cur_.iloc[-2] - 1) * 100})
    T = pd.DataFrame(rows2); era_table(T, "r", "月替わり 4営業日保有", 0.05)
    for lo, hi, e_ in ERAS:
        s = T[(T.y >= lo) & (T.y <= hi)].r; print(f"   {e_}: 年換算 ≒ {s.mean()*12:+.1f}% (12回/年)")
open("_log_new_rules_26y_screen.txt", "w", encoding="utf-8").write("(コンソール出力参照)")
