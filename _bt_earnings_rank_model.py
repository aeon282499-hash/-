# -*- coding: utf-8 -*-
"""_bt_earnings_rank_model.py — 決算候補を「勝てる順」に並べられるのか、総当たりで決着（2026-07-31・本番無変更）。

本人「いろんなくまなく探してもシグナルを勝てる順に並び替えるのは無理なん?」

これまでやったのは「単一の特徴量で並べ替える」を数通り（RSI昇順/代金順/スコア順/
業績YoY順/進捗順…）だけ。**全特徴量を使った学習モデルで並べる**のは一度もやっていない。
ここで決着させる。

問いの正確な形: ある晩に候補がN件あり枠は8。**その晩の中での順位**を当てられるか。
  → だから評価は「晩ごとの横断的な順位相関(IC)」で見る。プールした相関ではない。
  → 学習は必ずウォークフォワード（過去だけで学習し未来で測る）。

特徴量: 価格系(rsi/runup5/atr_pct/price/代金/信用) + 決算書(YoY/進捗/上方修正/連続増益)
        + 場況(日経25MA乖離) + その晩の候補数 + その銘柄の前回決算の反応

実行: python -X utf8 _bt_earnings_rank_model.py
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

warnings.filterwarnings("ignore")

import _bt_earnings_fundamentals as B   # 特徴量つき候補 E と本番同一の sim

E = B.E.copy()
SLOTS = 8

# ── 追加の特徴量 ─────────────────────────────────────────
E["n_night"] = E.groupby("d0")["ticker"].transform("size")      # 決算集中度
E = E.sort_values(["ticker", "d0"])
E["prev_gap"] = E.groupby("ticker")["gap"].shift(1)             # 前回決算の反応（過去のみ）
E["logtov"] = np.log10(E["tov20"].clip(lower=1))
E["logadv"] = np.log10(E["adv20"].clip(lower=1))

FEATS = ["rsi", "runup5", "atr_pct", "price", "logtov", "logadv", "days_cover", "ratio",
         "op_yoy", "sales_yoy", "prog", "guid_up", "up_cnt", "n_night", "prev_gap"]
E = E.sort_values(["d0", "rsi"]).reset_index(drop=True)
print(f"[データ] 候補{len(E):,}件 / {E.d0.min()}〜{E.d0.max()} / 特徴量{len(FEATS)}本")
print("[欠測率] " + " ".join(f"{c}={E[c].isna().mean()*100:.0f}%" for c in FEATS))


# ── 1. 単変量の「晩ごと横断IC」（そもそも順位情報があるのか）─────
def nightly_ic(col: str, y: str = "gap") -> tuple[float, float, int]:
    ics = []
    for _, g in E.groupby("d0"):
        g = g[[col, y]].dropna()
        if len(g) < 4 or g[col].nunique() < 3:
            continue
        r = spearmanr(g[col], g[y]).statistic
        if np.isfinite(r):
            ics.append(r)
    if not ics:
        return np.nan, np.nan, 0
    a = np.array(ics)
    return a.mean(), a.mean() / (a.std() / np.sqrt(len(a))), len(a)


print("\n" + "=" * 92)
print("① 単変量: その晩の中での順位相関（IC）。|t|>2 でようやく «情報がある» と言える")
print("=" * 92)
print(f"  {'特徴量':<14}{'平均IC':>10}{'t値':>9}{'有効な晩':>10}   判定")
rows = []
for c in FEATS:
    ic, t, n = nightly_ic(c)
    rows.append((c, ic, t, n))
for c, ic, t, n in sorted(rows, key=lambda x: -abs(x[2] if np.isfinite(x[2]) else 0)):
    v = "情報あり" if abs(t) > 2 else ("弱い" if abs(t) > 1.5 else "—")
    print(f"  {c:<14}{ic:>+10.4f}{t:>9.2f}{n:>10}   {v}")


# ── 2. 学習モデルで並べる（ウォークフォワード）───────────────
from lightgbm import LGBMRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

E["fold"] = pd.to_datetime(E["d0"]).dt.year


def walk_forward(model_fn, tag: str):
    """2016-YYYYで学習し YYYY+1 を予測。全年ぶんの予測を集めてICと選定シムで測る。"""
    pred = pd.Series(np.nan, index=E.index)
    for y in range(2019, 2027):                       # 最低3年分の学習を確保
        tr = E[E.fold < y]
        te = E[E.fold == y]
        if len(tr) < 300 or not len(te):
            continue
        Xtr = tr[FEATS].astype(float)
        Xte = te[FEATS].astype(float)
        ytr = tr["gap"].astype(float)
        ok = ytr.notna()
        m = model_fn()
        if isinstance(m, Ridge):
            med = Xtr.median()
            sc = StandardScaler().fit(Xtr.fillna(med)[ok.values])
            m.fit(sc.transform(Xtr.fillna(med)[ok.values]), ytr[ok])
            pred.loc[te.index] = m.predict(sc.transform(Xte.fillna(med)))
        else:
            m.fit(Xtr[ok.values], ytr[ok])
            pred.loc[te.index] = m.predict(Xte)
    E[tag] = pred
    sub = E[E[tag].notna()]
    ics = []
    for _, g in sub.groupby("d0"):
        g = g[[tag, "gap"]].dropna()
        if len(g) < 4 or g[tag].nunique() < 3:
            continue
        r = spearmanr(g[tag], g["gap"]).statistic
        if np.isfinite(r):
            ics.append(r)
    a = np.array(ics)
    t = a.mean() / (a.std() / np.sqrt(len(a))) if len(a) > 1 else np.nan
    print(f"  {tag:<22} OOS平均IC {a.mean():+.4f} (t={t:.2f}, 晩数{len(a)})")
    return sub


print("\n" + "=" * 92)
print("② 全特徴量の学習モデルで並べる（過去だけで学習→翌年で測る＝ウォークフォワード）")
print("=" * 92)
walk_forward(lambda: Ridge(alpha=10.0), "pred_ridge")
walk_forward(lambda: LGBMRegressor(n_estimators=300, learning_rate=0.03, num_leaves=15,
                                   min_child_samples=40, subsample=0.8,
                                   colsample_bytree=0.8, verbose=-1), "pred_lgbm")


# ── 3. 実際に並べ替えて選定シムに乗せる（本命の答え）───────────
def sim_order(cols, asc, tag, sub=None):
    D = (sub if sub is not None else E)
    A = D.sort_values(["d0"] + cols, ascending=[True] + asc)
    P = B.sim(A, slots=SLOTS)
    y = P["pnl"] * 1_000_000 / 100
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = (P.groupby("year")["pnl"].sum()).reindex(range(2016, 2027), fill_value=0)
    print(f"  {tag:<30}{len(P):>6}{y.sum()/1e4:>+11,.0f}万{dd/1e4:>+10,.0f}万"
          f"{(P.pnl > 0).mean()*100:>7.1f}%{int((yr > 0).sum()):>5}/11")


print("\n" + "=" * 92)
print("③ 実際に並べ替えて8枠シム（2019年以降＝モデルの予測がある期間だけで公平に比較）")
print("=" * 92)
S = E[E["pred_lgbm"].notna()].copy()
print(f"  {'並び順':<30}{'件数':>6}{'累計':>12}{'最大DD':>11}{'勝率':>8}{'陽性年':>8}")
sim_order(["rsi"], [True], "RSI昇順（現行）", S)
sim_order(["pred_lgbm"], [False], "モデル予測の高い順", S)
sim_order(["pred_ridge"], [False], "Ridge予測の高い順", S)
sim_order(["rsi"], [False], "RSI降順（現行の逆＝対照）", S)
S["rnd"] = np.random.RandomState(0).rand(len(S))
sim_order(["rnd"], [True], "ランダム（対照）", S)

print("\n[読み方] モデルがRSI昇順とランダムの両方に勝てなければ、順位に情報は無い。")
print("         ランダムと現行の差が小さいこと自体が «並び順は効かない» の証拠になる。")
