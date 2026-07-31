# -*- coding: utf-8 -*-
"""_bt_fade_market.py — 売りフェード：まだ使っていないデータを全部つなぐ（2026-07-31）。

これまで使っていたのは「その銘柄自身の日足」だけだった。未使用のデータを接続する:
  A. `_indices_10y.pkl`   TOPIX＋33業種指数の10年日足（79本）
  B. `_short_ratio_10y.pkl` 業種別の空売り比率（日次）
  C. jquantsキャッシュ全体から計算する **市場ブレス**（その日に上げた銘柄の比率）
  D. JPXの**値幅制限**テーブル（S高までの距離／実際にS高だったか）
  E. `_earnings_events_rich2.csv` の決算発表日（「当日が決算だったか」＝急騰の原因）

■ 中心の仮説（機構が説明できるもの）
  フェードは「過熱の反動」を取る。同じ+7%でも
    ・**業種ごと上がっている** → テーマ/セクターの実需 → 翌日も買われる → 垂れない
    ・**その銘柄だけ上がっている** → 個別の思惑 → 反動が出やすい
  ＝ **銘柄の騰落 − 業種指数の騰落（超過リターン）** が効くはず。これは今まで一度も見ていない。

■ 指数コードの対応は記憶に頼らず実証で決める
  33業種の各指数について「その業種の銘柄群の等加重リターン」との相関を取り、最大のものを採用する。
  （コード表を思い出して間違えると、以降の分析が全部静かに壊れるため）

出力: _fade_pool_v4.pkl
実行: python -X utf8 _bt_fade_market.py
"""
from __future__ import annotations

import json
import pickle

import numpy as np
import pandas as pd

print("[load] 読込中...", flush=True)
P = pd.read_pickle("_fade_pool_v3.pkl")
IDX = pd.read_pickle("_indices_10y.pkl")
SR = pd.read_pickle("_short_ratio_10y.pkl")
SEC = json.load(open("sector33_map.json", encoding="utf-8"))
old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
new = pickle.load(open("jquants_cache.pkl", "rb"))

# ── 指数の日次リターン ────────────────────────────────────────────────
IDX = IDX.sort_values(["Code", "Date"])
IDX["ret"] = IDX.groupby("Code")["C"].pct_change() * 100
IR = IDX.pivot(index="Date", columns="Code", values="ret")
IC = IDX.pivot(index="Date", columns="Code", values="C")

# ── 全銘柄の日次リターン行列（市場ブレスと業種等加重リターンの土台）──────────
print("[build] 全銘柄の日次リターン行列を作成中...", flush=True)
rets = {}
for tk in set(old["all_data"]) | set(new["all_data"]):
    dfs = [d for d in (old["all_data"].get(tk), new["all_data"].get(tk)) if d is not None and len(d)]
    if not dfs:
        continue
    d = pd.concat(dfs).sort_index()
    d = d[~d.index.duplicated(keep="last")]
    c = d["Close"].astype(float)
    if len(c) < 30:
        continue
    r = c.pct_change() * 100
    r.index = r.index.strftime("%Y-%m-%d")
    rets[tk] = r
R = pd.DataFrame(rets)
print(f"[build] リターン行列 {R.shape[0]:,}日 × {R.shape[1]:,}銘柄", flush=True)

# 市場ブレス＝その日に上げた銘柄の比率（当日の引けで確定＝シグナル日に使ってよい）
breadth = (R > 0).sum(axis=1) / R.notna().sum(axis=1) * 100

# ── 指数コード ↔ 業種名 を相関で実証的に決める ───────────────────────────
print("[map] 指数コードと業種名の対応を相関で決定中...", flush=True)
sec_of = pd.Series(SEC)
eq = {}                                   # 業種ごとの等加重リターン
for nm, grp in sec_of.groupby(sec_of):
    cols = [c for c in grp.index if c in R.columns]
    if len(cols) >= 5:
        eq[nm] = R[cols].mean(axis=1)
EQ = pd.DataFrame(eq)
common = IR.index.intersection(EQ.index)
# JPX標準の33業種指数コード（0040〜0060）。貪欲な相関マッチはTOPIX(0000)を拾って壊れるので、
# 表を明示した上で「相関で妥当性を検証する」形にする。空売り比率のS33コードもこの体系。
JPX33 = {
    "水産・農林業": "0040", "鉱業": "0041", "建設業": "0042", "食料品": "0043",
    "繊維製品": "0044", "パルプ・紙": "0045", "化学": "0046", "医薬品": "0047",
    "石油･石炭製品": "0048", "ゴム製品": "0049", "ガラス･土石製品": "004A", "鉄鋼": "004B",
    "非鉄金属": "004C", "金属製品": "004D", "機械": "004E", "電気機器": "004F",
    "輸送用機器": "0050", "精密機器": "0051", "その他製品": "0052", "電気･ガス業": "0053",
    "陸運業": "0054", "海運業": "0055", "空運業": "0056", "倉庫･運輸関連業": "0057",
    "情報･通信業": "0058", "卸売業": "0059", "小売業": "005A", "銀行業": "005B",
    "証券･商品先物取引業": "005C", "保険業": "005D", "その他金融業": "005E",
    "不動産業": "005F", "サービス業": "0060",
}
pairs, bad = {}, []
for nm, cd in JPX33.items():
    if nm not in EQ.columns or cd not in IR.columns:
        bad.append((nm, cd, "データ無し")); continue
    v = IR.loc[common, cd].corr(EQ.loc[common, nm])
    if not np.isfinite(v) or v < 0.5:
        bad.append((nm, cd, f"相関{v:.2f}")); continue
    pairs[nm] = cd
print(f"[map] {len(pairs)}/33業種が相関0.5以上で検証OK")
lo = sorted(((IR.loc[common, cd].corr(EQ.loc[common, nm]), nm, cd) for nm, cd in pairs.items()))[:5]
print("    相関が低い順5件: " + " / ".join(f"{nm}({cd}) {v:.2f}" for v, nm, cd in lo))
if bad:
    print(f"    ⚠️割当できず: {bad}")

# ── 空売り比率（業種別・S33コードは指数コードと同体系）─────────────────────
SR = SR.copy()
tot = SR.SellExShortVa + SR.ShrtWithResVa + SR.ShrtNoResVa
SR["sratio"] = (SR.ShrtWithResVa + SR.ShrtNoResVa) / tot * 100
SRP = SR.pivot(index="Date", columns="S33", values="sratio")
SRZ = (SRP - SRP.rolling(60).mean()) / SRP.rolling(60).std()      # 業種内での相対的な高さ

# ── 値幅制限（JPX・普通株）─────────────────────────────────────────────
LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300),
       (2000, 400), (3000, 500), (5000, 700), (7000, 1000), (10000, 1500),
       (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000)]


def limit_width(base: float) -> float:
    for hi, w in LIM:
        if base < hi:
            return w
    return 10000.0


# ── 決算発表日 ────────────────────────────────────────────────────────
try:
    E = pd.read_csv("_earnings_events_rich2.csv", usecols=["ticker", "d0"])
    EARN = set(zip(E.ticker, E.d0))
except Exception:
    EARN = set()

# ── 特徴量を付ける ────────────────────────────────────────────────────
print("[join] 特徴量を付与中...", flush=True)
P = P.copy()
P["sec_code"] = P.sector.map(pairs)
P["s33"] = P.sector.map(JPX33)
sec_ret = np.full(len(P), np.nan)
for i, (d, cd) in enumerate(zip(P.sig, P.sec_code)):
    if isinstance(cd, str) and d in IR.index and cd in IR.columns:
        sec_ret[i] = IR.at[d, cd]
P["sec_chg"] = sec_ret                       # 当日の業種指数の騰落%
P["excess"] = P.gain - P.sec_chg             # 業種を超えた分＝個別の思惑度
P["tpx_chg"] = P.sig.map(IR["0000"]) if "0000" in IR.columns else np.nan
_t = IC["0000"] if "0000" in IC.columns else None
if _t is not None:
    tdev = (_t / _t.rolling(25).mean() - 1) * 100
    P["tpx_dev"] = P.sig.map(tdev)
P["breadth"] = P.sig.map(breadth)            # その日に上げた銘柄の比率%
P["sratio_z"] = [SRZ.at[d, c] if (isinstance(c, str) and d in SRZ.index and c in SRZ.columns)
                 else np.nan for d, c in zip(P.sig, P.s33)]
w = P.prev.map(limit_width) if "prev" in P.columns else P.px.map(limit_width)
_base = P.px / (1 + P.gain / 100)            # 前日終値＝値幅制限の基準値段
w = _base.map(limit_width)
P["limit_up"] = _base + w
P["limit_room"] = (P.limit_up - P.px) / P.px * 100     # S高まであと何%か（0=S高）
P["was_limit"] = P.limit_room <= 0.01
P["earn_day"] = [(t, d) in EARN for t, d in zip(P.ticker, P.sig)]
P.to_pickle("_fade_pool_v4.pkl")
cov = {k: f"{P[k].notna().mean()*100:.0f}%" for k in
       ("sec_chg", "tpx_chg", "breadth", "sratio_z", "limit_room")}
print(f"[save] _fade_pool_v4.pkl {len(P):,}件 / カバー率 {cov}")
print(f"  S高だった玉 {P.was_limit.mean()*100:.1f}% / 決算当日だった玉 {P.earn_day.mean()*100:.1f}%")
