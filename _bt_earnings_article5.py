# -*- coding: utf-8 -*-
"""_bt_earnings_article5.py — 「見極め5ヶ条」のうち未測定だった2軸（2026-08-01・本番無変更）。

本人が持ってきた記事の5ヶ条を本システムに当てると、3つは今日すでに測って全滅している:
  第1条 進捗率        → prog。素の層別は単調だが8枠シムで -347万
  第2条 利益の伸び     → op_yoy。全滅（4期連続増益が最悪で -980万）
  第3条 ガイダンス     → guid_up。素はPF1.45だがシムで -143万
  第5条 地合い        → 市場の決算反応ゲート/TOPIXゲートとも棄却済み

残る未測定は2つ:
  A. 利益の「質」= 営業利益と純利益の乖離（純利益だけ伸びている＝特別利益依存を弾く）
  B. サプライズ = 増配（通期の配当予想を引き上げてきた会社か）

【重要な前提のズレ】記事の5ヶ条はどれも「決算が **出た後** に数字を見て判断する」もの。
本システムは「決算が **出る前** の大引けで仕込んで翌朝の寄りで売る」ので、今夜出る
増配も進捗率も買う時点では存在しない。よって **過去の傾向** に翻訳して測るしかない
（＝「過去に増配を続けてきた会社か」「過去の利益の質が良い会社か」）。
look-ahead防止のため DiscDate < d0 の開示だけを使う。

実行: python -X utf8 _bt_earnings_article5.py
"""
from __future__ import annotations

import pickle
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── 実像ベースの候補（BT⇔本番のズレを全部補正した条件）────────────
R = pd.read_csv("_earnings_rsi_prod.csv")
E = pd.read_csv("_earnings_events_rich2.csv").sort_values(["ticker", "d0"])
E["vol"] = E.groupby("ticker")["gap"].transform(
    lambda s: s.abs().shift(1).expanding(min_periods=3).median())
E = E.merge(R, on=["ticker", "d0"], how="inner")

F = pd.read_pickle("_fins_history.pkl")
F["tk"] = [(str(c)[:4] if len(str(c)) == 5 and str(c).endswith("0") else str(c)) + ".T"
           for c in F.Code]
F["d"] = F.DiscDate.astype(str)
Q = F[F.DocType.astype(str).str.contains("FinancialStatements", na=False)].copy()


def num(s):
    return pd.to_numeric(s, errors="coerce")


Q["op"], Q["np_"] = num(Q.OP), num(Q.NP)
# 配当は fetch_fins_history.py の KEEP 列に入れ忘れており _fins_history.pkl に無い。
# 再取得（約40分）が要るので、無ければ増配パートを黙って飛ばす。
HAS_DIV = "FDivAnn" in Q.columns or "DivFY" in Q.columns
Q["fdiv"] = num(Q.FDivAnn) if "FDivAnn" in Q.columns else np.nan
Q["div"] = num(Q.DivFY) if "DivFY" in Q.columns else np.nan
if not HAS_DIV:
    print("[warn] 配当データが _fins_history.pkl に無い → 増配パートはスキップ")
Q["ptype"] = Q.CurPerType.astype(str)
Q = Q[Q.ptype.isin(["1Q", "2Q", "3Q", "FY"])].sort_values(["tk", "d"])

# ── 点in時間の特徴量を作る（その銘柄の «過去の» 開示だけから）──────────
HIST: dict[str, list[dict]] = {}
for tk, g in Q.groupby("tk", sort=False):
    recs, prev_same, prev_div = [], {}, None
    for r in g.itertuples():
        p = prev_same.get(r.ptype)
        op_g = np_g = np.nan
        if p is not None:
            if np.isfinite(p["op"]) and abs(p["op"]) > 0 and np.isfinite(r.op):
                op_g = (r.op - p["op"]) / abs(p["op"]) * 100
            if np.isfinite(p["np_"]) and abs(p["np_"]) > 0 and np.isfinite(r.np_):
                np_g = (r.np_ - p["np_"]) / abs(p["np_"]) * 100
        # A. 利益の質: 純利益の伸び − 営業利益の伸び。
        #    大きいほど «本業以外で嵩上げ» ＝記事の言う打ち上げ花火。
        quality = (op_g - np_g) if (np.isfinite(op_g) and np.isfinite(np_g)) else np.nan
        # B. 増配: 通期配当予想が前回開示から増えたか
        cur_div = r.fdiv if np.isfinite(r.fdiv) else r.div
        div_up = np.nan
        if prev_div is not None and np.isfinite(cur_div) and prev_div > 0:
            div_up = (cur_div - prev_div) / prev_div * 100
        rec = dict(disc=r.d, ptype=r.ptype, op=r.op, np_=r.np_,
                   op_g=op_g, np_g=np_g, quality=quality, div_up=div_up)
        recs.append(rec)
        prev_same[r.ptype] = rec
        if np.isfinite(cur_div):
            prev_div = cur_div
    HIST[tk] = recs


def feat(tk: str, d0: str) -> dict:
    rs = [r for r in HIST.get(tk, ()) if r["disc"] < d0]
    if not rs:
        return {}
    tail = rs[-4:]
    qs = [r["quality"] for r in tail if np.isfinite(r["quality"])]
    ds = [r["div_up"] for r in tail if np.isfinite(r["div_up"])]
    return dict(
        quality=float(np.median(qs)) if qs else np.nan,       # 高い＝本業で稼いでいる
        div_up=float(np.median(ds)) if ds else np.nan,        # 高い＝増配基調
        div_up_cnt=float(sum(1 for x in ds if x > 0)) if ds else np.nan,
    )


E = pd.concat([E, pd.DataFrame([feat(t, d) for t, d in zip(E.ticker, E.d0)],
                               index=E.index)], axis=1)
kn = set(zip(Q.tk, Q.d))
E["isq"] = [(t, d) in kn for t, d in zip(E.ticker, E.d0)]
print(f"[data] 候補{len(E):,}件 / 利益の質 付与{E.quality.notna().mean()*100:.0f}% "
      f"/ 増配 付与{E.div_up.notna().mean()*100:.0f}%")


def shares(p, s=1_000_000):
    return max(100, int(s / p / 100) * 100)


BASE_M = ((E.rsi_prod <= 55) & (E.runup5 < -3.0) & (E.tov20 >= 7.5e8)
          & (E.price <= 10000) & E.isq & (E.vol.isna() | (E.vol >= 2.0)))


def run(mask, tag, base=None):
    A = E[mask].sort_values(["d0", "rsi_prod"])
    cd = sorted(A.d0.unique())
    ci = {d: i for i, d in enumerate(cd)}
    busy, held, out = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = ci[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= 8:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            pnl, span = (r.r5, 5) if (r.gap > 8.0 and np.isfinite(r.r5)) else (r.gap, 1)
            busy.append(i + span)
            held[r.ticker] = i + span
            out.append(dict(y=r.year, yen=pnl / 100 * shares(r.price) * r.price, pnl=pnl))
    P = pd.DataFrame(out)
    y = P.yen
    c = y.cumsum()
    dd = float((c - c.cummax()).min())
    yr = P.groupby("y").yen.sum().reindex(range(2016, 2027), fill_value=0)
    d = f"{(y.sum()-base)/1e4:+,.0f}万" if base is not None else ""
    print(f"  {tag:<34}{len(P):>5}件{y.sum()/1e4:>+8,.0f}万 DD{dd/1e4:>+7,.0f}万 "
          f"勝率{(P.pnl>0).mean()*100:>4.1f}% 陽性{int((yr>0).sum())}/11 "
          f"前半{yr[yr.index<=2021].sum()/1e4:>+6,.0f}万 後半{yr[yr.index>=2022].sum()/1e4:>+7,.0f}万{d:>10}")
    return float(y.sum())


print("\n" + "=" * 124)
print("A. 第2条『利益の質』= 営業利益の伸び − 純利益の伸び（プラス＝本業で稼いでいる）")
print("=" * 124)
b = run(BASE_M, "現行（ゲート済み）")
for thr in (-20, -10, 0, 10):
    run(BASE_M & (E.quality.isna() | (E.quality >= thr)), f"利益の質 >= {thr:+d}pt だけ買う", b)
run(BASE_M & (E.quality.isna() | (E.quality < 0)), "逆: 特別利益依存だけ買う（対照）", b)

if HAS_DIV:
    print("\n" + "=" * 124)
    print("B. 第4条『サプライズ』= 増配基調（過去4回の配当予想の動き）")
    print("=" * 124)
    run(BASE_M, "現行（ゲート済み）")
    for thr in (0, 3):
        run(BASE_M & (E.div_up.isna() | (E.div_up > thr)), f"増配率の中央値 > {thr}% だけ買う", b)
    for c_ in (1, 2):
        run(BASE_M & (E.div_up_cnt.isna() | (E.div_up_cnt >= c_)), f"直近4回で{c_}回以上の増配", b)
    run(BASE_M & (E.div_up.isna() | (E.div_up <= 0)), "逆: 増配していない銘柄だけ（対照）", b)

else:
    print("\n[B] 第4条『増配』は配当データ未取得のためスキップ")
    print("    → fetch_fins_history.py の KEEP に配当列(FDivAnn/DivFY)を追加して再取得が必要")

print("\n" + "=" * 124)
print("C. 素の層別（選定を通す前・そもそも関係があるのか）")
print("=" * 124)
for col in (["quality"] + (["div_up"] if HAS_DIV else [])):
    d = E[BASE_M & E[col].notna()].copy()
    if len(d) < 200:
        print(f"  [{col}] n={len(d)} 不足")
        continue
    d["b"] = pd.qcut(d[col].rank(method="first"), 5, labels=False)
    print(f"\n  [{col}] n={len(d):,}")
    for bb, g in d.groupby("b"):
        neg = abs(g.gap[g.gap <= 0].sum())
        print(f"    Q{int(bb)+1} {g[col].min():>+9.1f}〜{g[col].max():>+9.1f}  n={len(g):>4}"
              f"  平均gap{g.gap.mean():>+6.2f}%  勝率{(g.gap>0).mean()*100:>4.1f}%"
              f"  PF{(g.gap[g.gap>0].sum()/neg if neg else 9):>5.2f}")
