# -*- coding: utf-8 -*-
"""_bt_vcp_10y_1115.py — VCP（ボラ収縮）ブレイクのスイングを10年・1銘柄1枠で検証し直す（2026-11-15）。

経緯: kabuai/bt_vcp_recent.py（Actions・直近約2年）で「①VCP pivot 逆指値」が 20日保有/損切15% で PF2.14・
  VCP流（損切8%・20日線割れ）で PF2.43 と過去最高。ただし ①同じ銘柄の連日エントリーが重複カウント
  ②2024年は負け ③期間2年 の3つの理由で未確定。本人「かいて」→ 10年・重複なしで測り直す。

データ: jquants_cache_2016_2021.pkl + jquants_cache.pkl（_bt_shodo_slotsim.py と同じ結合・上場廃止込み）
VCP の形（設定日 t の大引けで判定。bt_vcp_recent.py と同一）
  トレンド: 終値 > 50日線 > 200日線、50日線/200日線が20日前より上、250日高値の85%以上、250日安値の130%以上
  収縮   : 押しの深さ 60〜30日前 > 30〜10日前 > 直近10日、かつ直近10日 ≤10%
  出来高 : 直近5日平均 < 50日平均×0.8 ／ 流動性: 20日平均代金 ≥1億
  pivot = 直近10日の高値。翌日 高値 ≥ pivot で約定（寄りが上なら寄り値）。寄り > pivot+3% は見送り。S高寄りは買えない。
出口: 現行=20日保有・損切15% ／ VCP流=損切5% or 8%・+10%で損切りを建値へ・終値が10日/20日線割れで翌寄り（最長120日）
重複の扱い（今回の本題）:
  [全件]   旧方式（連日エントリーを全部数える）＝ Actions 結果との突き合わせ用
  [1銘柄1枠] その銘柄を持っている間（出口ごとに決済日まで）は新しいシグナルを無視
  [枠シム]  N枠×1玉SIZE円・同じ日に複数なら代金の大きい順・空き枠が無ければ見送り（実運用の形）
比較: 🚩初動ブレイク全件・🆕新高値 に同じ出口・同じ重複ルールを当てたもの。
コスト 0.2%/往復。期間末で未決済の玉は除外。
実行: python -X utf8 _bt_vcp_10y_1115.py > _log_vcp_10y_1115.txt
"""
from __future__ import annotations

import os
import pickle
import sys

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

EVENTS = "_vcp_10y_events.pkl"
COST, TOV_MIN, CHASE, MAXH = 0.2, 1e8, 3.0, 120
SLOTS, SIZE = 5, 300_000
EXITS = (("現行 20日・損切15%", 15, None, 20), ("VCP流 損切5%・10日線", 5, 10, None), ("VCP流 損切5%・20日線", 5, 20, None),
         ("VCP流 損切8%・10日線", 8, 10, None), ("VCP流 損切8%・20日線", 8, 20, None))
_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400),
        (3000, 500), (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000),
        (30000, 5000), (50000, 7000), (70000, 10000)]


def lim_w(base: float) -> float:
    for hi, w in _LIM:
        if base < hi:
            return w
    return 10000.0


def dd_at(h: np.ndarray, l: np.ndarray, t: int, a: int, b: int) -> float:
    """[t-a, t-b] 区間（b=0 なら t まで）の押しの深さ%: 区間の最高値 → それ以降の区間内最安値。"""
    hs = h[t - a:t - b + 1]; ls = l[t - a:t - b + 1]
    k = int(np.argmax(hs))
    return (1 - ls[k:].min() / hs[k]) * 100


def exit_trade(o, h, l, c, ma, j, e, sl, hold):
    """戻り値 (損益%, 決済インデックス)。決済が期間外なら (nan, None)。"""
    n = len(c)
    stop = e * (1 - sl / 100)
    if hold:
        end = j + hold - 1
        if end > n - 1:
            return np.nan, None
    else:
        end = min(j + MAXH, n - 1)
    for k in range(j, end + 1):
        if k > j and o[k] <= stop:
            return (o[k] / e - 1) * 100, k
        if l[k] <= stop:
            return (stop / e - 1) * 100, k
        if not hold:
            if h[k] >= e * 1.10:
                stop = max(stop, e)
            if k > j and np.isfinite(ma[k]) and c[k] < ma[k]:
                return ((o[k + 1] / e - 1) * 100, k + 1) if k + 1 <= n - 1 else (np.nan, None)
    if hold:
        return (c[end] / e - 1) * 100, end
    return (np.nan, None) if end == n - 1 else ((c[end] / e - 1) * 100, end)


def build() -> pd.DataFrame:
    old = pickle.load(open("jquants_cache_2016_2021.pkl", "rb"))
    new = pickle.load(open("jquants_cache.pkl", "rb"))
    tks = set(old["all_data"]) | set(new["all_data"])
    rows = []
    for n_tk, tk in enumerate(sorted(tks)):
        dfs = [d for src in (old["all_data"], new["all_data"]) if (d := src.get(tk)) is not None and len(d)]
        if not dfs:
            continue
        df = pd.concat(dfs).sort_index()
        df = df[~df.index.duplicated(keep="last")]
        n = len(df)
        if n < 300:
            continue
        o, h, l, c, v = (df[k].astype(float).to_numpy() for k in ("Open", "High", "Low", "Close", "Volume"))
        dates = df.index
        cs = pd.Series(c)
        ma = {10: cs.rolling(10).mean().to_numpy(), 20: cs.rolling(20).mean().to_numpy(), None: None}
        ma50, ma200 = cs.rolling(50).mean().to_numpy(), cs.rolling(200).mean().to_numpy()
        hi250, lo250 = cs.rolling(250, min_periods=200).max().to_numpy(), cs.rolling(250, min_periods=200).min().to_numpy()
        tov = pd.Series(c * v).rolling(20).mean().to_numpy()
        v5, v50 = pd.Series(v).rolling(5).mean().to_numpy(), pd.Series(v).rolling(50).mean().to_numpy()
        piv = pd.Series(h).rolling(10).max().to_numpy()
        ma50_20, ma200_20 = np.r_[np.full(20, np.nan), ma50[:-20]], np.r_[np.full(20, np.nan), ma200[:-20]]
        with np.errstate(invalid="ignore"):
            pre = (c > ma50) & (ma50 > ma200) & (ma50 > ma50_20) & (ma200 > ma200_20) & (c >= hi250 * 0.85) \
                & (c >= lo250 * 1.3) & (v5 < v50 * 0.8) & (tov >= TOV_MIN) & (np.arange(n) >= 260) & (np.arange(n) + 1 < n)

        def add(kind, j, e, t, extra=None):
            for lab, sl, mk, hold in EXITS:
                r, xj = exit_trade(o, h, l, c, ma[mk], j, e, sl, hold)
                if xj is None:
                    continue
                rows.append({"kind": kind, "ex": lab, "tk": tk, "ent": dates[j], "exit": dates[xj], "y": dates[j].year,
                             "px": e, "tov": tov[t], "r": r - COST, **(extra or {})})

        # ① VCP pivot 逆指値
        for t in np.where(pre)[0]:
            d3 = dd_at(h, l, t, 9, 0)
            if d3 > 10:
                continue
            d2, d1 = dd_at(h, l, t, 29, 10), dd_at(h, l, t, 59, 30)
            if not (d1 > d2 > d3):
                continue
            j = t + 1
            p = piv[t]
            if not (h[j] >= p) or o[j] > p * (1 + CHASE / 100) or o[j] >= c[t] + lim_w(c[t]) - 1e-6:
                continue
            add("①VCP", j, max(o[j], p), t)
        # ②③ 初動ブレイク（アプリ同一判定）
        hi60 = cs.rolling(60).max().shift(1).to_numpy()
        brk = np.nan_to_num(c > hi60).astype(bool)
        fresh = np.zeros(n, bool); last = -10**9
        for i in range(n):
            if brk[i]:
                if i - last > 30:
                    fresh[i] = True
                last = i
        volx = v / (pd.Series(v).rolling(20).mean().shift(1).to_numpy() + 1e-9)
        tovp = pd.Series(c * v).rolling(20).mean().shift(1).to_numpy()
        chg = np.r_[np.nan, c[1:] / c[:-1] - 1] * 100
        with np.errstate(invalid="ignore"):
            trig = fresh & (c > o) & (volx >= 1.5) & (volx < 10) & (tovp >= TOV_MIN) & (chg < 20) \
                & ((c / hi60 - 1) * 100 < 10) & (np.arange(n) + 1 < n) & (np.arange(n) >= 1)
        for i in np.where(trig)[0]:
            j = i + 1
            e = o[j]
            if not (e > 0) or e > c[i] * (1 + CHASE / 100) or e >= c[i] + lim_w(c[i]) - 1e-6:
                continue
            new250 = bool(np.isfinite(hi250[i - 1]) and c[i] > hi250[i - 1])
            add("②初動ブレイク", j, e, i)
            if new250:
                add("③🆕新高値", j, e, i)
        if (n_tk + 1) % 500 == 0:
            print(f"  [build] {n_tk + 1}/{len(tks)} 銘柄", flush=True)
    D = pd.DataFrame(rows).sort_values(["ent", "tk"]).reset_index(drop=True)
    D.to_pickle(EVENTS)
    print(f"[build] {len(D):,}行 保存 → {EVENTS}")
    return D


def dedupe(d: pd.DataFrame) -> pd.DataFrame:
    """1銘柄1枠: 保有中（決済日まで）の同じ銘柄の新シグナルは捨てる。"""
    keep = []
    for tk, g in d.sort_values("ent").groupby("tk", sort=False):
        busy_until = None
        for idx, r in g.iterrows():
            if busy_until is not None and r.ent <= busy_until:
                continue
            keep.append(idx); busy_until = r.exit
    return d.loc[sorted(keep)]


def stat(r: pd.Series, y: pd.Series, tk: pd.Series | None = None) -> str:
    if not len(r):
        return "n=0"
    gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
    yy = r.groupby(y).mean()
    h1, h2 = r[y <= 2020].mean(), r[y >= 2021].mean()
    s = (f"n={len(r):>6,} 平均{r.mean():>+6.2f}% 勝率{(r>0).mean()*100:>4.1f}% PF{(gp/gl if gl else np.inf):>5.2f} "
         f"勝ち{r[r>0].mean() if (r>0).any() else 0:>+6.2f}/負け{r[r<=0].mean() if (r<=0).any() else 0:>+6.2f} "
         f"前半(〜20){h1:>+6.2f}/後半(21〜){h2:>+6.2f} 陽性年{int((yy>0).sum())}/{len(yy)}")
    if tk is not None:
        top = r.groupby(tk).sum().nlargest(3).index
        rr = r[~tk.isin(top)]
        g2, l2 = rr[rr > 0].sum(), -rr[rr <= 0].sum()
        s += f" 上位3銘柄除去PF{(g2/l2 if l2 else np.inf):.2f}"
    return s


def slotsim(d: pd.DataFrame) -> str:
    d = d[d.px * 100 <= SIZE].sort_values(["ent", "tov"], ascending=[True, False])
    held: list = []          # (exit, tk)
    out = []
    for ent, g in d.groupby("ent", sort=True):
        held = [x for x in held if x[0] >= ent]
        for _, r in g.iterrows():
            if len(held) >= SLOTS:
                break
            if any(t == r.tk for _, t in held):
                continue
            sh = int(SIZE / r.px / 100) * 100
            if sh <= 0:
                continue
            held.append((r.exit, r.tk))
            out.append({"y": r.y, "ym": str(r.ent)[:7], "yen": r.r / 100 * sh * r.px})
    R = pd.DataFrame(out)
    if R.empty:
        return "0件"
    yy, mo = R.groupby("y").yen.sum(), R.groupby("ym").yen.sum()
    eq = mo.cumsum(); dd = (eq - eq.cummax()).min()
    gp, gl = R.yen[R.yen > 0].sum(), -R.yen[R.yen <= 0].sum()
    return (f"n={len(R):>5} 合計{R.yen.sum():>+11,.0f}円 PF{gp/gl if gl else np.inf:>5.2f} 陽性年{int((yy>0).sum())}/{len(yy)} "
            f"最悪年{yy.min():>+10,.0f} 最大DD(月次){dd:>+10,.0f} ｜年別 " + " ".join(f"{y}:{v/1e4:+.0f}万" for y, v in yy.items()))


def main() -> None:
    D = pd.read_pickle(EVENTS) if os.path.exists(EVENTS) else build()
    print(f"[pool] {len(D):,}行 期間 {D.ent.min():%Y-%m-%d}〜{D.ent.max():%Y-%m-%d} / コスト{COST}% / 枠シム {SLOTS}枠×{SIZE//10000}万")
    for kind in ("①VCP", "②初動ブレイク", "③🆕新高値"):
        print(f"\n================ {kind} ================")
        for lab, *_ in EXITS:
            d = D[(D.kind == kind) & (D.ex == lab)]
            dd = dedupe(d)
            print(f"\n  ◆ {lab}")
            print(f"    [全件・重複あり] {stat(d.r, d.y)}")
            print(f"    [1銘柄1枠]       {stat(dd.r, dd.y, dd.tk)}")
            print(f"    [枠シム]         {slotsim(d)}")
        dd = dedupe(D[(D.kind == kind) & (D.ex == EXITS[0][0])])
        print(f"\n  年別（1銘柄1枠・現行出口）: " + " ".join(f"{y}:{m:+.2f}%(n{int(k)})" for y, (m, k) in
                                                 dd.groupby("y").r.agg(["mean", "count"]).iterrows()))
    print("\n読み方: 判断は [1銘柄1枠] と [枠シム] で。[全件] は Actions の直近2年の結果（重複あり）と比べるためだけ。"
          "\n        前半(2016〜20)と後半(2021〜)の両方でプラス・上位3銘柄を除いても PF>1 が残れば本物の候補。")


if __name__ == "__main__":
    main()
