# -*- coding: utf-8 -*-
"""_bt_earnings_fundamentals.py — 決算持ち越しにファンダメンタル軸は効くか（2026-07-31・本番無変更）。

本人発案「決算てファンダメンタルやん。直近業績がうなぎのぼりとか」。
これまで測った軸（業種cap/買残/ボラ正規化/PEAD/選定順序/枠数/市場ゲート/発表時刻/
閾値グリッド/出口/銘柄固有クセ/月）は**全部が価格から作った特徴量**で、
決算書の中身は一度も使っていない。ここで初めて別の情報源を入れる。

筋: 現行の入口は「売られすぎ」だけ（RSI≤55×直前5日ランアップ<-3%）。これだと
    「売られすぎだが会社は好調＝反発の芽」と「売られすぎで業績も悪化＝万年割安の罠」が
    混ざっている。決算書で後者を分けられるなら、絞っても価値が残るかもしれない。

look-ahead防止（最重要）: 各イベント d0 に対し **DiscDate < d0 の開示だけ**を使う。
その晩に出る決算そのものは当然使わない。

採用条件（記憶のバー）: 両期間(2016-21/2022-26)で改善・面が高原・枠を振って符号が安定。
なお決算はテール依存（上位20玉で利益の84%）＝候補を削る系は基本マイナスに出る、
という既知の性質があるので、絞り込みで勝てなくても想定内。

実行: python -X utf8 _bt_earnings_fundamentals.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PEAD_THR, PEAD_DAYS, SLOTS = 8.0, 5, 8
RSI_MAX, RUNUP_MAX, TOV_MIN, PRICE_CAP = 55.0, -3.0, 7.5e8, 10_000
SIZE = 1_000_000
ERAS = [("2016-21", 2016, 2021), ("2022-26", 2022, 2026)]

# ── 1. 決算書の履歴を読み、点in時間の業績特徴量を作る ─────────────
F = pd.read_pickle("_fins_history.pkl")


def to_ticker(code) -> str:
    c = str(code).strip()
    if len(c) == 5 and c.endswith("0"):
        c = c[:4]
    return f"{c}.T"


def num(s):
    return pd.to_numeric(s, errors="coerce")


F = F.assign(
    ticker=[to_ticker(c) for c in F["Code"]],
    disc=F["DiscDate"].astype(str),
    sales=num(F["Sales"]), op=num(F["OP"]), np_=num(F["NP"]),
    fop=num(F["FOP"]), fsales=num(F["FSales"]),
    ptype=F["CurPerType"].astype(str),
)
# 本決算/四半期の実績が入っている行だけ（予想のみの修正開示などは実績比較に使えない）
F = F[F["ptype"].isin(["1Q", "2Q", "3Q", "FY"])].sort_values(["ticker", "disc"])
print(f"[fins] {len(F):,}行 / {F.ticker.nunique():,}銘柄 / {F.disc.min()}〜{F.disc.max()}")

PROG = {"1Q": 0.25, "2Q": 0.50, "3Q": 0.75, "FY": 1.00}


def build_history() -> dict[str, list[dict]]:
    """銘柄 → 開示日昇順のレコード列。YoYは同じ四半期種別の1年前と比べる。"""
    hist: dict[str, list[dict]] = {}
    for tk, g in F.groupby("ticker", sort=False):
        recs = []
        prev_same: dict[str, dict] = {}       # ptype -> 直近の同種別レコード（＝前年同期）
        prev_fop = None
        for r in g.itertuples():
            p = prev_same.get(r.ptype)
            op_yoy = sales_yoy = np.nan
            if p is not None:
                if np.isfinite(p["op"]) and abs(p["op"]) > 0 and np.isfinite(r.op):
                    op_yoy = (r.op - p["op"]) / abs(p["op"]) * 100
                if np.isfinite(p["sales"]) and p["sales"] > 0 and np.isfinite(r.sales):
                    sales_yoy = (r.sales - p["sales"]) / p["sales"] * 100
            # 会社通期予想に対する進捗（理論進捗で割る＝1超なら上振れペース）
            prog = np.nan
            if np.isfinite(r.op) and np.isfinite(r.fop) and r.fop > 0:
                prog = (r.op / r.fop) / PROG.get(r.ptype, 1.0)
            guid_up = np.nan
            if prev_fop is not None and np.isfinite(r.fop) and np.isfinite(prev_fop) and prev_fop != 0:
                guid_up = (r.fop - prev_fop) / abs(prev_fop) * 100
            rec = dict(disc=r.disc, ptype=r.ptype, op=r.op, sales=r.sales,
                       op_yoy=op_yoy, sales_yoy=sales_yoy, prog=prog, guid_up=guid_up)
            recs.append(rec)
            prev_same[r.ptype] = rec
            if np.isfinite(r.fop):
                prev_fop = r.fop
        hist[tk] = recs
    return hist


HIST = build_history()


def features_at(tk: str, d0: str) -> dict:
    """d0 より前に開示済みのものだけで作る（その晩の決算は使わない）。"""
    recs = [r for r in HIST.get(tk, ()) if r["disc"] < d0]
    if not recs:
        return {}
    last = recs[-1]
    tail = recs[-4:]
    ups = [r["op_yoy"] for r in tail if np.isfinite(r["op_yoy"])]
    # 「うなぎのぼり」= 直近4回のうち増益だった回数 / 直近4回の増益率の中央値
    return dict(
        op_yoy=last["op_yoy"], sales_yoy=last["sales_yoy"],
        prog=last["prog"], guid_up=last["guid_up"],
        up_cnt=float(sum(1 for x in ups if x > 0)) if ups else np.nan,
        op_yoy_med=float(np.median(ups)) if ups else np.nan,
        stale=(pd.Timestamp(d0) - pd.Timestamp(last["disc"])).days,
    )


# ── 2. イベント表に結合 ────────────────────────────────────
E = pd.read_csv("_earnings_events_rich2.csv")
base = ((E["rsi"] <= RSI_MAX) & (E["runup5"] < RUNUP_MAX)
        & (E["tov20"] >= TOV_MIN) & (E["price"] <= PRICE_CAP))
E = E[base].copy()
# 決算書が無い期間まで評価すると「欠測はフェイルオープン」で現行と同じ結果になり、
# 差が薄まって判断を誤る。決算書が届いている範囲だけで比べる。
FINS_END = str(F["disc"].max())
n_all = len(E)
E = E[E["d0"] <= FINS_END].copy()
print(f"[範囲] 決算書は {FINS_END} まで → 候補 {n_all:,}件のうち {len(E):,}件で評価")
feat = pd.DataFrame([features_at(t, d) for t, d in zip(E["ticker"], E["d0"])], index=E.index)
E = pd.concat([E, feat], axis=1)
cov = E["op_yoy"].notna().mean() * 100
print(f"[結合] 本番条件の候補 {len(E):,}件 / 業績YoYが付いたもの {cov:.1f}%"
      f" / 直近開示からの経過日数 中央値{E['stale'].median():.0f}日")
by_era = E.groupby(E["year"] <= 2021)["op_yoy"].apply(lambda s: s.notna().mean() * 100)
print(f"  カバー率 2022-26={by_era.get(False, float('nan')):.0f}% / 2016-21={by_era.get(True, float('nan')):.0f}%")


# ── 3. まず層別（選定を通す前の素の関係）─────────────────────
def layer(col: str, q: int = 5):
    d = E[E[col].notna()].copy()
    if len(d) < 200:
        print(f"\n[{col}] サンプル不足 {len(d)}件")
        return
    d["bin"] = pd.qcut(d[col], q, labels=False, duplicates="drop")
    print(f"\n[{col}] 五分位別の「翌寄りギャップ」素の平均（選定前・n={len(d):,}）")
    print(f"  {'帯':<6}{'範囲':>22}{'件数':>8}{'平均gap':>10}{'勝率':>8}{'PF':>7}"
          f"{'前半':>10}{'後半':>10}")
    for b, g in d.groupby("bin"):
        pos = g.gap[g.gap > 0].sum()
        neg = abs(g.gap[g.gap <= 0].sum())
        e1 = g[g.year <= 2021]["gap"].mean()
        e2 = g[g.year >= 2022]["gap"].mean()
        rng = f"{g[col].min():+.0f}〜{g[col].max():+.0f}"
        print(f"  Q{int(b)+1:<5}{rng:>22}{len(g):>8,}{g.gap.mean():>+9.2f}%"
              f"{(g.gap > 0).mean()*100:>7.1f}%{(pos/neg if neg else np.inf):>7.2f}"
              f"{e1:>+9.2f}%{e2:>+9.2f}%")


for c in ("op_yoy", "op_yoy_med", "up_cnt", "sales_yoy", "prog", "guid_up"):
    layer(c)


# ── 4. 選定シム（本番同一・8枠・PEAD延長込み）───────────────────
def sim(A, slots=SLOTS):
    days = sorted(A["d0"].unique())
    di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, g in A.groupby("d0", sort=True):
        i = di[d]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            if r.gap > PEAD_THR and np.isfinite(r.r5):
                pnl, span = r.r5, PEAD_DAYS
            else:
                pnl, span = r.gap, 1
            busy.append(i + span)
            held[r.ticker] = i + span
            out.append(dict(year=r.year, pnl=pnl))
    return pd.DataFrame(out)


def score(P):
    if P is None or not len(P):
        return None
    yen = P["pnl"] * SIZE / 100
    cum = yen.cumsum()
    o = dict(n=len(P), tot=yen.sum(), dd=float((cum - cum.cummax()).min()),
             win=int(sum(1 for y in range(2016, 2027)
                         if (P[P.year == y]["pnl"]).sum() > 0)))
    for lab, y0, y1 in ERAS:
        o[lab] = float((P[(P.year >= y0) & (P.year <= y1)]["pnl"] * SIZE / 100).sum())
    return o


def line(tag, r, b=None):
    if r is None:
        print(f"  {tag:<30}  —")
        return
    d = f"{(r['tot']-b['tot'])/1e4:+,.0f}万" if b else ""
    print(f"  {tag:<30}{r['n']:>7}{r['tot']/1e4:>11,.0f}万{r['dd']/1e4:>11,.0f}万"
          f"{r['win']:>6}/11{r['2016-21']/1e4:>11,.0f}万{r['2022-26']/1e4:>11,.0f}万{d:>12}")


ORD = ["d0", "rsi"]
BASE = score(sim(E.sort_values(ORD)))
print("\n" + "=" * 116)
print("選定シム（本番同一: RSI昇順8枠・PEAD延長込み・1玉100万）")
print("=" * 116)
print(f"  {'設定':<30}{'件数':>7}{'10年計':>13}{'最大DD':>13}{'勝ち年':>9}"
      f"{'前半16-21':>13}{'後半22-26':>13}{'現行差':>12}")
line("現行（ファンダ不使用）", BASE)

print("\n-- ① 業績で足切り（欠測は落とさない＝フェイルオープン）")
for col, thr, tag in (("op_yoy", 0, "直近が増益"), ("op_yoy", 10, "直近が増益+10%超"),
                      ("op_yoy_med", 0, "直近4回の中央値が増益"),
                      ("up_cnt", 3, "直近4回中3回以上増益"),
                      ("up_cnt", 4, "直近4回すべて増益＝うなぎのぼり"),
                      ("sales_yoy", 0, "増収"), ("prog", 1.0, "通期進捗が上振れペース"),
                      ("guid_up", 0, "前回開示で会社予想を上方修正")):
    m = E[col].isna() | (E[col] >= thr)
    line(tag, score(sim(E[m].sort_values(ORD))), BASE)

print("\n-- ② 逆側（業績が悪い方を狙う＝逆張りの純化）")
for col, thr, tag in (("op_yoy", 0, "直近が減益のものだけ"),
                      ("up_cnt", 1, "直近4回で増益1回以下")):
    m = E[col].isna() | (E[col] <= thr)
    line(tag, score(sim(E[m].sort_values(ORD))), BASE)

print("\n-- ③ 並び順に使う（枠は削らない＝テール依存と衝突しない）")
for cols, asc, tag in ((["d0", "op_yoy"], [True, False], "業績YoY降順で埋める"),
                       (["d0", "up_cnt", "rsi"], [True, False, True], "増益回数→RSI昇順"),
                       (["d0", "prog"], [True, False], "通期進捗の良い順")):
    line(tag, score(sim(E.sort_values(cols, ascending=asc))), BASE)

print("\n判定: 両期間で改善しないもの・枠で符号が反転するものは採らない。")
