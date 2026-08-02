# -*- coding: utf-8 -*-
"""_bt_earnings_shortpos.py — 空売り残高報告×決算持ち越し（27軸目・2026-08-03・本番無変更）。

新データ: /markets/short-sale-report（0.5%以上の大口空売り残高・報告者別）
＝2026-08-03のプラン棚卸しで発見。_BRIEF_for_fable5.mdの「潰した25軸」に入っていない新軸。

仮説: 決算前に大口空売りが積み上がった売られすぎ銘柄は、好決算で踏み上げ＝右テールが太る。
      本システムはテール依存（上位20玉が利益の84%）なので、右テールを太らせる軸は筋が良い。
検証形: ①素の層別（as-of残高合計%のバケット×gap/r5・両期間）
        ②ゲート（候補を削る系＝家訓的には死ぬはずだが対照として）
        ③サイズ傾斜（ボラ傾斜26軸目と同じ「削らず張り分け」形）＋逆傾斜対照
判定バー: 両期間改善 / 同DD換算 / 勝ち年維持 / 高原 / 上位20玉除去 / 対照劣化。

実行: python -X utf8 _bt_earnings_shortpos.py（要 _short_positions_10y.pkl）
"""
from __future__ import annotations

import pickle
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

RSI_MAX, RUNUP_MAX, TOV_MIN, PRICE_CAP = 55.0, -3.0, 7.5e8, 10_000
PEAD_THR, PEAD_DAYS, SLOTS = 8.0, 5, 8
SIZE = 1_000_000
DD_BUDGET = 2_400_000
VOL_GATE = 2.0

RAW = pd.read_csv("_earnings_events_rich2.csv")
RAW = RAW.sort_values(["ticker", "d0"])
g = RAW.groupby("ticker")["gap"]
RAW["egap_vol"] = g.transform(lambda s: s.abs().shift(1).expanding(min_periods=3).median())

E = RAW[(RAW["rsi"] <= RSI_MAX) & (RAW["runup5"] < RUNUP_MAX)
        & (RAW["tov20"] >= TOV_MIN) & (RAW["price"] <= PRICE_CAP)].copy()
E = E[E["egap_vol"].isna() | (E["egap_vol"] >= VOL_GATE)].copy()

# ── as-of の空売り残高合計%（報告者別の最新報告を合算・0.5%未満報告=そのファンドは0扱い）──
SP: dict = pickle.load(open("_short_positions_10y.pkl", "rb"))
short_so = np.full(len(E), 0.0)
n_rep = np.zeros(len(E), dtype=int)
E = E.sort_values(["ticker", "d0"]).reset_index(drop=True)
for tk, idxs in E.groupby("ticker").groups.items():
    rep = SP.get(str(tk).split(".")[0][:4])
    if rep is None or rep.empty:
        continue
    rep = rep.sort_values("DiscDate")
    rd = rep["DiscDate"].to_numpy()
    rn = rep["SSName"].to_numpy()
    rv = rep["ShrtPosToSO"].to_numpy(dtype=float)
    j = 0
    cur: dict = {}
    for i in idxs:
        d0 = E.at[i, "d0"]
        while j < len(rd) and rd[j] <= d0:
            cur[rn[j]] = rv[j] if np.isfinite(rv[j]) else 0.0
            j += 1
        tot = sum(v for v in cur.values() if v >= 0.005)
        short_so[i] = tot * 100
        n_rep[i] = sum(1 for v in cur.values() if v >= 0.005)
E["short_so"] = short_so
E["n_rep"] = n_rep
E = E.sort_values(["d0", "rsi"]).reset_index(drop=True)
print(f"[候補] {len(E):,}件 / 空売り残高あり {(E.short_so>0).mean()*100:.1f}% / "
      f"残高%分布(>0のみ) {E.loc[E.short_so>0,'short_so'].describe()[['25%','50%','75%','max']].round(2).to_dict()}")

# ── ① 素の層別（両期間）─────────────────────────────────────────────
BINS = [(-0.01, 0.0001, "残高なし"), (0.0001, 1.0, "0〜1%"), (1.0, 2.0, "1〜2%"),
        (2.0, 3.0, "2〜3%"), (3.0, 99.0, "3%超")]
print("\n① 素の層別（候補レベル・gap＝翌寄り%）")
print(f"  {'帯':<10}{'期間':<10}{'n':>6}{'平均gap':>9}{'勝率':>7}{'PF':>6}{'p5':>8}{'p95':>8}{'PEAD率':>8}{'平均r5':>8}")
for lo, hi, lab in BINS:
    for per, m_per in (("2016-21", E.year <= 2021), ("2022-26", E.year >= 2022)):
        m = (E.short_so > lo) & (E.short_so <= hi) & m_per & np.isfinite(E.gap)
        if m.sum() < 5:
            continue
        gg = E.loc[m, "gap"]
        gp = gg[gg > 0].sum(); gl = -gg[gg <= 0].sum()
        pead = (gg > PEAD_THR).mean() * 100
        r5m = E.loc[m & np.isfinite(E.r5), "r5"].mean()
        print(f"  {lab:<10}{per:<10}{m.sum():>6}{gg.mean():>+9.3f}{(gg>0).mean()*100:>6.1f}%"
              f"{gp/gl if gl else float('inf'):>6.2f}{gg.quantile(0.05):>+8.2f}{gg.quantile(0.95):>+8.2f}"
              f"{pead:>7.1f}%{r5m:>+8.2f}")

# ── シムエンジン（_bt_earnings_vol_sizing.py と同一・weight/フィルタ対応）──
def sim_w(weight_fn, mask=None):
    EE = E if mask is None else E[mask]
    days = sorted(EE["d0"].unique())
    di = {d: i for i, d in enumerate(days)}
    busy, held, out = [], {}, []
    for d, gg in EE.groupby("d0", sort=True):
        i = di[d]
        busy = [(x, w) for x, w in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in gg.itertuples():
            if len(busy) >= SLOTS:
                break
            if not np.isfinite(r.gap) or r.ticker in held:
                continue
            w = weight_fn(r)
            if r.gap > PEAD_THR and np.isfinite(r.r5):
                pnl, span = r.r5, PEAD_DAYS
            else:
                pnl, span = r.gap, 1
            busy.append((i + span, w))
            held[r.ticker] = i + span
            out.append(dict(year=r.year, pnl=pnl, w=w))
    return pd.DataFrame(out)


def score(P):
    yen = P["pnl"] * SIZE * P["w"] / 100
    cum = yen.cumsum()
    dd = float((cum - cum.cummax()).min())
    yr = yen.groupby(P["year"]).sum().reindex(range(2016, 2027), fill_value=0)
    top20 = yen.nlargest(20).sum()
    return dict(n=len(P), avgw=P.w.mean(), tot=float(yen.sum()), dd=dd,
                norm=float(yen.sum()) * DD_BUDGET / abs(dd),
                win=int((yr > 0).sum()),
                e1=float(yr[yr.index <= 2021].sum()), e2=float(yr[yr.index >= 2022].sum()),
                extop20=float(yen.sum() - top20))


def show(tag, r):
    print(f"{tag:<34}{r['n']:>6}{r['avgw']:>6.2f}{r['tot']/1e4:>8,.0f}万{r['dd']/1e4:>7,.0f}万"
          f"{r['norm']/1e4:>8,.0f}万{r['win']:>4}/11{r['e1']/1e4:>7,.0f}万"
          f"{r['e2']/1e4:>7,.0f}万{r['extop20']/1e4:>8,.0f}万")


HDR = (f"{'設定':<34}{'件数':>6}{'平均w':>6}{'10年計':>9}{'最大DD':>8}"
       f"{'同DD換算':>9}{'勝年':>5}{'前半':>8}{'後半':>8}{'上20除去':>9}")

print("\n② ゲート（候補を削る系＝家訓的には死ぬはず・対照確認）")
print(HDR)
show("現行（削らない）", score(sim_w(lambda r: 1.0)))
show("残高>0のみ買う", score(sim_w(lambda r: 1.0, mask=E.short_so > 0)))
show("残高≥1%のみ買う", score(sim_w(lambda r: 1.0, mask=E.short_so >= 1.0)))
show("残高3%超を除外", score(sim_w(lambda r: 1.0, mask=E.short_so <= 3.0)))

print("\n③ サイズ傾斜（削らず張り分け・ボラ傾斜26軸目と同形）")
print(HDR)
show("現行=フラット1.0", score(sim_w(lambda r: 1.0)))
show("残高>0→1.25", score(sim_w(lambda r: 1.25 if r.short_so > 0 else 1.0)))
show("残高>0→1.5", score(sim_w(lambda r: 1.5 if r.short_so > 0 else 1.0)))
show("段階 0=1.0/〜2%=1.25/2%+=1.5", score(sim_w(
    lambda r: 1.0 if r.short_so <= 0 else (1.25 if r.short_so < 2 else 1.5))))
show("連続 1+so/4 [1.0-2.0]", score(sim_w(
    lambda r: float(np.clip(1 + r.short_so / 4, 1.0, 2.0)))))
show("【対照】残高>0→0.75", score(sim_w(lambda r: 0.75 if r.short_so > 0 else 1.0)))
show("【対照】全部1.25", score(sim_w(lambda r: 1.25)))
