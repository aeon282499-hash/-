# -*- coding: utf-8 -*-
"""_bt_earnings_ultra.py — 決算持ち越しを究極まで磨く（2026-07-29・本番無変更）。

現行: 決算発表日の大引けで買い→翌寄りで売り。RSI≤45 / 5日騰落<-3% / 代金20日中央値≥7.5億 /
      株価≤1万 → RSI昇順に8枠。翌寄りが+8%超ならPEAD延長で5営業日目の大引けまで。
      10年 +1,069万 / 年+107万 / 勝率49.1% / PF1.32 / 勝ち9-11年。

**すでに測って棄却済みの軸は繰り返さない**:
  2026-07-26 9軸 = 業種cap / 買残フィルタ / 入口ボラ正規化 / PEAD閾値ボラ正規化 /
                   PEAD日数 / 下側PEAD / 選定順序 / 枠数 / 市場決算反応ゲート
  2026-07-29    = 並び順14通り / 同業種決算モメンタム / 信用倍率 / RSI下限 / 各種絞り込み

ここで初めて測る軸:
  A. **発表時刻**（引け後 / 場中 / 寄り前）… earnings_times.json のキーは {銘柄:{日付:時刻}}。
     前回 0/2,999件しか紐付かなかったのはキー指定の誤りで、中身は使える。
  B. **入口3条件の生の閾値グリッド**（RSI上限 / 5日騰落の下限 / 流動性の床）
     ※過去に測ったのは「ボラで正規化するか」であって、閾値そのものの掃引ではない
  C. **出口タイミング**（翌寄り＝現行 / 翌引け / 3日 / 5日）
  D. **PEAD閾値と延長先の面**（+4〜+15% × r3/r5/r8/r10）
  E. **銘柄固有の決算クセ**（その銘柄の過去の決算で翌寄りがどう動いたか・前日までの情報のみ）
  F. **決算の月**（5月と8月が2本柱という経験則の検証）

採用条件: 両期間（2016-21 / 2022-26）で改善・近傍が高原・機構が説明できる。
実行: python -X utf8 _bt_earnings_ultra.py
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

YEARS = list(range(2016, 2027))
SIZE, SLOTS = 1_000_000, 8
P_RSI, P_RUN, P_TOV, P_PX = 45.0, -3.0, 7.5e8, 10_000
P_PEAD, P_PEAD_COL = 8.0, "r5"

E = pd.read_csv("_earnings_events_rich2.csv")
E["d0"] = E["d0"].astype(str)
E["mon"] = E["d0"].str[5:7].astype(int)

# ── 発表時刻を付ける（キーは {銘柄:{日付:時刻}}）──
try:
    TM = json.load(open("earnings_times.json", encoding="utf-8"))
    E["t"] = [TM.get(tk, {}).get(d) for tk, d in zip(E["ticker"], E["d0"])]
    print(f"[time] 発表時刻の紐付け: {E['t'].notna().sum():,}/{len(E):,}件", flush=True)
except Exception as e:
    E["t"] = None
    print(f"[time] 読込失敗: {e}", flush=True)


def base_mask(d, rsi=P_RSI, run=P_RUN, tov=P_TOV, px=P_PX):
    return ((d.rsi <= rsi) & (d.runup5 < run) & (d.tov20 >= tov)
            & (d.price <= px) & np.isfinite(d.gap))


def pf(x):
    l = -x[x < 0].sum()
    return x[x > 0].sum() / l if l > 0 else np.inf


def sim(d, slots=SLOTS, size=SIZE, exit_col="gap", pead=P_PEAD, pead_col=P_PEAD_COL,
        pead_span=5, order="rsi", asc=True):
    """本番と同じ枠シム。exit_col='gap'なら翌寄り決済。PEAD延長は gap>pead のとき。"""
    d = d.sort_values(["d0", order], ascending=[True, asc])
    days = sorted(d.d0.unique()); di = {x: i for i, x in enumerate(days)}
    busy, held, out = [], {}, []
    for day, g in d.groupby("d0", sort=True):
        i = di[day]
        busy = [x for x in busy if x > i]
        held = {t: x for t, x in held.items() if x > i}
        for r in g.itertuples():
            if len(busy) >= slots:
                break
            if r.ticker in held:
                continue
            sh = int(size / r.price / 100) * 100
            if sh <= 0:
                continue
            base = getattr(r, exit_col)
            span = 1 if exit_col == "gap" else int(exit_col[1:])
            if pead is not None and r.gap > pead:
                ext = getattr(r, pead_col)
                if np.isfinite(ext):
                    base, span = ext, pead_span
            if not np.isfinite(base):
                continue
            busy.append(i + span); held[r.ticker] = i + span
            out.append({"y": r.year, "pnl": base, "yen": base / 100 * sh * r.price})
    if not out:
        return None
    B = pd.DataFrame(out)
    yr = B.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    return dict(n=len(B), tot=B.yen.sum(), win=int((yr > 0).sum()), worst=yr.min(),
                pf=pf(B.pnl), wr=(B.pnl > 0).mean() * 100,
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


A = E[base_mask(E)].copy()
BASE = sim(A)
print(f"[base] 現行: {BASE['n']}件 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} "
      f"{BASE['tot']:+,.0f}円 勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f}\n")

HDR = (f"  {'設定':<30}{'件数':>6}{'勝率':>8}{'PF':>7}{'10年計':>13}{'勝ち':>6}"
       f"{'最悪年':>11}{'前半':>12}{'後半':>12}{'判定':>12}")


def row(lab, s):
    if s is None:
        print(f"  {lab:<30}  —"); return
    mk = ("両期間改善" if (s["a"] > BASE["a"] and s["b"] > BASE["b"])
          else ("片側" if s["tot"] > BASE["tot"] else ""))
    print(f"  {lab:<30}{s['n']:>6}{s['wr']:>7.1f}%{s['pf']:>7.2f}{s['tot']:>+12,.0f}円"
          f"{s['win']:>4}/11{s['worst']:>+10,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>14}")


print("=" * 124)
print("A. 発表時刻（引け後 / 場中 / 寄り前）")
print("=" * 124)
got = A[A.t.notna()].copy()
print(f"  時刻が取れた候補: {len(got):,}/{len(A):,}件")
if len(got) >= 300:
    def bucket(s):
        try:
            hh, mm = int(str(s)[:2]), int(str(s)[3:5])
        except Exception:
            return "不明"
        v = hh * 60 + mm
        if v >= 15 * 60:
            return "引け後(15:00〜)"
        if v >= 11 * 60 + 30:
            return "昼休み(11:30-15:00)"
        if v >= 9 * 60:
            return "場中(9:00-11:30)"
        return "寄り前(〜9:00)"
    got["b"] = got.t.map(bucket)
    print(f"\n  {'区分':<22}{'n':>6}{'平均gap':>10}{'勝率':>8}{'前半平均':>10}{'後半平均':>10}")
    for b, g in got.groupby("b"):
        print(f"  {b:<22}{len(g):>6}{g.gap.mean():>+9.2f}%{(g.gap>0).mean()*100:>7.1f}%"
              f"{g[g.year<=2021].gap.mean():>+9.2f}%{g[g.year>=2022].gap.mean():>+9.2f}%")
    print(f"\n  ── 枠シムで各区分だけを撃った場合 ──")
    print(HDR)
    row("現行（全部）", BASE)
    for b in sorted(got.b.unique()):
        row(f"{b}のみ", sim(got[got.b == b]))
    # 時刻の細かい刻み（引け後の中でも差があるか）
    late = got[got.b == "引け後(15:00〜)"].copy()
    if len(late) >= 500:
        late["hm"] = late.t.str[:5]
        top = late.hm.value_counts().head(6)
        print(f"\n  ── 引け後の時刻別（上位6・n≥50のみ）──")
        for hm, cnt in top.items():
            if cnt < 50:
                continue
            g = late[late.hm == hm]
            print(f"    {hm}  n={len(g):>5} 平均{g.gap.mean():>+6.2f}% 勝率{(g.gap>0).mean()*100:>5.1f}% "
                  f"前半{g[g.year<=2021].gap.mean():>+6.2f}% 後半{g[g.year>=2022].gap.mean():>+6.2f}%")
else:
    print("  n不足 → 判定不能")

print("\n" + "=" * 124)
print("B. 入口3条件の生の閾値グリッド")
print("=" * 124)
print("\n  ── RSI上限（現行45）──")
print(HDR)
for v in (35, 40, 45, 50, 55, 60):
    row(f"RSI ≤ {v}", sim(E[base_mask(E, rsi=v)]))
print("\n  ── 5日騰落の下限（現行 <-3%）──")
print(HDR)
for v in (-1, -2, -3, -5, -7, -10):
    row(f"5日騰落 < {v}%", sim(E[base_mask(E, run=v)]))
print("\n  ── 流動性の床（現行 代金20日中央値 ≥7.5億）──")
print(HDR)
for v in (1, 3, 5, 7.5, 10, 20, 50):
    row(f"代金 ≥ {v}億", sim(E[base_mask(E, tov=v * 1e8)]))
print("\n  ── 株価上限（現行 1万円）──")
print(HDR)
for v in (3000, 5000, 8000, 10000, 15000, 30000):
    row(f"株価 ≤ {v:,}円", sim(E[base_mask(E, px=v)]))

print("\n" + "=" * 124)
print("C. 出口タイミング（現行＝翌寄り）")
print("=" * 124)
print(HDR)
row("翌寄り（現行・PEAD延長あり）", BASE)
for col, lab in (("gap", "翌寄り"), ("r1", "翌引け"), ("r3", "3営業日"),
                 ("r5", "5営業日"), ("r8", "8営業日"), ("r10", "10営業日")):
    row(f"{lab}・PEAD延長なし", sim(A, exit_col=col, pead=None))

print("\n" + "=" * 124)
print("D. PEAD（翌寄りが大きく上がった玉を持ち越す）の閾値×延長先")
print("=" * 124)
print(f"  {'閾値＼延長先':<16}" + "".join(f"{c:>15}" for c in ("r3", "r5", "r8", "r10")))
for th in (4, 6, 8, 10, 12, 15, None):
    cells = ""
    for col in ("r3", "r5", "r8", "r10"):
        s = sim(A, pead=th, pead_col=col, pead_span=int(col[1:]))
        cells += f"{s['tot']:>+14,.0f}" if s else f"{'—':>15}"
    lab = f"gap > +{th}%" if th else "延長なし"
    print(f"  {lab:<16}{cells}")
print(f"\n  ※現行 = gap>+8% → r5（5営業日）: {BASE['tot']:+,.0f}円")

print("\n" + "=" * 124)
print("E. 銘柄固有の決算クセ（その銘柄の過去の決算で翌寄りがどう動いたか・前日までの情報のみ）")
print("=" * 124)
ALLE = E[np.isfinite(E.gap)][["ticker", "d0", "gap"]].sort_values(["ticker", "d0"])
hist: dict[str, list] = {}
prev_mean, prev_n = [], []
seen: dict[str, list] = {}
for r in ALLE.itertuples():
    h = seen.setdefault(r.ticker, [])
    prev_mean.append(np.mean(h) if len(h) >= 2 else np.nan)
    prev_n.append(len(h))
    h.append(r.gap)
ALLE["hist_mean"] = prev_mean
ALLE["hist_n"] = prev_n
A2 = A.merge(ALLE[["ticker", "d0", "hist_mean", "hist_n"]], on=["ticker", "d0"], how="left")
ok2 = A2[np.isfinite(A2.hist_mean) & (A2.hist_n >= 3)]
print(f"  過去3回以上の実績がある候補: {len(ok2):,}/{len(A2):,}件")
if len(ok2) >= 500:
    q = pd.qcut(ok2.hist_mean, 5, duplicates="drop")
    print(f"\n  {'過去の翌寄り平均':<26}{'n':>6}{'平均gap':>10}{'勝率':>8}{'前半':>10}{'後半':>10}")
    for iv, g in ok2.groupby(q, observed=True):
        print(f"  {str(iv):<26}{len(g):>6}{g.gap.mean():>+9.2f}%{(g.gap>0).mean()*100:>7.1f}%"
              f"{g[g.year<=2021].gap.mean():>+9.2f}%{g[g.year>=2022].gap.mean():>+9.2f}%")
    print(f"\n  ── 枠シム ──")
    print(HDR)
    row("現行（クセを見ない）", BASE)
    for qq in (0.2, 0.4, 0.6, 0.8):
        thr = ok2.hist_mean.quantile(qq)
        row(f"過去平均 > {thr:+.2f}%（上位{int((1-qq)*100)}%）", sim(A2[A2.hist_mean > thr]))
    row("クセ順に並べる（良い順）", sim(A2[A2.hist_mean.notna()], order="hist_mean", asc=False))

print("\n" + "=" * 124)
print("F. 決算の月（5月と8月が2本柱という経験則の検証）")
print("=" * 124)
print(f"  {'月':<6}{'n':>6}{'平均gap':>10}{'勝率':>8}{'枠シム10年計':>16}{'前半':>12}{'後半':>12}")
for m in range(1, 13):
    g = A[A.mon == m]
    if len(g) < 30:
        continue
    s = sim(g)
    print(f"  {m:>2}月{'':2}{len(g):>6}{g.gap.mean():>+9.2f}%{(g.gap>0).mean()*100:>7.1f}%"
          f"{s['tot']:>+15,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円")
