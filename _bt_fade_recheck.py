# -*- coding: utf-8 -*-
"""_bt_fade_recheck.py — 売りフェード：過去の判断を全部やり直す＋未検証軸（2026-07-31）。

土台が変わったので過去の棄却は無効:
  ・本番の株価300円下限を撤廃（低位株が入った）
  ・BT側のETF誤判定・vr分母・株数基準を修正
  ・同点順位を ticker 昇順に固定（従来は並び順任せで10年±53.5万ブレていた）
現行 = 前日+6% × 貸借○ × 張り付き除外 × 出来高6倍未満 × 乖離80%未満 × ATR5%以上 × 乖離12%以上
     → 乖離+ATRの順位平均で上位2本 → 寄付成行で空売り → 引成買戻し / 1玉50万

採用条件: **両期間（2016-21 / 2022-26）とも改善** かつ 勝ち年を減らさない かつ 近傍が高原。
実行: python -X utf8 _bt_fade_recheck.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE = 500_000
YEARS = list(range(2016, 2027))
P = pd.read_pickle("_fade_pool_v2.pkl")
P["ym"] = P.ent.str[:7]

BASE_CFG = dict(gain=6.0, vr=6.0, devmax=80.0, atr=5.0, devmin=12.0,
                tov=3e8, sticky=5.0, n=2, sort=("dev", "atr"))


def run(gain=6.0, vr=6.0, devmax=80.0, atr=5.0, devmin=12.0, tov=3e8, sticky=5.0,
        n=2, sort=("dev", "atr"), pxmax=None, seccap=None, gu=None, earn=None,
        dow_skip=None, nk=None, cool=None, one_per_tk=False, volavg=100_000,
        stop=None, tp=None, extra=None):
    d = P
    if extra is not None:
        d = d[extra(d)]
    d = d[(d.gain >= gain) & (d.vr < vr) & (d.dev < devmax) & (d.atr >= atr)
          & (d.dev >= devmin) & (d.tov >= tov) & (d.rng > sticky) & (d.vol_avg >= volavg)]
    if pxmax is not None:
        d = d[d.px <= pxmax]
    if gu is not None:
        d = d[d.gu >= gu]
    if earn is False:
        d = d[~d.earn5]
    if dow_skip is not None:
        d = d[~d.dow.isin(dow_skip)]
    if nk is not None:
        d = d[d.nk_below == nk]
    d = d.copy()
    r = None
    for col in sort:
        asc = col.startswith("-")
        x = d.groupby("sig")[col.lstrip("-")].rank(ascending=asc, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / len(sort)
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    if seccap or one_per_tk:                       # 業種/銘柄の分散制限（次点繰り上げ）
        keep = []
        for _, g in d.groupby("sig", sort=False):
            used, tks, cnt = {}, set(), 0
            for row in g.itertuples():
                if cnt >= n:
                    break
                if one_per_tk and row.ticker in tks:
                    continue
                if seccap and row.sector and used.get(row.sector, 0) >= seccap:
                    continue
                if row.sector:
                    used[row.sector] = used.get(row.sector, 0) + 1
                tks.add(row.ticker); cnt += 1
                keep.append(row.Index)
        d = d.loc[keep]
    else:
        d["rk"] = d.groupby("sig").cumcount() + 1
        d = d[d.rk <= n]
    d = d.copy()
    if cool:                                        # 同一銘柄のクールダウン（cool営業日あける）
        last: dict[str, str] = {}
        keep = []
        for row in d.sort_values("sig").itertuples():
            prev = last.get(row.ticker)
            if prev is None or (pd.Timestamp(row.sig) - pd.Timestamp(prev)).days > cool:
                keep.append(row.Index); last[row.ticker] = row.sig
        d = d.loc[keep]
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    if stop is not None or tp is not None:          # 日中OCO（保守＝両方触ったらSTOP勝ち）
        o, h, l, c = d.o1.values, d.h1.values, d.l1.values, d.c1.values
        pnl = (o - c) / o * 100
        hs = h >= o * (1 + stop / 100) if stop is not None else np.zeros(len(d), bool)
        ht = l <= o * (1 - tp / 100) if tp is not None else np.zeros(len(d), bool)
        pnl = np.where(ht & ~hs, tp if tp is not None else 0.0, pnl)
        pnl = np.where(hs, -stop if stop is not None else 0.0, pnl)
        d["pnl"] = pnl
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def st(d):
    yr = d.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    mm = d.groupby("ym").yen.sum()
    p = d.pnl; loss = -p[p < 0].sum()
    return dict(n=len(d), days=d.sig.nunique(), wr=(p > 0).mean() * 100,
                pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d.yen.sum(), avg=d.yen.sum() / 11, win=int((yr > 0).sum()),
                worst=yr.min(), wm=mm.min(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


BASE = st(run())
print(f"[base] 現行: {BASE['n']}玉 撃つ日{BASE['days']} 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} "
      f"年{BASE['avg']:+,.0f}円 勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f} "
      f"最悪月{BASE['wm']:+,.0f}\n", flush=True)

HDR = (f"  {'設定':<26}{'玉数':>6}{'撃つ日':>7}{'勝率':>7}{'PF':>6}{'年平均':>12}{'勝ち':>6}"
       f"{'最悪月':>11}{'前半':>12}{'後半':>12}{'判定':>12}")


def row(lab, s):
    ok = s["a"] > BASE["a"] and s["b"] > BASE["b"] and s["win"] >= BASE["win"]
    mk = "★両期間改善" if ok else ("片側" if s["tot"] > BASE["tot"] else "")
    print(f"  {lab:<26}{s['n']:>6}{s['days']:>7}{s['wr']:>6.1f}%{s['pf']:>6.2f}{s['avg']:>+11,.0f}円"
          f"{s['win']:>4}/11{s['wm']:>+10,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>14}")


def sec(t):
    print("\n" + "=" * 130); print(t); print("=" * 130); print(HDR)
    row("現行", BASE)


# ═══════════ A. 過去に決めた軸の再検証 ═══════════
sec("A1. 本数（現行2本）")
for k in (1, 2, 3, 4, 5):
    row(f"上位{k}本", st(run(n=k)))

sec("A2. GO閾値（現行+6%）")
for g in (5, 5.5, 6, 7, 8, 10, 12):
    row(f"前日+{g}%以上", st(run(gain=g)))

sec("A3. 出来高比の上限（現行6倍未満）")
for v in (3, 4, 5, 6, 8, 12, 999):
    row("上限なし" if v == 999 else f"{v}倍未満", st(run(vr=v)))

sec("A4. 25MA乖離の上限（現行80%未満）")
for v in (40, 50, 60, 80, 100, 999):
    row("上限なし" if v == 999 else f"{v}%未満", st(run(devmax=v)))

sec("A5. 売買代金フロア（現行3億）")
for v in (1e8, 2e8, 3e8, 5e8, 1e9):
    row(f"{v/1e8:.0f}億以上", st(run(tov=v)))

sec("A6. 張り付き除外の閾値（現行レンジ5%超）")
for v in (0, 3, 4, 5, 6, 8):
    row("除外なし" if v == 0 else f"レンジ{v}%超", st(run(sticky=v)))

sec("A7. ATR下限の再最適化（現行5%）")
for v in (0, 3, 4, 4.5, 5, 5.5, 6, 7):
    row("下限なし" if v == 0 else f"ATR{v}%以上", st(run(atr=v)))

sec("A8. 25MA乖離下限の再最適化（現行12%）")
for v in (-999, 0, 5, 8, 10, 12, 15, 18, 20):
    row("下限なし" if v == -999 else f"乖離{v}%以上", st(run(devmin=v)))

sec("A9. 並び順（現行 乖離+ATR）")
for s_, lab in [(("dev",), "乖離だけ"), (("atr",), "ATRだけ"), (("gain",), "前日騰落だけ"),
                (("rng",), "レンジだけ"), (("dev5",), "5MA乖離だけ"),
                (("dev", "atr", "gain"), "乖離+ATR+騰落"), (("dev", "atr", "rng"), "乖離+ATR+レンジ"),
                (("dev", "atr", "dev5"), "乖離+ATR+5MA乖離"), (("dev", "atr", "pos"), "乖離+ATR+終値位置"),
                (("dev", "atr", "-tov"), "乖離+ATR+代金小さい順"), (("dev", "rng"), "乖離+レンジ"),
                (("atr", "rng"), "ATR+レンジ")]:
    row(lab, st(run(sort=s_)))

sec("A10. 曜日を落とす")
for i, nmm in enumerate(["月", "火", "水", "木", "金"]):
    row(f"{nmm}曜を撃たない", st(run(dow_skip=[i])))

sec("A11. 地合い（日経25MA）")
row("25MA以上の日だけ", st(run(nk=False)))
row("25MA以下の日だけ", st(run(nk=True)))

sec("A12. 日中OCO ─ 損切りだけ / 利確だけ")
for s_ in (3, 5, 8, 10, 12):
    row(f"損切り +{s_}%", st(run(stop=s_)))
for t_ in (2, 3, 5, 8, 10):
    row(f"利確 -{t_}%", st(run(tp=t_)))

sec("A13. 同一銘柄のクールダウン")
for c_ in (3, 5, 10, 20):
    row(f"{c_}暦日あける", st(run(cool=c_)))

sec("A14. 寄りギャップ下限（寄指に戻す＝過去に撤回した軸）")
for g in (-2, -1, 0, 0.5, 1, 2):
    row(f"GU{g:+.1f}%以上", st(run(gu=g)))
