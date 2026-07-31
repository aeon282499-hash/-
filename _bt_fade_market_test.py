# -*- coding: utf-8 -*-
"""_bt_fade_market_test.py — 未使用データを繋いだ新軸の検証（2026-07-31）。

新しく作った特徴量（_bt_fade_market.py / _fade_pool_v4.pkl）:
  excess     銘柄の騰落 − 当日の業種指数の騰落 ＝「その銘柄だけ上がったか」
  sec_chg    当日の業種指数の騰落%
  breadth    その日に上げた銘柄の比率%（市場ブレス・全5,107銘柄から計算）
  tpx_chg    TOPIXの当日騰落% / tpx_dev TOPIXの25MA乖離%
  limit_room S高まであと何%か（0=S高）/ was_limit S高だったか
  earn_day   その日が決算発表日だったか

本命の仮説: フェードは過熱の反動を取るので、
  業種ごと上がっている（テーマ/実需）→ 翌日も買われる → 垂れない
  その銘柄だけ上がっている（個別の思惑）→ 反動が出る
＝ excess が大きいほど良いはず。**フィルタとしても並び順としても試す。**

⚠️今日ここまでで約70通り振っている。名目上の「両期間改善」はもう弱い証拠なので、
  採用は 両期間改善＋高原＋機構＋**単独で年+5万以上** を全部満たすものだけ。
実行: python -X utf8 _bt_fade_market_test.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SIZE = 500_000
YEARS = list(range(2016, 2027))
D = pd.read_pickle("_fade_pool_v4.pkl")


def run(sort=("dev", "atr"), n=2, extra=None):
    d = D[(D.gain >= 7.0) & (D.vr < 6.0) & (D.atr >= 5.0) & (D.dev >= 12.0)
          & (D.tov >= 3e8) & (D.rng > 5.0) & (D.vol_avg >= 100_000)]
    if extra is not None:
        d = d[extra(d)]
    d = d.copy()
    r = None
    for col in sort:
        asc = col.startswith("-")
        x = d.groupby("sig")[col.lstrip("-")].rank(ascending=asc, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / len(sort)
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= n].copy()
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def st(d):
    yr = d.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    mm = d.groupby("ym").yen.sum()
    p = d.pnl; loss = -p[p < 0].sum()
    return dict(n=len(d), wr=(p > 0).mean() * 100, pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d.yen.sum(), avg=d.yen.sum() / 11, win=int((yr > 0).sum()), wm=mm.min(),
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


BASE = st(run())
HDR = (f"  {'設定':<28}{'玉数':>6}{'勝率':>7}{'PF':>6}{'年平均':>12}{'勝ち':>6}{'最悪月':>11}"
       f"{'前半':>12}{'後半':>12}{'判定':>13}")


def row(lab, d):
    s = st(d) if isinstance(d, pd.DataFrame) else d
    ok = s["a"] > BASE["a"] and s["b"] > BASE["b"] and s["win"] >= BASE["win"]
    mk = ("★採用候補" if ok and s["avg"] - BASE["avg"] >= 50_000
          else ("両期間◯だが小" if ok else ("片側" if s["tot"] > BASE["tot"] else "")))
    print(f"  {lab:<28}{s['n']:>6}{s['wr']:>6.1f}%{s['pf']:>6.2f}{s['avg']:>+11,.0f}円{s['win']:>4}/11"
          f"{s['wm']:>+10,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>15}")


def sec(t):
    print("\n" + "=" * 132); print(t); print("=" * 132); print(HDR); row("現行", BASE)


P0 = run()
print(f"[base] {BASE['n']}玉 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} 年{BASE['avg']:+,.0f}円 "
      f"勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f}")
print(f"  新特徴量の分布: excess 中央{P0.excess.median():.1f}% / sec_chg 中央{P0.sec_chg.median():.2f}% / "
      f"breadth 中央{P0.breadth.median():.0f}% / limit_room 中央{P0.limit_room.median():.1f}%")

sec("① 【本命】超過リターン excess = 銘柄の騰落 − 業種指数の騰落（下限）")
for v in (-999, 0, 3, 5, 7, 9, 11):
    row("下限なし" if v == -999 else f"excess {v}%以上", run(extra=lambda d, v=v: d.excess >= v))

sec("②-2 excess を並び順に混ぜる")
for s_, lab in [(("dev", "atr", "excess"), "乖離+ATR+excess"), (("excess",), "excessだけ"),
                (("dev", "excess"), "乖離+excess"), (("atr", "excess"), "ATR+excess"),
                (("dev", "atr", "-excess"), "乖離+ATR+excess小さい順")]:
    row(lab, run(sort=s_))

sec("③ 当日の業種指数の動き（業種ごと上がった日は垂れないはず）")
for lo, hi, lab in ((-99, 0, "業種がマイナスの日"), (0, 1, "業種0〜+1%"), (1, 2, "業種+1〜2%"),
                    (2, 99, "業種+2%以上"), (-99, 1, "業種+1%未満だけ撃つ"), (-99, 2, "業種+2%未満だけ撃つ")):
    row(lab, run(extra=lambda d, lo=lo, hi=hi: (d.sec_chg >= lo) & (d.sec_chg < hi)))

sec("④ 市場ブレス（その日に上げた銘柄の比率）")
for lo, hi, lab in ((0, 30, "ブレス30%未満(全面安)"), (30, 50, "ブレス30-50%"), (50, 70, "ブレス50-70%"),
                    (70, 101, "ブレス70%以上(全面高)"), (0, 70, "ブレス70%未満だけ"), (30, 101, "ブレス30%以上だけ")):
    row(lab, run(extra=lambda d, lo=lo, hi=hi: (d.breadth >= lo) & (d.breadth < hi)))

sec("⑤ TOPIX（当日騰落・25MA乖離）")
for lo, hi, lab in ((-99, -0.5, "TOPIX-0.5%未満"), (-0.5, 0.5, "TOPIX±0.5%"), (0.5, 99, "TOPIX+0.5%超")):
    row(lab, run(extra=lambda d, lo=lo, hi=hi: (d.tpx_chg >= lo) & (d.tpx_chg < hi)))
for v in (-3, 0, 3):
    row(f"TOPIX 25MA乖離{v:+}%以上", run(extra=lambda d, v=v: d.tpx_dev >= v))
    row(f"TOPIX 25MA乖離{v:+}%未満", run(extra=lambda d, v=v: d.tpx_dev < v))

sec("⑥ 値幅制限までの距離（S高に近いほど買い意欲が強い＝垂れない？）")
row("S高だった玉を外す", run(extra=lambda d: ~d.was_limit))
row("S高だった玉だけ", run(extra=lambda d: d.was_limit))
for v in (5, 10, 15, 20):
    row(f"S高まで{v}%以上余裕", run(extra=lambda d, v=v: d.limit_room >= v))
    row(f"S高まで{v}%未満", run(extra=lambda d, v=v: d.limit_room < v))

sec("⑦ 決算当日だったか（急騰の原因が決算＝本物の材料）")
row("決算当日を外す", run(extra=lambda d: ~d.earn_day))
row("決算当日だけ", run(extra=lambda d: d.earn_day))

sec("⑧ 効いたものの組み合わせ（上で★が付いたものを重ねる）")
row("excess7%以上 ＋ 決算当日を外す",
    run(extra=lambda d: (d.excess >= 7) & (~d.earn_day)))
row("excess7%以上 ＋ S高を外す",
    run(extra=lambda d: (d.excess >= 7) & (~d.was_limit)))
row("excess7%以上 ＋ ブレス70%未満",
    run(extra=lambda d: (d.excess >= 7) & (d.breadth < 70)))
