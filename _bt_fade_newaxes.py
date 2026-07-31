# -*- coding: utf-8 -*-
"""_bt_fade_newaxes.py — 売りフェード：未検証の軸＋再検証で残った候補の組み合わせ（2026-07-31）。

_bt_fade_recheck.py の結果、**両期間改善**で残ったのは4つ:
  ①GO閾値 +6%→+7%（年+66.6→+68.2万・PF1.43→1.52）
  ②出来高比の上限 6倍→12倍（年+69.2万）※現行の6倍は旧土台で決めた値
  ③25MA乖離の上限 80%→撤廃（年+69.1万・最悪月-35.2→-19.9万）
  ④本数 2→4/5本（年+70.9/71.5万・ただし日中資金が100万→200/250万・最悪月-47.0万）
ここでは (a)未検証の軸を潰し (b)残った候補の組み合わせと高原を見る。

⚠️多重比較の罠: 軸を振れば必ず「現行超え」は出る。採用は
  両期間改善 かつ 勝ち年維持 かつ **近傍が高原** かつ **機構が説明できる** の全部を満たすものだけ。
実行: python -X utf8 _bt_fade_newaxes.py
"""
from __future__ import annotations

import itertools

import numpy as np
import pandas as pd

exec(open("_bt_fade_recheck.py", encoding="utf-8").read().split("BASE = st(run())")[0])

BASE = st(run())
HDR = (f"  {'設定':<30}{'玉数':>6}{'撃つ日':>7}{'勝率':>7}{'PF':>6}{'年平均':>12}{'勝ち':>6}"
       f"{'最悪月':>11}{'前半':>12}{'後半':>12}{'判定':>12}")


def row(lab, s, base=None):
    b = base or BASE
    ok = s["a"] > b["a"] and s["b"] > b["b"] and s["win"] >= b["win"]
    mk = "★両期間改善" if ok else ("片側" if s["tot"] > b["tot"] else "")
    print(f"  {lab:<30}{s['n']:>6}{s['days']:>7}{s['wr']:>6.1f}%{s['pf']:>6.2f}{s['avg']:>+11,.0f}円"
          f"{s['win']:>4}/11{s['wm']:>+10,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>14}")


def sec(t):
    print("\n" + "=" * 134); print(t); print("=" * 134); print(HDR); row("現行", BASE)


# ═══════════ B. 未検証の軸 ═══════════
sec("B1. 業種分散キャップ（同一業種を何枠まで許すか）※スイングでは効いた機構")
for cp in (1, 2):
    row(f"同一業種{cp}枠まで", st(run(seccap=cp)))
row("同一銘柄は1日1枠まで", st(run(one_per_tk=True)))

sec("B2. 決算跨ぎ（直近5営業日に決算があった玉を外す）")
row("決算5日内を撃たない", st(run(earn=False)))
row("決算5日内だけ撃つ", st(run(extra=lambda d: d.earn5)))

sec("B3. 値がさ側を切る（株価上限・現行は5,000円＝予算制約）")
for v in (500, 1000, 2000, 3000, 4000):
    row(f"{v:,}円以下だけ", st(run(pxmax=v)))

sec("B4. 20日平均出来高の下限（現行10万株）")
for v in (0, 100_000, 300_000, 500_000, 1_000_000):
    row("下限なし" if v == 0 else f"{v//10000}万株以上", st(run(volavg=v)))

sec("B5. 前日の終値位置（高値引けほど大）")
for v in (0, 50, 70, 85, 95):
    row("下限なし" if v == 0 else f"終値位置{v}%以上", st(run(extra=lambda d, v=v: d.pos >= v)))

sec("B6. 前日の実体比（陽線の強さ）")
for v in (-100, 0, 50, 75, 90):
    row("下限なし" if v == -100 else f"実体{v}%以上", st(run(extra=lambda d, v=v: d.body >= v)))

sec("B7. 直近5日の陽線数（連騰）")
for v in (0, 3, 4, 5):
    row("下限なし" if v == 0 else f"5日中{v}日以上陽線", st(run(extra=lambda d, v=v: d.up_days >= v)))

sec("B8. 5MA乖離の下限（短期の伸びきり）")
for v in (-999, 0, 5, 10, 15):
    row("下限なし" if v == -999 else f"5MA乖離{v}%以上", st(run(extra=lambda d, v=v: d.dev5 >= v)))

sec("B9. 2日騰落（前々日からの積み上げ）")
for v in (-999, 5, 10, 15, 20):
    row("下限なし" if v == -999 else f"2日で{v}%以上", st(run(extra=lambda d, v=v: d.gain2 >= v)))

# ═══════════ C. 再検証で残った候補の組み合わせ ═══════════
CAND = {"GO+7%": dict(gain=7.0), "出来高12倍": dict(vr=12.0), "乖離上限なし": dict(devmax=1e9)}
sec("C1. 残った3候補を単独→2つ→3つ（本数は2本のまま）")
for k, v in CAND.items():
    row(k, st(run(**v)))
for a, b in itertools.combinations(CAND, 2):
    row(f"{a} + {b}", st(run(**{**CAND[a], **CAND[b]})))
row("3つ全部", st(run(**{k: v for c in CAND.values() for k, v in c.items()})))

ALL3 = {k: v for c in CAND.values() for k, v in c.items()}
B3S = st(run(**ALL3))
print(f"\n  ※以降は「3つ全部」(年{B3S['avg']:+,.0f}円)を新しい基準として比較する")

sec("C2. 3つ全部 × 本数（資金が増える点に注意：1本=50万・2本=100万…）")
for k in (1, 2, 3, 4, 5):
    row(f"3つ全部 × 上位{k}本", st(run(n=k, **ALL3)), B3S)

sec("C3. 3つ全部 の近傍が高原か（GO閾値を振る）")
for g in (6, 6.5, 7, 7.5, 8, 9):
    row(f"GO+{g}% (他2つON)", st(run(gain=g, vr=12.0, devmax=1e9)), B3S)

sec("C4. 3つ全部 の近傍が高原か（出来高比の上限を振る）")
for v in (6, 8, 10, 12, 20, 1e9):
    row(("上限なし" if v > 100 else f"{v:.0f}倍未満") + " (他2つON)",
        st(run(gain=7.0, vr=v, devmax=1e9)), B3S)

sec("C5. 3つ全部 × ATR/乖離の下限を振り直す")
for v in (4, 4.5, 5, 5.5, 6):
    row(f"ATR{v}%以上 (3つON)", st(run(atr=v, **ALL3)), B3S)
for v in (8, 10, 12, 15, 18):
    row(f"乖離{v}%以上 (3つON)", st(run(devmin=v, **ALL3)), B3S)

sec("C6. 3つ全部 × 未検証軸で効いたもの（あれば）")
row("3つ全部 ＋ 同一業種1枠", st(run(seccap=1, **ALL3)), B3S)
row("3つ全部 ＋ 決算5日内を外す", st(run(earn=False, **ALL3)), B3S)
row("3つ全部 ＋ レンジ6%超", st(run(sticky=6.0, **ALL3)), B3S)
