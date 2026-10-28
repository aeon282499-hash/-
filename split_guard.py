# -*- coding: utf-8 -*-
"""split_guard.py — 保有中の株式分割・併合で建値と値段の基準がずれる事故の防止（2026-10-28）

事故: 日本ハム(2282) 9/25建て・9/29に1→3分割。J-Quants の調整済み日足(AdjO/AdjH/AdjL/AdjC)は
過去分も÷3される一方、台帳の entry_open は約定時の生の値段(6022円)のまま。翌日の台帳更新で
9/25の安値(調整後2004円)が損切り水準5841円を割ったと誤判定し「9/25 STOP」と記帳した
（実際の9/25安値は6012円・本当の損切りは9/29）。

対処:
  ・日足がある所(台帳更新・14:55判定): 調整済み日足のエントリー日寄り値 ÷ 台帳の建値 = 分割係数。
    ±15%を超えてずれ、かつ分割/併合の比率(2・3・5倍など±3%)に近いときだけ、建値に係数を掛けて今の値段の基準にそろえて判定する（損益率は不変）。
  ・日足がまだ調整されていない分割当日(14:55のザラ場値・引け後の当日足): 建値から±40%超の値段は
    分割/併合の疑いとして約定・処分の判定に使わず「口座で要確認」に回す。
"""
from __future__ import annotations

SPLIT_TOL = 0.15        # 寄り値と建値のずれがこれを超えたら分割/併合とみなす（寄指は前日終値+1%以内なので通常は数%）
SPLIT_RATIOS = (1.5, 2, 2.5, 3, 4, 5, 10, 20, 50, 100)   # 1:1.5〜1:100 分割・併合はその逆数
RATIO_TOL = 0.03
SUSPECT_MOVE = 0.40     # 建値から±40%超（3日保有・損切り-3%/利確+5%の玉ではあり得ない）＝分割当日の疑い


def split_factor(df, entry_date_str: str, entry_open) -> float:
    """調整済み日足のエントリー日寄り値 ÷ 台帳の建値。分割/併合が無ければ 1.0。"""
    try:
        eo = float(entry_open or 0)
        if df is None or df.empty or eo <= 0:
            return 1.0
        rows = df[df.index.strftime("%Y-%m-%d") == entry_date_str]
        if rows.empty:
            return 1.0
        o = float(rows["Open"].iloc[0])
        if not (o > 0):
            return 1.0
        f = o / eo
        if abs(f - 1.0) <= SPLIT_TOL:
            return 1.0
        r = max(f, 1.0 / f)                      # 分割/併合の比率に近いときだけ採用（データ不整合で建値を動かさない）
        return f if any(abs(r / k - 1.0) < RATIO_TOL for k in SPLIT_RATIOS) else 1.0
    except Exception:
        return 1.0


def adjusted_entry(df, entry_date_str: str, entry_open):
    """今の(調整済み)値段の基準にそろえた建値。entry_open が無ければそのまま返す。"""
    if not entry_open:
        return entry_open
    return float(entry_open) * split_factor(df, entry_date_str, entry_open)


def suspect_split(entry, *prices) -> bool:
    """建値から±40%超の値段が混じる＝分割/併合の当日で基準がずれている疑い。"""
    try:
        e = float(entry or 0)
        if e <= 0:
            return False
        return any(p is not None and p == p and p > 0 and abs(float(p) / e - 1.0) > SUSPECT_MOVE for p in prices)
    except Exception:
        return False
