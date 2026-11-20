# -*- coding: utf-8 -*-
"""ichi_engine.py — 一目均衡表の分足総当たり(2026-11-20・検証のみ・発注なし)
bb_sigma/bb_engine.py の Data(データ読込・土俵・決算除外・約定/コスト・出口)をそのまま流用し、
「バンド」と「シグナル」だけを一目均衡表に差し替える。約定規約は BB と同一:
  足iの確定後にシグナル → 足i+1の始値(+不利1tick)で建て → 出口も次足始値(−不利1tick) → 往復0.10%。1銘柄1日1トレード。

一目の線(銘柄ごとに日をまたいで連続計算。前日以前の足も使う＝15分足では雲に約3.4日分の足が要る):
  転換線 = (直近T本の高値+安値)/2、基準線 = 同K本、先行A = (転換+基準)/2 をK本先へ、先行B = 同S本をK本先へ、遅行 = 終値 vs K本前の終値
  雲上限 = max(先行A,先行B)、雲下限 = min(...)。
パラメータ(grid の period 列 = 転換線の本数): 9 → (9,26,52) 標準 / 5 → (5,13,26) 半分。sigma 列は 0 固定(未使用)。

BBエンジンの部品への割り当て:
  ma = 基準線(出口「MA回帰」= 終値が基準線の反対側へ戻った次足)
  up = 雲上限 / lo = 雲下限(出口「反対バンド」= 好転シグナルなら終値が雲下限を割った次足、逆転なら雲上限を超えた次足)
  band="upper" = 好転(買いサイン)、band="lower" = 逆転(売りサイン)。side=+1/−1 との組で 順張り/逆張り の4向き。
  ※全シグナルに「好転なら終値>基準線、逆転なら終値<基準線」を課す(出口MA回帰の向きを揃えるため)。

シグナル定義(好転側。逆転は全部鏡写し):
  S1_雲抜け       終値が雲上限を下から上へ抜けた足
  S2_TK好転       転換線が基準線を下から上へ抜けた足
  S3_TK好転_雲上   S2 かつ 終値>雲上限
  S4_三役好転      (転換>基準)&(終値>雲上限)&(遅行>K本前の終値) が成立した最初の足
  S5_基準線抜け    終値が基準線を下から上へ抜けた足
  S6_雲上状態      終値>雲上限 の状態(時間帯の最初の該当足＝ほぼ固定時刻。状態フィルターとしての比較用)
使い方: python -X utf8 ichi_engine.py 15m   → grid_15m.pkl（5m も可）
"""
from __future__ import annotations
import sys, os
import numpy as np, pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bb_sigma"))
import bb_engine as B  # noqa: E402

PARAMS = {9: (9, 26, 52), 5: (5, 13, 26)}
DEFS = ("S1_雲抜け", "S2_TK好転", "S3_TK好転_雲上", "S4_三役好転", "S5_基準線抜け", "S6_雲上状態")


class IchiData(B.Data):
    def _roll_mid(self, n):
        h = pd.Series(self.h).rolling(n).max().to_numpy(); l = pd.Series(self.l).rolling(n).min().to_numpy()
        return (h + l) / 2

    def _shift(self, x, k):
        """同一銘柄内で k 本前の値(銘柄の先頭 k 本は NaN)"""
        out = np.full(self.n, np.nan); out[k:] = x[:-k]
        out[(np.arange(self.n) - self.row_sstart) < k] = np.nan
        return out

    def bb(self, p, sg=0.0):
        """BBエンジン互換: (ma, up, lo) = (基準線, 雲上限, 雲下限)。他の線は self._L に保持し signals() が使う"""
        T, K, S = PARAMS[int(p)]
        age = np.arange(self.n) - self.row_sstart
        ten = self._roll_mid(T); kij = self._roll_mid(K); sb = self._roll_mid(S)
        ten[age < T - 1] = np.nan; kij[age < K - 1] = np.nan; sb[age < S - 1] = np.nan
        sa = (ten + kij) / 2
        A = self._shift(sa, K); Bs = self._shift(sb, K)
        top = np.fmax(A, Bs); bot = np.fmin(A, Bs)
        bad = ~(np.isfinite(A) & np.isfinite(Bs) & np.isfinite(kij) & np.isfinite(ten))
        top[bad] = np.nan; bot[bad] = np.nan
        c_k = self._shift(self.c, K)
        self._L = dict(ten=ten, kij=kij, top=top, bot=bot, c_k=c_k)
        return kij.copy(), top, bot

    def _prev(self, x):
        """同一銘柄の1本前(銘柄先頭は False)"""
        out = np.r_[False, x[:-1]]
        out[np.arange(self.n) == self.row_sstart] = False
        return out

    def signals(self, up, lo, band, dfn):
        L = self._L; c = self.c; ten, kij, c_k = L["ten"], L["kij"], L["c_k"]
        s = 1 if band == "upper" else -1
        ok = np.isfinite(up) & np.isfinite(lo) & np.isfinite(c_k)
        edge = up if s == 1 else lo                     # 好転は雲上限・逆転は雲下限
        side_k = s * (c - kij) > 0                      # 終値が基準線のシグナル側
        out_cloud = s * (c - edge) > 0
        tk = s * (ten - kij) > 0
        chik = s * (c - c_k) > 0
        if dfn.startswith("S1"):
            sig = out_cloud & ~self._prev(out_cloud)
        elif dfn.startswith("S2"):
            sig = tk & ~self._prev(tk)
        elif dfn.startswith("S3"):
            sig = tk & ~self._prev(tk) & out_cloud
        elif dfn.startswith("S4"):
            three = tk & out_cloud & chik
            sig = three & ~self._prev(three)
        elif dfn.startswith("S5"):
            sig = side_k & ~self._prev(side_k)
        else:
            sig = out_cloud
        sig = sig & side_k & ok
        sig[np.arange(self.n) == self.row_sstart] = False  # 銘柄先頭足は除外
        return sig


def patch():
    """bb_engine / bb_analyze のグローバルを一目用に差し替える"""
    B.Data = IchiData
    B.PERIODS = tuple(PARAMS); B.SIGMAS = (0.0,); B.DEFS = DEFS


if __name__ == "__main__":
    patch()
    kind = sys.argv[1]
    here = os.path.dirname(os.path.abspath(__file__))
    B.run_grid(kind, os.path.join(here, f"grid_{kind}.pkl"), periods=B.PERIODS, sigmas=B.SIGMAS)
