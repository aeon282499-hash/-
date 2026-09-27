# -*- coding: utf-8 -*-
"""wariyasu_signal.py — 割安B（AI惣菜型の割安スイング）の毎日の選定・紙の帳簿・通知（夕方ジョブ）2026-09-27 新設

本人（2026-09-27）: 割安Bを通知で受け取り、発注は本人が手で行う。帳簿は紙（ルールどおりに建てた場合の記録）。
通知先: 本人「webhookは俺専用に配信」＝専用chは作らない。DISCORD_WEBHOOK_WARIYASU_URL が無ければ俺専用サーバーの
        極上ch（DISCORD_WEBHOOK_GOKUJO_URL）へ送る。タイトルは「💴【割安B】」で極上の配信と見分ける。

ルールは2つを定数で切り替える（WY_RULE。どちらを本番にするかは親が組み合わせの再測定で決める）:
■ R_opt（既定・2026-09-27 最適化の結果。BTの正は _bt_souzai_opt5_0927.py ＝ _bt_souzai_opt_0927.py の run()）
  割安: 0<PBR≤1.2・0<予想PER≤15・予想配当利回り≥3.0%（前営業日の値）× 20日平均売買代金1億以上
  入り方: T4押し目（21日の最高終値 ≥ 20〜59営業日前の最安終値×1.10、押し率=(最高−終値)÷(最高−最安)が0.382〜0.618）の日に、
          その日の終値×0.98 の指値を翌営業日から3営業日置く。約定は「安値≤指値」の日に min(寄り値, 指値)。
  並べ方: 合成スコア＝(z(1/PBR)+z(益回り)+z(配当利回り))/3 の高い順（流動性あり・1/PBRと益回りが有効な全銘柄で、日ごとに
          各項目を1%/99%点で丸めてからz。益回り=予想EPS(無ければ実績EPS)÷株価・配当予想なし=利回り0）。
          指値を置くのは空き枠の数だけスコア上位（WY_T4_ORDERS="top_free"）。BTは待ちの指値を全部持って安値が触れた中から
          スコア順に約定させている（"all"）＝本番ではできない理想化。差は _audit_wariyasu_parity.py が測る。
  10年BT（3×100万・待ちの指値を全部持つ理想化）+660万/PF1.80/勝率62%/DD-113万/月3.8件（_bt_souzai_opt5 の数字）。
  本番の条件（3×50万・空き枠の数だけ指値・値がさ見送り）では +185万/PF1.48/勝率56%/DD-61万/月3.0件（_audit_wariyasu_parity.py）。
■ C1（前の既定。BTの正は _bt_souzai_v2sim_0927.py「B×T1収縮・損切り7%・翌寄り」）
  割安: 0<PBR≤1.0・0<予想PER≤10・利回り≥3%。ボラ収縮(ATR5/ATR20≤0.6)の日の翌営業日の寄りで成行。利回りの高い順。
  寄りがストップ高張り付きなら見送り（次の候補へ）。10年BT（3×100万）+392万/PF1.67/勝率57%/DD-69万/年32玉。
  本番の条件（3×50万・値がさ見送り）では +207万/PF1.71/勝率58%/DD-34万/月2.6件（BTどおりに発注できる＝理想化なし）。
■ 共通: 3枠×1枠50万（2026-09-27 本人の資金=現金余力40万台・信用余力600万での推奨）・同じ銘柄は重ねない・ナンピンなし。
  1枠で100株に届かない値がさ株（50万ならシグナル日の終値5,000円超）は見送り＝次の候補へ。決算発表まで10営業日以内は建てない。
  出口（BTと同じ順序と価格）: 水準=max(建値×(1-7%), +5%乗った後の高値×(1-5%))。①買った翌日以降、寄りが水準以下なら寄り値
  ②安値が水準に触れたら水準の値 ③買った日を1日目として20営業日目の大引け ④決算発表の前営業日の大引け。
  高値の更新はその日の判定の後（今日の高値は明日の水準に効く）。5営業日を超えて値が付かなければ最後の終値で手仕舞い扱い。
  PBR/PER/BPS/EPSは前営業日のJ-Quants公式値（項目ごとに最大3営業日前まで遡る）、利回り=予想配当÷前営業日の株価(PBR×BPS)。
  配当予想は開示日（土日の開示はその前の営業日）の翌営業日から使う。帳簿のコスト=片道0.1%＋買方金利2.8%/年。
  ⚠ 財務データが2016年〜なので10年しか測れない・2023年（東証のPBR改善要請）の寄与が大きい。
  ⚠ 本番だけの近似: 次の決算日はJPX公式の発表予定（jpx_earnings_schedule.json・大引けジョブが毎営業日更新）→
    無ければ過去の開示パターンからの推定（earnings_calendar.json・週次）。BTは実際の開示日を使っている。
    保有中は毎日その時点の最新の予定で決算日を見直す（開示が予定より早く出たら、それを知った日の大引けで手仕舞い）。

ルールの値は全部下の「ルールの値」と RULES に集めてある。差し替えたら _audit_wariyasu_parity.py（10年のBTと玉単位で一致するか）
と _test_wariyasu.py をやり直す。

データ: J-Quants /equities/bars/daily（日付指定で全銘柄・調整後の四本値で指標、生の四本値で帳簿）・
        /equities/valuation（日付指定で全銘柄）・/fins/summary（日付指定の開示＝配当予想と決算の実績日）。
状態: wariyasu_state.json（銘柄ごとの配当予想・決算の実績日・割安の履歴・候補の履歴・待ちの指値・帳簿を進めた日・配信済みの日）
出力: wariyasu_signals.json（その日の候補・指値と見送り理由）・positions_wariyasu.json（紙の帳簿）・Discord
実行: python wariyasu_signal.py [--date YYYY-MM-DD] [--dry] [--force]
      python wariyasu_signal.py --init [--date 最初に動かす営業日]   ← ローカルで1回（_fins_history.pkl から状態を作る）
  夕方ジョブ(.github/workflows/schedule_evening.yml)から continue-on-error で呼ばれる。配信が欠けた日があれば次の回に
  帳簿を1日ずつ追いつかせる（BTと同じ順序で）。同じ日の再実行は帳簿をその日の前の状態に戻してからやり直す（二重計上しない）。
"""
from __future__ import annotations

import argparse
import bisect
import copy
import csv
import json
import math
import os
import sys
import time
from datetime import date, datetime, timedelta
import zoneinfo

import numpy as np
import pandas as pd

JST = zoneinfo.ZoneInfo("Asia/Tokyo")

# ══ ルールの値（ここだけ差し替える）════════════════════════════════════════════
WY_RULE = "R_opt"                # 本番のルール: "R_opt"(既定) / "C1"。下の RULES[WY_RULE] の値がこの下の既定値を上書きする
WY_B_MODE = "threshold"          # 割安の決め方: "threshold"=下の3つのしきい値 / "composite"=合成スコアの上位%
WY_PBR_MAX = 1.0                 # threshold: 0 < PBR ≤ これ
WY_PER_MAX = 10.0                # threshold: 0 < 予想PER ≤ これ
WY_DY_MIN = 3.0                  # threshold: 予想配当利回り(%) ≥ これ
WY_COMPOSITE_TOP_PCT = 10.0      # composite: その日の合成スコア(compz)の上位何%を割安とするか
WY_RANK_KEY = "dy"               # 候補の並べ方: "compz"(合成スコア高い順) / "dy"(利回り高い順) / "pbr"(低い順) / "per"(低い順)
WY_ENTRY = "T1"                  # 入り方: "T1"(ボラ収縮の日の翌寄り成行) / "first"(割安に入った初日の翌寄り成行) / "T4"(押し目の指値)
WY_T1_RATIO = 0.6                # T1: ATR5/ATR20 ≤ これ
WY_FIRST_LOOKBACK = 20           # first: 過去この営業日に一度も割安でなかった日が「初日」
WY_T4_LIMIT = 0.98               # T4: 指値=シグナル日の終値×これ
WY_T4_DAYS = 3                   # T4: 指値の有効営業日数（翌営業日から）
WY_T4_ORDERS = "top_free"        # T4: "top_free"=空き枠の数だけスコア上位に指値を置く（本番）/ "all"=待ちの指値を全部持つ（BTの理想化）
WY_TOV_MIN = 1e8                 # 20日平均売買代金(円) ≥ これ（15日以上の値が要る）
WY_SLOTS = 3                     # 同時に持つ銘柄数
WY_SIZE = 500_000                # 1枠の金額（帳簿の損益と株数の基準）。2026-09-27 今の資金(現金余力40万台・信用余力600万)での推奨=3枠×50万
WY_SIZES_SHOW = (500_000, 1_000_000)   # 配信で株数を出す金額（1枠の50万が主・100万も併記）
WY_SKIP_UNAFFORDABLE = True      # 1枠の金額で100株に届かない値がさ株（50万なら株価5,000円超）は見送り＝次の候補へ
WY_STOP = 0.07                   # 損切り（建値比）。0=使わない
WY_TRAIL = 0.05                  # トレーリング幅（高値比）。0=使わない
WY_TRAIL_ACT = 0.05              # トレーリングが効き始める含み益（高値 ≥ 建値×(1+これ)）
WY_MAXHOLD = 20                  # 最長保有（営業日・買った日が1日目）
WY_EARN_GAP = 10                 # 決算発表まで何営業日以内なら建てない
WY_TP = None                     # 固定利確（例 0.10）。None=使わない
WY_PBR_TP = None                 # 例 1.0=PBRがこれを超えた日の翌寄りで手仕舞い（C1のBT＝v2simの定義）。None=使わない
WY_SIDE_COST = 0.001             # 帳簿の損益に入れるコスト（片道）
WY_RATE = 0.028                  # 帳簿の損益に入れる買方金利（年・暦日）
WY_MISS_DAYS = 5                 # 値が付かない日がこれを超えたら最後の終値で手仕舞い扱い
WY_SHOW_N = 5                    # 配信に出す候補・指値（置く分＋補欠）の数
WY_REF_FIRST = False             # 参考（帳簿外）に「割安に入った初日」を出す
WY_REF_FIRST_STOP = 0.10         # 参考の型の損切り（表示だけ）
WY_BT_NOTE = ""                  # 配信の最後に出すBTの成績（RULES で入れる）
RULES = {
    # 2026-09-27 最適化の結果（_bt_souzai_opt5_0927.py の最終構成）
    "R_opt": dict(WY_PBR_MAX=1.2, WY_PER_MAX=15.0, WY_DY_MIN=3.0, WY_RANK_KEY="compz", WY_ENTRY="T4", WY_T4_ORDERS="top_free",
                  WY_STOP=0.07, WY_REF_FIRST=False,
                  WY_BT_NOTE="10年BT(2016-07〜2026-09-18・3×50万・空き枠の数だけ指値・値がさ見送り) +185万/PF1.48/勝率56%/DD-61万/月3.0件"
                             "（待ちの指値を全部持つ理想化なら+273万）"),
    # 前の既定（_bt_souzai_v2sim_0927.py の B×T1収縮・損切り7%・翌寄り）
    "C1": dict(WY_PBR_MAX=1.0, WY_PER_MAX=10.0, WY_DY_MIN=3.0, WY_RANK_KEY="dy", WY_ENTRY="T1", WY_STOP=0.07, WY_REF_FIRST=True,
               WY_BT_NOTE="10年BT(2016-07〜2026-09-18・3×50万・値がさ見送り) +207万/PF1.71/勝率58%/DD-34万/月2.6件"),
}


def use_rule(name: str) -> None:
    """RULES[name] の値で上の定数を上書きする（監査・テストからも呼ぶ）"""
    globals().update(RULES[name])
    globals()["WY_RULE"] = name


use_rule(WY_RULE)
# ═══════════════════════════════════════════════════════════════════════════════

STATE_FILE = "wariyasu_state.json"
SIG_FILE = "wariyasu_signals.json"
BOOK_FILE = "positions_wariyasu.json"
CAL_FILE = "market_calendar.csv"
EARN_CAL_FILE = "earnings_calendar.json"
JPX_SCHED_FILE = "jpx_earnings_schedule.json"
FINS_PKL = "_fins_history.pkl"
WEBHOOK_ENV = "DISCORD_WEBHOOK_WARIYASU_URL"
WEBHOOK_FALLBACK_ENV = "DISCORD_WEBHOOK_GOKUJO_URL"   # 2026-09-27 本人「webhookは俺専用に配信」
TITLE_MARK = "💴【割安B】"
WEEKDAY_JA = "月火水木金土日"
NO_EARN = 10 ** 6                # 決算日が分からない時の番号（=決算で止めない・決算で見送らない。BTの T+99 と同じ扱い）
CATCHUP_MAX = 60                 # 帳簿を追いつかせる最大の営業日数
F32 = np.float32                 # 出口・約定の判定は float32（BTは float32 の株価で計算＝建値1000円なら損切り水準はちょうど930円。
                                 # float64 だと929.9999…になり、安値930円ちょうどで切れない玉が出てBTとずれる）
NAN4 = (F32("nan"),) * 4
VAL_FIELDS = ("PBR", "FwdPER", "BPS", "EPS", "FwdEPS")

_LIM = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500),
        (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000),
        (50000, 7000), (70000, 10000), (100000, 15000), (150000, 30000), (200000, 40000),
        (300000, 50000), (500000, 70000), (700000, 100000), (1000000, 150000), (float("inf"), 300000)]


def _webhook_url() -> str:
    """専用URLが無ければ俺専用サーバー(極上ch)のURL。どちらも無ければ空＝投稿しない"""
    return os.environ.get(WEBHOOK_ENV, "").strip() or os.environ.get(WEBHOOK_FALLBACK_ENV, "").strip()


def _fin(x) -> bool:
    try:
        return x is not None and math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def limit_width(p: float) -> float:
    for b, w in _LIM:
        if p < b:
            return float(w)
    return 300000.0


def shares_for(size: float, price: float) -> int:
    if not (_fin(price) and price > 0):
        return 0
    return int(size / price / 100) * 100


def affordable(close_raw, size=None) -> bool:
    """1枠の金額で100株買えるか（シグナル日の終値＝配信に出す株数で判定）。WY_SKIP_UNAFFORDABLE=False なら常に True"""
    size = WY_SIZE if size is None else size
    return (not WY_SKIP_UNAFFORDABLE) or shares_for(size, close_raw) >= 100


def stuck_up(prev_close_raw, o, h, ratio_open=None) -> bool:
    """寄りがストップ高張り付き（BTと同じ: 寄りの前日比 ≥ 値幅上限-0.3% かつ 高値 ≤ 寄り×1.0005）。
    prev_close_raw=前日終値(生・値幅の区分用) / ratio_open=寄り÷前日終値（調整後の比。分割の日も正しく出る。無ければ生値で計算）。
    型もBTと同じ（前日比は float32・値幅の率は float64・高値の比較は float32）"""
    if not (_fin(prev_close_raw) and prev_close_raw > 0 and _fin(o) and _fin(h)):
        return False
    pc = float(prev_close_raw)
    w = limit_width(pc) / pc
    r = F32(ratio_open) if _fin(ratio_open) else F32(o) / F32(pc)
    return bool((float(r - F32(1)) >= w - 0.003) and (F32(h) <= F32(o) * F32(1.0005)))


# ── 指標（BTの計算をそのまま移したもの。行=営業日・列=銘柄の float32 配列）──────────────────
def calc_t1(H, L, C, ratio=None):
    """ATR5/ATR20 ≤ ratio。TRは前日終値が無い日は高値-安値。rolling は窓が全部そろった日だけ値が出る"""
    ratio = WY_T1_RATIO if ratio is None else ratio
    prevC = np.vstack([np.full((1, C.shape[1]), np.nan, np.float32), C[:-1]])
    with np.errstate(invalid="ignore"):
        TR = np.fmax(H - L, np.fmax(np.abs(H - prevC), np.abs(L - prevC)))
    atr5 = pd.DataFrame(TR).rolling(5).mean().to_numpy(np.float32)
    atr20 = pd.DataFrame(TR).rolling(20).mean().to_numpy(np.float32)
    with np.errstate(invalid="ignore", divide="ignore"):
        return (atr5 / atr20) <= ratio


def calc_tov20(C, V):
    with np.errstate(invalid="ignore", divide="ignore"):
        return pd.DataFrame(C * V).rolling(20, min_periods=15).mean().to_numpy(np.float32)


def calc_liq(C, V, tov_min=None):
    tov_min = WY_TOV_MIN if tov_min is None else tov_min
    return calc_tov20(C, V) >= tov_min


def calc_t4(C):
    """押し目: 直近21日(今日を含む)の終値の最高が20〜59営業日前の最安の+10%以上、かつ今日の終値がその上げの38.2〜61.8%戻し"""
    cmax20 = pd.DataFrame(C).rolling(21).max().to_numpy(np.float32)
    cmin_old = pd.DataFrame(C).shift(20).rolling(40).min().to_numpy(np.float32)
    with np.errstate(invalid="ignore", divide="ignore"):
        retr = (cmax20 - C) / (cmax20 - cmin_old)
        return (cmax20 >= 1.10 * cmin_old) & (retr >= 0.382) & (retr <= 0.618)


def calc_dy(DIV, PBR1, BPS1):
    """予想配当利回り(%) = 配当予想 ÷ (PBR×BPS=前営業日の生の株価) ×100（BTと同じく float32 で同じ順に計算）"""
    with np.errstate(invalid="ignore", divide="ignore"):
        PRAW1 = PBR1 * BPS1
        return DIV / PRAW1 * 100


def calc_ep(PBR, BPS, EPS, FwdEPS):
    """益回り = 予想EPS(無ければ実績EPS) ÷ 株価(PBR×BPS)（BTの EP と同じ・float32）。前営業日の値を渡す"""
    with np.errstate(invalid="ignore", divide="ignore"):
        PRAW = PBR * BPS
        eps = np.where(np.isfinite(FwdEPS), FwdEPS, EPS)
        return (eps / PRAW).astype(np.float32)


def _z_w(x):
    """1%/99%点で丸めてzスコア（BTの z_w と同じ）"""
    lo, hi = np.nanpercentile(x, [1, 99]); x = np.clip(x, lo, hi)
    return (x - np.nanmean(x)) / (np.nanstd(x) + 1e-12)


def calc_compz(PBR1, EP1, DY, LIQ, min_n=100):
    """合成スコア（BTの COMPZ と同じ）: 行ごとに、流動性あり・1/PBR と益回りが有効な銘柄で、1/PBR・益回り・配当利回り
    (予想なし=0)をそれぞれ1%/99%点で丸めてzスコアにし3つを平均。対象が100銘柄未満の日は NaN"""
    out = np.full(PBR1.shape, np.nan, np.float32)
    with np.errstate(invalid="ignore", divide="ignore"):
        BP = np.where(PBR1 > 0, 1.0 / PBR1, np.nan)
        DYz = np.where(np.isfinite(DY), DY, 0.0)
    for t in range(PBR1.shape[0]):
        m = LIQ[t] & np.isfinite(BP[t]) & np.isfinite(EP1[t])
        if m.sum() < min_n:
            continue
        out[t, m] = (_z_w(BP[t, m]) + _z_w(EP1[t, m]) + _z_w(DYz[t, m])) / 3.0
    return out


def calc_b_threshold(PBR1, PER1, DY, pbr_max=None, per_max=None, dy_min=None):
    pbr_max = WY_PBR_MAX if pbr_max is None else pbr_max
    per_max = WY_PER_MAX if per_max is None else per_max
    dy_min = WY_DY_MIN if dy_min is None else dy_min
    with np.errstate(invalid="ignore"):
        return (PBR1 <= pbr_max) & (PBR1 > 0) & (PER1 > 0) & (PER1 <= per_max) & (DY >= dy_min)


def calc_b_composite(COMPZ, top_pct=None):
    """合成スコアの日ごとの順位が上位 top_pct%（BTの COMPP ≤ x/100 と同じ）"""
    top_pct = WY_COMPOSITE_TOP_PCT if top_pct is None else top_pct
    B = np.zeros(COMPZ.shape, bool)
    for t in range(COMPZ.shape[0]):
        m = np.isfinite(COMPZ[t])
        k = int(m.sum())
        if k < 100:
            continue
        idx = np.nonzero(m)[0]
        order = np.argsort(-COMPZ[t, idx], kind="stable")
        pr = np.empty(k, np.float32)
        pr[order] = (np.arange(k) + 0.5) / k
        B[t, idx] = pr <= top_pct / 100
    return B


def entry_day(M, lookback=None):
    """過去 lookback 営業日に M が一度も無かった日（BTの entry_day と同じ）"""
    lookback = WY_FIRST_LOOKBACK if lookback is None else lookback
    prior = pd.DataFrame(M.astype(np.float32)).shift(1).rolling(lookback, min_periods=1).max().fillna(0).to_numpy() > 0
    return M & ~prior


def rank_score(DY, PBR1, PER1, COMPZ=None, key=None):
    """候補の並べ方の値（大きいほど先）"""
    key = WY_RANK_KEY if key is None else key
    if key == "compz" and COMPZ is not None:
        return COMPZ
    if key == "dy":
        return DY
    if key == "pbr":
        return -PBR1
    if key == "per":
        return -PER1
    raise ValueError(f"WY_RANK_KEY={key}")


def entry_mask(B, LIQ, T1=None, T4=None, first=None, mode=None):
    """建てる候補の印。first を渡さなければ B の全履歴から entry_day で作る"""
    mode = WY_ENTRY if mode is None else mode
    if mode == "T1":
        return B & T1 & LIQ
    if mode == "first":
        return first if first is not None else (entry_day(B) & LIQ)
    if mode == "T4":
        return B & T4 & LIQ
    raise ValueError(f"WY_ENTRY={mode}")


def cands_of_day(mask_row, score_row, codes) -> list:
    """その日の候補 [(code, score)]。スコア降順・同じ値は銘柄の並び順（BTの to_cands＋安定ソートと同じ）"""
    idx = np.nonzero(mask_row)[0]
    out = [(codes[j], float(score_row[j]) if np.isfinite(score_row[j]) else -1e9) for j in idx]
    out.sort(key=lambda x: -x[1])
    return out


def needs_compz() -> bool:
    return WY_RANK_KEY == "compz" or WY_B_MODE == "composite"


# ── 配当予想・決算の実績日の状態 ───────────────────────────────────────────────────
def div_value(row: dict) -> float:
    """BTと同じ: 今期の年間配当予想(FDivAnn)があればそれ、無くて年次決算(DocTypeがFYで始まる)なら来期の予想(NxFDivAnn)"""
    def f(x):
        try:
            v = float(x)
            return v if math.isfinite(v) else float("nan")
        except (TypeError, ValueError):
            return float("nan")
    d = f(row.get("FDivAnn"))
    if math.isfinite(d):
        return d
    if str(row.get("DocType", "")).startswith("FY"):
        return f(row.get("NxFDivAnn"))
    return float("nan")


def is_statement(doc_type) -> bool:
    """BTの EARN と同じ: 決算短信（FinancialStatements を含み Revision を含まない）"""
    s = str(doc_type or "")
    return "FinancialStatements" in s and "Revision" not in s


def _disc_key(r: dict):
    t = r.get("DiscTime")
    miss = t is None or t == "" or (isinstance(t, float) and math.isnan(t))
    return (str(r.get("DiscDate", ""))[:10], 1 if miss else 0, "" if miss else str(t))


def apply_fins_rows(divs: dict, rows: list, eff_date_fn, stmt: dict | None = None, keep: int = 3) -> int:
    """開示の行を状態に反映（開示日→時刻の順。時刻が無い行はその日の最後＝BTの並べ方と同じ）。
    divs[code4] = [[有効日, 配当予想], ...]（有効日の新しい順・同じ有効日なら後の開示が先＝後勝ち・新しい keep 件だけ残す）。
    有効日 = 開示日以前の最後の営業日（BTの sidx）。その値は有効日の「翌営業日」の判定から使う（div_as_of に前営業日を渡す）。
    stmt[code4] = 決算短信の有効日の一覧（新しい6件＝約1年半。保有中の決算日の見直しと、予定の無い銘柄の決算日の推定に使う）。
    戻り値=配当予想を反映した行数"""
    n = 0
    for r in sorted(rows, key=_disc_key):
        code = str(r.get("Code", "")).strip()
        if len(code) != 5:
            continue
        eff = eff_date_fn(str(r.get("DiscDate", ""))[:10])
        if not eff:
            continue
        c4 = code[:4]
        if stmt is not None and is_statement(r.get("DocType")):
            s = stmt.setdefault(c4, [])
            if eff not in s:
                s.append(eff)
                s.sort()
                del s[:-6]
        dv = div_value(r)
        if not math.isfinite(dv):
            continue
        h = divs.setdefault(c4, [])
        h.insert(0, [eff, dv])
        h.sort(key=lambda x: x[0], reverse=True)
        del h[keep:]
        n += 1
    return n


def div_as_of(divs: dict, code4: str, d: str) -> float:
    """有効日 ≤ d の最新の配当予想"""
    for eff, v in divs.get(code4, []):
        if eff <= d:
            return float(v)
    return float("nan")


def val_prev(val_days: list, codes: list):
    """前営業日のバリュエーション。val_days=[前営業日, 2日前, 3日前, 4日前] の {code: (PBR, FwdPER, BPS, EPS, FwdEPS)}。
    項目ごとに最初に値のある日を使う（=BTの reindex→ffill(limit=3) を前営業日の行で見たのと同じ）。項目の数だけ float32 配列を返す"""
    N = len(codes)
    nf = max((len(v) for vd in val_days for v in vd.values()), default=len(VAL_FIELDS))
    outs = tuple(np.full(N, np.nan, np.float32) for _ in range(nf))
    for j, c in enumerate(codes):
        for k in range(nf):
            for vd in val_days:
                v = vd.get(c)
                if v is not None and k < len(v) and _fin(v[k]):
                    outs[k][j] = v[k]
                    break
    return outs


# ── 帳簿のエンジン（BTの run() の1日分をそのまま移したもの。監査もこれを回す）────────────────
class Engine:
    """held: {code: dict(e, px, peak, ne, last, miss, pbrx)}（e/ne は営業日の通し番号）・
    pend: [(code, 指値, 期限, スコア, ne, [情報...])]（T4の待ちの指値。6番目以降は表示用でエンジンは見ない）"""

    def __init__(self, slots=None, stop=None, trail=None, act=None, maxhold=None, earn_gap=None, tp="default", pbr_tp="default",
                 entry=None, side=None, rate=None, miss_days=None, t4_limit=None, t4_days=None, t4_orders=None):
        self.slots = WY_SLOTS if slots is None else slots
        self.stop = WY_STOP if stop is None else stop
        self.trail = WY_TRAIL if trail is None else trail
        self.act = WY_TRAIL_ACT if act is None else act
        self.maxhold = WY_MAXHOLD if maxhold is None else maxhold
        self.earn_gap = WY_EARN_GAP if earn_gap is None else earn_gap
        self.tp = WY_TP if tp == "default" else tp
        self.pbr_tp = WY_PBR_TP if pbr_tp == "default" else pbr_tp
        self.entry = WY_ENTRY if entry is None else entry
        self.side = WY_SIDE_COST if side is None else side
        self.rate = WY_RATE if rate is None else rate
        self.miss_days = WY_MISS_DAYS if miss_days is None else miss_days
        self.t4_limit = WY_T4_LIMIT if t4_limit is None else t4_limit
        self.t4_days = WY_T4_DAYS if t4_days is None else t4_days
        self.t4_orders = WY_T4_ORDERS if t4_orders is None else t4_orders

    def ret(self, x, px, days_cal) -> float:
        """損益率（BTと同じ式・同じ型: 株価の比は float32、金利は暦日×年率）"""
        return float(F32(x) / F32(px) - 1 - 2 * self.side - self.rate * np.int64(days_cal) / 365)

    def live_orders(self, pend: list, held, free: int, t: int) -> list:
        """t 日に生きている指値の pend の位置（"top_free": スコア上位から空き枠の数だけ・同じ銘柄は1本 / "all": 全部）"""
        pend.sort(key=lambda x: -x[3])
        out, seen = [], set()
        for i, it in enumerate(pend):
            if t > it[2] or it[0] in held:
                continue
            if self.t4_orders == "top_free":
                if len(out) >= free:
                    break
                if it[0] in seen:
                    continue
                seen.add(it[0])
            out.append(i)
        return out

    def step(self, held: dict, pend: list, t: int, cands_prev, cands_today, bar, ne_of, cal_days,
             is_stuck=None, pbr_of=None, skip=None) -> list:
        """t 日の処理（BTと同じ順: 前日の候補で寄りに建てる(T4は待ちの指値の約定) → 保有全部の出口 → (T4)今日の候補で指値を置く）。
        bar(t, code)->(O,H,L,C) / ne_of(s, code)->s日に見た次の決算の営業日番号 / cal_days(t)->暦日の通し番号 /
        is_stuck(t, code)->寄りS高張り付き / pbr_of(t, code)->その日のPBR / skip(s, code)->監査用の除外。
        株価・水準は float32 のまま持つ（BTと同じ丸め）。戻り値: [{kind: entry|stuck|exit, code, t, ...}]"""
        ev = []
        free = self.slots - len(held)
        if self.entry in ("T1", "first"):
            for code, _sc in (cands_prev or []):
                if free <= 0:
                    break
                if code in held:
                    continue
                o = F32(bar(t, code)[0])
                if not (_fin(o) and o > 0):
                    continue
                ne = ne_of(t - 1, code)
                if ne - (t - 1) <= self.earn_gap:
                    continue
                if skip is not None and skip(t - 1, code):
                    continue
                if is_stuck is not None and is_stuck(t, code):
                    ev.append({"kind": "stuck", "code": code, "t": t})
                    continue
                held[code] = dict(e=t, px=o, peak=o, ne=int(ne), last=o, miss=0, pbrx=False)
                free -= 1
                ev.append({"kind": "entry", "code": code, "t": t, "px": float(o), "ne": int(ne)})
        else:
            pend.sort(key=lambda x: -x[3])
            live = set(self.live_orders(pend, held, free, t)) if self.t4_orders == "top_free" else None
            kp = []
            for i, it in enumerate(pend):
                code, lim, exp, sc, ne = it[:5]
                if t > exp or code in held:
                    continue
                o, h, l, c = (F32(v) for v in bar(t, code))
                if free > 0 and (live is None or i in live) and _fin(l) and l <= lim and _fin(o):
                    px = min(o, F32(lim))
                    held[code] = dict(e=t, px=px, peak=px, ne=int(ne), last=px, miss=0, pbrx=False)
                    free -= 1
                    ev.append({"kind": "entry", "code": code, "t": t, "px": float(px), "ne": int(ne), "lim": float(lim),
                               "info": it[5] if len(it) > 5 else {}})
                else:
                    kp.append(it)
            pend[:] = kp
        for code in list(held):
            p = held[code]
            o, h, l, c = (F32(v) for v in bar(t, code))
            if not _fin(c):
                p["miss"] = p.get("miss", 0) + 1
                if p["miss"] > self.miss_days:
                    r = self.ret(p["last"], p["px"], cal_days(t) - cal_days(p["e"]))
                    ev.append({"kind": "exit", "code": code, "t": t, "e": p["e"], "px": float(p["px"]), "x": float(p["last"]),
                               "r": r, "why": "廃止等"})
                    del held[code]
                continue
            p["miss"] = 0
            x = why = None
            if p.get("pbrx") and _fin(o):
                x, why = o, "PBR>1"
            ls = p["px"] * F32(1 - self.stop) if self.stop else -math.inf
            lt = p["peak"] * F32(1 - self.trail) if (self.trail and p["peak"] >= p["px"] * F32(1 + self.act)) else -math.inf
            lvl, lw = (lt, "トレーリング") if lt > ls else (ls, "損切り")
            tpl = p["px"] * F32(1 + self.tp) if self.tp else math.inf
            if x is None and p["e"] < t and _fin(o):
                if o <= lvl:
                    x, why = o, lw
                elif o >= tpl:
                    x, why = o, "利確"
            if x is None:
                if _fin(l) and l <= lvl:
                    x, why = lvl, lw
                elif _fin(h) and h >= tpl:
                    x, why = tpl, "利確"
            if x is None:
                if (t - p["e"] + 1) >= self.maxhold:
                    x, why = c, "期限"
                elif t >= p["ne"] - 1:
                    x, why = c, "決算前"
            if x is not None:
                r = self.ret(x, p["px"], cal_days(t) - cal_days(p["e"]))
                ev.append({"kind": "exit", "code": code, "t": t, "e": p["e"], "px": float(p["px"]), "x": float(x), "r": r, "why": why})
                del held[code]
            else:
                if _fin(h):
                    p["peak"] = max(p["peak"], h)
                p["last"] = c
                if self.pbr_tp is not None and pbr_of is not None:
                    v = pbr_of(t, code)
                    if _fin(v) and v > self.pbr_tp:
                        p["pbrx"] = True
        if self.entry == "T4":
            for code, sc in (cands_today or []):
                c = F32(bar(t, code)[3])
                ne = ne_of(t, code)
                if _fin(c) and ne - t > self.earn_gap and not (skip is not None and skip(t, code)):
                    pend.append((code, c * F32(self.t4_limit), t + self.t4_days, sc, int(ne)))
        return ev

    def levels(self, px: float, peak: float) -> dict:
        """明日の売りの水準（損切り/トレーリング）。判定と同じ float32 で計算"""
        px, peak = F32(px), F32(peak)
        ls = px * F32(1 - self.stop) if self.stop else -math.inf
        on = bool(self.trail) and bool(peak >= px * F32(1 + self.act))
        lt = peak * F32(1 - self.trail) if on else -math.inf
        return {"stop": float(ls), "trail_on": on, "trail": float(lt), "level": float(max(ls, lt)), "act_px": float(px * F32(1 + self.act))}


# ── 営業日 ────────────────────────────────────────────────────────────────────
class Cal:
    """market_calendar.csv（HolDiv=1=株式の営業日）。ファイルの先は祝日ライブラリで補う"""

    def __init__(self, path: str = CAL_FILE, days: list | None = None):
        if days is not None:
            self.days = sorted(days)
        else:
            self.days = []
            try:
                with open(path, encoding="utf-8") as f:
                    for r in csv.DictReader(f):
                        if str(r.get("HolDiv", "")).strip() == "1":
                            self.days.append(str(r["Date"])[:10])
            except FileNotFoundError:
                pass
            self.days.sort()
            self._extend(400)
        self.pos = {d: i for i, d in enumerate(self.days)}

    def _extend(self, n_days: int) -> None:
        try:
            import jpholiday
            hol = jpholiday.is_holiday
        except Exception:
            hol = lambda _d: False
        d = date.fromisoformat(self.days[-1]) + timedelta(days=1) if self.days else date.today()
        end = d + timedelta(days=n_days)
        while d <= end:
            if d.weekday() < 5 and not hol(d) and not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3)):
                self.days.append(d.isoformat())
            d += timedelta(days=1)

    def is_trading(self, d: str) -> bool:
        return d in self.pos

    def idx(self, d: str) -> int:
        """d 以前の最後の営業日の番号"""
        return bisect.bisect_right(self.days, str(d)[:10]) - 1

    def add(self, d: str, n: int) -> str:
        return self.days[self.idx(d) + n]


# ── 決算の予定 ─────────────────────────────────────────────────────────────────
class EarnCal:
    """次の決算発表日: JPX公式の予定 → 無ければ earnings_calendar.json の今日より先の日（過去の開示パターンからの推定）"""

    def __init__(self, sched_path: str = JPX_SCHED_FILE, cal_path: str = EARN_CAL_FILE,
                 official: dict | None = None, est: dict | None = None):
        self.official = official if official is not None else {}
        self.est = est if est is not None else {}
        if official is None:
            try:
                s = json.load(open(sched_path, encoding="utf-8")).get("schedule", {})
                for d, lst in s.items():
                    for x in lst:
                        self.official.setdefault(str(x.get("code", ""))[:4], []).append(str(d)[:10])
            except (FileNotFoundError, json.JSONDecodeError, AttributeError):
                pass
        if est is None:
            try:
                for k, v in json.load(open(cal_path, encoding="utf-8")).items():
                    self.est[str(k)[:4]] = sorted(set(str(x)[:10] for x in v))
            except (FileNotFoundError, json.JSONDecodeError, AttributeError):
                pass

    def next_date(self, code4: str, s_date: str, today: str, cal: Cal, stmt: dict | None = None) -> str | None:
        """s_date（シグナル日）より後の最初の決算日。JPX公式と開示の実績（決算短信）を優先、無ければ今日より先の推定
        （earnings_calendar.json）。それも無い銘柄（2026-09-27時点で4分の1）は決算短信の実績から「1年前の同じ四半期＋364日」と
        「最後の短信＋91日」の早い方＝決算をまたがない側に寄せる"""
        s_i = cal.idx(s_date)
        a = [x for x in self.official.get(code4, []) if cal.idx(x) > s_i]
        a += [x for x in (stmt or {}).get(code4, []) if cal.idx(x) > s_i]
        if a:
            return min(a)
        b = [x for x in self.est.get(code4, []) if x > today and cal.idx(x) > s_i]
        if b:
            return min(b)
        hist = (stmt or {}).get(code4, [])
        if not hist:
            return None
        c = [(date.fromisoformat(x) + timedelta(days=364)).isoformat() for x in hist]
        c.append((date.fromisoformat(max(hist)) + timedelta(days=91)).isoformat())
        c = [x for x in c if x > today and cal.idx(x) > s_i]
        return min(c) if c else None


def ne_idx(cal: Cal, d: str | None) -> int:
    """決算日→営業日の番号（その日以前の最後の営業日＝BTの sidx）"""
    return cal.idx(d) if d else NO_EARN


# ── J-Quants ──────────────────────────────────────────────────────────────────
def _token() -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    except Exception:
        pass
    return os.environ.get("JQUANTS_API_KEY", "").strip()


def _get_all(path: str, token: str, params: dict) -> list:
    from screener import _jquants_get
    out, key = [], None
    for _ in range(100):
        p = dict(params)
        if key:
            p["pagination_key"] = key
        for attempt in range(5):
            try:
                r = _jquants_get(path, token, p)
                break
            except Exception as e:
                if "429" in str(e) and attempt < 4:
                    time.sleep(60)
                    continue
                raise
        out.extend(r.get("data", []) or [])
        key = r.get("pagination_key")
        if not key:
            break
        time.sleep(1.0)
    return out


def _f(x) -> float:
    try:
        return float(x) if x not in (None, "") else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def fetch_bars_day(token: str, d: str) -> dict:
    """{code4: (AdjO, AdjH, AdjL, AdjC, AdjVo, O, H, L, C, AdjFactor)}（普通株だけ・終値のある銘柄だけ）"""
    from screener import is_common_stock_code
    out = {}
    for x in _get_all("/equities/bars/daily", token, {"date": d}):
        code = str(x.get("Code", ""))
        if len(code) < 4 or not is_common_stock_code(code):
            continue
        c = _f(x.get("AdjC"))
        if not (math.isfinite(c) and c > 0):
            continue
        out[code[:4]] = (_f(x.get("AdjO")), _f(x.get("AdjH")), _f(x.get("AdjL")), c, _f(x.get("AdjVo")),
                         _f(x.get("O")), _f(x.get("H")), _f(x.get("L")), _f(x.get("C")), _f(x.get("AdjFactor")))
    time.sleep(0.5)
    return out


def fetch_valuation_day(token: str, d: str) -> dict:
    """{code4: (PBR, FwdPER, BPS, EPS, FwdEPS)}（普通株だけ）"""
    out = {}
    for x in _get_all("/equities/valuation", token, {"date": d}):
        code = str(x.get("Code", ""))
        if len(code) < 4 or (len(code) >= 5 and code[4] != "0"):
            continue
        out[code[:4]] = tuple(_f(x.get(k)) for k in VAL_FIELDS)
    time.sleep(0.5)
    return out


def fetch_fins_day(token: str, d: str) -> list:
    rows = _get_all("/fins/summary", token, {"date": d})
    time.sleep(0.5)
    return rows


def fetch_names(token: str, codes: list) -> dict:
    from screener import _jquants_get
    names = {}
    for c in codes:
        try:
            m = _jquants_get("/equities/master", token, {"code": c + "0"}).get("data", []) or []
            if m:
                names[c] = m[-1].get("CoName", "") or ""
        except Exception as e:
            print(f"[wariyasu] 銘柄名の取得失敗 {c}: {e}")
        time.sleep(0.3)
    return names


# ── 状態と帳簿 ────────────────────────────────────────────────────────────────
def _load(path: str, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _save(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        if path == STATE_FILE:           # 状態は大きい（配当予想4,500銘柄）ので詰めて書く
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        else:
            json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def update_fins(state: dict, cal: Cal, token: str, until: str, fetch=None) -> int:
    """state['fins_last'] の翌日〜until（暦日）の開示を取り込む（配当予想・決算短信の日）。
    判定日 t に使うのは「t より前の暦日」の開示だけ（有効日 ≤ t-1）なので、until=判定日の前日で足りる（その日の開示は出そろっている）"""
    fetch = fetch or fetch_fins_day
    last = state.get("fins_last")
    if not last:
        return 0
    d = date.fromisoformat(last) + timedelta(days=1)
    end = date.fromisoformat(until)
    n_all = 0
    while d <= end:
        ds = d.isoformat()
        rows = fetch(token, ds)
        n = apply_fins_rows(state.setdefault("divs", {}), rows, lambda s: cal.days[cal.idx(s)] if cal.idx(s) >= 0 else None,
                            stmt=state.setdefault("stmt", {}))
        state["fins_last"] = ds
        n_all += n
        if rows:
            print(f"[wariyasu] 開示 {ds}: {len(rows)}行（配当予想の更新 {n}）")
        d += timedelta(days=1)
    return n_all


# ── 1日の計算 ──────────────────────────────────────────────────────────────────
def need_days() -> int:
    """指標に要る過去の営業日数（T4は 21日最高と20〜59日前の最安で60日＋余裕）"""
    return 65 if WY_ENTRY == "T4" else 25


def compute_day(d: str, cal: Cal, state: dict, token: str, fetch_bars=None, fetch_val=None,
                bars_cache: dict | None = None, val_cache: dict | None = None) -> dict:
    """d（引け後）の割安・候補を計算する（状態は読むだけ）。今日の足が無ければ {"nodata": True}"""
    fetch_bars = fetch_bars or fetch_bars_day
    fetch_val = fetch_val or fetch_valuation_day
    bars_cache = {} if bars_cache is None else bars_cache
    val_cache = {} if val_cache is None else val_cache
    i = cal.idx(d)
    days = cal.days[max(0, i - need_days() + 1): i + 1]
    if d not in bars_cache or not bars_cache[d]:
        bars_cache[d] = fetch_bars(token, d)
    if not bars_cache[d]:
        return {"nodata": True}
    for x in days:
        if x not in bars_cache:
            bars_cache[x] = fetch_bars(token, x)
    codes = sorted(set().union(*[set(bars_cache[x]) for x in days]))
    T, N = len(days), len(codes)
    cix = {c: j for j, c in enumerate(codes)}
    A = {k: np.full((T, N), np.nan, np.float32) for k in ("O", "H", "L", "C", "V")}
    for t, x in enumerate(days):
        for c, v in bars_cache[x].items():
            j = cix[c]
            A["O"][t, j], A["H"][t, j], A["L"][t, j], A["C"][t, j], A["V"][t, j] = v[0], v[1], v[2], v[3], v[4]
    T1 = calc_t1(A["H"], A["L"], A["C"])[-1] if WY_ENTRY == "T1" else np.zeros(N, bool)
    tov20 = calc_tov20(A["C"], A["V"])[-1]
    LIQ = tov20 >= WY_TOV_MIN
    T4 = calc_t4(A["C"])[-1] if WY_ENTRY == "T4" else np.zeros(N, bool)
    vdays = [cal.days[i - k] for k in range(1, 5)]
    for x in vdays:
        if x not in val_cache:
            val_cache[x] = fetch_val(token, x)
    PBR1, PER1, BPS1, EPS1, FEPS1 = val_prev([val_cache[x] for x in vdays], codes)[:5]
    d_prev = cal.days[i - 1]
    divs = state.get("divs", {})
    DIV = np.array([div_as_of(divs, c, d_prev) for c in codes], np.float32)
    DY = calc_dy(DIV, PBR1, BPS1)
    COMPZ = calc_compz(PBR1[None], calc_ep(PBR1, BPS1, EPS1, FEPS1)[None], DY[None], LIQ[None])[0] if needs_compz() else None
    B = calc_b_threshold(PBR1, PER1, DY) if WY_B_MODE == "threshold" else calc_b_composite(COMPZ[None])[0]
    bh = state.get("b_hist", {})
    was = set().union(*[set(bh.get(cal.days[i - k], [])) for k in range(1, WY_FIRST_LOOKBACK + 1)])
    FIRST = np.array([bool(B[j]) and codes[j] not in was for j in range(N)], bool) & LIQ
    score = rank_score(DY, PBR1, PER1, COMPZ)
    M = entry_mask(B, LIQ, T1, T4, first=FIRST)
    pbr_today = {}
    if WY_PBR_TP is not None:
        if d not in val_cache:
            val_cache[d] = fetch_val(token, d)
        pv = bars_cache.get(d_prev, {})
        for c, v in bars_cache[d].items():
            vv = val_cache[d].get(c)
            if vv is not None and _fin(vv[0]):
                pbr_today[c] = vv[0]
            elif c in cix and _fin(PBR1[cix[c]]) and c in pv and pv[c][3] > 0:   # 未公表なら前日のPBR×終値の比（BTの延長と同じ）
                pbr_today[c] = float(PBR1[cix[c]]) * v[3] / pv[c][3]
    bars = bars_cache[d]
    cands_all = cands_of_day(M, score, codes)
    return {"date": d, "codes": codes, "cix": cix, "B": B, "T1": T1, "LIQ": LIQ, "T4": T4, "FIRST": FIRST, "DY": DY, "PBR1": PBR1, "PER1": PER1,
            "COMPZ": COMPZ, "tov20": tov20, "score": score, "cands_all": cands_all,
            "cands": [x for x in cands_all if affordable(bars[x[0]][8])],                  # 帳簿が使う候補（値がさは見送り）
            "first_cands": [x for x in cands_of_day(FIRST, DY, codes) if affordable(bars[x[0]][8])],
            "bars": bars, "prev_bars": bars_cache.get(d_prev, {}), "pbr_today": pbr_today}


def _info(day: dict, code: str) -> dict:
    """配信用の銘柄の値（シグナル日の値）"""
    j = day["cix"][code] if "cix" in day else day["codes"].index(code)
    v = day["bars"][code]
    return {"sig": day["date"], "close": v[8], "PBR": round(float(day["PBR1"][j]), 3), "PER": round(float(day["PER1"][j]), 2),
            "DY": round(float(day["DY"][j]), 2), "score": round(float(day["score"][j]), 4) if np.isfinite(day["score"][j]) else None,
            "tov20_oku": round(float(day["tov20"][j]) / 1e8, 2)}


def settle_day(book: list, state: dict, d: str, cal: Cal, day: dict, eng: Engine, earn: EarnCal, refine: bool = True) -> list:
    """紙の帳簿を d の1日分進める（BTと同じ順: 前営業日の候補で寄りに建てる／待ちの指値の約定 → 保有の出口 → 今日の指値）。
    帳簿は生の株価。分割の日は調整係数で建値・高値・終値・指値を換算する"""
    t = cal.idx(d)
    prev_d = cal.days[t - 1]
    bars, pbars = day["bars"], day["prev_bars"]
    stmt = state.get("stmt", {})
    held, openp = {}, {}
    for p in book:
        if p.get("status") != "open":
            continue
        h = dict(e=cal.idx(p["entry_date"]), px=F32(p["px"]), peak=F32(p["peak"]), last=F32(p["last"]), miss=p.get("miss", 0),
                 pbrx=p.get("pbrx", False), ne=ne_idx(cal, p.get("ne_date")))
        v = bars.get(p["code"])
        if v is not None and _fin(v[9]) and abs(v[9] - 1.0) > 1e-9 and p.get("adj_date") != d:
            for k in ("px", "peak", "last"):
                h[k] = F32(float(h[k]) * v[9])
            p["shares"] = int(round(p.get("shares", 0) / v[9]))
            p["adj_date"] = d
            print(f"[wariyasu] {p['code']} 分割・併合（調整係数{v[9]}）→ 帳簿の株価を換算")
        if refine:   # 保有中は毎日、その時点の最新の予定で決算日を見直す
            nd = earn.next_date(p["code"], p["signal_date"], d, cal, stmt)
            if nd:
                h["ne"] = ne_idx(cal, nd)
                p["ne_date"] = nd
        held[p["code"]] = h
        openp[p["code"]] = p
    pend = []
    for x in state.get("pend", []):
        lim = x[1]
        v = bars.get(x[0])
        if v is not None and _fin(v[9]) and abs(v[9] - 1.0) > 1e-9:
            lim = float(lim) * v[9]
        pend.append((x[0], F32(lim), cal.idx(x[2]), x[3], ne_idx(cal, x[4]), x[5] if len(x) > 5 else {}))
    hist = state.get("cands_hist", {}).get(prev_d, [])
    cands_prev = [(x[0], x[1]) for x in hist]
    ne_sig = {x[0]: ne_idx(cal, x[2] if len(x) > 2 else None) for x in hist}

    def bar(ti, code):
        v = bars.get(code)
        return (F32(v[5]), F32(v[6]), F32(v[7]), F32(v[8])) if v is not None else NAN4

    def ne_of(ti, code):
        if ti == t - 1:                       # 通知した時点（前営業日の夜）に見た決算日で判定＝本人が見た通知と同じ
            return ne_sig.get(code, NO_EARN)
        return ne_idx(cal, earn.next_date(code, cal.days[ti], d, cal, stmt))

    def cal_days(ti):
        return date.fromisoformat(cal.days[ti]).toordinal()

    def is_stuck(ti, code):
        v, pv = bars.get(code), pbars.get(code)
        if v is None or pv is None:
            return False
        ratio = v[0] / pv[3] if (_fin(v[0]) and _fin(pv[3]) and pv[3] > 0) else None
        return stuck_up(pv[8], v[5], v[6], ratio_open=ratio)

    def pbr_of(ti, code):
        return day.get("pbr_today", {}).get(code, float("nan"))

    ev = eng.step(held, pend, t, cands_prev, day["cands"], bar, ne_of, cal_days, is_stuck=is_stuck,
                  pbr_of=pbr_of if eng.pbr_tp is not None else None)
    for k, it in enumerate(pend):              # 今日置いた指値に表示用の値を付ける
        if len(it) == 5:
            pend[k] = it + (_info(day, it[0]),)
    out = []
    for e in ev:
        e2 = {k: v for k, v in e.items() if k not in ("t", "e", "ne", "info")}
        e2["date"] = d
        if e["kind"] == "entry":
            sig = prev_d if eng.entry != "T4" else (e.get("info") or {}).get("sig", prev_d)
            p = {"code": e["code"], "name": "", "signal_date": sig, "entry_date": d,
                 "px": e["px"], "peak": e["px"], "last": e["px"], "ne_date": cal.days[e["ne"]] if e["ne"] < len(cal.days) else None,
                 "shares": shares_for(WY_SIZE, e["px"]), "status": "open", "miss": 0, "pbrx": False}
            if "lim" in e:
                p["limit"] = e["lim"]
            book.append(p)
            openp[e["code"]] = p
        elif e["kind"] == "exit":
            p = openp.pop(e["code"], None)
            if p is not None:
                p.update(status="closed", exit_date=d, exit_px=round(e["x"], 4), why=e["why"], r=round(e["r"], 6),
                         pnl_yen=round(WY_SIZE * e["r"]), hold_days=t - cal.idx(p["entry_date"]) + 1)
                e2["entry_date"] = p["entry_date"]
        out.append(e2)
    for code, h in held.items():
        p = openp.get(code)
        if p is not None:
            p.update(px=float(h["px"]), peak=float(h["peak"]), last=float(h["last"]), miss=h["miss"], pbrx=bool(h["pbrx"]))
    state["pend"] = [[x[0], float(x[1]), cal.days[x[2]], float(x[3]), cal.days[x[4]] if x[4] < len(cal.days) else None,
                      x[5] if len(x) > 5 else {}] for x in pend]
    return out


def cand_rows(day: dict, cal: Cal, earn: EarnCal, state: dict, book: list, names: dict) -> list:
    """今日の候補の表示用の行（保有中・値がさ・決算が近い＝建てない理由も付ける）"""
    d = day["date"]
    s_i = cal.idx(d)
    held = {p["code"] for p in book if p.get("status") == "open"}
    rows = []
    for c, s in day.get("cands_all", day["cands"]):
        inf = _info(day, c)
        v = day["bars"][c]
        nd = earn.next_date(c, d, d, cal, state.get("stmt", {}))
        why = ""
        if c in held:
            why = "保有中"
        elif not affordable(v[8]):
            why = f"値がさ（100株{v[8] * 100 / 1e4:,.0f}万＞1枠{WY_SIZE // 10000}万）"
        elif ne_idx(cal, nd) - s_i <= WY_EARN_GAP:
            why = f"決算{_md(nd)}まで{ne_idx(cal, nd) - s_i}営業日"
        rows.append({"code": c, "name": names.get(c, ""), **inf, "ne_date": nd,
                     "shares": {str(z): shares_for(z, v[8]) for z in WY_SIZES_SHOW}, "ok": not why, "skip": why})
    return rows


def order_rows(state: dict, book: list, cal: Cal, today: str, names: dict) -> list:
    """(T4) 明日生きている指値（待ちの指値をスコア順に並べ、置く分＝空き枠の数だけ上から・残りは補欠）"""
    t1 = cal.idx(today) + 1
    held = {p["code"] for p in book if p.get("status") == "open"}
    free = WY_SLOTS - len(held)
    pend = [(x[0], x[1], cal.idx(x[2]), x[3], ne_idx(cal, x[4]), x[5] if len(x) > 5 else {}) for x in state.get("pend", [])]
    live = set(Engine(t4_orders="top_free").live_orders(pend, held, max(free, 0), t1))
    rows, seen = [], set()
    for i, it in enumerate(pend):
        code, lim, exp, sc = it[0], it[1], it[2], it[3]
        if t1 > exp or code in held or code in seen:
            continue
        seen.add(code)
        inf = it[5] or {}
        rows.append({"code": code, "name": names.get(code, ""), "limit": float(lim), "exp": cal.days[exp] if exp < len(cal.days) else None,
                     "score": sc, "live": i in live, **{k: inf.get(k) for k in ("sig", "close", "PBR", "PER", "DY", "tov20_oku")},
                     "shares": {str(z): shares_for(z, float(lim)) for z in WY_SIZES_SHOW},
                     "ne_date": cal.days[it[4]] if it[4] < len(cal.days) else None})
    return rows


# ── 配信 ───────────────────────────────────────────────────────────────────────
def _md(d: str | None) -> str:
    if not d:
        return "?"
    x = date.fromisoformat(d)
    return f"{x.month}/{x.day}({WEEKDAY_JA[x.weekday()]})"


def _yen(x: float) -> str:
    return f"{x:,.0f}" if abs(x) >= 100 else f"{x:,.1f}"


_TICK = [(3000, 1), (5000, 5), (30000, 10), (50000, 50), (300000, 100), (500000, 500), (3000000, 1000), (float("inf"), 5000)]


def tick_floor(p: float) -> float:
    """呼値（普通株の標準の刻み）に切り捨て。指値・逆指値の表示用＝「安値≤水準」と同じ条件になる値
    （帳簿は BT と同じく刻みに丸めない水準で判定。刻みの上の株価なら「≤水準」と「≤切り捨てた値」は同じ）"""
    for b, t in _TICK:
        if p < b:
            return math.floor(p / t + 1e-9) * t
    return p


def _nm(names: dict, book: list, code: str, fallback: str = "") -> str:
    n = names.get(code) or fallback or next((p.get("name") for p in book if p["code"] == code and p.get("name")), "")
    return f"{n}（{code}）" if n else code


def _sh(shares: dict, price: float) -> str:
    return " / ".join((f"{int(z / 1e4)}万={shares[str(z)]:,}株" if shares[str(z)]
                       else f"{int(z / 1e4)}万=買えない(100株{price * 100 / 1e4:,.0f}万)") for z in WY_SIZES_SHOW)


def build_embed(today: str, cal: Cal, day: dict, rows: list, book: list, events: list, names: dict, eng: Engine,
                late_days: list | None = None, orders: list | None = None) -> dict:
    nxt = cal.add(today, 1)
    opn = [p for p in book if p.get("status") == "open"]
    free = WY_SLOTS - len(opn)
    L = []
    if WY_ENTRY == "T4":
        orders = orders or []
        live = [r for r in orders if r["live"]]
        if not orders:
            L.append(f"**{_md(nxt)} の指値なし**（割安{int(day['B'].sum())}銘柄のうち押し目の条件に当たる銘柄なし）")
        elif free <= 0:
            L.append(f"**枠が埋まっている（{len(opn)}/{WY_SLOTS}）ので{_md(nxt)}の指値なし**（待ちの候補{len(orders)}件）")
        else:
            L.append(f"**{_md(nxt)} の指値買い（空き枠{free}・合成スコアの高い順に空き枠の数だけ置く）**")
            for k, r in enumerate(orders[:WY_SHOW_N], 1):
                mark = "🟢" if r["live"] else "⚪補欠"
                sig = f"{_md(r['sig'])}の押し目・" if r.get("sig") and r["sig"] != today else ""
                L.append(f"{mark} {k}. **{_nm(names, book, r['code'], r.get('name', ''))}** 指値**{_yen(tick_floor(r['limit']))}円**（{_md(r['exp'])}まで）"
                         f" → {_sh(r['shares'], r['limit'])}\n"
                         f"　{sig}終値{_yen(r['close'] or 0)}円・PBR{r['PBR'] or 0:.2f}・予想PER{r['PER'] or 0:.1f}・利回り{r['DY'] or 0:.1f}%・"
                         f"スコア{r['score']:+.2f}・決算{_md(r['ne_date'])}")
            if len(orders) > len(live):
                L.append("　⚪補欠は🟢が約定して枠が埋まらなかった次の晩に、スコアの順で入れ替わる（毎晩この一覧どおりに置き直す）")
            if len(orders) > WY_SHOW_N:
                L.append(f"　…ほか{len(orders) - WY_SHOW_N}件")
    else:
        act = [r for r in rows if r["ok"]]
        if not act:
            L.append(f"**{_md(nxt)} の買い候補なし**（割安{int(day['B'].sum())}銘柄のうち条件に当たる銘柄なし）")
        elif free <= 0:
            L.append(f"**枠が埋まっている（{len(opn)}/{WY_SLOTS}）ので{_md(nxt)}の新規買いなし**（候補{len(act)}件）")
        else:
            L.append(f"**{_md(nxt)} の寄りで成行買い**（空き枠{free}・上から順に空き枠の数だけ）")
            for k, r in enumerate(act[:WY_SHOW_N], 1):
                mark = "🟢" if k <= free else "⚪補欠"
                L.append(f"{mark} {k}. **{_nm(names, book, r['code'], r.get('name', ''))}** 終値{_yen(r['close'])}円 → {_sh(r['shares'], r['close'])}\n"
                         f"　PBR{r['PBR']:.2f}・予想PER{r['PER']:.1f}・利回り{r['DY']:.1f}%・代金{r['tov20_oku']:.1f}億・決算{_md(r['ne_date'])}")
            if len(act) > free:
                L.append("　⚪補欠は🟢が寄りでストップ高に張り付いた・寄らなかった時だけ、上から順に")
            if len(act) > WY_SHOW_N:
                L.append(f"　…ほか{len(act) - WY_SHOW_N}件")
    sk = [r for r in rows if not r["ok"]]
    if sk:
        L.append("今日の候補の見送り: " + "・".join(f"{_nm(names, book, r['code'], r.get('name', ''))} {r['skip']}" for r in sk[:4])
                 + (f" ほか{len(sk) - 4}件" if len(sk) > 4 else ""))
    if opn:
        L.append("")
        L.append(f"**保有 {len(opn)}/{WY_SLOTS}（{_md(nxt)}の売り方）**")
        for p in opn:
            lv = eng.levels(p["px"], p["peak"])
            k = cal.idx(today) - cal.idx(p["entry_date"]) + 1
            due = cal.add(p["entry_date"], WY_MAXHOLD - 1)
            ne = ne_idx(cal, p.get("ne_date"))
            ne_exit = cal.days[ne - 1] if ne < len(cal.days) else None
            if due <= nxt or (ne_exit and ne_exit <= nxt):
                close_note = f"**{_md(nxt)}の大引けで売り（{'期限' if due <= nxt else '決算前'}）**"
            else:
                close_note = f"期限{_md(due)}の大引け" + (f"・決算前{_md(ne_exit)}の大引け" if ne_exit and ne_exit < due else "")
            tr = (f"トレーリング中（高値{_yen(p['peak'])}円の-{WY_TRAIL * 100:.0f}%）" if lv["trail_on"]
                  else f"損切り-{WY_STOP * 100:.0f}%（高値が{_yen(lv['act_px'])}円に届いたらトレーリングへ）")
            L.append(f"・**{_nm(names, book, p['code'])}** {p.get('shares', 0):,}株 建値{_yen(p['px'])}円({_md(p['entry_date'])})"
                     f" → 今{_yen(p['last'])}円（{(p['last'] / p['px'] - 1) * 100:+.1f}%）{k}日目\n"
                     f"　└ 逆指値の売り **{_yen(tick_floor(lv['level']))}円**（{tr}・寄りがそれ以下なら寄りで）｜{close_note}")
    ex = [e for e in events if e["kind"] == "exit"]
    en = [e for e in events if e["kind"] == "entry"]
    st = [e for e in events if e["kind"] == "stuck"]
    if en or ex or st:
        L.append("")
        L.append(f"**今日の帳簿（{_md(today)}）**" if not late_days
                 else f"**帳簿（{'・'.join(_md(x) for x in late_days)}の配信が欠けた分も追いつかせた）**")
        for e in en:
            lim = f"指値{_yen(e['lim'])}円 → " if "lim" in e else ""
            L.append(f"🛒 {_md(e['date'])} 約定 {_nm(names, book, e['code'])} {lim}{_yen(e['px'])}円")
        for e in st:
            L.append(f"⏭ {_md(e['date'])} 見送り {_nm(names, book, e['code'])}（寄りがストップ高張り付き）")
        for e in ex:
            L.append(f"{'✅' if e['r'] > 0 else '❌'} {_md(e['date'])} {e['why']} {_nm(names, book, e['code'])} "
                     f"{_yen(e['px'])}→{_yen(e['x'])}円 {e['r'] * 100:+.2f}%（{WY_SIZE * e['r'] / 1e4:+.1f}万・コスト込み）")
    if WY_REF_FIRST and WY_ENTRY != "first":
        shown = {r["code"] for r in rows[:WY_SHOW_N]}
        fc = [(c, s) for c, s in day.get("first_cands", []) if c not in shown][:3]
        if fc:
            L.append("")
            L.append(f"参考（帳簿外・損切り{WY_REF_FIRST_STOP * 100:.0f}%で使う型）今日はじめて割安に入った: "
                     + " ・ ".join(f"{_nm(names, book, c)} 利回り{s:.1f}%" for c, s in fc))
    closed = [p for p in book if p.get("status") == "closed"]
    tot = sum(p.get("pnl_yen", 0) for p in closed)
    wins = sum(1 for p in closed if p.get("pnl_yen", 0) > 0)
    rule_b = (f"PBR≤{WY_PBR_MAX:g}・予想PER≤{WY_PER_MAX:g}・利回り≥{WY_DY_MIN:g}%" if WY_B_MODE == "threshold"
              else f"割安の合成スコア上位{WY_COMPOSITE_TOP_PCT:g}%")
    rule_e = {"T1": f"ボラ収縮(ATR5/ATR20≤{WY_T1_RATIO:g})の翌朝の寄りで成行", "first": "割安に入った初日の翌朝の寄りで成行",
              "T4": f"押し目(21日高値から38〜62%戻し)の終値×{WY_T4_LIMIT:g}に{WY_T4_DAYS}営業日の指値"}[WY_ENTRY]
    rank = {"dy": "利回りの高い順", "pbr": "PBRの低い順", "per": "PERの低い順", "compz": "合成スコアの高い順"}[WY_RANK_KEY]
    sell = [f"損切り-{WY_STOP * 100:g}%" if WY_STOP else "", f"+{WY_TRAIL_ACT * 100:g}%乗った後は高値-{WY_TRAIL * 100:g}%" if WY_TRAIL else "",
            f"利確+{WY_TP * 100:g}%" if WY_TP else "", f"PBR>{WY_PBR_TP:g}の翌寄り" if WY_PBR_TP is not None else "",
            f"{WY_MAXHOLD}営業日目の大引け", "決算前日の大引け"]
    footer = (f"ルール[{WY_RULE}]: {rule_b}・代金{WY_TOV_MIN / 1e8:g}億以上 → {rule_e}（{rank}・同時{WY_SLOTS}銘柄・1枠{WY_SIZE // 10000}万）。"
              f"売り={'・'.join(x for x in sell if x)}の早い方。決算まで{WY_EARN_GAP}営業日以内は買わない。{WY_BT_NOTE}。"
              f"紙の帳簿: {len(closed)}件 {tot / 1e4:+.1f}万" + (f"（勝ち{wins}）" if closed else ""))
    if WY_ENTRY == "T4":
        n = len([r for r in (orders or []) if r["live"]]) if free > 0 else 0
        title = f"{TITLE_MARK}{_md(today)}引け → {_md(nxt)} 指値{n}件（保有{len(opn)}/{WY_SLOTS}）"
    else:
        n = min(len([r for r in rows if r["ok"]]), max(free, 0))
        title = f"{TITLE_MARK}{_md(today)}引け → {_md(nxt)} 買い{n}件（保有{len(opn)}/{WY_SLOTS}）"
    desc = "\n".join(L)
    if len(desc) > 4000:
        desc = desc[:3990] + "\n…（省略）"
    return {"title": title[:256], "description": desc, "color": 0x2E8B57, "footer": {"text": footer[:2000]}}


def post_discord(embed: dict, dry: bool) -> bool:
    url = _webhook_url()
    if not url:
        print(f"[wariyasu] {WEBHOOK_ENV} も {WEBHOOK_FALLBACK_ENV} も未設定 → 投稿しない（JSONとログだけ）")
        return False
    if dry:
        print("[wariyasu] --dry → 投稿しない")
        return False
    try:
        import requests
        r = requests.post(url, json={"embeds": [embed]}, timeout=20, headers={"User-Agent": "Mozilla/5.0 (wariyasu-signal)"})
        ok = r.status_code in (200, 204)
        print(f"[wariyasu] Discord {'OK' if ok else 'NG'} HTTP {r.status_code}")
        return ok
    except Exception as e:
        print(f"[wariyasu] Discord 送信失敗: {e}")
        return False


# ── 本体 ───────────────────────────────────────────────────────────────────────
def run(today: str, dry: bool = False, force: bool = False, token: str | None = None, cal: Cal | None = None,
        earn: EarnCal | None = None, fetch_bars=None, fetch_val=None, fetch_fins=None, fetch_names_fn=None, post_fn=None) -> dict:
    token = _token() if token is None else token
    cal = cal or Cal()
    earn = earn or EarnCal()
    fetch_names_fn = fetch_names_fn or fetch_names
    post_fn = post_fn or post_discord
    state = _load(STATE_FILE, {})
    book = _load(BOOK_FILE, [])
    if not state.get("fins_last"):
        print(f"[wariyasu] {STATE_FILE} が無い/未初期化 → ローカルで `python wariyasu_signal.py --init` を先に")
        return {"skipped": "noinit"}
    sent = state.setdefault("sent", [])
    if today in sent and not force:
        print(f"[wariyasu] {today} は配信済み → 何もしない（--force で再実行）")
        return {"skipped": "sent"}
    bb = state.get("book_before")
    if bb and bb.get("date") == today:          # 同じ日の再実行: 帳簿をその日の前に戻してからやり直す
        book = copy.deepcopy(bb["book"])
        state["pend"] = copy.deepcopy(bb["pend"])
        state["settled"] = bb["settled"]
    else:
        state["book_before"] = {"date": today, "book": copy.deepcopy(book), "pend": copy.deepcopy(state.get("pend", [])),
                                "settled": state.get("settled")}
    update_fins(state, cal, token, (date.fromisoformat(today) - timedelta(days=1)).isoformat(), fetch=fetch_fins)
    t_today = cal.idx(today)
    settled = state.get("settled")
    t0 = cal.idx(settled) + 1 if settled else t_today
    if t_today - t0 + 1 > CATCHUP_MAX:
        print(f"[wariyasu] ⚠ 帳簿が{t_today - t0 + 1}営業日遅れ → 直近{CATCHUP_MAX}営業日だけ追いつかせる")
        t0 = t_today - CATCHUP_MAX + 1
    eng = Engine()
    bars_cache, val_cache = {}, {}
    events, late, day = [], [], None
    for ti in range(t0, t_today + 1):
        d = cal.days[ti]
        day = compute_day(d, cal, state, token, fetch_bars=fetch_bars, fetch_val=fetch_val, bars_cache=bars_cache, val_cache=val_cache)
        if day.get("nodata"):
            print(f"[wariyasu] {d} の足がまだ無い → 配信せず終了（次の回でやり直し）")
            return {"skipped": "nodata", "date": d}
        events += settle_day(book, state, d, cal, day, eng, earn)
        ch = state.setdefault("cands_hist", {})
        ch[d] = [[c, round(s, 6), earn.next_date(c, d, d, cal, state.get("stmt", {}))] for c, s in day["cands"]]
        bh = state.setdefault("b_hist", {})
        bh[d] = [day["codes"][j] for j in np.nonzero(day["B"])[0]]
        for k in sorted(ch)[:-10]:
            del ch[k]
        for k in sorted(bh)[:-(WY_FIRST_LOOKBACK + 5)]:
            del bh[k]
        state["settled"] = d
        if d != today:
            late.append(d)
    need = sorted({c for c, _ in day["cands_all"][:WY_SHOW_N + 5]} | {c for c, _ in day["first_cands"][:5]}
                  | {p["code"] for p in book if p.get("status") == "open"} | {e["code"] for e in events}
                  | {x[0] for x in state.get("pend", [])})
    names = fetch_names_fn(token, need) if need else {}
    for p in book:
        if not p.get("name") and names.get(p["code"]):
            p["name"] = names[p["code"]]
    rows = cand_rows(day, cal, earn, state, book, names)
    orders = order_rows(state, book, cal, today, names) if WY_ENTRY == "T4" else None
    embed = build_embed(today, cal, day, rows, book, events, names, eng, late_days=late, orders=orders)
    out = {"date": today, "next_date": cal.add(today, 1),
           "rule": {"name": WY_RULE, "b_mode": WY_B_MODE, "pbr_max": WY_PBR_MAX, "per_max": WY_PER_MAX, "dy_min": WY_DY_MIN,
                    "entry": WY_ENTRY, "rank": WY_RANK_KEY, "t4_orders": WY_T4_ORDERS if WY_ENTRY == "T4" else None, "slots": WY_SLOTS,
                    "size": WY_SIZE, "stop": WY_STOP, "trail": WY_TRAIL, "trail_act": WY_TRAIL_ACT, "maxhold": WY_MAXHOLD,
                    "earn_gap": WY_EARN_GAP},
           "n_B": int(day["B"].sum()), "n_liq": int(day["LIQ"].sum()), "cands": rows[:30], "orders": orders,
           "first": [{"code": c, "name": names.get(c, ""), "DY": round(s, 2)} for c, s in day["first_cands"][:10]],
           "events": events, "late_days": late}
    if dry:
        print(json.dumps(embed, ensure_ascii=False, indent=1))
        return {**out, "embed": embed}
    posted = post_fn(embed, dry)
    if posted or not _webhook_url():
        sent.append(today)
        del sent[:-30]
    _save(BOOK_FILE, book)
    _save(SIG_FILE, out)
    _save(STATE_FILE, state)
    return {**out, "embed": embed, "posted": posted}


def init_state(cal: Cal, token: str, first_day: str, fetch_bars=None, fetch_val=None, fetch_fins=None, fins_pkl: str = FINS_PKL,
               bars_cache: dict | None = None, val_cache: dict | None = None) -> dict:
    """状態ファイルを作る（ローカル）。配当予想は _fins_history.pkl → その先を /fins/summary で first_day の前日まで。
    割安の履歴（初日の判定用）は first_day の前 WY_FIRST_LOOKBACK+2 営業日をライブのデータで計算。
    帳簿と待ちの指値は first_day から（その前の候補は通知していないので建てない）"""
    state = {"version": 1, "start": first_day, "divs": {}, "stmt": {}, "b_hist": {}, "cands_hist": {}, "sent": [], "pend": [],
             "settled": cal.days[cal.idx(first_day) - 1] if cal.is_trading(first_day) else cal.days[cal.idx(first_day)]}
    F = pd.read_pickle(fins_pkl)[["Code", "DiscDate", "DiscTime", "DocType", "FDivAnn", "NxFDivAnn"]]
    F = F[F.Code.astype(str).str.len() == 5]
    apply_fins_rows(state["divs"], F.to_dict("records"), lambda s: cal.days[cal.idx(s)] if cal.idx(s) >= 0 else None, stmt=state["stmt"])
    state["fins_last"] = str(F.DiscDate.astype(str).max())[:10]
    print(f"[wariyasu] 配当予想: {len(state['divs'])}銘柄（{fins_pkl} 〜{state['fins_last']}）")
    del F
    update_fins(state, cal, token, (date.fromisoformat(first_day) - timedelta(days=1)).isoformat(), fetch=fetch_fins)
    i0 = cal.idx(state["settled"])
    bars_cache = {} if bars_cache is None else bars_cache
    val_cache = {} if val_cache is None else val_cache
    if not (WY_ENTRY == "first" or WY_REF_FIRST):
        print("[wariyasu] 割安の履歴は「初日」の判定にしか使わない → 今のルールでは作らない（夕方ジョブが毎日足していく）")
        return state
    for ti in range(i0 - WY_FIRST_LOOKBACK - 1, i0 + 1):
        d = cal.days[ti]
        day = compute_day(d, cal, state, token, fetch_bars=fetch_bars, fetch_val=fetch_val, bars_cache=bars_cache, val_cache=val_cache)
        if day.get("nodata"):
            print(f"[wariyasu] {d} の足が無い → 割安の履歴を飛ばす")
            continue
        state["b_hist"][d] = [day["codes"][j] for j in np.nonzero(day["B"])[0]]
        print(f"[wariyasu] {d} 割安{int(day['B'].sum())}・候補{len(day['cands'])}", flush=True)
    return state


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="判定する日(YYYY-MM-DD)。省略時は今日(JST)。--init では最初に動かす営業日")
    ap.add_argument("--dry", action="store_true", help="ファイルを書かず・投稿せず、判定だけ表示")
    ap.add_argument("--force", action="store_true", help="配信済みでもやり直す（帳簿はその日の前に戻してから）")
    ap.add_argument("--init", action="store_true", help="状態ファイルを作る（ローカル・_fins_history.pkl が要る）")
    a = ap.parse_args()
    now = datetime.now(JST)
    cal = Cal()
    if a.init:
        first = a.date or next(x for x in cal.days if x > now.date().isoformat())
        st = init_state(cal, _token(), first)
        _save(STATE_FILE, st)
        if not os.path.exists(BOOK_FILE):
            _save(BOOK_FILE, [])
        print(f"[wariyasu] {STATE_FILE} を作成（帳簿は {first} から）")
        return 0
    today = a.date or now.date().isoformat()
    if not cal.is_trading(today):
        print(f"[wariyasu] {today} は休場 → スキップ")
        return 0
    if not a.date and now.hour * 60 + now.minute < 16 * 60 + 45:
        print(f"[wariyasu] {now:%H:%M} JST は引け後の四本値公開(16:30頃)前 → スキップ")
        return 0
    run(today, dry=a.dry, force=a.force, cal=cal)
    return 0


if __name__ == "__main__":
    sys.exit(main())
