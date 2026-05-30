"""
theme_tracker.py — テーマ熱ランキング + 初動候補抽出

各テーマ(theme_members.json)のバスケット熱を算出してランク付けし、
点火中テーマの中から「出来高急増 & まだ伸びきってない」初動候補を抽出する。

熱 (heat) = 直近リターン + 中期リターン + 25MA上の銘柄比率(breadth) + 出来高ブレイク比率
を percentage-point 換算で合算した合成スコア。

単体スクリーナー(初動キャッチャー/逆張り)に「テーマ文脈」を足す層。
「この急騰はテーマ全体の点火の一部か、単独スパイクか」を判定するのが狙い。

実行: python theme_tracker.py        (J-Quants から取得してランキング表示)
GitHub Actions 自動化と Discord 通知は notifier_theme / main_theme で行う(次タスク)。
"""
import json
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from screener import (
    _jquants_id_token,
    batch_download_jquants,
    calc_rsi,
    calc_ma_deviation,
    calc_turnover,
    LOOKBACK_DAYS,
)

THEME_MEMBERS_PATH = Path("theme_members.json")

# --- heat 構成の重み (percentage-point 換算で合算) ---
W_R1 = 1.5        # 前日比リターン(%)
W_R5 = 1.0        # 5営業日リターン(%)
W_R20 = 0.5       # 20営業日リターン(%)
W_BREADTH = 20.0  # 25MA上の銘柄比率 (0-1)
W_BREAKOUT = 30.0 # 出来高ブレイク銘柄比率 (0-1)

# --- 出来高ブレイク / 初動候補の閾値 ---
# 注: 初動はモメンタム・ブレイクなので RSI が既に70-85なのが普通(=点火、警告ではない)。
#     RSIで切らず、「出来高点火 ＆ まだ走り始め(乖離小・20日リターン未拡大)」で判定する。
#     = ホットテーマ内の "出遅れ初動" を拾う。
VOL_SURGE = 1.5       # 当日出来高 / 直近20日平均 がこれ以上で「出来高急増」
EARLY_VR_MIN = 1.5    # 初動候補: 出来高比がこれ以上(点火)
EARLY_DEV_MAX = 12.0  # 初動候補: 25MA乖離がこれ以下(まだ伸びきってない・走り始め)
EARLY_DEV_MIN = -5.0  # 初動候補: 25MA乖離の下限。これ未満=MAから大きく下＝下落トレンドの出来高リバ(落ちるナイフ)→除外。
                      #          "走り始め" は MA近辺〜やや上で点火する銘柄に限定する。
EARLY_R20_MAX = 30.0  # 初動候補: 20日リターン(%)がこれ以下(既に大相場化した銘柄は除く)

# 出遅れ初動候補をスキャンする対象テーマの heat 下限。
# 旧仕様は「heat上位3テーマ固定」で候補抽出していたが、テーマを増やしても
# ローテ先(銀行/インバウンド等)が4位以下だとチャンスを取りこぼした。
# "点火中" と言える heat>=HEAT_FLOOR の全テーマを対象にして取りこぼしを無くす。
HEAT_FLOOR = 25.0


def load_theme_members() -> dict:
    with open(THEME_MEMBERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)["themes"]


def _ret(close: pd.Series, n: int) -> float | None:
    """n営業日リターン (最新終値 / n本前の終値 - 1)。"""
    if len(close) < n + 1:
        return None
    a = float(close.iloc[-1])
    b = float(close.iloc[-(n + 1)])
    if b == 0:
        return None
    return a / b - 1.0


def _vol_ratio_latest(df: pd.DataFrame, period: int = 20) -> float | None:
    """最新バーの出来高 / 直近period日平均(最新除く)。calc_volume_ratioはiloc[-2]基準なので
    "今まさに点火"を捉えるため最新バー基準の比率を別途算出する。"""
    vol = df["Volume"].dropna()
    if len(vol) < period + 1:
        return None
    avg = float(vol.iloc[-(period + 1):-1].mean())
    last = float(vol.iloc[-1])
    if avg == 0:
        return None
    return round(last / avg, 2)


def _member_metrics(df: pd.DataFrame) -> dict | None:
    close = df["Close"].dropna()
    if len(close) < 26:
        return None
    ma25 = float(close.rolling(25).mean().iloc[-1])
    cur = float(close.iloc[-1])
    return {
        "r1": _ret(close, 1),
        "r5": _ret(close, 5),
        "r20": _ret(close, 20),
        "above_ma25": cur >= ma25,
        "vr": _vol_ratio_latest(df),
        "dev": calc_ma_deviation(close),
        "rsi": calc_rsi(close),
        "turnover": calc_turnover(df),
        "close": cur,
    }


def _avg(mets: list[dict], key: str) -> float:
    vals = [m[key] for m in mets if m.get(key) is not None]
    return sum(vals) / len(vals) if vals else 0.0


def compute_theme_heat(themes: dict, data: dict[str, pd.DataFrame]) -> list[dict]:
    """各テーマの heat を算出し、降順ソートして返す。"""
    rows: list[dict] = []
    for tname, tinfo in themes.items():
        mets: list[dict] = []
        for m in tinfo.get("members", []):
            df = data.get(m["ticker"])
            if df is None:
                continue
            mm = _member_metrics(df)
            if mm is None:
                continue
            mm["ticker"] = m["ticker"]
            mm["name"] = m["name"]
            mm["role"] = m.get("role", "")
            mets.append(mm)
        if not mets:
            continue

        n = len(mets)
        avg_r1, avg_r5, avg_r20 = _avg(mets, "r1"), _avg(mets, "r5"), _avg(mets, "r20")
        pct_above = sum(1 for m in mets if m["above_ma25"]) / n
        breakout = sum(1 for m in mets if (m["vr"] or 0) >= VOL_SURGE and (m["r1"] or 0) > 0)

        heat = (
            avg_r1 * 100 * W_R1
            + avg_r5 * 100 * W_R5
            + avg_r20 * 100 * W_R20
            + pct_above * W_BREADTH
            + (breakout / n) * W_BREAKOUT
        )

        rows.append({
            "theme": tname,
            "heat": round(heat, 1),
            "n": n,
            "n_total": len(tinfo.get("members", [])),
            "avg_r1": round(avg_r1 * 100, 2),
            "avg_r5": round(avg_r5 * 100, 2),
            "avg_r20": round(avg_r20 * 100, 2),
            "pct_above_ma25": round(pct_above, 2),
            "breakout": breakout,
            "us_drivers": tinfo.get("us_drivers", []),
            "members": mets,
        })

    rows.sort(key=lambda r: r["heat"], reverse=True)
    return rows


def early_candidates(theme_row: dict) -> list[dict]:
    """点火中テーマ内の "出遅れ初動" を抽出。
    出来高点火(vr) ＆ 当日上昇(r1) ＆ まだ走り始め(乖離がMA近辺〜やや上・20日リターン未拡大)。
    RSIでは切らない(初動はRSI70-85が普通)。
    乖離下限(EARLY_DEV_MIN)で「MAから大きく下＝下落トレンドの出来高リバ」を除外する。"""
    out: list[dict] = []
    for m in theme_row["members"]:
        vr, dev, r1, r20 = m["vr"], m["dev"], m["r1"], m["r20"]
        if vr is None or dev is None:
            continue
        if (vr >= EARLY_VR_MIN
                and (r1 or 0) > 0
                and EARLY_DEV_MIN <= dev <= EARLY_DEV_MAX
                and (r20 or 0) * 100 <= EARLY_R20_MAX):
            out.append(m)
    out.sort(key=lambda m: (m["vr"] or 0), reverse=True)
    return out


def run_theme_tracker(
    data: dict[str, pd.DataFrame] | None = None,
    heat_floor: float = HEAT_FLOOR,
) -> tuple[list[dict], list[dict]]:
    """
    戻り値:
      ranked — 全テーマを heat 降順で
      hot    — heat>=heat_floor の "点火中" テーマのうち出遅れ初動候補が1件以上あるもの
               (各に "early" = 初動候補リストを付与)。= その日に実際に拾えるチャンスだけ。
    data を渡せば再ダウンロードせず使い回す(共通ワークフロー用)。
    """
    themes = load_theme_members()

    if data is None:
        token = _jquants_id_token()
        data = batch_download_jquants(token, lookback_trading_days=LOOKBACK_DAYS)
        if not data:
            print("[theme_tracker] J-Quants データ取得失敗")
            return [], []

    ranked = compute_theme_heat(themes, data)
    # 点火中(heat>=floor)の全テーマで出遅れ初動候補をスキャンし、候補ありのテーマだけ hot に。
    hot: list[dict] = []
    for tr in ranked:
        if tr["heat"] >= heat_floor:
            tr["early"] = early_candidates(tr)
            if tr["early"]:
                hot.append(tr)
    return ranked, hot


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    ranked, hot = run_theme_tracker()

    print()
    print("=" * 78)
    print("  テーマ熱ランキング (heat 降順)")
    print("=" * 78)
    for i, r in enumerate(ranked, 1):
        drv = "/".join(r["us_drivers"][:3]) if r["us_drivers"] else "国内発"
        print(f"  {i:2d}. {r['theme']:<18} heat={r['heat']:6.1f}  "
              f"1d={r['avg_r1']:+5.1f}% 5d={r['avg_r5']:+6.1f}% 20d={r['avg_r20']:+6.1f}%  "
              f"25MA上={r['pct_above_ma25']*100:3.0f}% ブレイク={r['breakout']}/{r['n']}  "
              f"[{drv}]")

    print()
    print("=" * 78)
    print(f"  点火中テーマ(heat>={HEAT_FLOOR:.0f})の出遅れ初動候補 — 候補あり{len(hot)}テーマ")
    print("=" * 78)
    if not hot:
        print("  本日は点火中テーマに出遅れ初動候補なし(各テーマ伸びきり or 出来高待ち)")
    for r in hot:
        drv = "/".join(r["us_drivers"]) if r["us_drivers"] else "国内発(米連動薄)"
        print(f"\n■ {r['theme']}  (heat={r['heat']} / 米震源: {drv})")
        early = r.get("early", [])
        for m in early:
            print(f"    - [{m['ticker']}] {m['name']}  "
                  f"出来高{m['vr']:.1f}倍 RSI={m['rsi']} 25MA乖離={m['dev']:+.1f}% "
                  f"5d={m['r5']*100:+.1f}%  〔{m['role']}〕")
    print("=" * 78)
