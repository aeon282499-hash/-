# -*- coding: utf-8 -*-
"""kiwami_vi.py — 極み買いの「玉サイズ傾斜」（日経VI代替・2026-09-04・既定OFF）。

根拠（本番無変更のBT・再提案時はこの数字）:
  10年(J-Quants・実VI代替=日経225オプション BaseVol): 現行+311万 → 傾斜+358万・PF1.21→1.24・
    前半+27→+29/後半+283→+329・勝年9/10・最悪年-38→-34（_bt_kiwami_nkvi_tilt2_0903.py）
  26年(立花日足・市場ボラ代替をVIに較正): 現行+187万 → +218万・勝年14/26→16/26・4期間全部改善か同等
    （_bt_kiwami_vol_tilt_26y_0904.py）。逆傾斜は両方とも悪化＝方向は本物。
  帯別(1玉平均): VI≤15 +0.11% / 15-20 +0.08% / 20-25 +0.30% / >25 +0.58%（悪い時代01-16でも単調）。

ルール: 前々日(=シグナル日の前営業日)の日経VI代替（日経225オプション四本値の BaseVol 中央値・
J-Quants は翌27:00公開なので前夜18:50時点で使えるのはこれ）が
  ≥ VI_HI(20) → 基準サイズ×1.3 ／ ≤ VI_LO(15) → ×0.7 ／ それ以外 ×1.0。
  >25 でさらに増やさない（2018/2020の暴落週は負ける）。
有効化: 環境変数 KIWAMI_VI_TILT=1（ワークフローの env）。未設定/0 なら従来どおり一律サイズ。
出力: kiwami_vi.json {"for": 配信対象日, "vi_date": 使ったVIの日付, "vi": 値, "mult": 倍率, "enabled": bool}
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta

VI_HI, VI_LO = 20.0, 15.0
MULT_HI, MULT_LO = 1.3, 0.7
STATE_FILE = "kiwami_vi.json"
_LOOKBACK_DAYS = 10          # 連休を跨いでも最新の公表日を拾う


def enabled() -> bool:
    return os.getenv("KIWAMI_VI_TILT", "0").strip() == "1"


def size_mult(vi: float | None) -> float:
    """VI → 玉サイズ倍率。VIが取れない日は1.0（従来どおり）。"""
    if vi is None or not (vi == vi):
        return 1.0
    if vi >= VI_HI:
        return MULT_HI
    if vi <= VI_LO:
        return MULT_LO
    return 1.0


def _tse_open(d: date) -> bool:
    import jpholiday
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def fetch_vi(token: str, signal_day: date) -> tuple[date, float] | None:
    """シグナル日の前営業日以前で最新の BaseVol 中央値を返す。(日付, VI) / 取れなければ None。
    J-Quants /derivatives/bars/daily/options/225 は翌27:00公開＝当日分は前夜には無いので、前営業日から遡る。"""
    from screener import _jquants_get
    import statistics
    d = signal_day
    for _ in range(_LOOKBACK_DAYS):
        d -= timedelta(days=1)
        if not _tse_open(d):
            continue
        rows: list = []
        params = {"date": d.strftime("%Y-%m-%d")}
        for _p in range(6):                      # ページング（1日1万行超）
            j = _jquants_get("/derivatives/bars/daily/options/225", token, params)
            rows += j.get("data", [])
            pk = j.get("pagination_key")
            if not pk:
                break
            params["pagination_key"] = pk
        bv = [float(r["BaseVol"]) for r in rows if r.get("BaseVol") not in (None, "", 0, 0.0)]
        if bv:
            return d, float(statistics.median(bv))
    return None


def update(signal_day: date, target_day: date, token: str | None = None) -> dict:
    """前夜ランで1回呼ぶ。VIを取って kiwami_vi.json に保存し、{"mult","vi","vi_date","enabled"} を返す。
    失敗時は mult=1.0（従来どおり）で、理由を note に残す。"""
    info = {"for": target_day.strftime("%Y-%m-%d"), "vi_date": None, "vi": None,
            "mult": 1.0, "enabled": enabled(), "note": ""}
    try:
        if token is None:
            from screener import _jquants_id_token
            token = _jquants_id_token()
        got = fetch_vi(token, signal_day)
        if got is None:
            info["note"] = "VI取得なし→一律サイズ"
        else:
            vd, vi = got
            info.update(vi_date=vd.strftime("%Y-%m-%d"), vi=round(vi, 2))
            if info["enabled"]:
                info["mult"] = size_mult(vi)
            else:
                info["note"] = "KIWAMI_VI_TILT未設定→表示のみ（サイズは一律）"
    except Exception as e:
        info["note"] = f"VI取得失敗: {e}"
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=1)
    except Exception:
        pass
    print(f"[kiwami_vi] {info}")
    return info


def load(target_day: date) -> dict:
    """配信側が読む。対象日が違えば一律サイズ扱い。"""
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            info = json.load(f)
        if info.get("for") == target_day.strftime("%Y-%m-%d"):
            return info
    except Exception:
        pass
    return {"for": target_day.strftime("%Y-%m-%d"), "vi": None, "vi_date": None, "mult": 1.0, "enabled": enabled(), "note": ""}


def line(info: dict, base_size: int) -> str:
    """配信に添える1行（利用者目線レビュー2026-09-04: 用語は「相場ボラ」・日付は終値時点・売りは不変を明示）。"""
    vi = info.get("vi"); m = float(info.get("mult") or 1.0); base = base_size // 10000
    if vi is None:
        return f"🧭 相場ボラ: 取得できず → 今回は1件{base}万（通常どおり）"
    would = size_mult(vi)                          # OFFでも「ONならいくらか」を見せる
    lvl = "高ボラ" if would > 1 else ("低ボラ" if would < 1 else "通常")
    try:
        from datetime import date as _d
        _vd = _d.fromisoformat(info.get("vi_date")); when = f"{_vd.month}/{_vd.day}終値時点"
    except Exception:
        when = str(info.get("vi_date"))
    if info.get("enabled"):
        if m == 1.0:
            return f"🧭 相場ボラ（日経VI相当）{vi:.1f}・{when}＝{lvl} → 今回の買いは1件 {base}万円（通常）"
        return (f"🧭 相場ボラ（日経VI相当）{vi:.1f}・{when}＝{lvl} → **今回の買いは1件 {int(base_size * m) // 10000}万円**"
                f"（{lvl}→{m:.1f}倍・売りは{base}万のまま）")
    on_size = int(base_size * would) // 10000
    return f"🧭 相場ボラ（日経VI相当）{vi:.1f}・{when}＝{lvl} → ONなら1件{on_size}万。今はOFF＝{base}万のまま"
