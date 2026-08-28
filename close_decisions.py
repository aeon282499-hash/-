"""14:55 大引け前判定（close_check / kiwami_close）の結果を日付×銘柄で記録し、
翌朝の帳簿確定（tracker.update_positions / shadow_exit.advance）がそれに従うための台帳。

背景（2026-08-28 キヤノンMJ 8060）: 14:55通知は「RSI47.3<50 保有継続」だったが、引けまでの
35分で+0.9%上げてRSI50.66となり、翌朝の帳簿は引け値で再計算して「8/27 RSI回復決済 +1.82%」と
遡って確定。本人は通知どおり持ち越して翌日OCO+5%利確＝帳簿と実弾が乖離し、さらに翌日の
14:55チェックから銘柄が消えて処分指示も出なかった。
→ ユーザーが行動できるのは14:55通知だけなので、帳簿は通知の判定を正とする。
   HOLD と記録された日は RSI決済しない / RSI と記録された日はその引けで決済する。
   OCO(STOP/TP)は高安で自動約定するので記録の影響を受けない。MAXHOLD も従来どおり。
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta

FILE = "close_decisions.json"
KEEP_DAYS = 120

# decision の語彙
HOLD    = "HOLD"      # 14:55時点で処分対象外（保有継続を通知）
RSI     = "RSI"       # 14:55時点でRSI回復→大引け処分を通知
MAXHOLD = "MAXHOLD"   # 保有最終日→大引け強制処分を通知


def _load() -> dict:
    if not os.path.exists(FILE):
        return {}
    try:
        with open(FILE, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _key(scope: str, direction: str, ticker: str) -> str:
    return f"{scope}:{direction}:{ticker}"


def record(today: date, scope: str, direction: str,
           targets: list[dict], checked: list[dict]) -> int:
    """collect_targets の戻り値をそのまま渡す。note付き(異常)の checked は記録しない＝
    翌朝の帳簿は従来どおり引け値で判定する。"""
    d = _load()
    day = d.setdefault(today.isoformat(), {})
    n = 0
    for t in targets:
        dec = MAXHOLD if t.get("reason_type") == "MAXHOLD" else RSI
        day[_key(scope, direction, t["ticker"])] = {
            "decision": dec, "rsi": t.get("rsi_now"), "price": t.get("current_price"),
        }
        n += 1
    for c in checked:
        if c.get("note") is not None or c.get("settled"):
            continue
        day[_key(scope, direction, c["ticker"])] = {
            "decision": HOLD, "rsi": c.get("rsi_now"), "price": c.get("current_price"),
        }
        n += 1
    cutoff = (today - timedelta(days=KEEP_DAYS)).isoformat()
    for k in [k for k in d if k < cutoff]:
        del d[k]
    with open(FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    return n


def lookup(date_str: str, scope: str, direction: str, ticker: str,
           table: dict | None = None) -> str | None:
    """その日の14:55判定。記録が無ければ None（＝従来の引け値判定）。"""
    d = table if table is not None else _load()
    rec = d.get(date_str, {}).get(_key(scope, direction, ticker))
    return rec.get("decision") if rec else None


def apply(rsi_exit: bool, date_str: str, scope: str, direction: str, ticker: str,
          table: dict | None = None) -> bool:
    """引け値で計算した rsi_exit を 14:55判定で上書きする。"""
    dec = lookup(date_str, scope, direction, ticker, table)
    if dec == HOLD:
        return False
    if dec == RSI:
        return True
    return rsi_exit
