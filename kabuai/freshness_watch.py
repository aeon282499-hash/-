# -*- coding: utf-8 -*-
"""freshness_watch.py — 公開サイトの鮮度監視（2026-08-12）。

背景: 8/10のGitHub側不調でビルドが「queued」のまま2日間詰まり、サイトが金曜断面で
凍結したのに誰も気づかなかった（本人の目視で発覚）。この「静かな凍結」を自動検知する。

判定: 営業日の昼に https://…/data/latest.json の data_date を見て、
「今日より前の直近営業日」に届いていなければ KABUAI_DISCORD_WEBHOOK へ警報1通。
取得自体の失敗も警報（サイト死亡はより重い障害）。正常時は無音。
実行: CI（kabuai_watch.yml・平日12:35 JST）。手動: python freshness_watch.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))
URL = "https://aeon282499-hash.github.io/-/data/latest.json"


def is_trading_day(d: date) -> bool:
    import jpholiday
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        return False
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3):
        return False
    return True


def prev_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while not is_trading_day(p):
        p -= timedelta(days=1)
    return p


def alert(msg: str) -> None:
    hook = os.getenv("KABUAI_DISCORD_WEBHOOK", "").strip()
    print(f"[watch] ALERT: {msg}")
    if not hook:
        print("[watch] webhook未設定 → ログのみ")
        return
    body = json.dumps({"embeds": [{
        "title": "🧊 モメンタムチンパン 鮮度警報",
        "description": msg + "\n\nActions: https://github.com/aeon282499-hash/-/actions/workflows/kabuai.yml",
        "color": 0xE67E22,
    }]}).encode("utf-8")
    req = urllib.request.Request(hook, data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=15)


def main() -> int:
    today = datetime.now(JST).date()
    if not is_trading_day(today):
        print("[watch] 休場日 → スキップ")
        return 0
    expected = prev_trading_day(today)
    try:
        with urllib.request.urlopen(URL, timeout=30) as r:
            j = json.load(r)
    except Exception as e:
        alert(f"サイトのlatest.jsonが取得できません（{e}）＝Pages障害の可能性。")
        return 0
    dd = str(j.get("data_date") or "")
    print(f"[watch] data_date={dd} / 期待={expected}")
    if dd < expected.strftime("%Y-%m-%d"):
        alert(f"データが古いまま凍結しています: data_date **{dd}**（期待: {expected}）。"
              f"ビルド/デプロイの詰まりを確認してください（8/10型のqueuedゾンビはrunキャンセルで復旧）。")
    else:
        print("[watch] 鮮度OK → 無音")
    return 0


if __name__ == "__main__":
    sys.exit(main())
