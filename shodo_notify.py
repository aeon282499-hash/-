# -*- coding: utf-8 -*-
"""shodo_notify.py — 大相場の前兆（初動ブレイク＋立花型出来高理論）のDiscord配信（2026-08-24）。

本人「スイングで出来高理論とか駆使しながら大相場の前兆をモメンタムチンパンアプリに配信できるといいな」。
判定はアプリ(🧭探検タブ)と同一＝新しい判定は作らない。Pagesに公開済みの explorer.json を
読んで、データ日付の新規分だけを流す（ビルドに触らない・J-Quants負荷ゼロ）。

内容:
  🚩初動ブレイク = 60日高値を30営業日ぶり初更新×陽線×出来高1.5倍（10年BT: PF1.22・
     平均+0.86%/件・陽性9/11年・勝率48.7%のバーベル型＝太陽誘電/キオクシア級の起点を拾う代わり
     半分は外れる）
  🤫静かな初動 = 立花型出来高理論（凪ATR<4%×出来高蓄積×高値ブレイク・探索用/優位性は参考水準）

重複ガード: shodo_last_send.json に配信済み data_date を記録（ワークフローがコミット）。
webhook未設定(DISCORD_WEBHOOK_SHODO_URL)は無言スキップ＝誤爆なし（友達フェードと同方式）。
実行: python -X utf8 shodo_notify.py [--dry]
"""
from __future__ import annotations

import json
import os
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL = "https://aeon282499-hash.github.io/-/data/explorer.json"
MARKER = "shodo_last_send.json"
WEBHOOK_ENV = "DISCORD_WEBHOOK_SHODO_URL"
MAX_LINES = 10


def main() -> int:
    dry = "--dry" in sys.argv
    hook = os.getenv(WEBHOOK_ENV, "").strip()
    if not hook and not dry:
        print(f"[shodo] {WEBHOOK_ENV} 未設定 → スキップ（チャンネル開設待ち）")
        return 0

    d = requests.get(URL, timeout=60, verify=False).json()
    data_date = d.get("data_date") or ""
    if not data_date:
        print("[shodo] data_date が取れない → スキップ")
        return 0

    last = {}
    if os.path.exists(MARKER):
        try:
            with open(MARKER, encoding="utf-8") as f:
                last = json.load(f)
        except Exception:
            last = {}
    if last.get("date") == data_date and not dry:
        print(f"[shodo] {data_date} は配信済み → スキップ")
        return 0

    cats = d.get("categories") or {}
    b60 = [x for x in (cats.get("break60") or []) if x.get("date") == data_date]
    nagi = [x for x in (cats.get("nagi") or []) if x.get("date") == data_date]
    b60.sort(key=lambda x: -(x.get("volx") or 0))
    nagi.sort(key=lambda x: -(x.get("volr") or 0))
    if not b60 and not nagi:
        print(f"[shodo] {data_date} は新規なし → 配信なし（マーカーだけ更新）")
        if not dry:
            with open(MARKER, "w", encoding="utf-8") as f:
                json.dump({"date": data_date}, f)
        return 0

    lines = []
    if b60:
        lines.append(f"🚩 **初動ブレイク {len(b60)}件**（10年検証済み・60日高値を30営業日ぶり初更新×出来高）")
        for x in b60[:MAX_LINES]:
            lines.append(f"・**{x.get('name','?')}** ({x.get('code','?')}) {x.get('price',0):,.0f}円 "
                         f"+{x.get('r1',0):.1f}%・出来高×{x.get('volx',0):.1f}")
        if len(b60) > MAX_LINES:
            lines.append(f"　…ほか{len(b60)-MAX_LINES}件（アプリ🧭探検タブに全件）")
        lines.append("")
    if nagi:
        lines.append(f"🤫 **静かな初動 {len(nagi)}件**（立花型出来高理論＝凪×出来高蓄積・探索用）")
        for x in nagi[:MAX_LINES]:
            lines.append(f"・{x.get('name','?')} ({x.get('code','?')}) {x.get('price',0):,.0f}円 "
                         f"出来高蓄積×{x.get('volr',0):.1f}・ATR{x.get('atr_pct',0):.1f}%")
        if len(nagi) > MAX_LINES:
            lines.append(f"　…ほか{len(nagi)-MAX_LINES}件")

    payload = {"embeds": [{
        "title": f"🚩【大相場の前兆】{data_date} EOD",
        "description": "\n".join(lines),
        "color": 0xF39C12,
        "footer": {"text": "初動ブレイク=10年PF1.22/平均+0.86%/勝率48.7%のバーベル型（半分は外れる前提の観察用）｜"
                           "EOD・1日遅れ｜買い推奨ではない・実弾は検証済みシステム優先｜判定はアプリ🧭探検と同一"},
    }]}

    if dry:
        print(json.dumps(payload, ensure_ascii=False, indent=1))
        return 0
    r = requests.post(hook, json=payload, timeout=30)
    print(f"[shodo] 配信 HTTP {r.status_code}（🚩{len(b60)}件・🤫{len(nagi)}件）")
    if 200 <= r.status_code < 300:
        with open(MARKER, "w", encoding="utf-8") as f:
            json.dump({"date": data_date}, f)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
