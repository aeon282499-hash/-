"""notify_discord.py — 全面リバウンド日（広がり日）のDiscord通知。

kabuai.yml の notify ジョブ（deploy後）から実行。公開済みの latest.json を読み、
rebound.mode == "broad" の時だけ通知する（散発日・通常日は何も送らない＝S/N比優先）。
朝ビルド（JST07:00）のみ発火する設計＝寄り（09:00）の2時間前に「今日が買い日」を知らせる。
夕方ビルドは同じ data_date の重複になるため通知しない（workflow 側の if で制御）。

実行: python notify_discord.py [latest.jsonのURL or ローカルパス]
環境変数: KABUAI_DISCORD_WEBHOOK（未設定なら何もせず正常終了＝非致命）
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request

LIVE_URL = "https://aeon282499-hash.github.io/-/data/latest.json"
APP_URL = "https://aeon282499-hash.github.io/-/"
TOP_N = 8


def load(src: str) -> dict:
    if src.startswith("http"):
        with urllib.request.urlopen(src, timeout=30) as r:
            return json.load(r)
    with open(src, encoding="utf-8") as f:
        return json.load(f)


def build_message(d: dict) -> dict:
    rb = d["rebound"]
    b = (rb.get("stats") or {}).get("broad") or {}
    g = (d.get("signals") or {}).get("groups", {}).get("strong_reversal") or {}
    members = list(g.get("members") or [])
    # アプリの買い候補タイブレークと同じ rsi 昇順（深い売られすぎ優先・BT裏付け）
    members.sort(key=lambda r: r.get("rsi") if r.get("rsi") is not None else 50)
    lines = []
    for r in members[:TOP_N]:
        lines.append(f"・**{r['code']} {r['name']}** ¥{r.get('price'):,} "
                     f"(1ヶ月{r.get('r20'):+.1f}% / 5日{r.get('r5'):+.1f}%)")
    more = f"\n…ほか{len(members) - TOP_N}銘柄" if len(members) > TOP_N else ""
    h = (rb.get("history") or {}).get("summary") or {}
    hist = (f"\n直近実績（2023年以降{h.get('days')}日・{h.get('trades'):,}トレード）: "
            f"勝率{h.get('win')}%・平均+{h.get('avg')}%/件" if h.get("days") else "")
    desc = (
        f"強反転が **{rb['sr_count']}件** 点灯（しきい値{rb['threshold']}件以上・データ日付 {d.get('data_date')} 終値）\n"
        f"過去検証: **勝率{b.get('win')}%・平均+{b.get('avg')}%/件**（陽性年{b.get('pos_years')}・{b.get('n', 0):,}回）{hist}\n\n"
        f"⚡ **強反転（買われすぎ度が低い順）**\n" + "\n".join(lines) + more + "\n\n"
        f"🚪 出口の目安: **保有8日めど・損切り-12%・利確なし**（翌朝寄りエントリー前提）\n"
        f"📌 1点集中より**広く分散**が過去実績では有利\n"
        f"[アプリで全買い候補を見る]({APP_URL}#/signals/buycand)\n\n"
        f"※固定ルールの過去シミュレーション（理論値・手数料/スリッページ未考慮）。投資判断は自己責任。"
    )
    return {"embeds": [{
        "title": "🌊 全面リバウンド日 — モメンタムチンパン",
        "description": desc[:4000],
        "color": 0x4DABF7,
    }]}


def main() -> int:
    hook = os.environ.get("KABUAI_DISCORD_WEBHOOK", "").strip()
    if not hook:
        print("KABUAI_DISCORD_WEBHOOK 未設定 → skip（非致命）")
        return 0
    src = sys.argv[1] if len(sys.argv) > 1 else LIVE_URL
    try:
        d = load(src)
    except Exception as e:
        print(f"latest.json 取得失敗 → skip: {e}")
        return 0
    rb = d.get("rebound") or {}
    if rb.get("mode") != "broad":
        print(f"rebound.mode={rb.get('mode')!r} → 通知なし（broadの時だけ送る）")
        return 0
    body = json.dumps(build_message(d), ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(hook, data=body,
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "kabuai-notify/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print(f"Discord通知OK: HTTP {r.status} (強反転{rb.get('sr_count')}件・{d.get('data_date')})")
    except Exception as e:
        print(f"Discord送信失敗（非致命・ビルドは成功扱い）: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
