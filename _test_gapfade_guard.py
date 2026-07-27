# -*- coding: utf-8 -*-
"""gapfade の同日配信ガードの回帰テスト（ネットワーク不要・数秒で終わる）。

背景: 後場寄り12:30の発注に間に合わせるため、gapfade.yml の GitHub cron を4本に増やし
      さらに cron-job.org→Cloudflare Worker の外部トリガーを本命に据えた（2026-07-28）。
      多重トリガーなので「最初の1本だけ配信し、以降はスキップ」が壊れていないことを守る。
      投げ売りブースターが同じガード漏れで金曜に4連投した前例がある。

実行: python -X utf8 _test_gapfade_guard.py
"""
import json
import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

os.chdir(tempfile.mkdtemp())   # 本物の台帳・マーカーを汚さない

import gapfade  # noqa: E402

ok = fail = 0


def check(cond, label):
    global ok, fail
    if cond:
        ok += 1
        print(f"  PASS {label}")
    else:
        fail += 1
        print(f"  FAIL {label}")


print("=== 同日配信ガード ===")
check(gapfade.already_sent("2026-07-28") is False, "マーカー無し→未配信")
gapfade.mark_sent("2026-07-28")
check(gapfade.already_sent("2026-07-28") is True, "配信後→同日はスキップ")
check(gapfade.already_sent("2026-07-29") is False, "翌日はまた配信できる")
saved = json.load(open(gapfade.SEND_MARKER, encoding="utf-8"))
check(saved["date"] == "2026-07-28" and "sent_at" in saved, "マーカーに日付と時刻が入る")

with open(gapfade.SEND_MARKER, "w", encoding="utf-8") as f:
    f.write("{壊れたJSON")
check(gapfade.already_sent("2026-07-28") is False,
      "マーカー破損→フェイルオープン(黙って無配信にしない)")

print("=== notify の戻り値＝マーカーを書く条件 ===")
check(gapfade.notify("2026-07-28", [], "stats", dry=True) is False,
      "--dry では送信扱いにしない")
for _k in ("DISCORD_WEBHOOK_GAPFADE_URL", "DISCORD_WEBHOOK_DAY_URL", "DISCORD_WEBHOOK_URL_DAY"):
    os.environ.pop(_k, None)
check(gapfade.notify("2026-07-28", [], "stats", dry=False) is False,
      "webhook未設定は送信扱いにしない")


class _Resp:
    def __init__(self, code):
        self.status_code = code


class _FakeRequests:
    """import requests を差し替えて実際にPOSTしない。"""

    def __init__(self, code):
        self.code = code
        self.calls = 0

    def post(self, *a, **kw):
        self.calls += 1
        return _Resp(self.code)


os.environ["DISCORD_WEBHOOK_GAPFADE_URL"] = "https://example.invalid/hook"
for code, expect in ((204, True), (200, True), (404, False), (500, False)):
    fake = _FakeRequests(code)
    sys.modules["requests"] = fake
    got = gapfade.notify("2026-07-28", [], "stats", dry=False)
    check(got is expect and fake.calls == 1,
          f"HTTP {code} → 送信扱い={expect}（失効時は保険トリガーに再挑戦させる）")

cand = [{"ticker": "9999.T", "gap": 4.2, "open": 1500.0, "shares": 300}]
fake = _FakeRequests(204)
sys.modules["requests"] = fake
check(gapfade.notify("2026-07-28", cand, "stats", dry=False) is True,
      "候補ありでも送信できる")

print(f"\n{ok} passed / {fail} failed")
sys.exit(1 if fail else 0)
