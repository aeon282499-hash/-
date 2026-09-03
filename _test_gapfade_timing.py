# -*- coding: utf-8 -*-
"""_test_gapfade_timing.py — ギャップフェードの時刻ガード（2026-07-29）。

発端: GitHub cron が毎日3時間以上遅れて起動し、7/29は12:56に「12:30に撃て」と配信していた。
検証点:
  ①timing_state の境界（早すぎ/間に合う/手遅れ）
  ②late のときの文面に「撃て」系の指示が入らない・「見送り」と明示される
  ③ok のときは従来どおり発注指示が出る
  ④候補ゼロでも late なら遅延を告げる
"""
from __future__ import annotations

import os
import sys

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gapfade as G

PASS = FAIL = 0


def ok(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {msg}")
    else:
        FAIL += 1
        print(f"  ❌ {msg}")


def body_of(cand, state, hm):
    """notify を webhook 無し・dry で呼んで、組み立てた本文を捕まえる。"""
    import io
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        G.notify("2026-07-29", cand, "（記録なし）", dry=True, state=state, now_hm=hm)
    return buf.getvalue()


CAND = [{"ticker": "1234.T", "gap": 5.2, "open": 2000.0, "shares": 200},
        {"ticker": "5678.T", "gap": 3.8, "open": 1500.0, "shares": 300}]

print("\n■ ① timing_state の境界")
for hm, want in (("08:00", "early"), ("09:45", "early"), ("09:46", "ok"), ("11:30", "ok"),
                 ("12:20", "ok"), ("12:21", "late"), ("12:56", "late"), ("15:00", "late")):
    got = G.timing_state(hm)
    ok(got == want, f"{hm} → {got}（期待 {want}）")

print("\n■ ② late の文面には発注指示を入れない")
b = body_of(CAND, "late", "12:56")
ok("本日は見送りです" in b, "「本日は見送りです」と明示される")
ok("12:56" in b, "着弾時刻が出る")
ok("成行で空売り" not in b, "「成行で空売り」という指示が出ない")
ok("引成の返済予約" not in b, "「引成の返済予約」の手順が出ない")
ok("撃たない" in b or "参考" in b, "銘柄は参考表示に留まる")
ok("1234.T" in b, "該当銘柄は記録用に残る")

print("\n■ ③ ok の文面は従来どおり発注指示")
b = body_of(CAND, "ok", "11:30")
ok("成行で空売り" in b, "発注指示が出る")
ok("本日は見送りです" not in b, "「本日は見送りです」とは書かない")
ok("11:30" in b, "現在時刻が入る（昼休みに仕込む導線）")
ok("1234.T" in b and "5678.T" in b, "候補が並ぶ")

print("\n■ ④ 候補ゼロのとき")
b = body_of([], "ok", "11:00")
ok("該当なし" in b and "撃つ日ではありません" in b, "ok・候補ゼロ＝従来の文面")
b = body_of([], "late", "13:30")
ok("本日は見送りです" in b and "該当なし" in b,
   "late・候補ゼロ＝遅延を告げたうえで該当なしも伝える")

print("\n■ ⑤ 定数")
ok(G.EARLIEST_HM == "09:46", "当日15分足4本の確定待ちは9:46（2026-09-04）")
ok(G.DEADLINE_HM == "12:20", "締切は12:20（後場寄り12:30の10分前）")
ok(G.ENTRY_HM == "12:30", "エントリー時刻は12:30のまま（ロジック無変更）")

print("\n" + "=" * 60)
print(f"結果: {PASS}/{PASS + FAIL} 合格" + ("" if FAIL == 0 else f"  ❌ {FAIL}件 失敗"))
print("=" * 60)
sys.exit(1 if FAIL else 0)
