# -*- coding: utf-8 -*-
"""_test_sector_cap.py — 業種分散キャップ(main.SECTOR_CAP=3)の回帰テスト。
実行: python _test_sector_cap.py
"""
import sys

import main

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  OK {name}")
    else:
        FAIL += 1
        print(f"  NG {name}")


TIER = {"key": "t", "label": "テスト", "size": 1_000_000}


def cand(tk, pc=1000):
    return {"ticker": tk, "prev_close": pc}


def select(buys, sells=None, pos=None, spos=None):
    b, s = main._select_tier_signals(buys, sells or [], TIER, pos or [], spos or [], 5)
    return [c["ticker"] for c in b], [c["ticker"] for c in s]


# 業種マップをテスト用に注入（半導体5・銀行2・不明2）
main._SECTOR33 = {
    "6146.T": "電気機器", "6857.T": "電気機器", "6920.T": "電気機器",
    "6723.T": "電気機器", "8035.T": "電気機器",
    "8306.T": "銀行業", "8316.T": "銀行業",
}

print("== 業種分散キャップ ==")

# ① 同一業種はcap=3まで・4件目以降は次点(別業種)が繰り上がる
buys = [cand(t) for t in ("6146.T", "6857.T", "6920.T", "6723.T", "8035.T", "8306.T", "8316.T")]
got, _ = select(buys)
check("同一業種は3件まで＋次点繰り上げで5枠埋まる",
      got == ["6146.T", "6857.T", "6920.T", "8306.T", "8316.T"])

# ② 保有中(pending/open)の業種もカウントされる（電気機器2保有→新規は1件だけ）
pos = [{"ticker": "6501.T", "status": "open"}, {"ticker": "6702.T", "status": "pending"}]
main._SECTOR33.update({"6501.T": "電気機器", "6702.T": "電気機器"})
got, _ = select(buys, pos=pos)
check("保有中の業種を含めてcap=3（電気機器は新規1件のみ）",
      got == ["6146.T", "8306.T", "8316.T"] + [t for t in got[3:]] and got[0] == "6146.T"
      and sum(1 for t in got if main._SECTOR33.get(t) == "電気機器") == 1)

# ③ closed保有はカウントしない
pos_closed = [{"ticker": "6501.T", "status": "closed"}]
got, _ = select(buys, pos=pos_closed)
check("closedポジは業種カウント外", got == ["6146.T", "6857.T", "6920.T", "8306.T", "8316.T"])

# ④ 業種不明銘柄は相互キャップしない（4件全部通る）
buys_unk = [cand(t) for t in ("9991.T", "9992.T", "9993.T", "9994.T")]
got, _ = select(buys_unk)
check("業種不明は相互キャップなし（マップ欠損のフェイルセーフ）", got == ["9991.T", "9992.T", "9993.T", "9994.T"])

# ⑤ 集中がない日は従来と同一のtop5
buys_mix = [cand(t) for t in ("6146.T", "8306.T", "9991.T", "6857.T", "8316.T", "6920.T")]
got, _ = select(buys_mix)
check("業種分散日の選定は従来と不変", got == ["6146.T", "8306.T", "9991.T", "6857.T", "8316.T"])

# ⑥ 価格カット・保有中除外は従来どおり効く
buys_px = [cand("6146.T", pc=20000)] + [cand(t) for t in ("6857.T", "8306.T")]
got, _ = select(buys_px, pos=[{"ticker": "6857.T", "status": "open"}])
check("価格カット＋保有中除外は従来どおり", got == ["8306.T"])

# ⑦ SELLはキャップ対象外（同一業種5件そのまま）
sells = [cand(t) for t in ("6146.T", "6857.T", "6920.T", "6723.T", "8035.T")]
_, gots = select([], sells=sells)
check("SELLは業種キャップ非適用", gots == ["6146.T", "6857.T", "6920.T", "6723.T", "8035.T"])

print(f"\nRESULT: {PASS} PASS / {FAIL} FAIL")
sys.exit(1 if FAIL else 0)
