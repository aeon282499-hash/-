# -*- coding: utf-8 -*-
"""make_picks_scoreboard.py — ④スコアボードの「手元pklから長期版を事前計算→コミット」用。

本番CIは build_data.py が picks_scoreboard.compute() で毎日 fresh 計算する。これは
そのフォールバック＆手元での確認用（picks_scoreboard.json をコミットしておくと、
fresh計算が失敗した時に build_data が拾う）。計算ロジックは picks_scoreboard.compute に一元化。

実行: python make_picks_scoreboard.py
"""
import json
import os
import pickle
import sys

from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # 親dir(screener)
load_dotenv()
from screener import is_etf_ticker  # noqa: E402
import picks_scoreboard as psb  # noqa: E402


def main():
    c = pickle.load(open(os.path.join(os.path.dirname(HERE), "jquants_cache.pkl"), "rb"))
    try:
        secraw = json.load(open(os.path.join(os.path.dirname(HERE), "sector33_map.json"), encoding="utf-8"))
        SEC = {str(k)[:4]: v for k, v in secraw.items()}
    except Exception:
        SEC = {}
    out = psb.compute(c["all_data"], c.get("name_map", {}), SEC, str(c.get("end", "")),
                      skip=is_etf_ticker)   # 手元pklはETF混在→is_etf_tickerで除外
    if not out:
        print("[scoreboard] 発動なし → スキップ")
        return
    path = os.path.join(HERE, "picks_scoreboard.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    rc = out["recent"]
    print(f"[scoreboard] {path}")
    print(f"  as_of {out['as_of']} / 直近{out['recent_weeks']}週: {rc['n']}件 "
          f"勝率{rc['win']}% 平均{rc['avg']:+}% → regime={out['regime']}")
    for m in out["months"]:
        print(f"    {m['m']}: {m['n']}件 勝率{m['win']}% 平均{m['avg']:+}%")
    for b in out["by_signal"]:
        print(f"    {b['label']}: {b['n']}件 勝率{b['win']}% 平均{b['avg']:+}%")


if __name__ == "__main__":
    main()
