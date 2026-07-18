# -*- coding: utf-8 -*-
"""買残回転日数フィルタ（2026-07-19新設）のローカル検証（ネット不要）。

検証対象:
  screener._margin_days_cover — 回転日数計算・フェイルオープン各系
  screener.MARGIN_DAYS_COVER_MAX — 有効値であること
"""
import os
import sys
import types

import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))

try:
    import jpholiday  # noqa: F401
except ImportError:
    m = types.ModuleType("jpholiday")
    m.is_holiday = lambda d: False
    sys.modules["jpholiday"] = m

import screener as sc  # noqa: E402

results = []


def check(label, cond):
    results.append((label, bool(cond)))
    print(("OK " if cond else "NG ") + label)


# 20日間: 終値1000円・出来高10万株 → adv20=1億円/日
df = pd.DataFrame({"Close": [1000.0] * 25, "Volume": [100_000.0] * 25},
                  index=pd.date_range("2026-06-01", periods=25, freq="B"))

# 買残20万株 × 1000円 = 2億円 ÷ 1億円/日 = 2.0日
snap = {"7203": 200_000.0}
dc = sc._margin_days_cover("7203.T", df, snap)
check("回転日数の計算（20万株×1000円÷1億=2.0日）", dc is not None and abs(dc - 2.0) < 1e-6)

check("閾値0.8日超は除外対象になる値", dc > sc.MARGIN_DAYS_COVER_MAX)

snap_small = {"7203": 40_000.0}   # 4千万円÷1億=0.4日
dc2 = sc._margin_days_cover("7203.T", df, snap_small)
check("回転0.4日は閾値以下（除外されない）", dc2 is not None and dc2 < sc.MARGIN_DAYS_COVER_MAX)

check("スナップショットNone→None（フェイルオープン）",
      sc._margin_days_cover("7203.T", df, None) is None)
check("銘柄がスナップに無い→None（除外しない）",
      sc._margin_days_cover("9999.T", df, snap) is None)
check("買残ゼロ→None（除外しない）",
      sc._margin_days_cover("7203.T", df, {"7203": 0.0}) is None)

df_novol = pd.DataFrame({"Close": [1000.0] * 25, "Volume": [float("nan")] * 25},
                        index=pd.date_range("2026-06-01", periods=25, freq="B"))
check("出来高欠損→None（除外しない）",
      sc._margin_days_cover("7203.T", df_novol, snap) is None)

check("MARGIN_DAYS_COVER_MAX が有効域(0.5〜1.5日)",
      sc.MARGIN_DAYS_COVER_MAX is None or 0.5 <= sc.MARGIN_DAYS_COVER_MAX <= 1.5)

ng = [l for l, ok in results if not ok]
print("\n==== 結果: {}/{} OK ====".format(len(results) - len(ng), len(results)))
if ng:
    print("NG:", ng)
    sys.exit(1)
print("ALL PASS")
