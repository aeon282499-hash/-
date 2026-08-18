# -*- coding: utf-8 -*-
"""_test_earnings_jpx_filter.py — 決算「前」除外にJPX公式予定表を使う修正の回帰テスト（2026-07-30）。

背景: 決算±3日除外は2026-05-21から本番稼働しているが、未来日は
predict_next_earnings.py の「直近4回の平均間隔」推定に頼っており、実測すると
翌営業日発表262社のうち16%しか捕捉できていなかった（＝「決算後」しか効いていない）。
実害: 7/30朝に太陽ホールディングス(4626)を配信 → 翌7/31が1Q発表で決算跨ぎ保有。
修正: 決算持ち越しシグナルが毎日更新している jpx_earnings_schedule.json を共用する。

実行: python -X utf8 _test_earnings_jpx_filter.py
"""
from __future__ import annotations

import datetime
import json
import os
import tempfile
import unittest
from unittest import mock

import screener

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEDULE_PATH = os.path.join(HERE, "jpx_earnings_schedule.json")


class TestExpandWindow(unittest.TestCase):
    def test_pm3_calendar_days(self):
        w = screener._expand_window(["2026-07-31"], 3)
        self.assertEqual(len(w), 7)
        self.assertIn("2026-07-28", w)
        self.assertIn("2026-08-03", w)
        self.assertNotIn("2026-07-27", w)
        self.assertNotIn("2026-08-04", w)

    def test_bad_dates_are_dropped_not_raised(self):
        w = screener._expand_window(["2026-07-31", "", None, "xx", 20260731], 1)
        self.assertEqual(w, {"2026-07-30", "2026-07-31", "2026-08-01"})

    def test_empty(self):
        self.assertEqual(screener._expand_window([], 3), set())


class TestLoadJpxSchedule(unittest.TestCase):
    def _with_schedule(self, blob):
        """一時ファイルを screener と同じ場所に見せかけて _load_jpx_schedule を回す。"""
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "jpx_earnings_schedule.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False)
        with mock.patch.object(screener.os.path, "dirname", return_value=tmpdir):
            return screener._load_jpx_schedule(3)

    def test_code_to_ticker(self):
        out = self._with_schedule({
            "fetched": "2026-07-30",
            "schedule": {"2026-07-31": [{"code": "4626", "name": "太陽HD", "type": "第1四半期"}]},
        })
        self.assertIn("4626.T", out)
        self.assertIn("2026-07-30", out["4626.T"])

    def test_alnum_code_kept(self):
        """新規上場の英字混じりコード(192A等)も落とさない。"""
        out = self._with_schedule({
            "fetched": "2026-07-30",
            "schedule": {"2026-08-05": [{"code": "192A", "name": "テスト", "type": "本決算"}]},
        })
        self.assertIn("192A.T", out)

    def test_multiple_dates_merged_per_ticker(self):
        out = self._with_schedule({
            "fetched": "2026-07-30",
            "schedule": {
                "2026-07-31": [{"code": "1111"}],
                "2026-11-05": [{"code": "1111"}],
            },
        })
        self.assertIn("2026-07-31", out["1111.T"])
        self.assertIn("2026-11-05", out["1111.T"])

    def test_garbage_rows_skipped(self):
        out = self._with_schedule({
            "fetched": "2026-07-30",
            "schedule": {"2026-07-31": [{"code": ""}, {}, None, {"code": "2222"}]},
        })
        self.assertEqual(set(out), {"2222.T"})

    def test_missing_file_is_fail_open(self):
        tmpdir = tempfile.mkdtemp()
        with mock.patch.object(screener.os.path, "dirname", return_value=tmpdir):
            self.assertEqual(screener._load_jpx_schedule(3), {})

    def test_broken_json_is_fail_open(self):
        tmpdir = tempfile.mkdtemp()
        with open(os.path.join(tmpdir, "jpx_earnings_schedule.json"), "w", encoding="utf-8") as f:
            f.write("{ not json")
        with mock.patch.object(screener.os.path, "dirname", return_value=tmpdir):
            self.assertEqual(screener._load_jpx_schedule(3), {})


class TestMergedCalendar(unittest.TestCase):
    """本物の同梱データで、実際に踏んだ取りこぼしが塞がったかを見る。"""

    @classmethod
    def setUpClass(cls):
        screener._load_earnings_calendar()
        cls.excl = screener._EARNINGS_EXCLUDED_DATES
        cls.have_schedule = os.path.exists(SCHEDULE_PATH)

    def test_live_miss_is_now_excluded(self):
        """7/30の実害ケース: 太陽HD(4626)は翌7/31が1Q → 7/30のBUYは除外されるべき。"""
        if not self.have_schedule:
            self.skipTest("jpx_earnings_schedule.json 未取得")
        sched = json.load(open(SCHEDULE_PATH, encoding="utf-8"))["schedule"]
        if not any(r.get("code") == "4626" for r in sched.get("2026-07-31", [])):
            self.skipTest("予定表が更新されて7/31の4626が消えている（時間経過による正常な変化）")
        self.assertTrue(screener._is_near_earnings("4626.T", "2026-07-30"))

    def test_history_still_covers_post_earnings(self):
        """過去実績側（決算後の除外）が壊れていないこと。"""
        cal = json.load(open(os.path.join(HERE, "earnings_calendar.json"), encoding="utf-8"))
        ticker, dates = next((t, d) for t, d in cal.items() if d)
        self.assertTrue(screener._is_near_earnings(ticker, sorted(dates)[0]))

    def test_unrelated_date_not_excluded(self):
        """全銘柄が常に除外される、みたいな壊れ方をしていないこと。"""
        n = sum(1 for t, s in self.excl.items() if "2026-06-18" in s)
        self.assertLess(n, len(self.excl) * 0.30, "除外が広すぎる（凪の日に3割超は異常）")

    def test_coverage_improved_vs_history_only(self):
        """JPXマージで『翌営業日発表』の捕捉が大幅に増えることを同梱データだけで確認。"""
        if not self.have_schedule:
            self.skipTest("jpx_earnings_schedule.json 未取得")
        sched = json.load(open(SCHEDULE_PATH, encoding="utf-8"))["schedule"]
        # 「今日以降」の予定日で測る（2026-08-18修正: 旧 '2026-07-31' 固定はその日付が
        # 過去に回った時点で実績側も100%捕捉になり assertGreater が時限式に落ちていた）
        today_s = datetime.date.today().isoformat()
        future = sorted(d for d in sched if d >= today_s)
        if not future:
            self.skipTest("予定表に未来日が無い")
        target = future[0]
        codes = [f"{r['code']}.T" for r in sched[target] if r.get("code")]

        hist_only = {}
        cal = json.load(open(os.path.join(HERE, "earnings_calendar.json"), encoding="utf-8"))
        for t, ds in cal.items():
            hist_only[t] = screener._expand_window(ds, 3)

        hit_hist = sum(1 for c in codes if target in hist_only.get(c, set()))
        hit_merged = sum(1 for c in codes if target in self.excl.get(c, set()))
        print(f"\n  [coverage] {target}発表 {len(codes)}社 → 実績のみ {hit_hist}社 "
              f"({hit_hist/len(codes)*100:.0f}%) / JPXマージ後 {hit_merged}社 "
              f"({hit_merged/len(codes)*100:.0f}%)")
        self.assertEqual(hit_merged, len(codes), "予定表にある銘柄は全部除外されるはず")
        # 実績側が偶然±3日窓で拾う銘柄はあり得るので「マージで減らない」を固定（>=）
        self.assertGreaterEqual(hit_merged, hit_hist)


if __name__ == "__main__":
    unittest.main(verbosity=2)
