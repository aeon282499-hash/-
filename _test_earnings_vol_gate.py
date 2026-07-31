# -*- coding: utf-8 -*-
"""_test_earnings_vol_gate.py — 決算ボラゲートの回帰テスト（2026-07-31）。

ゲートの怖いところは «買わない» 判断を増やすこと。データが壊れたときに黙って
全銘柄を止めると、決算シーズンに丸ごと機会を失う。だからフェイルオープンを
念入りに検査する（データ無し・ファイル無し・壊れたJSON・閾値None）。

実行: python -X utf8 _test_earnings_vol_gate.py
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

import main_earnings_hold as M


def reset_cache():
    M._EVOL = None
    M._EVOL_BUILT = "?"


class TestVolPass(unittest.TestCase):
    def setUp(self):
        reset_cache()
        self._min = M.EARNINGS_VOL_MIN

    def tearDown(self):
        reset_cache()
        M.EARNINGS_VOL_MIN = self._min

    def _with(self, table: dict):
        M._EVOL = table
        M._EVOL_BUILT = "2026-07-31"

    def test_above_threshold_passes(self):
        self._with({"9999.T": 5.0})
        ok, v = M.vol_pass("9999.T")
        self.assertTrue(ok)
        self.assertEqual(v, 5.0)

    def test_exactly_threshold_passes(self):
        """境界は «以上» で通す（BTの面が2.0〜4.5%の高原なので境界の向きは実害なし）。"""
        self._with({"9999.T": 2.0})
        self.assertTrue(M.vol_pass("9999.T")[0])

    def test_below_threshold_blocked(self):
        self._with({"9999.T": 1.99})
        ok, v = M.vol_pass("9999.T")
        self.assertFalse(ok)
        self.assertEqual(v, 1.99)

    def test_unknown_ticker_is_fail_open(self):
        """実績が足りない銘柄は «買う» 側に倒す。"""
        self._with({"9999.T": 5.0})
        ok, v = M.vol_pass("1234.T")
        self.assertTrue(ok)
        self.assertIsNone(v)

    def test_empty_table_is_fail_open(self):
        self._with({})
        self.assertTrue(M.vol_pass("9999.T")[0])

    def test_gate_disabled_by_none(self):
        """EARNINGS_VOL_MIN=None の1行で無効化できること。"""
        self._with({"9999.T": 0.1})
        M.EARNINGS_VOL_MIN = None
        ok, v = M.vol_pass("9999.T")
        self.assertTrue(ok)
        self.assertIsNone(v)


class TestLoad(unittest.TestCase):
    def setUp(self):
        reset_cache()

    def tearDown(self):
        reset_cache()

    def _in_tmp(self, content: str | None):
        d = tempfile.mkdtemp()
        if content is not None:
            with open(os.path.join(d, M.EARNINGS_VOL_FILE), "w", encoding="utf-8") as f:
                f.write(content)
        return mock.patch.object(M.os.path, "dirname", return_value=d)

    def test_missing_file_is_fail_open(self):
        with self._in_tmp(None):
            self.assertEqual(M.load_earnings_vol(), {})
        self.assertTrue(M.vol_pass("9999.T")[0])

    def test_broken_json_is_fail_open(self):
        with self._in_tmp("{ not json"):
            self.assertEqual(M.load_earnings_vol(), {})

    def test_valid_file(self):
        blob = json.dumps({"built": "2026-07-31",
                           "vol": {"1234.T": {"vol": 3.5, "n": 12},
                                   "5678.T": {"vol": 1.0, "n": 9}}})
        with self._in_tmp(blob):
            t = M.load_earnings_vol()
        self.assertEqual(t["1234.T"], 3.5)
        self.assertFalse(M.vol_pass("5678.T")[0])

    def test_malformed_rows_skipped_not_raised(self):
        blob = json.dumps({"built": "2026-07-31",
                           "vol": {"1.T": {"vol": None}, "2.T": "x", "3.T": {"vol": 4.0}}})
        with self._in_tmp(blob):
            t = M.load_earnings_vol()
        self.assertEqual(set(t), {"3.T"})


class TestRejectLog(unittest.TestCase):
    def test_writes_and_appends(self):
        d = tempfile.mkdtemp()
        with mock.patch.object(M.os.path, "dirname", return_value=d):
            M._log_vol_rejected("2026-07-31", [{"ticker": "1.T", "vol": 1.0}])
            M._log_vol_rejected("2026-08-01", [{"ticker": "2.T", "vol": 1.5}])
            with open(os.path.join(d, M.VOL_REJECT_FILE), encoding="utf-8") as f:
                log = json.load(f)
        self.assertEqual(sorted(log), ["2026-07-31", "2026-08-01"])
        self.assertEqual(log["2026-08-01"][0]["ticker"], "2.T")

    def test_failure_does_not_raise(self):
        """ログが書けなくても本処理を止めない。"""
        with mock.patch.object(M.os.path, "dirname", return_value="/nonexistent/zzz"):
            M._log_vol_rejected("2026-07-31", [{"ticker": "1.T", "vol": 1.0}])   # 例外が出なければ合格


class TestRealFile(unittest.TestCase):
    """同梱の earnings_vol.json が実際に使える形か。"""

    def setUp(self):
        reset_cache()

    def tearDown(self):
        reset_cache()

    def test_file_loads_and_is_sane(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), M.EARNINGS_VOL_FILE)
        if not os.path.exists(path):
            self.skipTest("earnings_vol.json 未生成")
        t = M.load_earnings_vol()
        self.assertGreater(len(t), 3000, "銘柄数が少なすぎる＝生成に失敗している疑い")
        vals = list(t.values())
        # 寄りで値が付かない極端な低流動性銘柄は 0.0 になる（実測11件）。
        # ゲートは当然弾くし、その手前の流動性フロア7.5億でも落ちるので実害はない。
        self.assertTrue(all(v >= 0 for v in vals))
        self.assertLess(sum(1 for v in vals if v == 0) / len(vals), 0.01,
                        "ゼロが1%超＝価格データの取得に失敗している疑い")
        self.assertLess(max(vals), 30.0, "異常値が混入している")
        blocked = sum(1 for v in vals if v < M.EARNINGS_VOL_MIN)
        self.assertLess(blocked / len(vals), 0.80, "8割超を弾くのは想定外＝閾値かデータの異常")
        print(f"\n  [実データ] {len(t):,}銘柄 / ゲートで弾かれる {blocked/len(vals)*100:.0f}%")

    def test_known_tickers(self):
        """BTで検証した銘柄と本番テーブルの判定が一致すること。"""
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), M.EARNINGS_VOL_FILE)
        if not os.path.exists(path):
            self.skipTest("earnings_vol.json 未生成")
        M.load_earnings_vol()
        # BT値: アンリツ5.9% / 清水建設1.1% / 滋賀銀行0.9% / 第一工業製薬1.5%
        self.assertTrue(M.vol_pass("6754.T")[0], "アンリツは通るはず")
        for tk in ("1803.T", "8366.T", "4461.T"):
            ok, v = M.vol_pass(tk)
            self.assertFalse(ok, f"{tk} は弾かれるはず（実測{v}）")


class TestIntradayGate(unittest.TestCase):
    """場中発表の除外（2026-08-01 本人指示）。"""

    def setUp(self):
        self._flag = M.EXCLUDE_INTRADAY_DISC

    def tearDown(self):
        M.EXCLUDE_INTRADAY_DISC = self._flag

    def test_after_close_passes(self):
        M.EXCLUDE_INTRADAY_DISC = True
        for t in ("15:30:00", "16:00:00", "17:00:00"):
            self.assertTrue(M.disc_time_pass(t), t)

    def test_intraday_blocked(self):
        """大引けは15:30（2024-11の東証延長）＝15:00発表も場中扱い。"""
        M.EXCLUDE_INTRADAY_DISC = True
        for t in ("15:00:00", "15:29:00", "14:00:00", "11:30:00", "08:00:00"):
            self.assertFalse(M.disc_time_pass(t), t)

    def test_no_history_is_fail_open(self):
        M.EXCLUDE_INTRADAY_DISC = True
        self.assertTrue(M.disc_time_pass(None))
        self.assertTrue(M.disc_time_pass(""))
        self.assertTrue(M.disc_time_pass("xx:yy"))

    def test_disabled_passes_everything(self):
        M.EXCLUDE_INTRADAY_DISC = False
        for t in ("14:00:00", None, "15:30:00"):
            self.assertTrue(M.disc_time_pass(t))


class TestBuildCandidatesWiring(unittest.TestCase):
    """build_candidates にゲートが正しく刺さっているか（配線の検査）。"""

    def setUp(self):
        reset_cache()
        self._min = M.EARNINGS_VOL_MIN

    def tearDown(self):
        reset_cache()
        M.EARNINGS_VOL_MIN = self._min

    def _fixture(self):
        """ルールAを確実に通る値動き（RSI低め・直前5日で下落・十分な代金）を作る。"""
        import numpy as np
        import pandas as pd
        idx = pd.date_range("2026-05-01", periods=40, freq="B")
        close = np.linspace(1200, 1000, 40)          # 一貫した下落＝RSI低・runup5マイナス
        df = pd.DataFrame({"Close": close, "Open": close,
                           "Volume": np.full(40, 3_000_000.0)}, index=idx)
        codes = [{"code": "1111", "name": "動く社", "type": "第1四半期"},
                 {"code": "2222", "name": "動かない社", "type": "第1四半期"},
                 {"code": "3333", "name": "実績なし社", "type": "第1四半期"}]
        all_data = {"1111.T": df, "2222.T": df, "3333.T": df}
        return codes, all_data

    def _run(self):
        codes, all_data = self._fixture()
        import pandas as pd

        class FakeTicker:
            def __init__(self, *_a, **_k):
                pass

            def history(self, *_a, **_k):
                return pd.DataFrame({"Close": [980.0]})

        d = tempfile.mkdtemp()
        with mock.patch.dict("sys.modules", {"yfinance": mock.Mock(Ticker=FakeTicker)}), \
             mock.patch.object(M.os.path, "dirname", return_value=d):
            return M.build_candidates(codes, all_data, {}), d

    def test_intraday_gate_wired(self):
        """前回が場中発表の銘柄が build_candidates で落ちること。"""
        M._EVOL = {"1111.T": 5.0, "2222.T": 5.0, "3333.T": 5.0}
        M._EVOL_BUILT = "2026-07-31"
        times = {"1111.T": {"2026-05-01": "16:00:00"},    # 引け後 → 通す
                 "2222.T": {"2026-05-01": "14:00:00"}}    # 場中   → 落とす
        codes, all_data = self._fixture()
        import pandas as pd

        class FakeTicker:
            def __init__(self, *_a, **_k):
                pass

            def history(self, *_a, **_k):
                return pd.DataFrame({"Close": [980.0]})

        d = tempfile.mkdtemp()
        with mock.patch.dict("sys.modules", {"yfinance": mock.Mock(Ticker=FakeTicker)}), \
             mock.patch.object(M.os.path, "dirname", return_value=d):
            out = M.build_candidates(codes, all_data, times)
        got = {r["ticker"] for r in out}
        self.assertIn("1111.T", got, "前回16:00＝引け後は通るはず")
        self.assertIn("3333.T", got, "時刻履歴なしはフェイルオープンで通るはず")
        self.assertNotIn("2222.T", got, "前回14:00＝場中は落ちるはず")
        with open(os.path.join(d, M.VOL_REJECT_FILE), encoding="utf-8") as f:
            rows = next(iter(json.load(f).values()))
        self.assertEqual(rows[0]["why"], "intraday")

    def test_gate_filters_and_logs(self):
        M._EVOL = {"1111.T": 5.0, "2222.T": 1.0}      # 3333.Tは実績なし＝フェイルオープン
        M._EVOL_BUILT = "2026-07-31"
        out, d = self._run()
        got = {r["ticker"] for r in out}
        self.assertIn("1111.T", got, "決算ボラ5.0%は通るはず")
        self.assertIn("3333.T", got, "実績なしはフェイルオープンで通るはず")
        self.assertNotIn("2222.T", got, "決算ボラ1.0%は弾かれるはず")
        self.assertEqual(out[0].get("evol"), 5.0 if out[0]["ticker"] == "1111.T" else out[0].get("evol"))
        with open(os.path.join(d, M.VOL_REJECT_FILE), encoding="utf-8") as f:
            log = json.load(f)
        rows = next(iter(log.values()))
        self.assertEqual([r["ticker"] for r in rows], ["2222.T"])

    def test_gate_off_keeps_everything(self):
        M._EVOL = {"1111.T": 5.0, "2222.T": 1.0}
        M._EVOL_BUILT = "2026-07-31"
        M.EARNINGS_VOL_MIN = None
        out, _ = self._run()
        self.assertEqual({r["ticker"] for r in out}, {"1111.T", "2222.T", "3333.T"})

    def test_no_table_keeps_everything(self):
        """テーブルが空でも1件も落とさない＝決算シーズンを丸ごと失わない。"""
        M._EVOL = {}
        M._EVOL_BUILT = "?"
        out, _ = self._run()
        self.assertEqual(len(out), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
