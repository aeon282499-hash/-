# -*- coding: utf-8 -*-
"""ichi_analyze.py — grid_{kind}.pkl(ichi_engine.py の出力)を bb_sigma/bb_analyze.py と同じ物差しで評価する。
分割OOS(前半/後半)・同銘柄日×同時間帯のランダム建て200回との z・固定時刻比較・上3日/上3銘柄除外・DD。
表の「BB」列は「転換線本数/0σ」と読み替え(9=標準9-26-52、5=半分5-13-26)。「乖離」列 = 一目のシグナル定義。
使い方: python -X utf8 ichi_analyze.py 15m   → analysis_15m.md / top_15m_*.csv / survivors_15m.csv / grid_15m.csv
"""
from __future__ import annotations
import sys, os
import ichi_engine as I

I.patch()
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bb_sigma"))
import bb_analyze as A  # noqa: E402

A.HERE = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    A.main(sys.argv[1])
