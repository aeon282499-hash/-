"""make_track_longterm.py — ローカル jquants_cache.pkl から長期トラックレコードを事前計算。

CI(GitHub Actions) は J-Quants を直近140営業日しか取得しないため、ライブの
「ヒストリカル実績／得意保有」が約3ヶ月の薄い窓になってしまう。CI には親の
jquants_cache.pkl(227MB/gitignore)が無いので長期窓を作れない。

→ 本人の手元 pkl(数年分)で SINCE 以降の長期トラックレコードを計算し
  `track_longterm.json` にコミットする。build_data.py がビルド時にこれを
  読んで latest.json の signal_track に注入する。実績統計はゆっくりしか
  変わらないので、月1回程度この스크립트を再実行→コミットすれば十分。

  実行: python make_track_longterm.py
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from momentum import indicators
import build_data as bd
import signal_track

SINCE = "2023-01-01"          # 長期検証の開始日（約3年窓）
HERE = Path(__file__).resolve().parent


def main() -> None:
    data, _name_map, data_date, _seg = bd.load_jquants_cache()

    # build() と同じ流動性フィルタで対象銘柄を選ぶ（ETF等は親pkl=株式ユニバースに無い）
    scored: list[str] = []
    for tk, df in data.items():
        ind = indicators(df)
        if ind is None or ind["turnover"] < bd.MIN_TURNOVER:
            continue
        scored.append(tk)

    print(f"[longterm] 対象 {len(scored)} 銘柄 / SINCE {SINCE} で集計 …")
    track = signal_track.build_track(data, scored, horizons=(5, 10, 20),
                                     lookback=100_000, since=SINCE)
    track["window"] = f"{SINCE}以降の長期検証（手元pkl・{data_date}時点）"
    track["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    track["source"] = "longterm_pkl"

    out = HERE / "track_longterm.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(track, f, ensure_ascii=False, separators=(",", ":"))

    p = track["period"]
    print(f"[longterm] 出力 {out.name}: 期間 {p['from']}〜{p['to']} / {p['names']}銘柄 "
          f"/ available={track['available']}")
    for k in track["order"]:
        g = track["groups"][k]
        if g["n"] <= 0:
            continue
        h5 = g["h"].get("5") or {}
        h10 = g["h"].get("10") or {}
        h20 = g["h"].get("20") or {}
        print(f"  {g['label']:6s} n{g['n']:>7d}  "
              f"5d勝{h5.get('win','-')}%/{h5.get('avg','-')}%  "
              f"10d勝{h10.get('win','-')}%/{h10.get('avg','-')}%  "
              f"20d勝{h20.get('win','-')}%/{h20.get('avg','-')}%")


if __name__ == "__main__":
    main()
