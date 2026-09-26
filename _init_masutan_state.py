# -*- coding: utf-8 -*-
"""_init_masutan_state.py — masutan_state.json（増担保の規制エピソード）を正確に初期化する一回きりのスクリプト（2026-09-27）。
本番(GitHub Actions)には10年の規制データ(pkl)が無いので、手元で「10年分(〜2026-07-31)を本番と同じ関数で流す
→ 2026-08-03以降はAPIで足す」をやって、状態ファイルをリポジトリに入れる。長く続いている規制の開始日を取り違えると
偽のシグナルが出るため（本番の自動初期化は250営業日遡りの保険）。
実行: python -X utf8 _init_masutan_state.py [--until YYYY-MM-DD]
"""
import argparse
from collections import defaultdict
from datetime import date
import pandas as pd
import masutan_signal as MS

ap = argparse.ArgumentParser(); ap.add_argument("--until", default="2026-09-25"); a = ap.parse_args()
A = pd.read_pickle("_margin_alert_bal_10y.pkl")
R = A[A.Restricted == 1][["PubDate", "Code", "TSEMrgnRegCls"]]
by_date = defaultdict(dict)
for r in R.itertuples():
    c = str(r.Code)
    if len(c) < 5 or c[4] == "0":
        by_date[r.PubDate][c[:4]] = str(r.TSEMrgnRegCls)
state = {"episodes": {}, "closed": [], "sent": []}
for d in sorted(A.PubDate.unique()):
    MS.apply_flags(state["episodes"], d, by_date.get(d, {}))
    MS.prune_episodes(state["episodes"], state["closed"], d)
    state["last_pubdate"] = d
print(f"pkl 〜{state['last_pubdate']}: 進行中のエピソード {len(state['episodes'])} 本 {state['episodes']}")
latest = MS.update_state(state, MS._token(), date.fromisoformat(a.until))
state["note"] = f"_init_masutan_state.py で初期化（10年pkl〜2026-07-31＋API〜{latest}）"
MS._save(MS.STATE_FILE, state)
print(f"最新の公表日 {latest}・規制中(最新の公表日に規制あり): "
      + ", ".join(f"{c}(公表{e['start']}〜・区分{e['cls']})" for c, e in sorted(state['episodes'].items()) if e['last'] == latest))
