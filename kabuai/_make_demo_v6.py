# -*- coding: utf-8 -*-
"""v6アプリのデモデータ（web/demo_fibo.json）を、立花の毎分断面の再生で作る。index.html?demo で表示される。
実行（リポジトリ直下）: python -X utf8 kabuai/_make_demo_v6.py 2026-09-30 09:50
  別フォルダのworktreeなら FIBO_DATA_ROOT=本体フォルダ を付ける"""
import json, sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fibo_daytrade as FD  # noqa: E402
import fibo_oct  # noqa: E402

day = sys.argv[1] if len(sys.argv) > 1 else "2026-09-30"
upto = sys.argv[2] if len(sys.argv) > 2 else "09:50"
rows, _ = fibo_oct.read_rows(fibo_oct.MINUTES_DIR / f"{day}.jsonl")
names = {k[:-2]: v for k, v in FD.load_name_map().items()}
ses = fibo_oct.OctSession(day, names, notify=False, log_path=None)
last = None
for r in rows:
    if r["ts"][11:16] > upto:
        break
    ses.feed_row(r); last = r["ts"]
now = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
fibo = ses.live_json(now)
payload = {"ts": last, "date": day, "hhmm": last[11:16], "state": "am" if last[11:16] < "11:30" else "pm", "fibo": fibo}
out = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).resolve().parent / "web" / "demo_fibo.json"
out.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")
from collections import Counter
print(out, f"{len(fibo['candidates'])}件", dict(Counter(c['status_jp'] for c in fibo['candidates'])), fibo["paper"])
