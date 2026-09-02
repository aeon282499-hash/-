# 01:05 JST まで待ってから全銘柄20年日足を取得（AM0-1時は前日分反映処理のため回避）
import time, datetime, subprocess, sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
target = datetime.datetime.now().replace(hour=1, minute=5, second=0, microsecond=0)
if datetime.datetime.now() > target and datetime.datetime.now().hour >= 2:
    target += datetime.timedelta(days=1)
wait = max(0, (target - datetime.datetime.now()).total_seconds())
print(f"wait {wait:.0f}s until {target}", flush=True); time.sleep(wait)
sys.exit(subprocess.call([sys.executable, "-X", "utf8", "tachibana_fetch_history.py", "--universe", "--sleep", "0.5"]))
