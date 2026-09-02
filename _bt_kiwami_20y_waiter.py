# 立花20年日足の取得(_tachibana_fetch_history_bg.py)完了を待って _bt_kiwami_20y.py --src both を実行
import os, subprocess, sys, time, re
os.chdir(os.path.dirname(os.path.abspath(__file__)))
def fetch_running():
    out = subprocess.run(["powershell","-NoProfile","-Command",
        "(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*fetch_histor*' } | Measure-Object).Count"],
        capture_output=True, text=True).stdout.strip()
    return out not in ("", "0")
t0 = time.time()
while fetch_running() and time.time() - t0 < 5 * 3600:
    time.sleep(60)
print("fetch finished; log tail:", flush=True)
print(open("_tachibana_history_fetch.log", encoding="utf-8", errors="replace").read()[-600:], flush=True)
if not os.path.exists("tachibana_history.pkl"):
    print("tachibana_history.pkl が無い → BT中止"); sys.exit(1)
with open("_log_kiwami_20y_run.txt", "w", encoding="utf-8") as f:
    rc = subprocess.call([sys.executable, "-X", "utf8", "_bt_kiwami_20y.py", "--src", "both"], stdout=f, stderr=subprocess.STDOUT)
print("BT exit", rc, flush=True)
