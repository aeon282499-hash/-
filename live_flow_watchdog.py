# -*- coding: utf-8 -*-
"""live_flow_watchdog.py — 🔥ライブ配信の見張り（2026-09-14 本人「エラーで止まったりしない？」）。

場中（平日 09:05〜15:30）に chimp-live ハブの updated_at が STALE_MIN 分より古ければ
  ①TachibanaLiveFlow タスクが動いていなければ起動し直す ②Discord（俺専用サーバー・極上ch）へ1通。
復旧したら「復旧」を1通。状態は live_flow/_watchdog_state.json で持ち、連投しない。
Windowsタスク: LiveFlowWatchdog 平日 09:05 から10分おき（ログオン中のみ）。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
STATE = ROOT / "live_flow" / "_watchdog_state.json"
LOG = ROOT / "logs" / "live_flow_watchdog.log"
STATUS_URL = "https://chimp-live.aeon282499.workers.dev/status"
STALE_MIN = 5
TASK = "TachibanaLiveFlow"


def log(msg: str) -> None:
    LOG.parent.mkdir(exist_ok=True)
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def webhook() -> str:
    env = {}
    p = ROOT / ".env"
    if p.exists():
        for ln in p.read_text(encoding="utf-8-sig").splitlines():
            if "=" in ln and not ln.startswith("#"):
                k, v = ln.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return os.environ.get("DISCORD_WEBHOOK_GOKUJO_URL") or env.get("DISCORD_WEBHOOK_GOKUJO_URL", "")


def post(text: str) -> None:
    url = webhook()
    if not url:
        log("webhook未設定 → ログのみ"); return
    body = json.dumps({"content": text}).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "User-Agent": "chimp-watchdog/1.0"})
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as e:  # noqa: BLE001
        log(f"Discord送信失敗: {e}")


def task_running() -> bool:
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-Command", f"(Get-ScheduledTask -TaskName '{TASK}').State"], creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                           capture_output=True, text=True, timeout=30)
        return "Running" in r.stdout
    except Exception:  # noqa: BLE001
        return False


def start_task() -> None:
    subprocess.run(["powershell", "-NoProfile", "-Command", f"Start-ScheduledTask -TaskName '{TASK}'"], timeout=30, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))


def main() -> int:
    now = datetime.now()
    hm = now.strftime("%H:%M")
    if now.weekday() >= 5 or not ("09:05" <= hm <= "15:30"):
        return 0
    st = {}
    try:
        st = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
    except Exception:  # noqa: BLE001
        st = {}
    age_min = None
    try:
        req = urllib.request.Request(STATUS_URL, headers={"User-Agent": "chimp-watchdog/1.0"})
        j = json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8"))
        ua = j.get("updated_at")
        if ua:
            t = datetime.fromisoformat(ua.replace("Z", "+00:00"))
            age_min = (datetime.now(timezone.utc) - t).total_seconds() / 60
    except Exception as e:  # noqa: BLE001
        log(f"status取得失敗: {e}")
    stale = age_min is None or age_min > STALE_MIN
    running = task_running()
    log(f"age={None if age_min is None else round(age_min,1)}分 task_running={running} stale={stale}")
    if stale and not running:
        log(f"{TASK} が動いていない → 起動")
        start_task()
    if stale and not st.get("alerted"):
        post(f"⚠️ 🔥ライブ配信が止まっています（最終更新 {('%.0f分前' % age_min) if age_min is not None else '取得不能'}・"
             f"巡回タスク{'稼働中' if running else '停止→再起動を試行'}）。PCログオン/立花ログイン/ネットを確認。{hm}")
        st["alerted"] = True
    elif not stale and st.get("alerted"):
        post(f"✅ 🔥ライブ配信が復旧しました（最終更新 {age_min:.0f}分前）{hm}")
        st["alerted"] = False
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(st), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
