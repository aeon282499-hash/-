# -*- coding: utf-8 -*-
"""earnings_parity_monitor.py — 決算持ち越しの月次パリティ監視（2026-08-04新設）。

毎月1〜3日に前月のシステム帳簿実績(positions_earnings.json)をBT想定と比較してDiscordへ配信。
「BT健在なら続行・崩壊なら停止」の生存判断(feedback_bt_vs_live_divergence)を数字で回す:
  - 月次平均pnl%のzスコア = (実績平均 - BT平均) / (BTσ / √n)
  - |z|<2 なら統計的にBT想定内＝単月の赤字でも続行が正しい
  - z≤-2 は警戒。ただし単月では停止しない（連続下振れ・累積で判断）
  - 件数がBT想定月の4割未満なら配信/執行の穴を疑う⚠️

⚠️これは「システム帳簿」の成績＝シグナルの生死判定。本人の実約定とは別物
  （部分執行はテール取り逃しで帳簿と大きく乖離する: feedback_ledger_vs_user_fills）。

実行: python -X utf8 earnings_parity_monitor.py [--dry] [--force] [--month YYYY-MM]
  --dry   Discord送信せず内容表示・stateも書かない
  --force 日付ガード/月次重複ガードを無視
  --month 対象月を指定（既定=前月・指定時は日付ガード無効）
"""
from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

POSITIONS_FILE = Path("positions_earnings.json")
STATE_FILE = Path("parity_state.json")
WEBHOOK_ENV = "DISCORD_WEBHOOK_EARNINGS_URL"

# BT基準値（2026-08-04 _derive_parity_constants.py で導出・live構成:
# RSI≤55×runup<-3×代金≥7.5億×株価≤5千×ボラゲート2.0×8枠RSI昇順×PEAD+8%→5日・
# 2016-10〜2026-07の10年シム n=1,722。帳簿にない場中除外/TDnet除外の影響は10年-24万で微小）
BT_REF = {
    "mean_pct": 0.97,
    "std_pct": 9.37,
    "win_rate": 50.6,
    "n": 1722,
    # 暦月ごとの平均決済件数（決算の季節性: 2月/5月/8月/11月がピーク・6月/9月/12月は閑散）
    "monthly_n": {1: 14.6, 2: 28.6, 3: 7.5, 4: 12.7, 5: 23.7, 6: 4.0,
                  7: 9.7, 8: 26.1, 9: 4.8, 10: 16.4, 11: 23.0, 12: 5.5},
}

JST = timezone(timedelta(hours=9))


def today_jst():
    return datetime.now(JST).date()


def prev_month_ym(d) -> str:
    first = d.replace(day=1)
    last_prev = first - timedelta(days=1)
    return last_prev.strftime("%Y-%m")


def summarize_month(positions: list[dict], ym: str) -> dict:
    """対象月に決済(exit_date)された玉の帳簿集計。expiredは件数のみ・open系は現在残として別掲。"""
    closed = [p for p in positions
              if p.get("status") == "closed"
              and str(p.get("exit_date", ""))[:7] == ym
              and isinstance(p.get("pnl_pct"), (int, float))]
    expired = [p for p in positions
               if p.get("status") == "expired"
               and str(p.get("expired_date", ""))[:7] == ym]
    open_now = [p for p in positions if p.get("status") in ("pending", "extended")]
    n = len(closed)
    mean_pct = sum(p["pnl_pct"] for p in closed) / n if n else 0.0
    wins = sum(1 for p in closed if p["pnl_pct"] > 0)
    return {
        "ym": ym, "n": n,
        "mean_pct": round(mean_pct, 3),
        "win_rate": round(wins / n * 100, 1) if n else 0.0,
        "total_yen": sum(int(p.get("pnl_yen") or 0) for p in closed),
        "pead_n": sum(1 for p in closed if p.get("exit_kind") == "PEAD延長"),
        "expired_n": len(expired),
        "open_n": len(open_now),
        "open_list": [f"{p.get('name', p.get('ticker', '?'))}({p.get('status')})"
                      for p in open_now],
    }


def summarize_all(positions: list[dict]) -> dict:
    closed = [p for p in positions if p.get("status") == "closed"
              and isinstance(p.get("pnl_pct"), (int, float))]
    n = len(closed)
    wins = sum(1 for p in closed if p["pnl_pct"] > 0)
    first = min((str(p.get("date", "")) for p in closed), default="")
    return {
        "n": n,
        "mean_pct": round(sum(p["pnl_pct"] for p in closed) / n, 3) if n else 0.0,
        "win_rate": round(wins / n * 100, 1) if n else 0.0,
        "total_yen": sum(int(p.get("pnl_yen") or 0) for p in closed),
        "since": first,
    }


def judge(n: int, mean_pct: float, month: int) -> dict:
    """zスコアと判定文。判定基準はBT_REF（1件あたり分布）に対する月次平均の標準誤差。"""
    out = {"z": None, "verdict": "", "warns": []}
    bt_n = BT_REF["monthly_n"].get(month, 0.0)
    if n == 0:
        out["verdict"] = "対象月の決済なし"
        if bt_n >= 8:
            out["warns"].append(
                f"⚠️ BT想定では月{bt_n:.0f}件前後の月。配信・記帳が生きているか要確認")
        return out
    z = (mean_pct - BT_REF["mean_pct"]) / (BT_REF["std_pct"] / math.sqrt(n))
    out["z"] = round(z, 2)
    if n < 5:
        out["verdict"] = f"サンプル不足(n={n})・参考値のみ"
    elif z <= -2.0:
        out["verdict"] = ("⚠️ BT想定から-2σ超の下振れ。ただし単月では停止しない"
                          "（判断ルール: 連続下振れ・累積で監査）。翌月も-2σなら要ロジック監査")
    elif z <= -1.0:
        out["verdict"] = "想定内（やや下振れ・-1σ〜-2σ）→ 続行"
    elif z >= 2.0:
        out["verdict"] = "上振れ(+2σ)→ 続行（テールを引いた月）"
    else:
        out["verdict"] = "BT想定内 → 続行"
    if bt_n >= 8 and n < bt_n * 0.4:
        out["warns"].append(
            f"⚠️ 件数{n}件はBT想定({bt_n:.0f}件)の4割未満。配信/執行の穴 or 相場閑散を確認")
    return out


def build_embed(m: dict, j: dict, cum: dict) -> dict:
    z_txt = f"{j['z']:+.2f}" if j["z"] is not None else "—"
    color = 0x2ECC71
    if j["z"] is not None and j["z"] <= -2.0:
        color = 0xE74C3C
    elif j["z"] is not None and j["z"] <= -1.0:
        color = 0xF1C40F
    lines = [
        f"決済 **{m['n']}件**（うちPEAD延長 {m['pead_n']}件・失効 {m['expired_n']}件）",
        f"平均 **{m['mean_pct']:+.2f}%/件** ・勝率 {m['win_rate']:.0f}% ・損益 **{m['total_yen']:+,}円**",
        f"BT想定: 平均+{BT_REF['mean_pct']:.2f}%/σ{BT_REF['std_pct']:.1f}/勝率{BT_REF['win_rate']:.0f}%"
        f" → zスコア **{z_txt}**",
        f"**判定: {j['verdict']}**",
    ]
    lines += j["warns"]
    if m["open_n"]:
        lines.append(f"未決済の持ち越し {m['open_n']}件: " + "、".join(m["open_list"][:8]))
    fields = [{
        "name": f"📒 通算（{cum['since']}〜・帳簿ベース）",
        "value": (f"{cum['n']}件 / 平均{cum['mean_pct']:+.2f}% / 勝率{cum['win_rate']:.0f}%"
                  f" / **{cum['total_yen']:+,}円**"),
        "inline": False,
    }]
    return {
        "title": f"📊 決算持ち越し 月次パリティ監視 — {m['ym']}",
        "description": "\n".join(lines),
        "fields": fields,
        "color": color,
        "footer": {"text": ("システム帳簿の成績＝シグナル生死の判定用。本人の実約定とは別物。"
                            "BT基準=10年live構成n=1,722（2026-08-04導出）")},
    }


def send_discord(embed: dict, webhook: str, dry: bool = False) -> None:
    if dry:
        print("[dry] Discord送信スキップ:")
        print(json.dumps(embed, ensure_ascii=False, indent=1))
        return
    if not webhook:
        print("[warn] Webhook未設定 → 送信不可（内容はログのみ）")
        print(json.dumps(embed, ensure_ascii=False, indent=1))
        return
    import time as _time
    for attempt in range(3):
        try:
            r = requests.post(webhook, json={"embeds": [embed]}, timeout=15)
            if r.status_code in (200, 204):
                print("[discord] 送信OK")
                return
            print(f"[discord] HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[discord] attempt{attempt + 1} 失敗: {e}")
        if attempt < 2:
            _time.sleep(2 * (attempt + 1))
    print("[discord] 3回失敗 → 断念")


def main(argv: list[str]) -> int:
    dry = "--dry" in argv
    force = "--force" in argv
    ym = None
    if "--month" in argv:
        i = argv.index("--month")
        if i + 1 >= len(argv):
            print("[error] --month YYYY-MM の形式で指定")
            return 1
        ym = argv[i + 1]

    today = today_jst()
    if ym is None:
        if today.day > 3 and not force:
            print(f"[skip] {today} は月初1〜3日でない（--forceで無視可）")
            return 0
        ym = prev_month_ym(today)

    state = {}
    if STATE_FILE.exists():
        try:
            state = json.load(open(STATE_FILE, encoding="utf-8"))
        except Exception:
            state = {}
    if state.get("last_reported") == ym and not (dry or force):
        print(f"[skip] {ym} は配信済み")
        return 0

    if not POSITIONS_FILE.exists():
        print(f"[error] {POSITIONS_FILE} が無い")
        return 1
    store = json.load(open(POSITIONS_FILE, encoding="utf-8"))
    positions = store.get("positions", [])

    m = summarize_month(positions, ym)
    j = judge(m["n"], m["mean_pct"], int(ym.split("-")[1]))
    cum = summarize_all(positions)
    embed = build_embed(m, j, cum)

    print(f"[parity] {ym}: n={m['n']} 平均{m['mean_pct']:+.2f}% 勝率{m['win_rate']:.0f}% "
          f"{m['total_yen']:+,}円 z={j['z']} → {j['verdict']}")
    send_discord(embed, os.getenv(WEBHOOK_ENV, "").strip(), dry=dry)

    if not dry:
        state["last_reported"] = ym
        json.dump(state, open(STATE_FILE, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        print(f"[state] {STATE_FILE} 更新 → {ym}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
