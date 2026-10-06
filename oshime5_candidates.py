# -*- coding: utf-8 -*-
"""oshime5_candidates.py — 前夜の「✂️上ヒゲ刈り取り候補」（5分足 陰線高値+1ティック買いの監視リスト）。2026-10-03 本人依頼・10/4 条件確定。

■ 何を出すか
  翌日の場中に「5分足の陰線高値抜け」（Downloads\\trade_bt\\RULE_15m.md）を見る銘柄を10本に絞る:
    TOPIX500（呼値が細かく板が厚い）／前日までの5日騰落 −5%以下（当日終値 ÷ 5営業日前の終値 − 1）／
    終値 2,000〜10,000円／当日の日足が+3%以上の大陽線ではない／20日平均売買代金の多い順に上位10
  発注はしない（通知・アプリ表示だけ）。場中の判定はフェーズ2（_design_oshime5.md）。

■ 検証（2026-10-04・trade_bt RESULTS_1004.md / RESULTS_1004b.md・呼値コスト込み・前場・利確+1.5%/損切り−3%/14:45）
  この選び方: 4/24〜6/22(15分足) PF2.10・6/23〜8/12 PF1.96・8/13〜10/1 PF1.86・1回+0.26〜0.31%・1日約2.5回（計200回）。
  呼値が重い銘柄（1ティック≧株価の0.1%）は3期間ともPF0.9未満＝TOPIX500に絞る理由。前日+3%以上の陽線は3期間とも悪い。
  弱点: 回数が少ない・8〜10月は同じ10本のランダム買いもほぼ同じ・25年日足ではこの選び方だけでは日中に強くない（RESULTS_1003d/e）。

■ 出力
  oshime5_candidates.json（前夜ランがコミット → チンパン🎯前夜の準備タブ）
  Discord: DISCORD_WEBHOOK_OSHIME5_URL（未設定なら標準出力のみ）

■ 実行
  python oshime5_candidates.py [--date YYYY-MM-DD] [--dry] [--force] [--out PATH]
  前夜配信ワークフロー(schedule_evening.yml)で arena_watchlist.py の後に走る。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

from arena_watchlist import is_trading_day, next_trading_day

load_dotenv()
JST = timezone(timedelta(hours=9))
OUT_FILE = "oshime5_candidates.json"
R5_MAX = -5.0            # 5日騰落（%）の上限
PX_LO, PX_HI = 2_000, 10_000
TOV_MIN = 5e7            # 20日平均売買代金の下限（円）
OC1_MAX = 3.0            # 当日の日足（終値÷始値−1）がこれ以上の大陽線は外す（翌日の成績が3期間とも悪い）
TOP_N = 10
TOPIX500 = ("TOPIX Core30", "TOPIX Large70", "TOPIX Mid400")
ENTRY = "5分足25MA上向き（確定済みの足で判定）・1本前が陰線・その安値が25MAより上 → 陰線の高値+1ティックに逆指値の買い"
EXIT = "利確+1.5%・損切り−3%・14:45に手じまい。1銘柄1日1回"
SKIP_RULES = ["9:30までに前日比+3%以上上げた日は見送り", "朝の最初の15分足が+1%以上の大陽線なら見送り",
              "押し目の陰線の幅（高値÷安値−1）0.8%以上はパス", "入るのは9:30〜11:29・12:30以降は入らない",
              "1日の停止＝−4万円／2連敗／8回（紙の実現損で判定）"]


def tick500(px: float) -> float:
    """TOPIX500 の呼値"""
    for lim, t in ((1000, 0.1), (3000, 0.5), (10000, 1), (30000, 5), (100000, 10)):
        if px <= lim:
            return t
    return 50


def fetch_topix500(token: str) -> set[str]:
    """J-Quants /equities/master の規模区分 Core30/Large70/Mid400 の4桁コード"""
    from screener import _jquants_get, is_common_stock_code
    items = _jquants_get("/equities/master", token).get("data", [])
    return {str(r["Code"])[:4] for r in items if r.get("ScaleCat") in TOPIX500 and is_common_stock_code(r.get("Code", ""))}


def build(sig_date: date) -> dict:
    from screener import batch_download_jquants, _jquants_id_token, fetch_tse_universe, is_etf_ticker

    token = _jquants_id_token()
    names = dict(fetch_tse_universe(token))
    t500 = fetch_topix500(token)
    if len(t500) < 400:
        raise RuntimeError(f"TOPIX500の銘柄が取れない（{len(t500)}銘柄）")
    data = batch_download_jquants(token, start=(sig_date - timedelta(days=45)).strftime("%Y-%m-%d"),
                                  end=sig_date.strftime("%Y-%m-%d"))
    rows, latest_seen, n_price, n_bigup = [], None, 0, 0
    for tk, df in data.items():
        if df is None or len(df) < 21:
            continue
        name = names.get(tk, "")
        if not name or is_etf_ticker(tk, name):
            continue
        d0 = df.index[-1].date() if hasattr(df.index[-1], "date") else df.index[-1]
        if latest_seen is None or d0 > latest_seen:
            latest_seen = d0
        if d0 != sig_date:
            continue   # 当日の足が無い（未公開/出来ず）
        code = tk.replace(".T", "")
        if code not in t500:
            continue
        c = df["Close"].astype(float); o = df["Open"].astype(float); v = df["Volume"].astype(float)
        h = df["High"].astype(float); l = df["Low"].astype(float)
        close, c5, op = float(c.iloc[-1]), float(c.iloc[-6]), float(o.iloc[-1])
        if not (close > 0 and c5 > 0 and op > 0) or not (PX_LO <= close <= PX_HI):
            continue
        n_price += 1
        r5 = (close / c5 - 1) * 100
        oc1 = (close / op - 1) * 100
        tov20 = float((c * v).iloc[-20:].mean())
        if r5 > R5_MAX or not (tov20 >= TOV_MIN):
            continue
        if oc1 >= OC1_MAX:
            n_bigup += 1
            continue
        tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
        atr = float(tr.iloc[-14:].mean()) / close * 100      # 表示用（選び方には使わない＝ATRで選ぶと悪化した）
        rows.append({"code": code, "name": name, "close": close, "r5": round(r5, 2), "oc1": round(oc1, 2),
                     "tov20": round(tov20), "tov20_oku": round(tov20 / 1e8, 1), "tick": tick500(close), "atr_pct": round(atr, 2) if atr == atr else None})
    if latest_seen != sig_date:
        raise RuntimeError(f"{sig_date} の当日足がありません（J-Quants最新={latest_seen}）")
    rows.sort(key=lambda r: -r["tov20"])
    return {"date": sig_date.strftime("%Y-%m-%d"), "target_date": next_trading_day(sig_date).strftime("%Y-%m-%d"),
            "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
            "rule": (f"TOPIX500・5日騰落{R5_MAX:.0f}%以下・終値{PX_LO:,}〜{PX_HI:,}円・当日+{OC1_MAX:.0f}%以上の陽線を除く・"
                     f"20日平均代金の上位{TOP_N}"),
            "universe_price": n_price, "matched": len(rows), "skipped_bigup": n_bigup, "rows": rows[:TOP_N],
            "entry": ENTRY, "exit": EXIT, "skip_rules": SKIP_RULES,
            "backtest": "3期間（4〜6月/6〜8月/8〜10月）PF2.10/1.96/1.86・1回+0.26〜0.31%・1日約2.5回（計200回・呼値コスト込み）",
            "note": "発注はしない（通知・表示だけ）。回数が少なく、8〜10月は同じ10本をランダムな時刻に買ってもほぼ同じ成績。まず小さい量で記録を。"}


def fmt_discord(w: dict) -> str:
    lines = [f"✂️ **上ヒゲ刈り取り候補** {w['target_date']} 分（{w['date']} 引けデータ・該当{w['matched']}銘柄から代金上位{len(w['rows'])}）",
             f"条件: {w['rule']}"]
    for i, r in enumerate(w["rows"], 1):
        lines.append(f"{i:2d}. **{r['name']}**({r['code']}) ¥{r['close']:,.1f} 5日{r['r5']:+.1f}% 値幅ATR{r['atr_pct']:.1f}% 20日平均代金{r['tov20_oku']:.0f}億 呼値{r['tick']:g}円")
    if not w["rows"]:
        lines.append("該当なし")
    lines.append(f"📏 入る: {w['entry']}")
    lines.append(f"📏 出る: {w['exit']}")
    lines.append("⛔ " + " ／ ".join(w["skip_rules"]))
    return "\n".join(lines)


def post_discord(text: str, dry: bool) -> bool:
    url = os.environ.get("DISCORD_WEBHOOK_OSHIME5_URL", "").strip()
    if not url:
        print("[oshime5] DISCORD_WEBHOOK_OSHIME5_URL 未設定 → 配信スキップ（JSONのみ）")
        return False
    if dry:
        print("[oshime5] dry → 配信しない")
        return False
    r = requests.post(url, json={"content": text[:1990]}, timeout=15)
    ok = r.status_code in (200, 204)
    print(f"[oshime5] Discord {'OK' if ok else 'NG'} ({r.status_code})")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--out", default=OUT_FILE)
    a = ap.parse_args()
    sig = date.fromisoformat(a.date) if a.date else datetime.now(JST).date()
    if not is_trading_day(sig):
        print(f"[oshime5] {sig} は休場 → スキップ")
        return 0
    if os.path.exists(a.out) and not a.force:      # 前夜ランは保険で最大3回起動する → 同じ日は1回だけ送る
        try:
            if json.load(open(a.out, encoding="utf-8")).get("date") == sig.strftime("%Y-%m-%d"):
                print(f"[oshime5] {sig} 分は生成済み → スキップ（--forceで再生成）")
                return 0
        except Exception:
            pass
    t0 = time.time()
    try:
        w = build(sig)
    except Exception as e:
        print(f"[oshime5] 生成失敗: {e}")
        return 1
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(w, f, ensure_ascii=False, indent=1, allow_nan=False)   # NaN はアプリのビルドを落とすので書かない
    text = fmt_discord(w)
    print(text)
    post_discord(text, a.dry)
    print(f"[oshime5] {len(w['rows'])}本 / 該当{w['matched']} / 大陽線で除外{w['skipped_bigup']} / 株価帯{w['universe_price']} / {time.time()-t0:.0f}s → {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
