# -*- coding: utf-8 -*-
"""oshime5_candidates.py — 前夜の「押し目候補」（5分足 陰線高値抜けの監視リスト）。2026-10-03 本人依頼・フェーズ1。

■ 何を出すか
  翌日の場中に「5分足の陰線高値抜け」（Downloads\\trade_bt\\RULE_15m.md）を見る銘柄を10本に絞る:
    前日までの5日騰落 −5%以下（当日終値 ÷ 5営業日前の終値 − 1）／終値 2,000〜10,000円／
    直近5日の平均売買代金が5千万円以上のうち、多い順に上位10
  発注はしない（通知・表示・紙トレード記録だけ）。場中の判定はフェーズ2（_design_oshime5.md）。

■ 検証の位置づけ（2026-10-03・trade_bt RESULTS_1003〜1003e）
  60日の5分足では5日−5%以下の銘柄が良く見えたが、同じ銘柄のランダム買いもPF1.34で、
  立花日足25年ではこの選び方の上位10を寄りで買って引けで売っても同じ日の全体と差が無い（+0.012%・t0.8）。
  ＝銘柄の優位は未確認。紙で「ルール対ランダム」を比べながら観察する用。

■ 出力
  oshime5_candidates.json（CIがコミットする想定）
  Discord: DISCORD_WEBHOOK_OSHIME5_URL（未設定なら標準出力のみ）

■ 実行
  python oshime5_candidates.py [--date YYYY-MM-DD] [--dry] [--force] [--out PATH]
  前夜配信ワークフロー(schedule_evening.yml)で arena_watchlist.py の後に走らせる想定（組み込みは本人確認後）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

from arena_watchlist import is_trading_day, next_trading_day

load_dotenv()
JST = timezone(timedelta(hours=9))
OUT_FILE = "oshime5_candidates.json"
R5_MAX = -5.0            # 5日騰落（%）の上限
PX_LO, PX_HI = 2_000, 10_000
TOV5_MIN = 5e7           # 直近5日の平均売買代金の下限（円）
TOP_N = 10


def build(sig_date: date) -> dict:
    from screener import batch_download_jquants, _jquants_id_token, fetch_tse_universe, is_etf_ticker

    token = _jquants_id_token()
    names = dict(fetch_tse_universe(token))
    data = batch_download_jquants(token, start=(sig_date - timedelta(days=20)).strftime("%Y-%m-%d"),
                                  end=sig_date.strftime("%Y-%m-%d"))
    rows, latest_seen, n_price = [], None, 0
    for tk, df in data.items():
        if df is None or len(df) < 6:
            continue
        name = names.get(tk, "")
        if not name or is_etf_ticker(tk, name):
            continue
        d0 = df.index[-1].date() if hasattr(df.index[-1], "date") else df.index[-1]
        if latest_seen is None or d0 > latest_seen:
            latest_seen = d0
        if d0 != sig_date:
            continue   # 当日の足が無い（未公開/出来ず）
        c = df["Close"].astype(float); v = df["Volume"].astype(float)
        close, c5 = float(c.iloc[-1]), float(c.iloc[-6])
        if not (close > 0 and c5 > 0) or not (PX_LO <= close <= PX_HI):
            continue
        n_price += 1
        r5 = (close / c5 - 1) * 100
        tov5 = float((c * v).iloc[-5:].mean())
        if r5 > R5_MAX or not (tov5 >= TOV5_MIN):
            continue
        rows.append({"code": tk.replace(".T", ""), "name": name, "close": close, "r5": round(r5, 2),
                     "tov5": round(tov5), "tov5_oku": round(tov5 / 1e8, 2)})
    if latest_seen != sig_date:
        raise RuntimeError(f"{sig_date} の当日足がありません（J-Quants最新={latest_seen}）")
    rows.sort(key=lambda r: -r["tov5"])
    return {"date": sig_date.strftime("%Y-%m-%d"), "target_date": next_trading_day(sig_date).strftime("%Y-%m-%d"),
            "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"),
            "rule": f"5日騰落{R5_MAX:.0f}%以下・終値{PX_LO:,}〜{PX_HI:,}円・5日平均代金{TOV5_MIN/1e8:.1f}億円以上の代金上位{TOP_N}",
            "universe_price": n_price, "matched": len(rows), "rows": rows[:TOP_N],
            "note": "発注なし・紙で観察。場中は5分足25MA上向きの押し目で陰線高値+1円の逆指値ライン（9:30〜11:29）。銘柄の優位は長期では未確認。"}


def fmt_discord(w: dict) -> str:
    lines = [f"📉 **明日の押し目候補** {w['target_date']} 分（{w['date']} 引けデータ・該当{w['matched']}銘柄から代金上位{len(w['rows'])}）",
             f"条件: {w['rule']}"]
    for i, r in enumerate(w["rows"], 1):
        lines.append(f"{i:2d}. **{r['name']}**({r['code']}) ¥{r['close']:,.0f} 5日{r['r5']:+.1f}% 5日平均代金{r['tov5_oku']:.1f}億")
    if not w["rows"]:
        lines.append("該当なし")
    lines.append("📏 9:30〜11:29に5分足25MA上向き・陰線（幅1.5%以下・安値が25MAより上）の高値+1円を抜けたら紙で記録。発注なし")
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
        json.dump(w, f, ensure_ascii=False, indent=1)
    text = fmt_discord(w)
    print(text)
    post_discord(text, a.dry)
    print(f"[oshime5] {len(w['rows'])}本 / 該当{w['matched']} / 株価帯{w['universe_price']} / {time.time()-t0:.0f}s → {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
