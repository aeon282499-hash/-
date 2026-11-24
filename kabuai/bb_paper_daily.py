# -*- coding: utf-8 -*-
"""bb_paper_daily.py — 📉BB売り（GU株のBB40・2.07σ上抜け売り・前場）の紙の台帳を、大引け後に毎日つける（2026-11-23）。

なぜ大引け後か: 場中版 bb_sell_live.py は立花の毎分データを読むが、立花は 10/23 から停止中（このPCの分データも10/12で止まっている）。
  紙の記録はリアルタイムでなくてよいので、検証と「同じデータ（Yahoo 5分足）・同じ計算」で当日分を後から付ける。
  ＝ルールを固めた 11/23 より後の日だけを使う、本当の前向き検証（12月末に判定）。発注は一切しない。

■ ルール（11/23 固定・変えたら START をやり直す）
  対象   当日の寄値 ÷ 前日終値 ≥ +3%・前日の売買代金 1億以上・株価100〜10万円・決算(開示当日と翌営業日)は除く
  足     5分足（9:00の足は捨てる＝検証と同じ）・BB 40本・2.07σ（終値・単純移動平均・母集団の標準偏差＝ボリンジャー本人の定義と幅の目安）
  合図   10:00〜11:25 に始まった足の終値が ①上バンド×1.005 以上 ②寄値×1.02 以上（1銘柄1日1回・最初の合図）
  売り   合図の次の足の始値（不利1tick）
  買戻し 足の終値 ≤ 中心線（40本MA）→ 次の足の始値／戻らなければ大引け（不利1tick）
  保険   売値+7% の逆指値（なし/ありの両方を記録）
  コスト 往復0.10%
  上限   1日2回・同時に1銘柄・1日の損失−2万円（100株・保険ありで数える）。上限で見送った合図も taken=0 で記録

出力（リポジトリの bb_paper/）
  ledger.csv  1合図1行   days.csv  付けた日（合図0の日も）   summary.md  通算の成績
実行: cd kabuai && python bb_paper_daily.py   （GitHub Actions bb_paper_daily.yml が平日夜に実行）
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from bb_forward_1122 import COST_RT, is500_of, jq_daily, jq_fins, tick_of, yf_5m
from build_data import JST, _jquants_key, _recent_trading_days

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

START = os.getenv("BB_PAPER_START", "2026-11-24")   # ルールを固めた 11/23 の翌営業日から（環境変数は試運転用）
P, SIG, DEV, OPEN_UP, GU_MIN = 40, 2.07, 0.005, 0.02, 0.03
W_LO, W_HI = 10 * 60, 11 * 60 + 25        # 合図の足の開始時刻（前場だけ）
STOP = 0.07
# 制限値幅（JPX・基準値=前日終値）。11/24 本人「(a) 合図の足がストップ高の値段なら売らない」
LIMIT_TBL = [(100, 30), (200, 50), (500, 80), (700, 100), (1000, 150), (1500, 300), (2000, 400), (3000, 500), (5000, 700),
             (7000, 1000), (10000, 1500), (15000, 3000), (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
             (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000), (500000, 70000), (1e18, 100000)]


def limit_up(pc: float) -> float:
    return pc + next(w for u, w in LIMIT_TBL if pc < u)
SHARES, MAX_TRADES, MAX_LOSS_YEN = 100, 2, 20000

OUT = Path(os.getenv("BB_PAPER_OUT", str(Path(__file__).resolve().parent.parent / "bb_paper")))
LEDGER, DAYS, SUMMARY = OUT / "ledger.csv", OUT / "days.csv", OUT / "summary.md"
FIELDS = ["date", "code", "gu_pct", "signal_bar", "signal_close", "upper", "ma", "open_up_pct",
          "entry_time", "entry", "exit_time", "exit", "exit_reason", "ret_pct", "stop_hit", "ret_pct_with_stop",
          "yen_100", "yen_100_with_stop", "at_limit", "taken", "skip_reason"]


def hm(ts) -> str:
    return pd.Timestamp(ts).strftime("%H:%M")


def day_signal(code: str, s: pd.DataFrame, ds: str, earn: set) -> dict | None:
    """1銘柄・1日: 検証（bb_stop_compare_1122.py / bb_sigma207_check_1123.py）と同じ式で合図と結果を出す。"""
    s = s[(s.c > 0) & (s.o > 0)]
    tm = s.index.hour * 60 + s.index.minute
    s = s[((tm > 540) & (tm < 690)) | ((tm >= 750) & (tm < 935))]
    if len(s) < 120 or (code, ds) in earn:
        return None
    full = float(pd.Series(s.index.normalize()).value_counts().median())
    o, h, c, v = (s[k].to_numpy(float) for k in ("o", "h", "c", "v"))
    ser = pd.Series(c); is5 = is500_of(c)
    ma = ser.rolling(P).mean().to_numpy(); up = ma + SIG * ser.rolling(P).std(ddof=0).to_numpy()
    day = s.index.normalize(); tmin = (s.index.hour * 60 + s.index.minute).to_numpy()
    udays = list(day.unique())
    target = pd.Timestamp(ds)
    if target not in udays or udays.index(target) == 0:
        return None
    ix = np.flatnonzero(day == target); st, en = ix[0], ix[-1] + 1
    pix = np.flatnonzero(day == udays[udays.index(target) - 1]); pst, pen = pix[0], pix[-1] + 1
    pc = c[pen - 1]; ptv = float((c[pst:pen] * v[pst:pen]).sum())
    if not ((100 <= pc <= 100000) and ptv >= 1e8 and (pen - pst) >= 0.9 * full and o[st] / pc - 1 >= GU_MIN):
        return None
    e = next((k for k in range(st, en - 1) if W_LO <= tmin[k] <= W_HI and np.isfinite(up[k])
              and c[k] >= up[k] * (1 + DEV) and c[k] >= o[st] * (1 + OPEN_UP)), None)
    if e is None:
        return None
    ep_raw = o[e + 1]; ep = ep_raw - tick_of(ep_raw, is5)
    x_idx, x_px, x_time, why = en - 1, c[en - 1], "15:30", "大引け"
    for k in range(e + 1, en):
        if c[k] <= ma[k]:
            if k + 1 < en:
                x_idx, x_px, x_time, why = k + 1, o[k + 1], hm(s.index[k + 1]), "中心線"
            break
    px_s, hit, t_s, why_s = x_px, False, x_time, why
    lvl = ep_raw * (1 + STOP)
    for k in range(e + 1, x_idx + 1 if x_idx < en else en):
        if k > e + 1 and o[k] >= lvl:
            px_s, hit, t_s, why_s = o[k], True, hm(s.index[k]), "保険+7%"; break
        if h[k] >= lvl:
            px_s, hit, t_s, why_s = lvl, True, hm(s.index[k]), "保険+7%"; break

    def ret(px):
        return -(px + tick_of(px, is5) - ep) / ep * 100 - COST_RT

    r, rs = ret(x_px), ret(px_s)
    return {"date": ds, "code": code, "gu_pct": round((o[st] / pc - 1) * 100, 2), "signal_bar": hm(s.index[e]),
            "signal_close": c[e], "upper": round(up[e], 2), "ma": round(ma[e], 2),
            "open_up_pct": round((c[e] / o[st] - 1) * 100, 2), "entry_time": hm(s.index[e + 1]), "entry": ep_raw,
            "exit_time": x_time, "exit": x_px, "exit_reason": why, "ret_pct": round(r, 3), "stop_hit": int(hit),
            "ret_pct_with_stop": round(rs, 3), "yen_100": round(r / 100 * ep * SHARES),
            "yen_100_with_stop": round(rs / 100 * ep * SHARES), "at_limit": int(c[e] >= limit_up(pc) - 1e-9),
            "_exit_time_s": t_s, "_why_s": why_s}


def apply_limits(rows: list[dict]) -> list[dict]:
    """S高の値段の合図は売らない・1日2回・同時に1銘柄・1日の損失−2万円（保険ありで数える）。合図の早い順。"""
    rows.sort(key=lambda x: (x["signal_bar"], x["code"]))
    n, busy_until, loss = 0, "00:00", 0
    for x in rows:
        if x.get("at_limit"):
            x["taken"], x["skip_reason"] = 0, "S高の値段"
        elif n >= MAX_TRADES:
            x["taken"], x["skip_reason"] = 0, "1日2回まで"
        elif x["entry_time"] < busy_until:
            x["taken"], x["skip_reason"] = 0, "建玉あり"
        elif loss <= -MAX_LOSS_YEN:
            x["taken"], x["skip_reason"] = 0, "1日の損失上限"
        else:
            x["taken"], x["skip_reason"] = 1, ""
            n += 1; busy_until = x["_exit_time_s"]; loss += min(0, x["yen_100_with_stop"])
    return rows


def write_summary() -> None:
    if not LEDGER.exists():
        return
    L = pd.read_csv(LEDGER, dtype={"code": str})
    T = L[L.taken == 1]
    nd = len(pd.read_csv(DAYS)) if DAYS.exists() else 0

    def line(r: pd.Series, yen: pd.Series) -> str:
        if not len(r):
            return "0回"
        gp, gl = r[r > 0].sum(), -r[r <= 0].sum()
        return (f"{len(r)}回・勝率{(r > 0).mean() * 100:.0f}%・PF{(gp / gl if gl else float('inf')):.2f}・"
                f"1回{r.mean():+.2f}%・合計{yen.sum():+,.0f}円（100株）・最大負け{yen.min():+,.0f}円")

    md = [f"# 📉BB売り 紙の台帳（{START}〜・{nd}営業日・更新 {datetime.now(JST):%Y-%m-%d %H:%M}）", "",
          "ルール: GU+3%・BB40/2.07σ・前場(10:00〜11:25)・寄値+2%・中心線で買戻し・保険+7%・1日2回・100株。発注なし。", "",
          f"- 撃った玉（保険あり）: {line(T.ret_pct_with_stop, T.yen_100_with_stop)}",
          f"- 撃った玉（保険なし）: {line(T.ret_pct, T.yen_100)}",
          f"- 合図すべて（上限で見送った分も・保険あり）: {line(L.ret_pct_with_stop, L.yen_100_with_stop)}",
          "", "判定（12月末）: 撃った玉が30回前後たまって PF1.3 以上・合計プラスなら、小さく実弾を相談。届かなければ止める。"]
    SUMMARY.write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


def main() -> None:
    OUT.mkdir(exist_ok=True)
    today = datetime.now(JST).strftime("%Y-%m-%d")
    recent = _recent_trading_days(8)
    done = set(pd.read_csv(DAYS).date.astype(str)) if DAYS.exists() else set()
    targets = [d for d in recent[1:] if START <= d <= today and d not in done]   # 先頭の日は前日が無いので付けない
    if not targets:
        print(f"[bb_paper] 付ける日なし（START={START}・済={sorted(done)[-3:]}）"); write_summary(); return
    token = _jquants_key()
    D = jq_daily(token, recent).sort_values(["code", "date"])
    D["date"] = D.date.astype(str).str[:10]
    D["pc"] = D.groupby("code").AdjC.shift(1); D["pva"] = D.groupby("code").Va.shift(1); D["gu"] = D.AdjO / D.pc - 1
    have = set(D.date)
    targets = [d for d in targets if d in have]
    if not targets:
        print("[bb_paper] J-Quants に当日分がまだ無い→次の実行で付ける"); write_summary(); return
    C = D[D.date.isin(targets) & (D.gu >= 0.025) & (D.pva >= 1e8) & (D.pc >= 100) & (D.pc <= 100000)]
    earn = jq_fins(token, recent)
    dl = sorted(have); nxt = {dl[i]: dl[i + 1] for i in range(len(dl) - 1)}
    earn |= {(c, nxt[d]) for c, d in list(earn) if d in nxt}
    print(f"[bb_paper] 対象日 {targets}・候補(日足GU+2.5%) {len(C)}銘柄日", flush=True)
    M = yf_5m(set(C.code)) if len(C) else {}
    new_rows, day_rows = [], []
    for ds in targets:
        codes = set(C[C.date == ds].code)
        got = [c for c in codes if c in M and (M[c].index.normalize() == pd.Timestamp(ds)).any()]
        if codes and not got:
            print(f"[bb_paper] {ds}: Yahoo の5分足がまだ無い→次の実行で付ける"); continue
        rows = [x for c in got if (x := day_signal(c, M[c], ds, earn))]
        rows = apply_limits(rows)
        new_rows += rows
        day_rows.append({"date": ds, "candidates": len(codes), "signals": len(rows), "taken": sum(x["taken"] for x in rows)})
        print(f"[bb_paper] {ds}: 候補{len(codes)}・合図{len(rows)}・撃った{sum(x['taken'] for x in rows)}")
        for x in rows:
            print(f"   {x['code']} 合図{x['signal_bar']} 売り{x['entry_time']} {x['entry']:.1f} → {x['_exit_time_s']} {x['_why_s']} "
                  f"{x['ret_pct_with_stop']:+.2f}% {x['yen_100_with_stop']:+,}円 {'撃つ' if x['taken'] else '見送り:' + x['skip_reason']}")
    if new_rows:
        new = not LEDGER.exists()
        with LEDGER.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
            if new:
                w.writeheader()
            w.writerows(new_rows)
    if day_rows:
        new = not DAYS.exists()
        with DAYS.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["date", "candidates", "signals", "taken"])
            if new:
                w.writeheader()
            w.writerows(day_rows)
    write_summary()


if __name__ == "__main__":
    main()
