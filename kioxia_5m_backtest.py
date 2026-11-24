# -*- coding: utf-8 -*-
"""kioxia_5m_backtest.py — キオクシア(285A.T) 窓開け順張りデイトレの5分足バックテスト（2026-11-21）。

■ ルール（本人指定）
  方向     前日終値 > 前日の20日移動平均 → 買いのみ ／ 前日終値 < 20日線 → 売りのみ
  ATR      前日までの5日間の平均真の値幅（当日の値は使わない＝先読み禁止）
  入口     寄り値が前日終値から 0.3×ATR を超えて「方向と同じ側」に窓を開けたら、寄り値で成行
  損切り   寄り値から 0.3×ATR 逆行（逆指値・成行）。窓で飛び越えたらその足の始値で約定
  利確     寄り値から 1.0×ATR 順行（指値）。足の始値で既に超えていれば始値で約定（指値は有利な側で付く）
  手仕舞い どちらも未達なら大引け＝その日最後の5分足の終値。持ち越しなし
  見送り   売りで寄り値が前日終値の−10%未満（空売り価格規制）／ストップ高・安で寄らなかった日
  判定     5分足を時間順に見る。同じ足で損切りと利確の両方に触れたら損切り（保守側）
  コスト   往復0.2%（寄り値×株数×0.2%）・1回100株

■ データ
  yfinance: 日足1年（ATR・20日線・寄り値・前日終値）＋5分足60日（Yahoo の5分足は直近60日まで）
  取れない時は --daily-csv / --m5-csv で CSV を渡せる（楽天マーケットスピードII RSS の書き出し等）。
    日足CSV: Date,Open,High,Low,Close,Volume ／ 5分足CSV: Datetime,Open,High,Low,Close,Volume（日本時間・足の開始時刻）

■ 出力（--out のフォルダ。既定 results_kioxia_5m/）
  trades.csv（取引一覧）・sensitivity.csv（感度分析）・equity.png（累計損益）・summary.txt（画面表示と同じ内容）

実行: python -X utf8 kioxia_5m_backtest.py [--ticker 285A.T] [--out results_kioxia_5m] [--daily-csv d.csv --m5-csv m.csv]
"""
from __future__ import annotations

import argparse
import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ───────────────────────── 定数（ルール） ─────────────────────────
GAP_K = 0.3          # 窓の閾値 ×ATR
SL_K = 0.3           # 損切り ×ATR
TP_K = 1.0           # 利確 ×ATR
COST = 0.002         # 往復0.2%
SHARES = 100
ATR_N = 5
MA_N = 20
SHORT_RULE = -0.10   # 寄りが前日終値−10%未満なら空売り見送り
SENS_SL = (0.2, 0.3, 0.5)
SENS_TP = (0.6, 1.0, 1.5)


# ───────────────────────── データ取得 ─────────────────────────
def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance の MultiIndex 列（('Open','285A.T') など）を普通の列名にする。"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df[["Open", "High", "Low", "Close", "Volume"]].astype(float)


def load_yf(ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    import yfinance as yf
    d = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=False)
    m = yf.download(ticker, period="60d", interval="5m", progress=False, auto_adjust=False)
    if d is None or d.empty or m is None or m.empty:
        raise RuntimeError(f"yfinance から取得できませんでした（日足 {0 if d is None else len(d)} 行 / 5分足 {0 if m is None else len(m)} 行）")
    d, m = _flatten(d), _flatten(m)
    d.index = pd.to_datetime(d.index).tz_localize(None).normalize()
    mi = pd.to_datetime(m.index)
    m.index = (mi.tz_convert("Asia/Tokyo") if mi.tz is not None else mi).tz_localize(None)
    return d, m


def load_csv(daily_csv: str, m5_csv: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = pd.read_csv(daily_csv, parse_dates=[0], index_col=0)
    m = pd.read_csv(m5_csv, parse_dates=[0], index_col=0)
    d.index = pd.to_datetime(d.index).normalize()
    return d[["Open", "High", "Low", "Close", "Volume"]].astype(float), m[["Open", "High", "Low", "Close", "Volume"]].astype(float)


# ───────────────────────── 日足の下ごしらえ ─────────────────────────
def prep_daily(d: pd.DataFrame) -> pd.DataFrame:
    d = d.sort_index().copy()
    d = d[d.Close.notna()]
    pc = d.Close.shift(1)
    tr = pd.concat([d.High - d.Low, (d.High - pc).abs(), (d.Low - pc).abs()], axis=1).max(axis=1)
    d["prev_close"] = pc
    d["ma20_prev"] = d.Close.rolling(MA_N).mean().shift(1)        # 前日時点の20日線
    d["atr_prev"] = tr.rolling(ATR_N).mean().shift(1)             # 前日までの5日のTR平均（当日を含まない）
    return d


# ───────────────────────── 1日ぶんの判定 ─────────────────────────
def simulate_day(bars: pd.DataFrame, side: int, entry: float, atr: float, sl_k: float, tp_k: float) -> tuple[float, str, str]:
    """5分足を時間順に見て (決済値, 理由, 時刻) を返す。side=+1買い/-1売り。"""
    stop = entry - side * sl_k * atr
    tgt = entry + side * tp_k * atr
    for i, (ts, b) in enumerate(bars.iterrows()):
        o, h, l = b.Open, b.High, b.Low
        if i > 0:  # 最初の足の始値＝寄り値＝建値なので、窓の飛び越えは2本目以降
            if (o - stop) * side <= 0:                      # 始値で既に損切りラインを越えている＝始値で約定
                return o, "損切り(窓で飛び越え)", ts.strftime("%H:%M")
            if (o - tgt) * side >= 0:                       # 始値で既に利確ラインを越えている＝指値は始値で付く
                return o, "利確(始値)", ts.strftime("%H:%M")
        hit_sl = (l <= stop) if side > 0 else (h >= stop)
        hit_tp = (h >= tgt) if side > 0 else (l <= tgt)
        if hit_sl:                                          # 同じ足で両方なら損切り（保守側）
            return stop, "損切り" + ("(同じ足で利確にも到達)" if hit_tp else ""), ts.strftime("%H:%M")
        if hit_tp:
            return tgt, "利確", ts.strftime("%H:%M")
    last = bars.iloc[-1]
    return last.Close, "大引け", bars.index[-1].strftime("%H:%M")


def simulate_day_daily(row: pd.Series, side: int, entry: float, atr: float, sl_k: float, tp_k: float) -> tuple[float, str]:
    """日足だけで判定（比較用）: 高値/安値が届いたかだけを見る。両方届いたら損切り（保守側）。"""
    stop = entry - side * sl_k * atr
    tgt = entry + side * tp_k * atr
    hit_sl = (row.Low <= stop) if side > 0 else (row.High >= stop)
    hit_tp = (row.High >= tgt) if side > 0 else (row.Low <= tgt)
    if hit_sl:
        return stop, "損切り" + ("(両方到達)" if hit_tp else "")
    if hit_tp:
        return tgt, "利確"
    return row.Close, "大引け"


def candidates(d: pd.DataFrame, m: pd.DataFrame) -> tuple[list[dict], list[str]]:
    """エントリー条件を満たした日と、見送った日の理由。"""
    days = sorted(set(m.index.normalize()))
    out, notes = [], []
    for day in days:
        if day not in d.index:
            notes.append(f"{day:%Y-%m-%d} 日足なし→除外"); continue
        r = d.loc[day]
        bars = m[(m.index.normalize() == day)]
        bars = bars[bars[["Open", "High", "Low", "Close"]].notna().all(axis=1)]
        if not np.isfinite(r.atr_prev) or not np.isfinite(r.ma20_prev) or not np.isfinite(r.prev_close):
            continue
        # ストップ高・安で寄らなかった日（出来高0・寄り値なし・5分足に約定がない）
        if not (r.Volume > 0) or not np.isfinite(r.Open) or bars.empty or not (bars.Volume.fillna(0) > 0).any():
            notes.append(f"{day:%Y-%m-%d} 寄らず（S高/S安張り付きの可能性）→除外"); continue
        # 出来高0の気配だけの足（9:00足に多い）は判定から外す
        bars = bars[bars.Volume.fillna(0) > 0]
        side = 1 if r.prev_close > r.ma20_prev else (-1 if r.prev_close < r.ma20_prev else 0)
        if side == 0:
            continue
        gap = r.Open - r.prev_close
        if gap * side <= GAP_K * r.atr_prev:
            continue
        if side < 0 and r.Open < r.prev_close * (1 + SHORT_RULE):
            notes.append(f"{day:%Y-%m-%d} 売りだが寄りが前日終値−10%未満（空売り価格規制）→見送り"); continue
        out.append({"day": day, "side": side, "entry": float(r.Open), "atr": float(r.atr_prev),
                    "prev_close": float(r.prev_close), "ma20": float(r.ma20_prev), "bars": bars, "drow": r})
    return out, notes


def run(cands: list[dict], sl_k: float, tp_k: float, mode: str = "5m") -> pd.DataFrame:
    rows = []
    for c in cands:
        if mode == "5m":
            px, why, t = simulate_day(c["bars"], c["side"], c["entry"], c["atr"], sl_k, tp_k)
        else:
            px, why = simulate_day_daily(c["drow"], c["side"], c["entry"], c["atr"], sl_k, tp_k); t = "-"
        pnl = (px - c["entry"]) * c["side"] * SHARES - c["entry"] * SHARES * COST
        rows.append({"日付": c["day"].strftime("%Y-%m-%d"), "売買": "買い" if c["side"] > 0 else "売り",
                     "前日終値": round(c["prev_close"], 1), "20日線": round(c["ma20"], 1), "ATR": round(c["atr"], 1),
                     "寄り値": round(c["entry"], 1), "決済値": round(px, 1), "決済理由": why, "決済時刻": t, "損益(円)": round(pnl)})
    T = pd.DataFrame(rows)
    if len(T):
        T["累計(円)"] = T["損益(円)"].cumsum()
    return T


def summary(T: pd.DataFrame) -> dict:
    if T.empty:
        return {"回数": 0}
    p = T["損益(円)"]
    win, loss = p[p > 0], p[p <= 0]
    eq = p.cumsum(); dd = (eq - np.maximum.accumulate(np.r_[0, eq.values])[1:]).min()
    streak = mx = 0
    for v in p:
        streak = streak + 1 if v <= 0 else 0; mx = max(mx, streak)
    return {"回数": len(p), "勝率": f"{(p > 0).mean()*100:.1f}%", "合計損益": f"{p.sum():+,.0f}円",
            "平均利益": f"{win.mean():+,.0f}円" if len(win) else "-", "平均損失": f"{loss.mean():+,.0f}円" if len(loss) else "-",
            "PF": f"{win.sum()/-loss.sum():.2f}" if loss.sum() < 0 else ("inf" if len(win) else "-"),
            "最大DD": f"{min(dd, 0):+,.0f}円", "最大連敗": mx}


def pf_of(T: pd.DataFrame) -> float:
    if T.empty:
        return np.nan
    p = T["損益(円)"]; gl = -p[p <= 0].sum()
    return p[p > 0].sum() / gl if gl > 0 else (np.inf if (p > 0).any() else np.nan)


# ───────────────────────── メイン ─────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="285A.T")
    ap.add_argument("--out", default="results_kioxia_5m")
    ap.add_argument("--daily-csv"); ap.add_argument("--m5-csv")
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)

    try:
        d, m = load_csv(a.daily_csv, a.m5_csv) if a.daily_csv and a.m5_csv else load_yf(a.ticker)
    except Exception as e:
        print(f"⚠ データ取得に失敗: {e}")
        print("  考えられる原因: Yahoo への接続がブロック（社内/クラウドのネットワーク制限）・yfinance の仕様変更・"
              "5分足は直近60日を超えると取れない・新規上場銘柄は日足が1年分ない")
        print("  代替案: ①楽天証券マーケットスピードII の RSS（Excel の RssChart 関数で 5分足・日足を出して CSV 保存）"
              "→ --daily-csv / --m5-csv で渡す ②立花証券e支店API（日足は取れる・5分足は毎分断面を自前で貯める）"
              " ③kabuステーションAPI（auカブコム）の板/時価を貯める")
        sys.exit(1)

    buf = io.StringIO()
    with redirect_stdout(buf):
        d = prep_daily(d)
        m = m.sort_index()
        m = m[(m.index.hour * 60 + m.index.minute >= 540) & (m.index.hour * 60 + m.index.minute < 930)]   # 9:00〜15:30
        m_days = sorted(set(m.index.normalize()))
        print(f"=== キオクシア {a.ticker} 窓開け順張り 5分足バックテスト ===")
        print(f"日足 {len(d)}日（{d.index.min():%Y-%m-%d}〜{d.index.max():%Y-%m-%d}）／ 5分足 {len(m):,}本・{len(m_days)}日"
              f"（{m_days[0]:%Y-%m-%d}〜{m_days[-1]:%Y-%m-%d}）")
        print(f"ルール: 窓>{GAP_K}×ATR・損切り{SL_K}×ATR・利確{TP_K}×ATR・大引け決済・往復{COST*100:.1f}%・{SHARES}株")

        cands, notes = candidates(d, m)
        print(f"エントリー条件を満たした日: {len(cands)}日 / 5分足のある {len(m_days)}日")
        for s in notes:
            print("  " + s)

        # 1. 取引一覧
        T = run(cands, SL_K, TP_K, "5m")
        print("\n【1. 取引一覧（5分足で判定）】")
        print(T.drop(columns=["前日終値", "20日線"]).to_string(index=False) if len(T) else "  取引なし")

        # 2. 集計
        print("\n【2. 集計（5分足）】")
        for k, v in summary(T).items():
            print(f"  {k}: {v}")

        # 3. 日足だけの判定との比較
        Td = run(cands, SL_K, TP_K, "daily")
        print("\n【3. 同じ取引を日足だけで判定した場合】")
        for k, v in summary(Td).items():
            print(f"  {k}: {v}")
        if len(T):
            diff = T.merge(Td[["日付", "決済理由", "損益(円)"]], on="日付", suffixes=("_5分足", "_日足"))
            ch = diff[diff["決済理由_5分足"].str[:2] != diff["決済理由_日足"].str[:2]]
            print(f"  決済理由が変わった日: {len(ch)}日 / {len(diff)}日 ／ 合計損益の差（5分足−日足）: "
                  f"{T['損益(円)'].sum() - Td['損益(円)'].sum():+,.0f}円")
            if len(ch):
                print(ch[["日付", "売買", "決済理由_日足", "損益(円)_日足", "決済理由_5分足", "損益(円)_5分足"]].to_string(index=False))
            both = Td["決済理由"].str.contains("両方").sum()
            print(f"  日足では高値・安値の両方に届いて順番が分からない日: {both}日（日足は損切り扱い・5分足で実際の順番を確定）")
            last_vs_close = [(c["bars"].iloc[-1].Close / c["drow"].Close - 1) * 100 for c in cands]
            print(f"  参考: 最後の5分足の終値と日足の終値の差 平均{np.mean(last_vs_close):+.2f}%"
                  f"（最大{np.max(np.abs(last_vs_close)):.2f}%・Yahoo の5分足に引けの板寄せが入らない日がある）")

        # 4. 感度分析（前半・後半）
        print("\n【4. 感度分析（損切り×利確・PF と合計損益）】")
        half = len(m_days) // 2
        cut = m_days[half] if m_days else None
        c1 = [c for c in cands if c["day"] < cut]; c2 = [c for c in cands if c["day"] >= cut]
        print(f"  前半 {m_days[0]:%m/%d}〜{m_days[half-1]:%m/%d}（{len(c1)}回）／ 後半 {cut:%m/%d}〜{m_days[-1]:%m/%d}（{len(c2)}回）")
        rows = []
        for sl in SENS_SL:
            for tp in SENS_TP:
                A, B, C = run(cands, sl, tp), run(c1, sl, tp), run(c2, sl, tp)
                rows.append({"損切り×ATR": sl, "利確×ATR": tp,
                             "全体_回数": len(A), "全体_合計": A["損益(円)"].sum() if len(A) else 0, "全体_PF": pf_of(A),
                             "前半_回数": len(B), "前半_合計": B["損益(円)"].sum() if len(B) else 0, "前半_PF": pf_of(B),
                             "後半_回数": len(C), "後半_合計": C["損益(円)"].sum() if len(C) else 0, "後半_PF": pf_of(C)})
        S = pd.DataFrame(rows)
        S["前半の順位"] = S["前半_合計"].rank(ascending=False, method="min").astype(int)
        S["後半の順位"] = S["後半_合計"].rank(ascending=False, method="min").astype(int)
        print(S.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
        best = S.sort_values("前半_合計", ascending=False).iloc[0]
        print(f"  前半で一番良かった組み合わせ: 損切り{best['損切り×ATR']}×ATR・利確{best['利確×ATR']}×ATR "
              f"（前半 {best['前半_合計']:+,.0f}円）→ 後半 {best['後半_合計']:+,.0f}円・後半の順位 {int(best['後半の順位'])}/9")
        rc = S[["前半_合計", "後半_合計"]].corr(method="spearman").iloc[0, 1]
        print(f"  前半と後半の順位の相関（スピアマン）: {rc:+.2f}（+1に近いほど前半の良し悪しが後半も続く。0付近なら偶然）")
        S.to_csv(out / "sensitivity.csv", index=False, encoding="utf-8-sig")

        # 5. グラフ
        if len(T):
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(9, 4.5))
            x = pd.to_datetime(T["日付"])
            ax.plot(x, T["累計(円)"], marker="o", label="5-min bars (rule)")
            ax.plot(x, Td["損益(円)"].cumsum(), marker=".", linestyle="--", label="daily bars only")
            ax.axhline(0, color="gray", lw=0.8)
            ax.set_title(f"{a.ticker} gap-follow daytrade  SL{SL_K}xATR / TP{TP_K}xATR  (100 shares, cost 0.2%)")
            ax.set_ylabel("cumulative P&L (JPY)"); ax.legend(); ax.grid(alpha=0.3)
            fig.autofmt_xdate(); fig.tight_layout(); fig.savefig(out / "equity.png", dpi=130)
            print(f"\n【5. 累計損益のグラフ】 {out / 'equity.png'}")
        # 6. CSV
        T.to_csv(out / "trades.csv", index=False, encoding="utf-8-sig")
        Td.to_csv(out / "trades_daily_only.csv", index=False, encoding="utf-8-sig")
        print(f"【6. CSV】 {out / 'trades.csv'} ・ {out / 'trades_daily_only.csv'} ・ {out / 'sensitivity.csv'}")

    text = buf.getvalue()
    print(text)
    (out / "summary.txt").write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
