# -*- coding: utf-8 -*-
"""_bt_fade_rebase100.py — 土台変更(1玉100万×1番・2026-08-05)後の全軸再検証（2026-08-06）。

7/31の教訓「土台を1つ変えたら過去の採否は全部やり直す」の実行。
これまでの採否は全部 1玉50万×上位2本 の土台の測定だった。8/5に
  ・1玉 50万→100万（株数丸めが変わる）
  ・値がさカット 5,000円→10,000円（候補プールが広がる）
  ・8月の実弾は「1番だけ」（n=2→実質n=1）
と土台が3つ同時に変わったので、採用済み・棄却済みの全軸を新土台で測り直す。

ベースライン = 前日+7% × 貸借○ × 出来高6倍未満 × ATR5%以上 × 乖離12%以上
             × 代金3億 × 張り付き除外(レンジ>5%) × 乖離+ATR順の1番だけ × 1玉100万
             × 寄成売り→引成買戻し

新軸: 前夜の米株（SPY・発注時点で既知＝ex-ante成立する唯一の未検証コンテキスト）
採用条件: 両期間（2016-21/2022-26）とも改善 かつ 勝ち年を減らさない かつ 近傍が高原。
実行: python -X utf8 _bt_fade_rebase100.py
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

SIZE = 1_000_000
YEARS = list(range(2016, 2027))
P = pd.read_pickle("_fade_pool_v5_100.pkl")
P["ym"] = P.ent.str[:7]

# ── 前夜の米株（SPY）。Stooqの日足CSV（キー不要）を一度だけ落としてローカル保存。
#    米国セッション(d0の米国日付)は翌朝6時JSTに引けている＝8:20の発注判断で既知＝ex-ante成立。
SPY_CSV = "_spy_daily_stooq.csv"
spy_last: dict[str, float] = {}
try:
    if not os.path.exists(SPY_CSV):
        import requests
        r = requests.get("https://stooq.com/q/d/l/?s=spy.us&i=d", timeout=30)
        r.raise_for_status()
        open(SPY_CSV, "w", encoding="utf-8").write(r.text)
    sp = pd.read_csv(SPY_CSV, parse_dates=["Date"])
    sp = sp[(sp.Date >= "2015-12-01")].sort_values("Date")
    sp["ret"] = sp.Close.pct_change() * 100
    # sig日d0に対し「日付<=d0の最後の米国リターン」= 翌朝の寄り前に判明している最新の夜
    ser = sp.set_index("Date").ret.dropna()
    all_days = pd.date_range("2016-01-01", "2026-12-31")
    spy_ff = ser.reindex(all_days).ffill()
    spy_last = {d.strftime("%Y-%m-%d"): float(v) for d, v in spy_ff.items() if np.isfinite(v)}
    print(f"[spy] {len(ser)}営業日 ({ser.index[0].date()}〜{ser.index[-1].date()})", flush=True)
except Exception as e:
    print(f"[spy] 取得失敗 → 米株軸はスキップ: {e}", flush=True)
P["spy"] = P.sig.map(spy_last) if spy_last else np.nan


def run(gain=7.0, vr=6.0, devmax=999.0, atr=5.0, devmin=12.0, tov=3e8, sticky=5.0,
        n=1, sort=("dev", "atr"), pxmax=None, pxmin=None, gu=None, earn=None,
        dow_skip=None, nk=None, cool=None, volavg=100_000,
        stop=None, tp=None, extra=None):
    d = P
    if extra is not None:
        d = d[extra(d)]
    d = d[(d.gain >= gain) & (d.vr < vr) & (d.dev < devmax) & (d.atr >= atr)
          & (d.dev >= devmin) & (d.tov >= tov) & (d.rng > sticky) & (d.vol_avg >= volavg)]
    if pxmax is not None:
        d = d[d.px <= pxmax]
    if pxmin is not None:
        d = d[d.px >= pxmin]
    if gu is not None:
        d = d[d.gu >= gu]
    if earn is False:
        d = d[~d.earn5]
    if dow_skip is not None:
        d = d[~d.dow.isin(dow_skip)]
    if nk is not None:
        d = d[d.nk_below == nk]
    d = d.copy()
    r = None
    for col in sort:
        asc = col.startswith("-")
        x = d.groupby("sig")[col.lstrip("-")].rank(ascending=asc, pct=True)
        r = x if r is None else r + x
    d["mix"] = r / len(sort)
    d = d.sort_values(["sig", "mix", "ticker"], kind="stable")
    d["rk"] = d.groupby("sig").cumcount() + 1
    d = d[d.rk <= n].copy()
    if cool:
        last: dict[str, str] = {}
        keep = []
        for row_ in d.sort_values("sig").itertuples():
            prev = last.get(row_.ticker)
            if prev is None or (pd.Timestamp(row_.sig) - pd.Timestamp(prev)).days > cool:
                keep.append(row_.Index); last[row_.ticker] = row_.sig
        d = d.loc[keep]
    d["sh"] = (SIZE / d.px // 100 * 100).astype(int)
    d = d[d.sh > 0].copy()
    if stop is not None or tp is not None:      # 日中OCO（保守＝両方触ったらSTOP勝ち）
        o, h, l, c = d.o1.values, d.h1.values, d.l1.values, d.c1.values
        pnl = (o - c) / o * 100
        hs = h >= o * (1 + stop / 100) if stop is not None else np.zeros(len(d), bool)
        ht = l <= o * (1 - tp / 100) if tp is not None else np.zeros(len(d), bool)
        pnl = np.where(ht & ~hs, tp if tp is not None else 0.0, pnl)
        pnl = np.where(hs, -stop if stop is not None else 0.0, pnl)
        d["pnl"] = pnl
    d["yen"] = d.pnl / 100 * d.sh * d.o1
    return d


def st(d):
    yr = d.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    mm = d.groupby("ym").yen.sum()
    p = d.pnl; loss = -p[p < 0].sum()
    return dict(n=len(d), days=d.sig.nunique(), wr=(p > 0).mean() * 100,
                pf=(p[p > 0].sum() / loss) if loss > 0 else np.inf,
                tot=d.yen.sum(), avg=d.yen.sum() / 11, win=int((yr > 0).sum()),
                worst=yr.min(), wm=mm.min(), w1=d.yen.min() if len(d) else 0.0,
                a=float(yr[yr.index <= 2021].sum()), b=float(yr[yr.index >= 2022].sum()))


BASE = st(run())
print(f"[base] 100万×1番: {BASE['n']}玉 撃つ日{BASE['days']} 勝率{BASE['wr']:.1f}% PF{BASE['pf']:.2f} "
      f"10年{BASE['tot']:+,.0f}円 勝ち{BASE['win']}/11 前半{BASE['a']:+,.0f} 後半{BASE['b']:+,.0f} "
      f"最悪月{BASE['wm']:+,.0f} 最悪1玉{BASE['w1']:+,.0f}\n", flush=True)

HDR = (f"  {'設定':<26}{'玉数':>6}{'撃つ日':>7}{'勝率':>7}{'PF':>6}{'10年計':>12}{'勝ち':>6}"
       f"{'最悪月':>11}{'最悪1玉':>10}{'前半':>12}{'後半':>12}{'判定':>12}")


def row(lab, s):
    ok = s["a"] > BASE["a"] and s["b"] > BASE["b"] and s["win"] >= BASE["win"]
    mk = "★両期間改善" if ok else ("片側" if s["tot"] > BASE["tot"] else "")
    print(f"  {lab:<26}{s['n']:>6}{s['days']:>7}{s['wr']:>6.1f}%{s['pf']:>6.2f}{s['tot']:>+11,.0f}円"
          f"{s['win']:>4}/11{s['wm']:>+10,.0f}円{s['w1']:>+9,.0f}円{s['a']:>+11,.0f}円{s['b']:>+11,.0f}円{mk:>14}")


def sec(t):
    print("\n" + "=" * 138); print(t); print("=" * 138); print(HDR)
    row("現行(100万×1番)", BASE)


# ═══════════ A. 採用済み・棄却済み全軸を新土台で再測定 ═══════════
sec("A1. 本数（8月=1番だけ・9月に2本目判断。各100万）")
for k in (1, 2, 3):
    row(f"上位{k}本", st(run(n=k)))

sec("A2. GO閾値（現行+7%）")
for g in (6, 6.5, 7, 7.5, 8, 9, 10):
    row(f"前日+{g}%以上", st(run(gain=g)))

sec("A3. 出来高比の上限（現行6倍未満）")
for v in (4, 5, 6, 8, 12, 999):
    row("上限なし" if v == 999 else f"{v}倍未満", st(run(vr=v)))

sec("A4. 25MA乖離の上限（現行なし＝7/31撤廃）")
for v in (40, 60, 80, 999):
    row("上限なし(現行)" if v == 999 else f"{v}%未満", st(run(devmax=v)))

sec("A5. 売買代金フロア（現行3億）")
for v in (2e8, 3e8, 5e8, 1e9):
    row(f"{v/1e8:.0f}億以上", st(run(tov=v)))

sec("A6. 張り付き除外の閾値（現行レンジ5%超・8/1に高原の頂上と確定済み）")
for v in (3, 4, 5, 6, 8):
    row(f"レンジ{v}%超", st(run(sticky=v)))

sec("A7. ATR下限（現行5%）")
for v in (4, 4.5, 5, 5.5, 6, 7):
    row(f"ATR{v}%以上", st(run(atr=v)))

sec("A8. 25MA乖離下限（現行12%）")
for v in (8, 10, 12, 14, 16, 18):
    row(f"乖離{v}%以上", st(run(devmin=v)))

sec("A9. 並び順（現行 乖離+ATR）")
for s_, lab in [(("dev",), "乖離だけ"), (("atr",), "ATRだけ"), (("gain",), "前日騰落だけ"),
                (("rng",), "レンジだけ"), (("dev5",), "5MA乖離だけ"),
                (("dev", "atr", "gain"), "乖離+ATR+騰落"), (("dev", "atr", "rng"), "乖離+ATR+レンジ"),
                (("dev", "atr", "dev5"), "乖離+ATR+5MA乖離"), (("dev", "atr", "pos"), "乖離+ATR+終値位置"),
                (("dev", "atr", "-tov"), "乖離+ATR+代金小さい順"), (("dev", "rng"), "乖離+レンジ"),
                (("atr", "rng"), "ATR+レンジ")]:
    row(lab, st(run(sort=s_)))

sec("A10. 曜日を落とす")
for i, nmm in enumerate(["月", "火", "水", "木", "金"]):
    row(f"{nmm}曜を撃たない", st(run(dow_skip=[i])))

sec("A11. 地合い（前日の日経・ex-ante成立）")
row("日経25MA以上の日だけ", st(run(nk=False)))
row("日経25MA以下の日だけ", st(run(nk=True)))
for lo, hi, lab in ((-99, 0, "前日日経マイナスの日だけ"), (0, 99, "前日日経プラスの日だけ"),
                    (-99, -1, "前日日経-1%超下げだけ"), (1, 99, "前日日経+1%超上げだけ")):
    row(lab, st(run(extra=lambda d, lo=lo, hi=hi: (d.nk_chg >= lo) & (d.nk_chg < hi))))

sec("A12. 日中OCO（損切りだけ/利確だけ）")
for s_ in (3, 5, 8, 10, 12):
    row(f"損切り +{s_}%", st(run(stop=s_)))
for t_ in (2, 3, 5, 8, 10):
    row(f"利確 -{t_}%", st(run(tp=t_)))

sec("A13. 同一銘柄のクールダウン")
for c_ in (3, 5, 10, 20):
    row(f"{c_}暦日あける", st(run(cool=c_)))

sec("A14. 寄りギャップ下限（寄指に戻す軸・現行は成行=下限なし）")
for g in (-1, 0, 0.5, 1):
    row(f"GU{g:+.1f}%以上", st(run(gu=g)))

sec("A15. 値がさ帯（8/5に上限5,000→10,000円へ拡大＝拡大分の寄与）")
row("〜5,000円(旧上限)", st(run(pxmax=5000)))
row("〜10,000円(現行)", BASE)
row("5,000〜10,000円だけ", st(run(pxmin=5000)))

sec("A16. 決算またぎ（直近5営業日に決算）")
row("決算5日内を除外", st(run(earn=False)))
row("決算5日内だけ", st(run(extra=lambda d: d.earn5)))

sec("A17. 単変量の下限（過去棄却軸の新土台確認）")
for col, v, lab in (("pos", 70, "終値位置70%以上"), ("pos", 50, "終値位置50%以上"),
                    ("body", 50, "実体比50%以上"), ("up_days", 3, "5日中3日以上上げ"),
                    ("prev_up", 0, "前々日もプラス"), ("gain2", 15, "2日騰落15%以上")):
    row(lab, st(run(extra=lambda d, c=col, x=v: d[c] >= x)))

# ═══════════ B. 新軸 ═══════════
if spy_last:
    sec("B1. 前夜の米株SPY（発注時点で既知・未検証の最後のコンテキスト軸）")
    for lo, hi, lab in ((-99, 0, "前夜SPYマイナスだけ"), (0, 99, "前夜SPYプラスだけ"),
                        (-99, -1, "前夜SPY-1%超下げだけ"), (1, 99, "前夜SPY+1%超上げだけ"),
                        (-1, 99, "前夜SPY-1%以上(暴落夜だけ回避)",), (-2, 99, "前夜SPY-2%以上")):
        row(lab, st(run(extra=lambda d, lo=lo, hi=hi: (d.spy >= lo) & (d.spy < hi))))
    b = run()
    print("\n  [層別] 現行1番玉を前夜SPYで層別（ゲートでなく構造の確認）")
    for lo, hi in ((-99, -2), (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 99)):
        s = b[(b.spy >= lo) & (b.spy < hi)]
        if len(s) < 5:
            continue
        p = s.pnl; loss = -p[p < 0].sum()
        pf = p[p > 0].sum() / loss if loss > 0 else np.inf
        print(f"    SPY {lo:+.0f}〜{hi:+.0f}%: {len(s):>4}玉 勝率{(p>0).mean()*100:>5.1f}% "
              f"PF{pf:>5.2f} 計{s.yen.sum()/1e4:>+8,.0f}万")

sec("B2. 順位の単独実力（9月の2本目判断の予習・各100万）")
for k in (1, 2, 3):
    d = run(n=3)
    s = st(d[d.rk == k])
    row(f"{k}番だけ単独", s)

# ═══════════ C. ベースラインの頑健性 ═══════════
print("\n" + "=" * 138)
print("C. ベースラインの頑健性（bt-4検査）")
print("=" * 138)
b = run()
for lab, dd in (("上位3日を除去", b[~b.sig.isin(b.groupby("sig").yen.sum().nlargest(3).index)]),
                ("上位3銘柄を除去", b[~b.ticker.isin(b.groupby("ticker").yen.sum().nlargest(3).index)])):
    s = st(dd)
    print(f"  {lab}: 10年{s['tot']:+,.0f}円 ({s['tot']/BASE['tot']*100:.0f}%残存) 勝ち{s['win']}/11")
for cost in (0.15, 0.30):
    dd = b.copy(); dd["yen"] = (dd.pnl - cost) / 100 * dd.sh * dd.o1
    yr = dd.groupby("y").yen.sum().reindex(YEARS, fill_value=0)
    print(f"  往復{cost:.2f}%控除: 10年{dd.yen.sum():+,.0f}円 勝ち{int((yr>0).sum())}/11 最悪年{yr.min():+,.0f}円")
