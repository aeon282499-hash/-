# -*- coding: utf-8 -*-
"""_bt_fade_60man_2026_pricey_0926.py — 本人「①60万なら今年の成績は？9千円の株とかどうする？」(2026-09-26)
A) 2026年(1/5〜9/25)を①だけ・前終+1%当日中指値・大型除外で: 100万 / 60万(100株>60万は見送り) / 60万(100株が100万以内なら100株撃つ)。
   9/2まではBTプール(build)、9/3〜9/25は帳簿(positions_day_paper.json)の玉＋J-Quants高値。帳簿は時価総額≥1000億を除いて最上位を①とみなす。
B) 10年で「100株が60万超(株価6,000円超)」の①玉の期待値と、撃つ/見送るで余力40万からの3か月15万割れ率がどう変わるか。
実行: python -X utf8 _bt_fade_60man_2026_pricey_0926.py > _log_fade_60man_2026_pricey_0926.txt"""
import os, json
exec(open(os.environ['TEMP'] + '/claude_10y_k.py', encoding='utf-8').read().split("years={}\nrows={}")[0])
from screener import fetch_tse_universe, batch_download_jquants, _jquants_id_token
Q = P10[~(np.isfinite(P10.mcap) & (P10.mcap > 1e11))]
d = rank(Q); d = d[d.rk == 1].reset_index(drop=True)
tok = _jquants_id_token(); names = dict(fetch_tse_universe(tok))
rows = [dict(ent=r.ent, ticker=r.ticker, name=names.get(r.ticker, ""), px=r.px, o=r.o1, h=r.h1, c=r.c1, src="BT") for r in d[(d.y == 2026) & (d.ent <= "2026-09-02")].itertuples()]
b = json.load(open("positions_day_paper.json", encoding="utf-8"))
T = [p for p in b["positions"] if (p.get("entry_session") or "") > "2026-09-02" and p["status"] == "closed"]
data = batch_download_jquants(tok, start="2026-09-01", end="2026-09-26")
for day in sorted({p["entry_session"] for p in T}):
    cand = sorted([p for p in T if p["entry_session"] == day], key=lambda p: p["rank"])
    cand = [p for p in cand if not (p.get("mcap_oku") and p["mcap_oku"] >= 1000)]
    if not cand: continue
    p = cand[0]; df = data[p["ticker"]]; hh = float(df.loc[df.index.strftime("%Y-%m-%d") == day].iloc[0]["High"])
    rows.append(dict(ent=day, ticker=p["ticker"], name=p["name"], px=p["prev_close"], o=p["entry_open"], h=hh, c=p["entry_close"], src="帳簿"))
R = pd.DataFrame(rows).sort_values("ent").reset_index(drop=True)
L = tickc(R.px.values * 1.01); o, h, c = R.o.values, R.h.values, R.c.values
R["how"] = np.where(o >= L, "寄り", np.where(h >= L, "場中", "見送り")); R["fillpx"] = np.where(o >= L, o, np.where(h >= L, L, np.nan))
R["ps"] = np.where(o >= L, o - c, np.where(h >= L, L - c, 0))
def shares(size, cap100=None):
    sh = (size / R.px // 100 * 100).astype(int)
    if cap100: sh = np.where((sh == 0) & (R.px * 100 <= cap100), 100, sh)
    return sh
pd.set_option("display.width", 250); pd.set_option("display.max_rows", 300)
print("■ A) 2026年 ①だけ・+1%指値・大型除外 (", R.ent.min(), "〜", R.ent.max(), " 候補", len(R), "日)")
out = {}
for lab, size, cap in (("100万", 1e6, None), ("60万・6千円超は見送り", 6e5, None), ("60万・100株が100万以内なら100株", 6e5, 1e6)):
    sh = shares(size, cap); yen = R.ps * sh; t = yen[(R.how != "見送り") & (sh > 0)]
    day = yen.groupby(R.ent).sum(); eq = day.cumsum(); m = yen.groupby(R.ent.str[:7]).sum() / 1e4
    out[lab] = m
    print(f" {lab}: 建った玉{len(t)} 損益{yen.sum()/1e4:+.1f}万 勝率{(t>0).mean()*100:.1f}% PF{t[t>0].sum()/-t[t<0].sum():.2f} DD{(eq-eq.cummax()).min()/1e4:+.1f}万 最悪日{day.min()/1e4:+.1f}万 -10万超の日{(day<-1e5).sum()}")
M = pd.DataFrame(out).round(1); M.loc["合計"] = M.sum(); print(M.to_string())
R["sh60"] = shares(6e5); R["yen60"] = R.ps * R.sh60
print("\n 6千円超で60万ルールだと撃てない①玉(2026):")
x = R[(R.sh60 == 0)]; print(x[["ent", "ticker", "name", "px", "how", "o", "c"]].assign(一株損益=x.ps, 百株なら万=(x.ps * 100 / 1e4).round(1)).to_string(index=False))
print("\n 月別玉(60万・見送りルール)の最近20件:")
print(R.tail(20)[["ent", "ticker", "name", "px", "how", "fillpx", "c", "sh60", "yen60", "src"]].to_string(index=False))

print("\n■ B) 10年(2017-01〜2026-09-01) 株価帯別の①玉(+1%指値)")
L = tickc(d.px.values * 1.01); oo, hh, cc = d.o1.values, d.h1.values, d.c1.values
d["fill"] = (oo >= L) | (hh >= L); d["pct"] = np.where(oo >= L, (oo - cc) / oo, np.where(hh >= L, (L - cc) / L, np.nan)) * 100
d["ps"] = np.where(oo >= L, oo - cc, np.where(hh >= L, L - cc, 0))
d["帯"] = pd.cut(d.px, [0, 1000, 3000, 6000, 10000, 1e9], labels=["〜1千", "1-3千", "3-6千", "6千-1万", "1万超"])
x = d[d.fill]
print(x.groupby("帯", observed=True).pct.agg(玉数="size", 平均="mean", 勝率=lambda s: (s > 0).mean() * 100, PF=lambda s: pf(s), 最悪="min").round(2).to_string())
days = pd.Series(sorted(set(P10.ent.astype(str))))
def risk(cap100, C=40, H=60):
    sh = (6e5 / d.px // 100 * 100).astype(int)
    if cap100: sh = np.where((sh == 0) & (d.px * 100 <= cap100), 100, sh)
    yen = d.ps * sh; day = yen.groupby(d.ent).sum().reindex(days, fill_value=0).values; cum = np.r_[0, np.cumsum(day)]
    mn = np.array([(cum[i + 1:i + 1 + H] - cum[i]).min() for i in range(len(day) - H)])
    return yen.sum() / 1e4 / 9.67, (mn <= -(C - 15) * 1e4).mean() * 100, (mn <= -C * 1e4).mean() * 100
for lab, cap in (("6千円超は見送り", None), ("100株が100万以内なら撃つ", 1e6), ("100株が80万以内なら撃つ", 8e5)):
    a, b15, z = risk(cap); print(f" {lab}: 年平均{a:+.1f}万  余力40万→3か月で15万割れ{b15:.1f}% ゼロ{z:.1f}%")

print("\n■ C) 2026年の資金の道のり(60万・100株が100万以内なら100株・1/5に余力40万で開始と仮定)")
sh = shares(6e5, 1e6); yen = R.ps * sh; day = yen.groupby(R.ent).sum(); eq = 40 + day.cumsum() / 1e4
pk = eq.cummax(); i = (eq - pk).idxmin()
print(f" 最低の余力 {eq.min():.1f}万({eq.idxmin()})  最大DD {(eq-pk).min():.1f}万 = ピーク{pk[i]:.1f}万({eq[:i].idxmax()})→{eq[i]:.1f}万({i})  9/25時点 {eq.iloc[-1]:.1f}万")
print(" 月末の余力:", eq.groupby(eq.index.str[:7]).last().round(1).to_dict())
print(" 4月の玉:"); x = R.assign(sh=sh, yen=yen); x = x[x.ent.str[:7] == "2026-04"]
print(x[["ent", "ticker", "name", "px", "how", "fillpx", "c", "sh", "yen"]].to_string(index=False))
