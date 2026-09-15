# -*- coding: utf-8 -*-
"""_bt_xm_short_0915.py — XM 短い時間軸の未検証軸を一括検証（2026-09-15 本人「未検証含めてもっと儲けれると思う・時間軸短くてもいい」）。
データ: _xm_multi_hist.pkl（H1 2010/2011〜2026・GOLD 2001〜／M15 2022-07〜）。サーバー時刻→JST変換は先行BTと同じ（欧州DST）。
コスト: 実スプレッド(spec)を1往復分・金は手数料0.002%・持ち越しはswap_long日割り。
判定バー（先行BTと同じ）: |t|≥3 ＋ 前半/後半とも同符号 ＋ 勝ち年≥8割。テスト数が多いので最後に「偶然の最大t」も出す。
族:
  A 寄り付き1時間の継続/反転（日経9時・US500 22/23時・GER40 16/17時・金16/17時）
  B 引け前の反転（日経 9→14 が大きく動いた日の 14→15・US500 23→04 の後の 04→05）
  C 月替わり効果（最終営業日15時→翌月3営業日目）
  D クロス市場の先行（前夜の米→日経夜／当日の日経昼→US500夜／米寄り2.5h→GER40夜）
  E 金の時間帯ドリフト（東京9→11/11→15・ロンドン前後・NY）
  F M15 寄り付き15分の継続/反転（2022-07〜・日経9:00/金16:00,17:00/US500 22:30,23:30）
"""
import numpy as np, pandas as pd, itertools, sys, io, warnings
warnings.simplefilter("ignore")
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
D = pd.read_pickle("_xm_multi_hist.pkl"); H = D["h1"]; M15 = D.get("m15", {}); S = D["spec"]
SYMS = ["JP225Cash", "US500Cash", "GER40Cash", "GOLD."]
COMM = {"GOLD.": 0.002}


def to_jst(df):
    df = df.copy(); ts = pd.to_datetime(df["time"], unit="s"); df.index = ts
    df = df[df.index >= "2011-01-01"]
    y = df.index.year
    # 欧州DST: 3月最終日曜01:00〜10月最終日曜01:00（サーバー=GMT+3、それ以外GMT+2）
    def last_sun(yy, m):
        d = pd.Timestamp(yy, m, 31); return d - pd.Timedelta(days=(d.dayofweek + 1) % 7)
    dst = np.zeros(len(df), dtype=bool)
    for yy in np.unique(y):
        a = last_sun(yy, 3) + pd.Timedelta(hours=1); b = last_sun(yy, 10) + pd.Timedelta(hours=1)
        dst |= (df.index >= a) & (df.index < b)
    off = np.where(dst, 3, 2)
    df.index = df.index - pd.to_timedelta(off, unit="h") + pd.Timedelta(hours=9)
    return df


def spread_pct(sym):
    return S[sym]["spread"] / S[sym]["bid"] * 100 + COMM.get(sym, 0.0)


def hour_table(df):
    """日付×時(JST)の始値テーブル"""
    t = df["open"].copy(); t.index = df.index
    tab = pd.DataFrame({"d": t.index.normalize(), "h": t.index.hour, "o": t.values}).pivot_table(index="d", columns="h", values="o")
    return tab


def stats(n, label):
    n = n.dropna()
    if len(n) < 200:
        return f"{label:44s} n={len(n)} 不足"
    y = n.groupby(n.index.year).sum(); yrs = len(y); win = int((y > 0).sum())
    mid = n.index[len(n) // 2]
    h1, h2 = n[n.index < mid].mean(), n[n.index >= mid].mean()
    t = n.mean() / n.std() * np.sqrt(len(n))
    ok = abs(t) >= 3 and np.sign(h1) == np.sign(h2) == np.sign(n.mean()) and win / yrs >= 0.8
    return f"{label:44s} n={len(n):5d} net/回{n.mean():+.3f}% t={t:+.2f} 前半{h1:+.3f} 後半{h2:+.3f} 勝ち年{win}/{yrs} {'★' if ok else ''}"


T = {s: hour_table(to_jst(H[s])) for s in SYMS}
SP = {s: spread_pct(s) for s in SYMS}
ALL_T = []


def rec(line):
    print(line); ALL_T.append(line)


def leg_same_day(sym, h_in, h_out):
    tab = T[sym]
    if h_in not in tab.columns or h_out not in tab.columns: return None
    r = (tab[h_out] / tab[h_in] - 1) * 100
    return r.dropna()


def leg_next_day(sym, h_in, h_out):
    tab = T[sym]
    if h_in not in tab.columns or h_out not in tab.columns: return None
    nxt = tab[h_out].shift(-1)
    r = (nxt / tab[h_in] - 1) * 100
    r = r[tab.index.dayofweek < 4]     # 月〜木（金曜は週末持ち越しになる）
    return r.dropna()


def cond_stats(sig, tgt, name, cost, swap_days=0.0, sym=None):
    """sig>0 で買い・sig<0 で売り（継続）と、その逆（反転）を両方出す"""
    j = pd.concat([sig.rename("s"), tgt.rename("t")], axis=1).dropna()
    j = j[j.s != 0]
    sw = (S[sym]["swap_long"] / 365 * swap_days) if (sym and swap_days) else 0.0
    cont = np.sign(j.s) * j.t - cost - abs(sw); rev = -np.sign(j.s) * j.t - cost - abs(sw)
    rec(stats(cont, name + " 継続"))
    rec(stats(rev, name + " 反転"))


print("=== A 寄り付き1時間の継続/反転（H1）===")
for sym, opens, ends in [("JP225Cash", [9], [11, 12, 15]), ("US500Cash", [22, 23], [1, 2, 5]), ("GER40Cash", [16, 17], [19, 20, 23]), ("GOLD.", [16, 17, 22, 23], [19, 1])]:
    for ho in opens:
        first = leg_same_day(sym, ho, ho + 1)
        if first is None: continue
        for he in ends:
            tgt = leg_same_day(sym, ho + 1, he) if he > ho + 1 else leg_next_day(sym, ho + 1, he)
            if tgt is None: continue
            cond_stats(first, tgt, f"{sym[:5]} {ho:02d}時1h→{ho+1:02d}→{he:02d}", SP[sym])

print("\n=== B 引け前の反転（大きく動いた日だけ）===")
for sym, a, b, c in [("JP225Cash", 9, 14, 15), ("US500Cash", 23, 4, 5), ("GER40Cash", 16, 22, 23), ("GOLD.", 9, 14, 15)]:
    pre = leg_same_day(sym, a, b) if b > a else leg_next_day(sym, a, b)
    last = leg_same_day(sym, b, c) if c > b else leg_next_day(sym, b, c)
    if pre is None or last is None: continue
    if b < a:   # 翌日にまたぐ（US500 23→04→05）: pre は前日起点、last は翌日 → 日付を合わせる
        pre = pre.copy(); pre.index = pre.index + pd.Timedelta(days=1)
    for th in (0.5, 1.0, 1.5):
        big = pre[abs(pre) >= th]
        cond_stats(big, last, f"{sym[:5]} |{a:02d}→{b:02d}|≥{th}% の {b:02d}→{c:02d}", SP[sym])

print("\n=== C 月替わり効果（最終営業日→翌月3営業日目・持ち越し4夜）===")
for sym, h in [("JP225Cash", 15), ("US500Cash", 5), ("GER40Cash", 16), ("GOLD.", 15)]:
    p = T[sym][h].dropna()
    r3 = (p.shift(-3) / p - 1) * 100
    m = pd.Series(p.index.to_period("M"), index=p.index); last = (m != m.shift(-1)).values  # 月の最終営業日
    tom = r3[last] - SP[sym] - abs(S[sym]["swap_long"]) / 365 * 4
    other = r3[~last] - SP[sym] - abs(S[sym]["swap_long"]) / 365 * 4
    rec(stats(tom, f"{sym[:5]} 月末→+3営業日 買い"))
    rec(stats(other, f"{sym[:5]} それ以外の日→+3営業日 買い(参考)"))

print("\n=== D クロス市場の先行 ===")
jp_n = leg_next_day("JP225Cash", 15, 9); us_n = leg_next_day("US500Cash", 15, 9); de_n = leg_next_day("GER40Cash", 1, 16)
us_prev = leg_next_day("US500Cash", 23, 5); us_prev.index = us_prev.index + pd.Timedelta(days=1)   # 前夜の米（当日05時に確定）→当日15時の日経夜
jp_day = leg_same_day("JP225Cash", 9, 15)
us_open = leg_same_day("US500Cash", 22, 1)   # 22→翌01(=GER40建て時刻)…同日テーブルなので 22→(翌)1 は next_day
us_open = leg_next_day("US500Cash", 22, 1); us_open.index = us_open.index + pd.Timedelta(days=1)   # GER40の01:00と同じ日付へ
for name, sig, tgt, sym, sw in [("前夜米→日経夜(15→9)", us_prev, jp_n, "JP225Cash", 0.75), ("当日日経昼→US500夜(15→9)", jp_day, us_n, "US500Cash", 0.75), ("米寄り2.5h→GER40(01→16)", us_open, de_n, "GER40Cash", 0.6)]:
    cond_stats(sig, tgt, name, SP[sym], sw, sym)
    # 現行フィルタ(前夜≤0)の上に重ねた場合
    own_prev = tgt.shift(1)
    j = pd.concat([sig.rename("s"), tgt.rename("t"), own_prev.rename("p")], axis=1).dropna()
    base = j[j.p <= 0]
    for lab, sub in [("現行(前夜≤0)", base), ("＋先行市場↑", base[base.s > 0]), ("＋先行市場↓", base[base.s < 0])]:
        rec(stats(sub.t - SP[sym] - abs(S[sym]["swap_long"]) / 365 * sw, f"  {name} {lab} 買い"))

print("\n=== E 金の時間帯ドリフト（買い・そのまま）===")
for a, b in [(9, 11), (11, 15), (15, 16), (16, 18), (18, 22), (22, 1), (1, 4)]:
    r = leg_same_day("GOLD.", a, b) if b > a else leg_next_day("GOLD.", a, b)
    rec(stats(r - SP["GOLD."], f"GOLD {a:02d}→{b:02d} 買い"))
    rec(stats(-r - SP["GOLD."], f"GOLD {a:02d}→{b:02d} 売り"))

print("\n=== F M15 寄り付き15分の継続/反転（2022-07〜・参考）===")
def m15_jst(sym):
    df = M15.get(sym)
    return to_jst(df) if df is not None else None
for sym, opens in [("JP225Cash", [(9, 0)]), ("GOLD.", [(16, 0), (17, 0)]), ("US500Cash", [(22, 30), (23, 30)]), ("GER40Cash", [(16, 0), (17, 0)])]:
    df = m15_jst(sym)
    if df is None: continue
    o = df["open"]
    for hh, mm in opens:
        t0 = o[(o.index.hour == hh) & (o.index.minute == mm)]
        for dur in (30, 60):
            res = []
            for ts, p0 in t0.items():
                p1 = o.get(ts + pd.Timedelta(minutes=15)); p2 = o.get(ts + pd.Timedelta(minutes=15 + dur))
                if p1 is None or p2 is None or np.isnan(p1) or np.isnan(p2): continue
                res.append((ts.normalize(), (p1 / p0 - 1) * 100, (p2 / p1 - 1) * 100))
            if not res: continue
            r = pd.DataFrame(res, columns=["d", "s", "t"]).set_index("d")
            cond_stats(r.s, r.t, f"{sym[:5]} M15 {hh:02d}:{mm:02d}+15m→+{dur}m", SP[sym])

print("\n=== 偶然の最大t（族全体の多重比較の目安）===")
ts = [float(l.split("t=")[1].split()[0]) for l in ALL_T if "t=" in l]
print(f"テスト数{len(ts)} |t|最大{max(abs(x) for x in ts):.2f} ★(|t|≥3・両半同符号・勝ち年8割)={sum(1 for l in ALL_T if l.endswith('★'))}")
print("★の行:"); [print("  " + l) for l in ALL_T if l.endswith("★")]
