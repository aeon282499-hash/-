# -*- coding: utf-8 -*-
"""VI傾斜×現金上限: 口座300万(3枠)で130万玉が入らない時の実挙動（超過玉→100万に落とす→それでも無理なら見送り）。2026-09-04。"""
import sys, io, numpy as np, pandas as pd
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
NV = pd.read_pickle("_nk225_iv_daily.pkl"); NV["Date"] = pd.to_datetime(NV.Date); NV = NV.sort_values("Date"); NV["v"] = pd.to_numeric(NV.bv1, errors="coerce").shift(1)
Ct = pd.DataFrame({"_i": np.arange(n), "entry": C0.entry}).sort_values("entry")
J = pd.merge_asof(Ct, NV[["Date", "v"]].dropna(), left_on="entry", right_on="Date", allow_exact_matches=False).sort_values("_i"); VL = J.v.to_numpy()
# stage1 picks は run2 と同じ（BASE）。stage2 を現金勘定に置き換える。
ok = np.isfinite(pnl0) & BASE
ou, os_, picks = {}, {}, []
for d in range(len(ALLDAYS)):
    for tk in [t for t, u in ou.items() if u < d]: del ou[tk]; del os_[tk]
    sc = {}
    for s_ in os_.values(): sc[s_] = sc.get(s_, 0) + 1
    cnt = 0
    for i in by_day.get(d, []):
        if cnt >= MAX_SIG: break
        if not ok[i] or TICK[i] in ou: continue
        s_ = SEC[i]
        if sc.get(s_, 0) >= SECTOR_CAP: continue
        cnt += 1; ex = min(d + int(exo0[i]), len(ALLDAYS) - 1)
        ou[TICK[i]] = ex; os_[TICK[i]] = s_; sc[s_] = sc.get(s_, 0) + 1
        picks.append((d, ex, int(YEAR[i]), float(pnl0[i]), float(E[i]), i))
def stage2(cash_cap, mult_fn, lot=True, slots=3):
    live, rows = [], []
    for d, ex, y, p, e, i in picks:
        live = [(x, c) for x, c in live if x >= d]
        if len(live) >= slots: continue
        want = 1_000_000 * mult_fn(i)
        used = sum(c for _, c in live)
        for sz in (want, 1_000_000) if want > 1_000_000 else (want,):
            sh = int(sz / e / 100) * 100 if lot else sz / e
            cost = sh * e
            if sh > 0 and (cash_cap is None or used + cost <= cash_cap + 1e-6):
                live.append((ex, cost)); rows.append({"y": y, "yen": p / 100 * cost}); break
    R = pd.DataFrame(rows); yy = R.groupby("y").yen.sum()
    return R, yy
def show(label, R, yy):
    gp = R.yen[R.yen > 0].sum(); gl = -R.yen[R.yen <= 0].sum()
    print(f"  {label:<40}{len(R):>5}玉 PF{gp/gl:.2f} 計{yy.sum()/1e4:+.0f}万 前半{yy[yy.index<=2021].sum()/1e4:+.0f} 後半{yy[yy.index>=2022].sum()/1e4:+.0f} 勝年{int((yy>0).sum())}/10 最悪年{yy.min()/1e4:+.0f}")
m13 = lambda i: 1.3 if VL[i] >= 20 else (0.7 if VL[i] <= 15 else 1.0)
m1 = lambda i: 1.0
print("■ 現金上限なし（BT前提）")
show("現行 100万", *stage2(None, m1)); show("傾斜 130/100/70（株数丸めあり）", *stage2(None, m13)); show("傾斜（丸めなし・BTと同じ連続）", *stage2(None, m13, lot=False))
print("■ 現金上限 300万（超過玉は100万に落とす→無理なら見送り）")
show("現行 100万", *stage2(3_000_000, m1)); show("傾斜 130/100/70", *stage2(3_000_000, m13))
print("■ 現金上限 390万")
show("現行 100万", *stage2(3_900_000, m1)); show("傾斜 130/100/70", *stage2(3_900_000, m13))
print("■ 参考: 現行を一律130万（現金390万）"); show("一律130万", *stage2(3_900_000, lambda i: 1.3))
print("■ 参考: 高ボラ日だけ130万・現金300万で入る分だけ"); show("上だけ130万", *stage2(3_000_000, lambda i: 1.3 if VL[i] >= 20 else 1.0))
print("[done]")
