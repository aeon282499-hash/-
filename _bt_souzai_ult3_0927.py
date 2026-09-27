# -*- coding: utf-8 -*-
"""_bt_souzai_ult3_0927.py — 割安B「究極版」事後の確認（2026-09-27・事前登録外）
①「C1＋PBR>1で利確」は事前の条件(ISシャープ+0.10)にわずかに届かなかったが IS/OOS/2026 のすべてで C1 を上回った → 利確のPBRの前後(0.9/1.1/1.2)で高原か
② C1 の同じ置き方でのランダム対照(30回)
③ データの誤り: 予想配当利回りが15%超(分割の前後で配当予想の単位がずれた等)の候補が C1 の玉にどれだけ入ったか・その損益
実行: python -X utf8 _bt_souzai_ult3_0927.py > _bt_souzai_ult3_0927.log
"""
import pickle, time, numpy as np, pandas as pd
src = open("_bt_souzai_ult_0927.py", encoding="utf-8").read()
exec(src.split("# ================= 基準: C1(本番) =================")[0])
C1M = DEFS[("thr", 1.0, 10, 3.0)] & T1 & VALID
cdC1 = to_cands(C1M, DY)
print("■ ① C1＋PBRで利確 の前後（配当込み・月次シャープ / 損益）")
for tp in (None, 0.9, 1.0, 1.1, 1.2):
    Ed = evals(cdC1, order=("mkt",), stop=0.07, pbr_tp=tp)
    En = evals(cdC1, order=("mkt",), stop=0.07, pbr_tp=tp, div=False)
    lab = "C1(利確なし)" if tp is None else f"PBR>{tp}で利確"
    print(f"   {lab:<14} " + " ｜ ".join(f"{p} {Ed[p]['sharpe']:.2f}/{Ed[p]['total']:+.1f}万" for p in ("IS", "OOS", "10年", "2026年"))
          + f" ｜ 配当なし10年 {En['10年']['total']:+.1f}万 PF{Ed['10年']['pf']:.2f} DD{Ed['10年']['dd']:+.1f} 月勝率{Ed['10年']['mwin']:.0f}%", flush=True)
print("\n■ ② C1 の対照: 同じ置き方で、同じ日に同じ数の候補を全銘柄(流動性・値がさ見送り)からランダムに30回（配当込み）")
cnt = np.zeros(T, int)
for s, lst in cdC1.items(): cnt[s] = len(lst)
def rand_c(seed):
    rng = np.random.default_rng(seed); out = {}
    for t in np.nonzero(cnt)[0]:
        pool_ = np.nonzero(VALID[t] & AFF[t])[0]; k = min(int(cnt[t]), len(pool_))
        out[int(t)] = [(int(j), 0.0) for j in rng.choice(pool_, k, replace=False)]
    return out
EC = evals(cdC1, order=("mkt",), stop=0.07)
for lab, a, b in (("10年", 1, T), ("OOS", IS_END, T), ("2026年", I2026, T)):
    u = np.array([metrics(real_run(rand_c(700 + sd), t_start=a, t_end=b, order=("mkt",), stop=0.07), SIZE0, a, b)["total"] for sd in range(30)])
    print(f"   {lab}: C1 {EC[lab]['total']:+.1f}万 / ランダム 平均{u.mean():+.1f}万 sd{u.std():.1f} → z{(EC[lab]['total']-u.mean())/max(u.std(),1e-9):+.1f}", flush=True)
print("\n■ ③ 予想配当利回り15%超の候補（データの誤りの疑い）")
bigdy = C1M & (DY > 15)
print(f"   C1の候補日×銘柄 {int(C1M.sum())} のうち利回り15%超 {int(bigdy.sum())}（銘柄 {int(bigdy.any(0).sum())}）: " +
      ", ".join(f"{codes[j]}" for j in np.nonzero(bigdy.any(0))[0][:15]))
tr, eq = real_run(cdC1, order=("mkt",), stop=0.07)
R = pd.DataFrame(tr, columns=["e", "x", "j", "r", "why"])
R["dy"] = [float(DY[e - 1, j]) for e, j in zip(R.e, R.j)]
bad_tr = R[R.dy > 15]
print(f"   C1の10年の玉 {len(R)} のうち、シグナル日の利回り15%超 {len(bad_tr)}玉 / その損益 {bad_tr.r.sum() * SIZE0 / 1e4:+.1f}万: " +
      "; ".join(f"{dates[int(r.e)].date()} {codes[int(r.j)]} 利回り{r.dy:.0f}% {r.r*100:+.1f}%" for r in bad_tr.itertuples()))
cdC1g = to_cands(C1M & ~(DY > 15), DY)
Eg = evals(cdC1g, order=("mkt",), stop=0.07)
print("   利回り15%超を候補から外した C1（配当込み）: " + " ｜ ".join(f"{p} {Eg[p]['sharpe']:.2f}/{Eg[p]['total']:+.1f}万" for p in ("IS", "OOS", "10年", "2026年")))
