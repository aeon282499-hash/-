# -*- coding: utf-8 -*-
"""_bt_souzai_ult2_0927.py — 割安B「究極版」段階D: 選ぶ期間(IS)で選んだ上位を、決まった並べ方で正しく評価して C1 と比べる（2026-09-27）
_bt_souzai_ult_0927.py の段階Bの1位は「並べ方=ランダム(10回平均)」だったのに、最後の確かめで合成スコア順として評価してしまった（ミス）。
本番は決まった順に並べる必要があるので、段階Bの上位(ISの「枠3/4/5の最小シャープ」)から決まった並べ方の構成を取り、C1 の出口だけ変えた形も加えて比べる。
■ 事前に決めた「C1を超えた」の条件（このファイルを書いた時点で固定・OOSを見る前）
   ISの月次シャープが C1 より+0.10以上 かつ OOSの月次シャープが C1 以上 かつ 10年の合計が C1 以上 かつ 回数の条件(月2件以上・撃たない月2割以下)。
   配当込み(信用買いの配当落調整金)で判定し、配当なし(本番の帳簿の数え方)も並べる。
 候補(ISだけで選んだもの): D1 PBR≤0.8・PER≤10・利回り≥2.5% T1成行 利回り順 トレール8% / D2 PBR≤0.8・PER≤10・利回り≥4.0% 無条件成行 利回り順 損切り5%
   / D3 同 合成順 トレール8% / D4 PBR≤1.0・PER≤12・利回り≥3.0% T1成行 合成順 PBR>1で利確
   / C1の出口違い: PBR>1で利確・トレール8%・トレール3%・最長30日
実行: python -X utf8 _bt_souzai_ult2_0927.py > _bt_souzai_ult2_0927.log
"""
import pickle, time, numpy as np, pandas as pd
src = open("_bt_souzai_ult_0927.py", encoding="utf-8").read()
exec(src.split("# ================= 基準: C1(本番) =================")[0])
C1M = DEFS[("thr", 1.0, 10, 3.0)] & T1 & VALID
cdC1 = to_cands(C1M, DY)
CANDS = {
    "C1(本番)": (cdC1, dict(order=("mkt",), stop=0.07)),
    "D1 PBR≤0.8・PER≤10・利回り≥2.5% T1成行 利回り順 トレール8%": (to_cands(DEFS[("thr", 0.8, 10, 2.5)] & T1 & VALID, DY), dict(order=("mkt",), stop=0.07, trail=0.08)),
    "D2 PBR≤0.8・PER≤10・利回り≥4.0% 無条件成行 利回り順 損切り5%": (to_cands(DEFS[("thr", 0.8, 10, 4.0)] & VALID, DY), dict(order=("mkt",), stop=0.05)),
    "D3 PBR≤0.8・PER≤10・利回り≥4.0% 無条件成行 合成順 トレール8%": (to_cands(DEFS[("thr", 0.8, 10, 4.0)] & VALID, COMPZ), dict(order=("mkt",), stop=0.07, trail=0.08)),
    "D4 PBR≤1.0・PER≤12・利回り≥3.0% T1成行 合成順 PBR>1で利確": (to_cands(DEFS[("thr", 1.0, 12, 3.0)] & T1 & VALID, COMPZ), dict(order=("mkt",), stop=0.07, pbr_tp=1.0)),
    "C1＋PBR>1で利確": (cdC1, dict(order=("mkt",), stop=0.07, pbr_tp=1.0)),
    "C1＋トレール8%": (cdC1, dict(order=("mkt",), stop=0.07, trail=0.08)),
    "C1＋トレール3%": (cdC1, dict(order=("mkt",), stop=0.07, trail=0.03)),
    "C1＋最長30日": (cdC1, dict(order=("mkt",), stop=0.07, maxhold=30)),
}
res = {}
for name, (cd, kw) in CANDS.items():
    Ed = evals(cd, **kw); En = evals(cd, **dict(kw, div=False))
    res[name] = (Ed, En)
    print(f"\n■ {name}", flush=True)
    for lab in ("IS", "OOS", "10年", "2026年"): print(line(lab + "(配当込み)", Ed[lab]))
    print("   配当なし: " + " / ".join(f"{lab} {En[lab]['total']:+.1f}万(シャープ{En[lab]['sharpe']:.2f})" for lab in ("IS", "OOS", "10年", "2026年")))
    print(f"   10年の時代別(配当込み) {Ed['10年']['eras']} ／ 出口 {Ed['10年']['why']}", flush=True)
c1 = res["C1(本番)"][0]
print("\n■ 事前に決めた条件で判定（配当込み）: ISシャープ≥C1+0.10 ・ OOSシャープ≥C1 ・ 10年合計≥C1 ・ 回数")
print(f"   C1: IS {c1['IS']['sharpe']:.2f} / OOS {c1['OOS']['sharpe']:.2f} / 10年 {c1['10年']['total']:+.1f}万 / 月{c1['10年']['epm']:.1f}件・撃たない月{c1['10年']['zero']:.0f}%")
winners = []
for name, (Ed, En) in res.items():
    if name == "C1(本番)": continue
    ok1 = Ed["IS"]["sharpe"] >= c1["IS"]["sharpe"] + 0.10; ok2 = Ed["OOS"]["sharpe"] >= c1["OOS"]["sharpe"]
    ok3 = Ed["10年"]["total"] >= c1["10年"]["total"]; ok4 = Ed["10年"]["epm"] >= 2 and Ed["10年"]["zero"] <= 20
    tag = "✅ C1超え" if (ok1 and ok2 and ok3 and ok4) else "✗"
    if tag.startswith("✅"): winners.append(name)
    print(f"   {name:<52} IS {Ed['IS']['sharpe']:.2f}{'○' if ok1 else '×'} OOS {Ed['OOS']['sharpe']:.2f}{'○' if ok2 else '×'} 10年 {Ed['10年']['total']:+.1f}万{'○' if ok3 else '×'} 月{Ed['10年']['epm']:.1f}件/ゼロ月{Ed['10年']['zero']:.0f}%{'○' if ok4 else '×'} → {tag}")
print(f"   → C1を超えたもの: {winners if winners else 'なし＝C1が究極'}")
# 勝者(あれば)の対照と月の安定
for name in winners[:2]:
    cd, kw = CANDS[name]
    cnt = np.zeros(T, int)
    for s, lst in cd.items(): cnt[s] = len(lst)
    def rand_c(seed):
        rng = np.random.default_rng(seed); out = {}
        for t in np.nonzero(cnt)[0]:
            pool_ = np.nonzero(VALID[t] & AFF[t])[0]; k = min(int(cnt[t]), len(pool_))
            out[int(t)] = [(int(j), 0.0) for j in rng.choice(pool_, k, replace=False)]
        return out
    Ed = res[name][0]
    for lab, a, b in (("10年", 1, T), ("OOS", IS_END, T), ("2026年", I2026, T)):
        u = np.array([metrics(real_run(rand_c(500 + sd), t_start=a, t_end=b, **kw), SIZE0, a, b)["total"] for sd in range(30)])
        print(f"   {name} 対照({lab}): 同じ置き方で全銘柄からランダム 平均{u.mean():+.1f}万 sd{u.std():.1f} → z{(Ed[lab]['total']-u.mean())/max(u.std(),1e-9):+.1f}", flush=True)
pickle.dump({k: v[0] for k, v in res.items()}, open("_bt_souzai_ult2_0927.pkl", "wb"))
