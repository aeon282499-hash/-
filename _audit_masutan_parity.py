# -*- coding: utf-8 -*-
"""_audit_masutan_parity.py — masutan_signal.py（本番ロジック）を過去10年に当てたシグナル一覧が、
9/19 BT(_bt_masutan_exante_0918.csv の k=2・892件) と 1件単位で一致するかを確かめる（2026-09-27）。

本番の流儀を再現:
  ・規制エピソード = masutan_signal.apply_flags / prune_episodes（公表日の穴3公表日以内は同じエピソード）
  ・シグナル = masutan_signal.calm_path の first_hit（公表日の翌営業日から±15%以内が2日連続になった最初の日）
  ・その夜に「最新の公表日に規制が付いている」銘柄だけ判定する
      A: 当日の公表分が夕方に取れている（9/25のランは18:26に当日分あり）→ first_hit の日に規制フラグが付いていること
      B: 当日分が間に合わず前営業日の公表分で判定 → first_hit の前の公表日に規制フラグが付いていること
  ・代金フィルタ = 直近5日の 終値×出来高 の平均 ≥ 3億（9/26 立花版の tov と同じ定義）
価格: 9/19 BT と同じ jquants_cache_2016_2021.pkl + jquants_cache.pkl（Close>0）。
実行: python -X utf8 _audit_masutan_parity.py > _audit_masutan_parity.log
"""
import pickle, gc
from collections import defaultdict
import pandas as pd

import masutan_signal as MS

A = pd.read_pickle("_margin_alert_bal_10y.pkl")
dates = sorted(A.PubDate.unique())
R = A[A.Restricted == 1][["PubDate", "Code", "TSEMrgnRegCls"]].copy()
R["code4"] = R.Code.astype(str).str[:4]
R["common"] = R.Code.astype(str).map(lambda c: len(c) < 5 or c[4] == "0")
print(f"規制行 {len(R)} / 普通株以外 {int((~R.common).sum())} 行 / 公表日 {len(dates)}日 {dates[0]}〜{dates[-1]}")

# 公表日インデックス（BTと同じ「データにある公表日」）で穴を数えるため、MS.trading_gap を公表日インデックスに差し替える
DI = {d: i for i, d in enumerate(dates)}
MS.trading_gap = lambda a, b: DI[b] - DI[a]

by_date = defaultdict(dict)
for r in R[R.common].itertuples():
    by_date[r.PubDate][r.code4] = str(r.TSEMrgnRegCls)

# 本番の状態更新を公表日ごとに流す → エピソード (code, start) と、そのエピソードに規制が付いていた公表日
episodes, closed = {}, []
ep_flag_days = defaultdict(set)      # (code, start) -> {規制が付いていた公表日}
for d in dates:
    MS.apply_flags(episodes, d, by_date.get(d, {}))
    MS.prune_episodes(episodes, closed, d)
    for code in by_date.get(d, {}):
        ep_flag_days[(code, episodes[code]["start"])].add(d)
EP = sorted(ep_flag_days)
print(f"本番ロジックのエピソード {len(EP)} 本（9/18 BT は 904 本・進行中を除く）")

need = sorted({c for c, _ in EP})
SER = {}
for fn in ("jquants_cache_2016_2021.pkl", "jquants_cache.pkl"):
    src = pickle.load(open(fn, "rb"))["all_data"]
    for c in need:
        df = src.get(c + ".T")
        if df is not None and len(df):
            SER.setdefault(c, []).append(df[["Close", "Volume"]].copy())
    del src; gc.collect()
PX = {}
for c, dfs in SER.items():
    df = pd.concat(dfs).sort_index(); df = df[~df.index.duplicated(keep="last")]
    df = df[df["Close"] > 0]
    PX[c] = (df["Close"].astype(float).tolist(), [x.strftime("%Y-%m-%d") for x in df.index], (df["Close"] * df["Volume"]).astype(float).tolist())
del SER; gc.collect()

rowsA, rowsB, why = [], [], []
for code, start in EP:
    if code not in PX:
        why.append((code, start, "株価データなし")); continue
    cl, ds, tv = PX[code]
    cp = MS.calm_path(cl, ds, start)
    if not cp["ok"]:
        why.append((code, start, "公表日の足なし")); continue
    fh = cp["first_hit"]
    if fh is None:
        continue
    flag_days = ep_flag_days[(code, start)]
    i = ds.index(fh)
    tov5 = sum(tv[max(0, i - 4):i + 1]) / min(5, i + 1) / 1e8
    prev_pub = dates[DI[fh] - 1] if fh in DI and DI[fh] > 0 else None
    a_ok = fh in flag_days
    b_ok = prev_pub in flag_days if prev_pub else False
    rec = {"code": code, "start": start, "sig": fh, "tov5": tov5, "last_flag": max(flag_days)}
    (rowsA if a_ok else why).append(rec if a_ok else (code, start, f"first_hit {fh} に規制フラグ無し(BTの区間内の穴 or 解除後)"))
    if b_ok:
        rowsB.append(rec)
LA = pd.DataFrame(rowsA); LB = pd.DataFrame(rowsB)
BT = pd.read_csv("_bt_masutan_exante_0918.csv", dtype={"code": str}); BT = BT[BT.k == 2]
bt = set(zip(BT.code, BT.sig)); la = set(zip(LA.code, LA.sig)); lb = set(zip(LB.code, LB.sig))
last_date = dates[-1]
print(f"\n■ 一致(本番A: 当日の公表分で判定) 本番{len(la)} / BT{len(bt)} / 共通{len(la & bt)} / BTだけ{len(bt - la)} / 本番だけ{len(la - bt)}")
print(f"■ 一致(本番B: 前日の公表分で判定) 本番{len(lb)} / 共通{len(lb & bt)} / BTだけ{len(bt - lb)} / 本番だけ{len(lb - bt)}")

# 不一致の理由（全件）
E = pd.read_csv("_bt_masutan_release_0918.csv", dtype={"code": str})
bt_ep = {(r.code, r.start): r.end for r in E.itertuples()}
live_ep = {(c, s) for c, s in EP}
print("\n■ BTにあって本番Aに無い玉（全件の理由）")
for c, s in sorted(bt - la):
    eps_c = [(st, en) for (cc, st), en in bt_ep.items() if cc == c and st <= s <= en]
    st = eps_c[0][0] if eps_c else None
    reason = []
    if st is None:
        reason.append("BTのエピソード不明")
    else:
        if (c, st) not in live_ep:
            reason.append(f"本番のエピソード開始が違う(BT start={st})")
        fl = ep_flag_days.get((c, st), set())
        if s not in fl:
            reason.append("シグナル日に規制フラグ無し＝公表日の穴(BTは区間内として数える)")
        m = R[(R.code4 == c) & (~R.common)]
        if len(m):
            reason.append("普通株以外のコード行あり")
    print(f"  {c} sig={s} : {' / '.join(reason) or '要確認'}")
print("\n■ 本番Aにあって BTに無い玉（全件の理由）")
for c, s in sorted(la - bt):
    r = LA[(LA.code == c) & (LA.sig == s)].iloc[0]
    reason = []
    if r.last_flag >= last_date or (r.start, ) and all(not (cc == c and st == r.start) for (cc, st) in bt_ep):
        if r.last_flag >= dates[-4]:
            reason.append(f"データ末尾で進行中のエピソード(last={r.last_flag})＝BTは除外")
        else:
            reason.append("BTにこの(code,start)のエピソードが無い")
    print(f"  {c} sig={s} start={r.start} : {' / '.join(reason) or '要確認'}")

# 代金フィルタ: 9/26 立花版(real)の tov≥3 と比べる
S = pd.read_csv("_bt_masutan_synth26_0926_real.csv", dtype={"code": str})
s3 = set(zip(S[S.tov >= 3].code, S[S.tov >= 3].sig))
la3 = set(zip(LA[LA.tov5 >= 3].code, LA[LA.tov5 >= 3].sig))
print(f"\n■ 代金≥3億: 本番A {len(la3)} / 9/26立花版 {len(s3)} / 共通 {len(la3 & s3)} / 立花だけ {len(s3 - la3)} / 本番だけ {len(la3 - s3)}")
bt3 = {(c, s) for c, s in bt if (c, s) in la}  # 参考
LA.to_csv("_audit_masutan_parity_live.csv", index=False, encoding="utf-8-sig")
print(f"\n本番Aの一覧 → _audit_masutan_parity_live.csv  (株価なし/公表日の足なし など判定外 {sum(1 for w in why if isinstance(w, tuple) and ('株価' in w[2] or '足なし' in w[2]))}件)")
