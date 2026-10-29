"""fade_notify_parity.py — fade_notify.py の選定ロジックを過去に当てて、友達の fade_rank_bt.csv と一致するか照合する。

照合の対象期間: fade_rank_bt.csv の末尾 3 か月（既定 2026-05-20〜2026-08-20）。
データ: _bars_daily_5y_1024.pkl（J-Quants bulk 日足・全銘柄・上場廃止込み）・_master_1024.pkl（S17・市場）・
        _fins_history.pkl（/fins/summary の履歴＝決算起因の判定）。全部 PC のローカル（親フォルダ）。

比べるもの（日ごと）:
  1. 全体 1 位の銘柄が同じか（除外前の順位・CSV に 1 位の行が無い日は「1 位が決算/張り付きで落ちた日」）
  2. ルールA（1 位・+17% 以上・除外なし）の 1 本が同じか（両方なしも一致）
  3. 上位 5 位の集合の重なり（Jaccard）
  4. 候補数と score の一致（同じ銘柄の score 差の最大）

使い方: python -X utf8 kabuai/fade_notify_parity.py --csv <fade_rank_bt.csv> [--start 2026-05-20 --end 2026-08-20]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
sys.path.insert(0, str(HERE))
import fade_notify as FN  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--start", default="2026-05-20")
    ap.add_argument("--end", default="2026-08-20")
    ap.add_argument("--bars", default=str(PARENT / "_bars_daily_5y_1024.pkl"))
    ap.add_argument("--master", default=str(PARENT / "_master_1024.pkl"))
    ap.add_argument("--fins", default=str(PARENT / "_fins_history.pkl"))
    a = ap.parse_args()

    ref = pd.read_csv(a.csv, dtype={"Code": str})
    ref["Date"] = pd.to_datetime(ref["Date"])
    ref = ref[(ref["Date"] >= a.start) & (ref["Date"] <= a.end)]
    days = sorted(ref["Date"].unique())
    print(f"照合期間 {a.start}〜{a.end}: CSV {len(ref)}行 / {len(days)}日")

    bars = pd.read_pickle(a.bars)
    bars = bars[(bars["Date"] >= pd.Timestamp(a.start) - pd.Timedelta(days=70)) & (bars["Date"] <= pd.Timestamp(a.end) + pd.Timedelta(days=7))]
    bars = FN._normalize_bars(bars)
    master = pd.read_pickle(a.master)
    universe = set(master.loc[master["S17"].astype(str) != "99", "Code"].astype(str))
    # CSV と同じ「現在の上場銘柄のみ」に揃える（CSV は crash_listed=現存銘柄で作られている）
    bars = bars[bars["Code"].isin(universe)]
    feat = FN.add_features(bars)
    fins = pd.read_pickle(a.fins)
    fins["d"] = pd.to_datetime(fins["DiscDate"]).dt.normalize()
    fins["h"] = pd.to_numeric(fins["DiscTime"].astype(str).str.slice(0, 2), errors="coerce")
    fins["c"] = fins["Code"].astype(str).str.zfill(5)
    all_days = np.array(sorted(feat["Date"].unique()))

    n_rank1_same = n_rank1_days = 0
    n_ruleA_same = n_ruleA_days = 0
    jac = []
    score_diff = []
    n_cand_diff = []
    mism = []
    for d in days:
        i = int(np.searchsorted(all_days, d))
        prev = all_days[i - 1]
        earn = set(fins.loc[fins["d"] == d, "c"]) | set(fins.loc[(fins["d"] == prev) & (fins["h"] >= 15), "c"])
        last = feat[feat["Date"] == d]
        cand = last[(last["C"] >= FN.PRICE_MIN) & (last["Va"] >= FN.VA_MIN) & (last["ret1"] >= FN.RET_MIN)].copy()
        # CSV 側は D+1 の寄値/終値がある行だけ候補にしている → 翌日の足が無い銘柄を落として揃える
        nxt = feat[feat["Date"] == all_days[i + 1]].set_index("Code") if i + 1 < len(all_days) else None
        if nxt is not None:
            cand = cand[cand["Code"].isin(nxt.index)]
        cand = FN.score_rank(cand)
        cand["UL1"] = cand["C"] + cand["C"].map(FN.band)
        cand["strong"] = cand["ret1"] >= FN.STRONG_RET
        cand["earn"] = cand["Code"].isin(earn)
        cand["pinned"] = False
        if nxt is not None:
            cand["pinned"] = (cand["Code"].map(nxt["O"]) >= cand["UL1"] - 1e-9).fillna(False)
        mine = cand.set_index("Code")
        r = ref[ref["Date"] == d].set_index("Code")
        # 候補数: CSV は除外後の行数なので、こちらも除外後で比べる
        mine_kept = mine[~mine["earn"] & ~mine["pinned"]]
        n_cand_diff.append(len(mine_kept) - len(r))
        # score 差（同じ銘柄）
        common = mine.index.intersection(r.index)
        if len(common):
            score_diff.append(float((mine.loc[common, "score"] - r.loc[common, "score"]).abs().max()))
        # 1. 全体 1 位
        my1 = mine.index[mine["rank_all"] == 1][0] if (mine["rank_all"] == 1).any() else None
        ref1 = r.index[r["rank_all"] == 1][0] if (r["rank_all"] == 1).any() else None
        n_rank1_days += 1
        if ref1 is None:
            # CSV に 1 位が無い＝1 位が決算/張り付きで落ちた日。こちらの 1 位がその理由で落ちていれば一致
            ok = my1 is not None and bool(mine.loc[my1, "earn"] or mine.loc[my1, "pinned"])
        else:
            ok = my1 == ref1
        n_rank1_same += int(ok)
        if not ok:
            mism.append((pd.Timestamp(d).date(), "1位", my1, ref1, None if my1 is None else f"earn={mine.loc[my1,'earn']} pin={mine.loc[my1,'pinned']} ret1={mine.loc[my1,'ret1']*100:+.1f}%"))
        # 2. ルールA
        myA = my1 if (my1 is not None and mine.loc[my1, "strong"] and not mine.loc[my1, "earn"] and not mine.loc[my1, "pinned"]) else None
        refA = ref1 if (ref1 is not None and bool(r.loc[ref1, "strong"])) else None
        n_ruleA_days += 1
        n_ruleA_same += int(myA == refA)
        if myA != refA:
            mism.append((pd.Timestamp(d).date(), "ルールA", myA, refA, ""))
        # 3. 上位 5 位（CSV は除外後の行だけなので、除外後の上位 5 で比べる）
        s_my = set(mine_kept.sort_values("rank_all").index[:5])
        s_ref = set(r.sort_values("rank_all").index[:5])
        jac.append(len(s_my & s_ref) / max(1, len(s_my | s_ref)))

    print(f"\n全体1位の一致: {n_rank1_same}/{n_rank1_days} = {n_rank1_same/n_rank1_days*100:.1f}%")
    print(f"ルールAの一致: {n_ruleA_same}/{n_ruleA_days} = {n_ruleA_same/n_ruleA_days*100:.1f}%"
          f"（CSV側でルールAあり {sum(1 for d in days if ((ref[ref['Date']==d]['rank_all']==1) & ref[ref['Date']==d]['strong']).any())}日）")
    print(f"上位5位(除外後)の Jaccard 平均: {np.mean(jac):.3f}（完全一致の日 {sum(1 for j in jac if j >= 0.999)}/{len(jac)}）")
    print(f"score 差の最大（同銘柄）: 中央 {np.median(score_diff):.4f} / 最大 {np.max(score_diff):.4f}")
    cd = pd.Series(n_cand_diff)
    print(f"候補数の差（自分−CSV・除外後）: 中央 {cd.median():.0f} / 最小 {cd.min()} / 最大 {cd.max()} / 0 の日 {(cd==0).sum()}/{len(cd)}")
    if mism:
        print(f"\n不一致 {len(mism)} 件:")
        for m in mism[:40]:
            print("  ", m)
    return 0


if __name__ == "__main__":
    sys.exit(main())
