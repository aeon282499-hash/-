"""bt_breakout_catch.py — 「初動キャッチ＝大化け(右の裾)」が事前に拾えるかを正直に検証する。

ユーザ要望「10日で+30%、1か月で+100%超」のような爆益初動を、エントリー時点の特徴量で
**事前に絞れるか**を東証プライム全銘柄・2021-2026(437万標本)で測る。狙いは2つ:
  (1) 素の発生率(base rate): P(fwd10>=+30%) / P(fwd20>=+100%) 等がそもそも何%か。
  (2) どの特徴量がそれを何倍に持ち上げるか(lift)。ただし「裾が増える」だけでは不十分で、
      フラグした集団を **実際に買ったときの期待値**(平均/中央値/勝率/出口8日-12%実現リターン)
      まで併記する。裾は太いが勝率50%・中央値0のバーベルなら「損切り前提の宝くじ」と正直に書く。

設計思想(既存BTの先験): テーマトラッカーBTで「大化けは継続(乖離大×r20高×大商い×超ホット)から
出る/出遅れ思想とは逆/勝率は52%止まりのバーベル」と判明済み。本BTは母集団を138テーマ銘柄・
1年から **プライム全銘柄・5年** に広げ、その所見が普遍か、+30%/10日や+100%/月という極端な裾でも
成り立つかを確認する。エッジが無ければ「事前選別は不能=スクリーナー不採用」と正直に記す。

実行: python bt_breakout_catch.py

── 結果(2026-06-05・4652銘柄/431万標本2021-2026): 裾は増やせるが「買うと負ける」=不採用 ──
素の発生率: +30%/10日=0.640%・+50%/20日=0.479%・+100%/20日=0.088%(5年で3787件=10万分の88の宝くじ)。
裾のlift(継続系が最強): mom>=90で+30%/10日がP11.8%(×18.4)・+100%/20日がP1.5%(×17.2)、合成 dev>=15&r20>=25&vr>=2
が+30%/10日 P4.58%(×7.15)。乖離/r20/r5を上げるほど裾は太る一方、出遅れ(dev<=-10)/深押し反発は×2.8-3.4と弱い
=「大化けは継続(もう走ってる×乖離大×大商い)から」というテーマトラッカーBTの先験がプライム全銘柄5年でも再現。
だが【実際に買うと負ける】(本BTの核心・出口8日/-12%): 継続系は裾が太くても出口平均≒0〜マイナス・勝率34-43%・
中央値が大幅マイナス(mom>=90 出口-0.04%/勝率28.6%/fwd10中央-7.05%、合成 出口-0.45%/勝率36.0%/fwd10-0.98%、
r5>=20 出口+0.41%だが勝率38.9%/中央-3.04%)。同じ過熱銘柄が時々ロケット・頻繁に暴落する純バーベルで、平均は
ほぼゼロ。bt_momentum_core(グレードS将来弱い)/bt_fade_short(過熱はAVOID)と完全一致。逆に期待値で勝てるのは
継続でなく出遅れ側: dev25<=-10%(出口+2.18%/勝率56.1%/中央+1.29%)、深押し反発 r20<=-10&r5>=4(出口+1.88%/
勝率58.2%/中央+1.47%=既存の強反転シグナル)。ただしこれは「初動の爆益」でなく「8日で+2%級の堅実」。
年別: +30%/10日の裾liftは全年頑健(継続合成 各年3.4-7.7% vs base0.3-0.94%)=崩れるのは期待値であって裾ではない。
+100%/月は base0.088%・最大liftでもP1.6%=98%外れ→事前選別は実質不能(宝くじ)。
→ 結論: 「もう走ってる銘柄を買う初動キャッチ」は大化け率を2-18倍にできるが期待値マイナスの宝くじ。買い系
スクリーナーとしては不採用が誠実。爆益候補を出すなら『過去◯%が大化け/ただし勝率3-4割・中央値マイナス・
損切り-12%必須の宝くじ枠』と明示が条件。堅実な買いエッジは継続でなく既存の強反転/押し目(出遅れ)側にある。
"""
from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np

from signal_track import _rolling, EXIT_T, EXIT_SL
from momentum import MIN_HISTORY

TMAX = 21  # fwd20 を取るため

# 大化けターゲット: (ラベル, リターン列, 閾値)
TARGETS = [
    ("+15%/10日", "fwd10", 0.15),
    ("+30%/10日", "fwd10", 0.30),
    ("+30%/20日", "fwd20", 0.30),
    ("+50%/20日", "fwd20", 0.50),
    ("+100%/20日", "fwd20", 1.00),
]


def collect():
    c = pickle.load(open(Path(__file__).resolve().parent.parent / "jquants_cache.pkl", "rb"))
    data = c["all_data"]
    cols = {k: [] for k in ("mom", "rsi", "vr", "power", "r1", "r5", "r20", "dev25",
                            "year", "fwd10", "fwd20", "exitret")}
    nn = 0
    sl = EXIT_SL / 100.0
    for tk, df in data.items():
        if df is None or len(df) < MIN_HISTORY + TMAX + 1:
            continue
        df = df.sort_index()
        R = _rolling(df)
        idx = R.index
        close = R["close"].to_numpy(dtype=float)
        low = df["Low"].astype(float).to_numpy()
        mom = R["momentum"].to_numpy(dtype=float)
        rsi = R["rsi"].to_numpy(dtype=float)
        vr = R["vr"].to_numpy(dtype=float)
        power = R["power"].to_numpy(dtype=float)
        r1 = R["r1"].to_numpy(dtype=float)
        r5 = R["r5"].to_numpy(dtype=float)
        r20 = R["r20"].to_numpy(dtype=float)
        ma25 = R["close"].rolling(25).mean().to_numpy(dtype=float)
        n = len(close)
        nn += 1
        for p in range(MIN_HISTORY - 1, n - 1 - TMAX):
            ep = close[p]
            if ep <= 0 or np.isnan(ep):
                continue
            if (np.isnan(mom[p]) or np.isnan(rsi[p]) or np.isnan(vr[p]) or np.isnan(power[p])
                    or np.isnan(r5[p]) or np.isnan(r20[p]) or np.isnan(r1[p]) or np.isnan(ma25[p])):
                continue
            x10 = close[p + 10]
            x20 = close[p + 20]
            if np.isnan(x10) or np.isnan(x20):
                continue
            f10 = x10 / ep - 1.0
            f20 = x20 / ep - 1.0
            # 出口8日/-12%(利確なし)の実現リターン＝買い系の確定policyと同じ
            stop = ep * (1.0 - sl)
            if (low[p + 1: p + 1 + EXIT_T] <= stop).any():
                ex = -sl
            else:
                xc = close[p + EXIT_T]
                ex = (xc / ep - 1.0) if not np.isnan(xc) else f10
            cols["mom"].append(mom[p]); cols["rsi"].append(rsi[p]); cols["vr"].append(vr[p])
            cols["power"].append(power[p]); cols["r1"].append(r1[p]); cols["r5"].append(r5[p])
            cols["r20"].append(r20[p]); cols["dev25"].append((ep / ma25[p] - 1.0) * 100.0)
            cols["year"].append(idx[p].year)
            cols["fwd10"].append(f10); cols["fwd20"].append(f20); cols["exitret"].append(ex)
    A = {k: np.asarray(v, dtype=np.float32) for k, v in cols.items()}
    A["year"] = A["year"].astype(int)
    print(f"集約: {nn}銘柄  全標本 {A['mom'].size:,}  年 {sorted(set(A['year'].tolist()))}")
    return A


def hit_rate(A, mask, retcol, thr):
    ret = A[retcol] if mask is None else A[retcol][mask]
    if ret.size == 0:
        return 0.0, 0
    return float((ret >= thr).mean() * 100), int(ret.size)


def evset(A, mask):
    """フラグ集団を買ったときの実態: 出口8日-12%の平均/中央/勝率 と fwd10平均/中央。"""
    ex = A["exitret"] if mask is None else A["exitret"][mask]
    f10 = A["fwd10"] if mask is None else A["fwd10"][mask]
    if ex.size == 0:
        return None
    return dict(n=ex.size,
                ex_avg=float(ex.mean() * 100), ex_med=float(np.median(ex) * 100),
                ex_win=float((ex > 0).mean() * 100),
                f10_avg=float(f10.mean() * 100), f10_med=float(np.median(f10) * 100))


def by_year_hit(A, mask, retcol, thr):
    yr = A["year"] if mask is None else A["year"][mask]
    ret = A[retcol] if mask is None else A[retcol][mask]
    out = []
    for y in sorted(set(yr.tolist())):
        m = yr == y
        if m.sum() < 200:
            continue
        out.append(f"{y}:{(ret[m] >= thr).mean()*100:.2f}%")
    return " ".join(out)


def main():
    A = collect()
    bn = A["mom"].size

    print("\n========== 0) 大化けの素の発生率（全標本ベースライン） ==========")
    base = {}
    for tag, col, thr in TARGETS:
        r, n = hit_rate(A, None, col, thr)
        base[(col, thr)] = r
        print(f"  P({tag:<11s}) = {r:6.3f}%   （{int(n*r/100):,}件 / {n:,}）")

    mom = A["mom"]; rsi = A["rsi"]; vr = A["vr"]; power = A["power"]
    r5 = A["r5"]; r20 = A["r20"]; r1 = A["r1"]; dev = A["dev25"]
    cands = [
        ("継続: r20>=20%", r20 >= 20),
        ("継続: r20>=40%", r20 >= 40),
        ("継続: r5>=10%", r5 >= 10),
        ("継続: r5>=20%", r5 >= 20),
        ("乖離: dev25>=10%", dev >= 10),
        ("乖離: dev25>=20%", dev >= 20),
        ("大商い: vr>=2", vr >= 2),
        ("大商い: vr>=3", vr >= 3),
        ("グレードS: mom>=80", mom >= 80),
        ("超過熱: mom>=90", mom >= 90),
        ("買われすぎ: rsi>=75", rsi >= 75),
        ("継続合成: dev>=15 & r20>=25 & vr>=2", (dev >= 15) & (r20 >= 25) & (vr >= 2)),
        ("噴き上げ: r5>=15 & vr>=2.5", (r5 >= 15) & (vr >= 2.5)),
        ("S延伸: mom>=80 & r5>=10", (mom >= 80) & (r5 >= 10)),
        ("(対照)出遅れ: dev25<=-10%", dev <= -10),
        ("(対照)深押し反発: r20<=-10 & r5>=4", (r20 <= -10) & (r5 >= 4)),
    ]

    for tgt_col, tgt_thr, tgt_tag in [("fwd10", 0.30, "+30%/10日"), ("fwd20", 0.50, "+50%/20日"),
                                       ("fwd20", 1.00, "+100%/20日")]:
        b = base[(tgt_col, tgt_thr)]
        print(f"\n========== 1) 各特徴量は『{tgt_tag}』の発生率を何倍にするか（base {b:.3f}%） ==========")
        for tag, m in cands:
            r, n = hit_rate(A, m, tgt_col, tgt_thr)
            frac = 100.0 * n / bn
            lift = (r / b) if b > 0 else float("inf")
            print(f"  {tag:<34s} n{n:>8,d}({frac:4.1f}%) P{r:6.3f}% lift×{lift:4.2f}")

    print("\n========== 2) フラグ集団を【実際に買う】とどうなるか（出口8日/-12%・利確なし） ==========")
    print("   ※裾(大化け率)が増えても、買って勝てるか＝平均/中央値/勝率/出口実現で正直に見る")
    eb = evset(A, None)
    print(f"  {'全銘柄(ベースライン)':<34s} n{eb['n']:>8,d} 出口平均{eb['ex_avg']:+5.2f}% 中央{eb['ex_med']:+5.2f}% "
          f"勝率{eb['ex_win']:4.1f}% | fwd10平均{eb['f10_avg']:+5.2f}% 中央{eb['f10_med']:+5.2f}%")
    for tag, m in cands:
        s = evset(A, m)
        if not s or s["n"] < 100:
            print(f"  {tag:<34s} 標本不足"); continue
        print(f"  {tag:<34s} n{s['n']:>8,d} 出口平均{s['ex_avg']:+5.2f}% 中央{s['ex_med']:+5.2f}% "
              f"勝率{s['ex_win']:4.1f}% | fwd10平均{s['f10_avg']:+5.2f}% 中央{s['f10_med']:+5.2f}%")

    print("\n========== 3) 年別の頑健性（『+30%/10日』発生率・上げ相場依存でないか） ==========")
    for tag, m in [("継続合成: dev>=15 & r20>=25 & vr>=2", (dev >= 15) & (r20 >= 25) & (vr >= 2)),
                   ("噴き上げ: r5>=15 & vr>=2.5", (r5 >= 15) & (vr >= 2.5)),
                   ("グレードS: mom>=80", mom >= 80)]:
        print(f"  {tag}")
        print(f"     base   : {by_year_hit(A, None, 'fwd10', 0.30)}")
        print(f"     flagged: {by_year_hit(A, m, 'fwd10', 0.30)}")

    print("\n判定基準: 『初動キャッチ(爆益スクリーナー)』採用は:"
          "\n  ①フラグ集団の大化け率が base比で明確に高い(lift>=~2) かつ ②年別でほぼ全年その傾向 "
          "かつ\n  ③買った実態(出口平均/勝率)がベースライン以上(=裾だけでなく期待値も負けない)。"
          "\n①だけ満たし②③が崩れるなら『損切り前提のバーベル/宝くじ』と正直表示に留める。"
          "\n+100%/月のように base が極小(<0.1%級)なら『事前選別は実質不能』と明記する。")


if __name__ == "__main__":
    main()
