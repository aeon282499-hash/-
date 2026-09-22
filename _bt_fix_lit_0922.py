# -*- coding: utf-8 -*-
"""_bt_fix_lit_0922.py — 文献の予言を実弾レッグに当てる。1分刻みの入口×出口。

■ 文献
Caminschi & Heaney (2014) "Fixing a Leaky Fixing", Journal of Futures Markets 34(11):1003-1039
  ・値決め【開始直後】に出来高と変動が跳ね、結果公表【前】に情報が漏れている
  ・開始後の数分の取引は値決め方向を高確率(一部90%超)で予測する
  ・【結果公表後には有意なリターンは無い】  ← これが今回の勝負どころ
Abrantes-Metz & Metz / LBMA Alchemist 73「London Bias」
  ・2001-2013のイントラデイ走査で2004年以降に系統的パターン・午後値決めで顕著・方向は下向き

■ 予言
公表後にリターンが無いなら、出口は「オークションが解けた直後」であるべきで、
そこから先を持つのは無駄どころか戻りを食らう。
実弾の1分解剖はこれと一致していた:
  AM 10:29 t+11.59 / 10:30 t+5.40 で稼ぎ、10:32 は t-4.60 で【戻す】
  PM 14:59 t+8.47 / 15:00 t+2.11 で稼ぎ、15:05 は t-2.55 で【戻す】
現行の出口は AM 10:35 / PM 15:05。

■ なぜ今まで見えなかったか
既存グリッド _bt_gold_fixgrid_0922.py は exit が range(625,676,5) ＝【5分刻み】。
10:31・10:32 は一度も評価されていない。ここを1分刻みで埋める。

規約は既存BTと完全に同一: Dukascopy M1・ロンドン時刻・SL$10・往復$0.21・
40日ゲート・AMは月曜ロット×2・円換算150。
"""
import pandas as pd, numpy as np

USD = 150.0; COST = 0.21; SL = 10.0
m1 = pd.read_pickle('_fx_xauusd_m1.pkl')
lon = m1.index.tz_localize('UTC').tz_convert('Europe/London')
df = m1.copy(); df['ld'] = lon.date; df['ltm'] = lon.time
DOW = None


def leg(s0, s1):
    a = pd.Timestamp(f'{s0//60:02d}:{s0%60:02d}').time()
    b = pd.Timestamp(f'{s1//60:02d}:{s1%60:02d}').time()
    w = df[(df.ltm >= a) & (df.ltm < b)]; g = w.groupby('ld')
    t = pd.DataFrame({'entry': g['open'].first(), 'exit': g['close'].last(),
                      'hi': g['high'].max(), 'n': g.size()})
    t = t[t.n >= (s1 - s0) * 0.6]
    t.index = pd.to_datetime(t.index)
    t = t[(t.index.dayofweek < 5) & (t.entry != t.exit)]
    gr = (t.entry - t.exit).where(~(t.hi >= t.entry + SL), -SL)
    return gr - COST


def gate(s, thr):
    return s.where((s.rolling(40).mean().shift(1) > thr).fillna(False), 0.0)


def yen(s, mon2):
    """円換算。mon2=True なら月曜だけロット2倍。"""
    w = s * USD
    if mon2:
        w = w * np.where(w.index.dayofweek == 0, 2.0, 1.0)
    return w


def score(s, thr, mon2):
    w = yen(gate(s, thr), mon2)
    a = w['2016':'2026']
    if (a != 0).sum() < 100:
        return None
    nz = a[a != 0]
    half = len(nz) // 2
    yr = nz.groupby(nz.index.year).sum()
    t = nz.mean() / nz.std() * np.sqrt(len(nz))
    top3 = nz.sort_values(ascending=False).iloc[3:]
    return dict(tot=a.sum(), y2=w['2025':'2026'].sum(), n=int((a != 0).sum()),
                t=t, t1=nz.iloc[:half].mean() / nz.iloc[:half].std() * np.sqrt(half),
                t2=nz.iloc[half:].mean() / nz.iloc[half:].std() * np.sqrt(len(nz) - half),
                wy=int((yr > 0).sum()), ny=len(yr), tt3=top3.sum())


CASES = [
    ('AM値決め 10:30', 630, 615, 635, 0.10, True,  range(605, 631), range(626, 651)),
    ('PM値決め 15:00', 900, 895, 905, 0.00, False, range(875, 901), range(896, 921)),
]

for name, fix, cur_a, cur_b, thr, mon2, ENT, EXT in CASES:
    print(f'\n{"="*104}\n===== {name} / 現行 {cur_a//60:02d}:{cur_a%60:02d}→{cur_b//60:02d}:{cur_b%60:02d} '
          f'(ゲート閾値{thr} / 月曜×2={mon2}) =====')
    cur = score(leg(cur_a, cur_b), thr, mon2)
    print(f'  現行: 11年{cur["tot"]:+,.0f}円 / 直近2年{cur["y2"]:+,.0f}円 / n={cur["n"]} / '
          f't={cur["t"]:.2f} / 前{cur["t1"]:+.1f}後{cur["t2"]:+.1f} / 勝ち年{cur["wy"]}/{cur["ny"]} / '
          f'上位3日除去{cur["tt3"]:+,.0f}円')

    R = []
    for a in ENT:
        for b in EXT:
            if b <= a:
                continue
            s = score(leg(a, b), thr, mon2)
            if s:
                s.update(a=a, b=b, hold=b - a)
                R.append(s)
    D = pd.DataFrame(R)
    NF = np.sqrt(2 * np.log(len(D)))
    print(f'  グリッド {len(D)}セル(1分刻み)・ノイズ床 {NF:.2f}')

    print(f'\n  【現行の入口 {cur_a//60:02d}:{cur_a%60:02d} のまま、出口だけ1分ずつ動かす】')
    print(f'  {"出口":>6s} {"保有":>4s} {"11年":>10s} {"直近2年":>9s} {"t":>6s} {"前/後":>10s} {"勝ち年":>6s} {"上3日除去":>10s}')
    sub = D[D.a == cur_a].sort_values('b')
    for _, r in sub.iterrows():
        if not (cur_b - 8 <= r.b <= cur_b + 10):
            continue
        mk = ' ←現行' if r.b == cur_b else ('  ★' if r.tot > cur['tot'] else '')
        print(f'  {int(r.b)//60:02d}:{int(r.b)%60:02d} {int(r.hold):3d}分 {r.tot:+10,.0f} {r.y2:+9,.0f} '
              f'{r.t:+6.2f} {r.t1:+4.1f}/{r.t2:+4.1f} {int(r.wy):3d}/{int(r.ny)} {r.tt3:+10,.0f}{mk}')

    best = D.sort_values('tot', ascending=False).iloc[0]
    print(f'\n  【グリッド全体の最良】{int(best.a)//60:02d}:{int(best.a)%60:02d}→{int(best.b)//60:02d}:{int(best.b)%60:02d} '
          f'({int(best.hold)}分)  11年{best.tot:+,.0f}円 (現行比 {best.tot-cur["tot"]:+,.0f}円 / '
          f'{(best.tot/cur["tot"]-1)*100:+.0f}%)')
    print(f'    直近2年{best.y2:+,.0f} / t={best.t:.2f} / 前{best.t1:+.1f}後{best.t2:+.1f} / '
          f'勝ち年{int(best.wy)}/{int(best.ny)} / 上位3日除去{best.tt3:+,.0f}')

    # 尾根か点か: 最良の周囲8セル
    nb = D[(D.a.between(best.a - 2, best.a + 2)) & (D.b.between(best.b - 2, best.b + 2)) &
           ~((D.a == best.a) & (D.b == best.b))]
    print(f'    周囲(±2分)の{len(nb)}セル: 中央値{nb.tot.median():+,.0f}円 / '
           f'現行超え{int((nb.tot>cur["tot"]).sum())}/{len(nb)} → '
          f'{"面(尾根)" if (nb.tot>cur["tot"]).mean()>0.6 else "点(単独スパイク)"}')

    print(f'\n  【現行を超えるセル】{int((D.tot>cur["tot"]).sum())}/{len(D)}')
    tp = D[D.tot > cur['tot']].sort_values('tot', ascending=False).head(8)
    for _, r in tp.iterrows():
        print(f'    {int(r.a)//60:02d}:{int(r.a)%60:02d}→{int(r.b)//60:02d}:{int(r.b)%60:02d} {int(r.hold):3d}分 '
              f'11年{r.tot:+9,.0f} 2年{r.y2:+8,.0f} t{r.t:+5.2f} 前{r.t1:+4.1f}/後{r.t2:+4.1f} '
              f'勝年{int(r.wy)}/{int(r.ny)} 上3除去{r.tt3:+9,.0f}')
    D.to_csv(f'_bt_fix_lit_{name[:2]}_0922.csv', index=False)
