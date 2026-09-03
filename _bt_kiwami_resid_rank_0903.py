# -*- coding: utf-8 -*-
"""_bt_kiwami_resid_rank_0903.py — 残差を「除外」でなく「並び順(選定ランク)」に使う（2026-09-03・本番無変更）。
残差5日の五分位は単調(Q1 +0.08%/47.6% → Q5 +0.43%/58.9%)だが除外は総額を減らす。
現行スコアは順位に予測力なし(8/6)ので、満杯日にどの3本を取るかの並び順を残差に替えたらどうなるか。
実行: python -X utf8 _bt_kiwami_resid_rank_0903.py
"""
import numpy as np, pandas as pd
_src = open("_bt_kiwami_resid_vol_0903.py", encoding="utf-8").read().split('print("\n" + "=" * 122); print("① 業種指数')[0]
exec(_src)
SCORE = C0["score"].to_numpy()
def run_rank(key, mask=BASE, pnl=None, exo=None, desc=True):
    """by_day の並びを key で置き換えて run2 と同じ選定→枠シム。"""
    pnl = pnl0 if pnl is None else pnl; exo = exo0 if exo is None else exo
    global by_day
    saved = by_day
    k = np.where(np.isfinite(key), key, -np.inf if desc else np.inf)
    by_day = {d: sorted(idx, key=(lambda i: -k[i]) if desc else (lambda i: k[i])) for d, idx in saved.items()}
    try:
        return stats(run2(mask, pnl, exo))
    finally:
        by_day = saved
rng = np.random.default_rng(0)
print(HDR); print(line("現行(スコア順)", st0))
print(line("ランダム順(seed0)", run_rank(rng.random(n))))
print(line("ランダム順(seed1)", run_rank(np.random.default_rng(1).random(n))))
print(line("残差5日 高い順(業種と一緒に下げた玉を先に)", run_rank(RES5, desc=True)))
print(line("残差5日 低い順(逆・固有下げを先に)", run_rank(RES5, desc=False)))
print(line("残差25MA 高い順", run_rank(RESD, desc=True)))
print(line("残差25MA 低い順(逆)", run_rank(RESD, desc=False)))
print(line("業種5日騰落 低い順(業種が最も下げた玉を先に)", run_rank(S_CHG5, desc=False)))
print(line("業種前日騰落 低い順", run_rank(S_CHG1, desc=False)))
print(line("銘柄5日騰落 低い順(深い下げ先)", run_rank(R5, desc=False)))
print(line("RSI風: スコア順のまま(参照)", run_rank(SCORE, desc=True)))
print(line("残差5日高い順×業種5日低い順(合成z)", run_rank(pd.Series(RES5).rank(pct=True).to_numpy() - pd.Series(S_CHG5).rank(pct=True).to_numpy(), desc=True)))
# 年別（残差5日 高い順 vs 現行）
def yearly(st_key=None):
    pass
print("\n[done]")
