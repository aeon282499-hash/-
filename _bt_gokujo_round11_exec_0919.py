# -*- coding: utf-8 -*-
"""_bt_gokujo_round11_exec_0919.py — 極上ラウンド11「執行と出口の未検証機構」（2026-09-19・R10の続き）
 G 建て値: シグナル日の引け(=price・14:50判定→引け成行の上限値)で建てる vs 翌寄り(現行) ／ 寄りが大きくGDした日は見送り(帯)
 H 分割利確: 半分を+2.5%/+3%で先に利確・残りは現行ルール
 I 勝ち玉の延長: 3日目引けで含み益≥+2%なら5日目まで延長(損切りは建値へ)
 K DAY1CUT閾値の再確認(0/0.5/1.0/1.5/2.0)
 L 乗せの時刻: 2日目寄り(現行) → 初日引け(14:55判定で引け成行)
土台＝極上新ルール 1×200＋同額乗せ・26年。判定=4分割すべて上回り＋DD/最悪年悪化なし。
実行: python -X utf8 _bt_gokujo_round11_exec_0919.py > _log_gokujo_round11_exec_0919.txt"""
import numpy as np, pandas as pd, json, time, pickle, re
src = open("_bt_gokujo_round6_26y.py", encoding="utf-8").read()
exec(src.split("# ── A 決算日近接 ──")[0])
r9 = open("_bt_gokujo_round9_capital_freq_0919.py", encoding="utf-8").read()
m = re.search(r"TICKv = C\.ticker\.to_numpy\(\).*?(?=\nprint\(\"\\n■ A 資金構成)", r9, re.S); exec(m.group(0))
PRICE0 = C.price.to_numpy(float)

def jl(r, b): return " ★" if (r["a"] > b["a"] and r["b"] > b["b"] and r["e3"] > b["e3"] and r["e4"] > b["e4"] and r["dd"] >= b["dd"] - 1 and r["worst"] >= b["worst"] - 1) else ""
def base_run(mask=None, mult=None, pnl=None, exo=None, size=2_000_000, addon_frac=1.0, key=None):
    return run_gen(G if mask is None else mask, SCORE if key is None else key, slots=1, size=size, addon_frac=addon_frac, mult=mult, pnl=pnl, exo=exo)
print("\n■ 土台（極上 新ルール・1×200＋同額乗せ・26年）", flush=True)
B0 = show("土台", base_run(), 400)

# ══ 汎用リプレイ: 建て値 Ev・分割利確・延長・DAY1CUT を1本で ══
def replay_full(Ev, tp=5.0, stop=3.0, hold=3, rsith=50.0, half_tp=None, extend=None, d1cut=1.0, start_k=0):
    """Ev=建て値。start_k=0: k=0(t+1)の足から評価（寄り建て）。start_k=-1: シグナル日引け建て＝k=0の寄りギャップも評価対象。
    half_tp: 半分を+half_tp%で先に利確（残り半分は通常ルール）→ 損益は加重平均。
    extend=(th, hold2): 最終日(hold-1)引けで含み益≥th%なら hold2 日目まで延長、延長中の損切りは建値(±0)。
    d1cut: 初日(k=0)引けが建値比 -d1cut% 以下なら k=1 寄りで処分。None=無し。
    戻り: pnl%, exo(手仕舞い日k), half_done(半分利確が発生したか)"""
    valid = np.isfinite(Ev) & (Ev > 0) & np.isfinite(CL[:, hold - 1]); pnl = np.full(n, np.nan); exo = np.zeros(n, dtype=np.int8); done = ~valid
    sl = Ev * (1 - stop / 100); tl = Ev * (1 + tp / 100); hl = Ev * (1 + half_tp / 100) if half_tp else None
    half = np.zeros(n, dtype=bool); half_pnl = np.zeros(n)
    def settle(mask, px_pct):
        # 残り玉の損益 px_pct(%) と半分利確を合成
        w = np.where(half[mask], 0.5, 1.0)
        pnl[mask] = w * px_pct + np.where(half[mask], 0.5 * half_pnl[mask], 0.0)
    for k in range(hold):
        live = ~done
        if k > 0 or start_k == -1:
            op = OP[:, k]; okop = np.isfinite(op) & (op > 0)
            g = live & okop & ((op <= sl) | (op >= tl)); settle(g, (op[g] - Ev[g]) / Ev[g] * 100); exo[g] = k; done |= g; live = ~done
            if d1cut is not None and k == 1:
                c1 = CL[:, 0]; m_ = live & okop & (c1 <= Ev * (1 - d1cut / 100)); settle(m_, (op[m_] - Ev[m_]) / Ev[m_] * 100); exo[m_] = k; done |= m_; live = ~done
        s = live & (LO[:, k] <= sl); settle(s, -stop); exo[s] = k; done |= s; live = ~done
        if hl is not None:
            h_ = live & ~half & (HI[:, k] >= hl); half[h_] = True; half_pnl[h_] = half_tp
        t_ = live & (HI[:, k] >= tl); settle(t_, tp); exo[t_] = k; done |= t_; live = ~done
        rc = (RS[:, k] >= rsith) & np.isfinite(RS[:, k]); last = (k == hold - 1)
        if extend is not None and last:
            th, hold2 = extend; ext = live & (CL[:, k] >= Ev * (1 + th / 100)) & np.isfinite(CL[:, hold2 - 1])
            r_ = live & (rc | last) & ~ext; settle(r_, (CL[r_, k] - Ev[r_]) / Ev[r_] * 100); exo[r_] = k; done |= r_
            # 延長: k+1..hold2-1 を建値損切りで
            for k2 in range(hold, hold2):
                live2 = ext & ~done; op = OP[:, k2]; okop = np.isfinite(op) & (op > 0)
                g = live2 & okop & (op <= Ev); settle(g, (op[g] - Ev[g]) / Ev[g] * 100); exo[g] = k2; done |= g; live2 = ext & ~done
                s2 = live2 & (LO[:, k2] <= Ev); settle(s2, 0.0); exo[s2] = k2; done |= s2; live2 = ext & ~done
                t2 = live2 & (HI[:, k2] >= tl); settle(t2, tp); exo[t2] = k2; done |= t2; live2 = ext & ~done
                f_ = live2 & (k2 == hold2 - 1); settle(f_, (CL[f_, k2] - Ev[f_]) / Ev[f_] * 100); exo[f_] = k2; done |= f_
        else:
            r_ = live & (rc | last); settle(r_, (CL[r_, k] - Ev[r_]) / Ev[r_] * 100); exo[r_] = k; done |= r_
    return pnl, exo, half

# 土台の再現チェック（replay_full(E0) == PNL1 ?）
p0, e0, _ = replay_full(E0)
print(f"[check] replay_full(寄り建て) と土台PNL1 の一致率 {np.nanmean(np.isclose(p0, PNL1, atol=1e-6)):.4f}（差の平均 {np.nanmean(p0-PNL1):+.4f}%）", flush=True)

def run_with(lab, Ev=None, size=2_000_000, cap=400, addon_frac=1.0, **kw):
    global E0
    Ev = E0 if Ev is None else Ev
    p, e, _ = replay_full(Ev, **kw)
    # run_gen は E0 で株数/乗せを計算するので Ev≠E0 の時は E0 を差し替えて回す
    E_save = E0.copy()
    try:
        E0[:] = Ev; R = base_run(pnl=p, exo=e, size=size, addon_frac=addon_frac)
    finally:
        E0[:] = E_save
    r = show(lab, R, cap); print(f"      判定:{jl(r, B0) or ' —'}", flush=True); return r

print("\n════ G 建て値（シグナル日引け vs 翌寄り）／寄りGD見送りの帯 ════", flush=True)
run_with("現行 翌寄り(寄指×1.01)")
run_with("シグナル日引けで建てる(14:50判定→引け成行の上限)", Ev=PRICE0, start_k=-1)
gap0 = OP[:, 0] / PRICE0 - 1
for th in (-1.0, -2.0, -3.0):
    m = G & ~(np.isfinite(gap0) & (gap0 * 100 <= th)); frac = 1 - m.sum() / G.sum()
    r = show(f"寄りGD {th:+.0f}%以下は見送り(除外{frac*100:.0f}%)", base_run(mask=m), 400); print(f"      判定:{jl(r, B0) or ' —'}")
for lo_, hi_ in ((-1.0, 0.0), (0.0, 1.0), (-3.0, -1.0), (-6.0, -3.0)):
    m = G & np.isfinite(gap0) & (gap0 * 100 > lo_) & (gap0 * 100 <= hi_)
    print(f"    寄りギャップ帯 ({lo_:+.0f},{hi_:+.0f}]  n={m.sum():>5} 件あたり{np.nanmean(PNL1[m]):+.2f}% 勝率{np.mean(PNL1[m]>0)*100:.1f}%")

print("\n════ H 分割利確（半分を先に利確・残りは現行） ════", flush=True)
for h in (2.0, 2.5, 3.0, 4.0):
    run_with(f"半分を+{h}%で利確", half_tp=h)

print("\n════ I 勝ち玉の延長（3日目引けで含み益≥th% → 5日目まで・損切りは建値） ════", flush=True)
for th in (0.0, 1.0, 2.0, 3.0):
    run_with(f"3日目引け≥+{th}%なら5日目まで延長", extend=(th, 5))
run_with("3日目引け≥+2%なら4日目まで延長", extend=(2.0, 4))

print("\n════ K DAY1CUT 閾値の再確認 ════", flush=True)
for x in (None, 0.0, 0.5, 1.0, 1.5, 2.0):
    run_with(f"DAY1CUT {'なし' if x is None else f'-{x}%'}", d1cut=x)

print("\n════ L 乗せの時刻: 2日目寄り(現行) → 初日引け ════", flush=True)
# 初日引け乗せ: 条件は同じ(初日終値>建値+1%)・乗せ値=初日終値(CL0)・手仕舞いは本玉と同時
def run_addon_close(lab, th=1.0, frac=1.0, size=2_000_000):
    R = base_run(addon_frac=0.0, size=size)      # 本玉だけ
    exit_px = E0 * (1 + PNL1 / 100); c1 = CL[:, 0]
    add_ok = (c1 > E0 * (1 + th / 100)) & (EXO1 >= 1) & np.isfinite(c1) & (c1 > 0)
    ii = R.i.to_numpy(); ok = add_ok[ii]; ash = np.where(ok, (size * frac / c1[ii] // 100 * 100), 0).astype(int)
    R = R.copy(); R["yen"] = R.yen + np.where(ok, (exit_px[ii] - c1[ii]) * ash, 0.0); R["expo"] = R.expo + ash * c1[ii]
    r = show(lab, R, 400); print(f"      判定:{jl(r, B0) or ' —'}", flush=True)
    R2 = base_run(addon_frac=1.0, size=size); o2 = OP[:, 1]
    print(f"      参考: 乗せ玉の建て値 初日引け{np.nanmean(c1[ii][ok]):,.0f} vs 2日目寄り{np.nanmean(o2[ii][ok]):,.0f}（引け→翌寄りの平均ギャップ {np.nanmean((o2[ii][ok]/c1[ii][ok]-1)*100):+.2f}%）")
run_addon_close("乗せを初日引けで(条件>+1%・同額)")
run_addon_close("乗せを初日引けで(条件>+0.5%・同額)", th=0.5)
run_addon_close("乗せを初日引けで(条件>+2%・同額)", th=2.0)
print(f"\n[done] {time.time()-t0:.0f}s")
