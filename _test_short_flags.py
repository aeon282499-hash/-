# -*- coding: utf-8 -*-
"""_test_short_flags.py — 極上の記録専用: 大口空売り報告「直近に増えた」フラグ（売買には使わない・2026-10-28）"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import screener, shadow_exit as SE
ok = fail = 0
def check(lab, cond):
    global ok, fail
    print(("  ok  " if cond else "  NG  ") + lab); ok += bool(cond); fail += (not cond)
screener._jquants_id_token = lambda: "x"
data = {"data": [
    {"DiscDate": "2026-10-01", "ShrtPosToSO": 0.0120, "PrevRptRatio": 0.0100},   # シグナル日(10/02)の前日に増
    {"DiscDate": "2026-10-02", "ShrtPosToSO": 0.0150, "PrevRptRatio": 0.0120},   # シグナル日当日＝数えない(BTと同じ)
    {"DiscDate": "2026-09-24", "ShrtPosToSO": 0.0090, "PrevRptRatio": 0.0110},   # 8日前の減
]}
screener._jquants_get = lambda path, tok, params=None: data
f = SE.short_sale_flags("2801.T", "2026-10-05")
check(f"直近5日/7日に増あり: {f}", f.get("ss_inc5") is True and f.get("ss_inc7") is True and f.get("ss_n7") == 1)
data["data"] = data["data"][1:]
f = SE.short_sale_flags("2801.T", "2026-10-05")
check(f"当日公表と8日前だけ→該当なし: {f}", f.get("ss_inc5") is False and f.get("ss_inc7") is False)
def boom(*a, **k): raise RuntimeError("API down")
screener._jquants_get = boom
check("APIが落ちても {} を返す(台帳は止めない)", SE.short_sale_flags("2801.T", "2026-10-05") == {})
print(f"\n{ok} PASS / {fail} FAIL"); sys.exit(1 if fail else 0)
