# -*- coding: utf-8 -*-
"""_test_kiwami_vi.py — 玉サイズ傾斜（kiwami_vi）の単体テスト（本番I/Oなし・Discord送信なし・J-Quants呼び出しなし）。
実行: python -X utf8 _test_kiwami_vi.py"""
import json, os, sys, tempfile
from datetime import date
ok_n = 0; ng = []
def t(name, cond):
    global ok_n
    if cond: ok_n += 1
    else: ng.append(name)

import kiwami_vi as KV
# 倍率
t("VI 25→1.3", KV.size_mult(25.0) == 1.3)
t("VI 20→1.3(境界)", KV.size_mult(20.0) == 1.3)
t("VI 19.9→1.0", KV.size_mult(19.9) == 1.0)
t("VI 15→0.7(境界)", KV.size_mult(15.0) == 0.7)
t("VI 15.1→1.0", KV.size_mult(15.1) == 1.0)
t("VI None→1.0", KV.size_mult(None) == 1.0)
t("VI nan→1.0", KV.size_mult(float("nan")) == 1.0)
t("VI 40→1.3(25超で増やさない)", KV.size_mult(40.0) == 1.3)
# 有効化フラグ
os.environ.pop("KIWAMI_VI_TILT", None); t("未設定→無効", not KV.enabled())
os.environ["KIWAMI_VI_TILT"] = "1"; t("1→有効", KV.enabled())
os.environ["KIWAMI_VI_TILT"] = "0"; t("0→無効", not KV.enabled())

cwd = os.getcwd()
with tempfile.TemporaryDirectory() as d:
    os.chdir(d)
    try:
        # update(): fetch_vi をスタブ
        KV.fetch_vi = lambda token, sd: (date(2026, 9, 2), 26.2)
        os.environ["KIWAMI_VI_TILT"] = "0"
        info = KV.update(date(2026, 9, 3), date(2026, 9, 4), token="x")
        t("OFF: VIは記録・mult=1.0", info["vi"] == 26.2 and info["mult"] == 1.0 and not info["enabled"])
        os.environ["KIWAMI_VI_TILT"] = "1"
        info = KV.update(date(2026, 9, 3), date(2026, 9, 4), token="x")
        t("ON: VI26.2→mult1.3", info["mult"] == 1.3 and info["vi_date"] == "2026-09-02")
        t("kiwami_vi.json 保存", json.load(open(KV.STATE_FILE, encoding="utf-8"))["for"] == "2026-09-04")
        t("load 対象日一致", KV.load(date(2026, 9, 4))["mult"] == 1.3)
        t("load 別日→1.0", KV.load(date(2026, 9, 7))["mult"] == 1.0)
        KV.fetch_vi = lambda token, sd: None
        info = KV.update(date(2026, 9, 3), date(2026, 9, 4), token="x")
        t("VI取得なし→mult1.0", info["mult"] == 1.0 and "取得なし" in info["note"])
        def _boom(token, sd): raise RuntimeError("api down")
        KV.fetch_vi = _boom
        info = KV.update(date(2026, 9, 3), date(2026, 9, 4), token="x")
        t("VI取得失敗→mult1.0で継続", info["mult"] == 1.0 and "失敗" in info["note"])
        # 配信行
        s = KV.line({"vi": 26.2, "vi_date": "2026-09-02", "mult": 1.3, "enabled": True}, 1_000_000)
        t("配信行 130万", "130万円" in s and "高ボラ" in s)
        s = KV.line({"vi": 12.0, "vi_date": "2026-09-02", "mult": 0.7, "enabled": True}, 1_000_000)
        t("配信行 70万", "70万円" in s and "低ボラ" in s)
        s = KV.line({"vi": 26.2, "vi_date": "2026-09-02", "mult": 1.0, "enabled": False}, 1_000_000)
        t("OFF表示", "表示のみ" in s)
        # record_signals: 記帳サイズと値がさカットが傾斜後
        import shadow_exit as SE
        os.environ["KIWAMI_VI_TILT"] = "1"
        KV.fetch_vi = lambda token, sd: (date(2026, 9, 2), 26.2)
        KV.update(date(2026, 9, 3), date(2026, 9, 4), token="x")
        json.dump({"date": "2026-09-04", "signals": [
            {"ticker": "1111.T", "name": "A", "direction": "BUY", "prev_close": 12000.0, "limit_price": 12120},
            {"ticker": "2222.T", "name": "B", "direction": "BUY", "prev_close": 2000.0, "limit_price": 2020}]},
            open(SE.KIWAMI_SIG_FILE, "w", encoding="utf-8"), ensure_ascii=False)
        n = SE.record_signals("main", date(2026, 9, 4), {})
        rows = SE.load_ledger("main")
        t("値がさ1.2万円は130万玉(上限1.3万)なら記帳される", any(r["ticker"] == "1111.T" for r in rows))
        t("記帳サイズ=130万・vi_mult=1.3", all(r["size"] == 1_300_000 and r.get("vi_mult") == 1.3 for r in rows))
        # OFF なら従来どおり（上限1万円で1111.Tは見送り・size=100万）
        for f in (SE.TIER_FILES["main"][0], "shadow_exit_main.json"):
            if os.path.exists(f): os.remove(f)
        os.environ["KIWAMI_VI_TILT"] = "0"; KV.update(date(2026, 9, 3), date(2026, 9, 4), token="x")
        n = SE.record_signals("main", date(2026, 9, 4), {})
        rows = SE.load_ledger("main")
        t("OFF: 1111.Tは値がさ見送り・2222.Tは100万", [r["ticker"] for r in rows] == ["2222.T"] and rows[0]["size"] == 1_000_000)
    finally:
        os.chdir(cwd)
print(f"\n==== 結果: {ok_n}/{ok_n + len(ng)} OK ====")
if ng: print("NG:", ng); sys.exit(1)
print("ALL PASS")
