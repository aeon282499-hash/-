# -*- coding: utf-8 -*-
"""_test_masutan.py — masutan_signal.py（増担保「再点火」シグナル）のテスト（2026-09-27）
①連続日数の数え方(公表日の翌日から・切れたらリセット・25日未満は外) ②エピソード(穴3公表日以内は同じ) ③エピソード内1回
④代金フィルタ ⑤3日目の日付(祝日またぎ) ⑥ストップ高張り付き見送り ⑦帳簿の決済(ストップ安張り付きは翌寄り)
⑧webhook無しでも落ちない ⑨配信文面 ⑩未公表日は前の公表日で判定
実データのファイルは触らない（一時フォルダで実行）。
"""
import json, os, shutil, sys, tempfile, math
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import masutan_signal as MS

MS.CAL_FILE = os.path.join(HERE, "market_calendar.csv"); MS._TRADING = None
PASS = FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ok  {name}")
    else:
        FAIL += 1; print(f"  NG  {name}")


def tdays(start: str, n: int) -> list[str]:
    d = date.fromisoformat(start); out = []
    while len(out) < n:
        if MS.is_trading_day(d):
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


# ── ① calm_path ──
ds = tdays("2026-06-01", 40)
# 30日は100で横ばい→急騰で25日線から+30%超→公表日(start)以降に戻る
cl = [100.0] * 30 + [150.0, 150.0, 150.0] + [150.0] * 7
start = ds[31]
cp = MS.calm_path(cl, ds, start)
# 150が続くと25日線が上がって乖離が縮む。自前で乖離を計算して期待値を作る
def dev_at(i):
    m = sum(cl[i - 24:i + 1]) / 25
    return (cl[i] / m - 1) * 100
exp = None; c = 0
for i in range(32, 40):
    c = c + 1 if abs(dev_at(i)) < 15 else 0
    if c == 2 and exp is None:
        exp = ds[i]
check("①公表日の翌日から数え、±15%以内2日連続の最初の日がfirst_hit", cp["ok"] and cp["first_hit"] == exp)
cl2 = [100.0] * 30 + [100.0, 100.0, 100.0, 100.0]
ds2 = tdays("2026-06-01", 34)
cp2 = MS.calm_path(cl2, ds2, ds2[30])
check("①公表日当日は数えない（翌日+翌々日で2日目＝start+2）", cp2["first_hit"] == ds2[32])
cl3 = [100.0] * 26 + [100.0, 130.0, 100.0, 100.0, 100.0]
ds3 = tdays("2026-06-01", 31)
cp3 = MS.calm_path(cl3, ds3, ds3[25])
check("①±15%の外に出たらリセット（26→外→28,29で2日目=29）", cp3["first_hit"] == ds3[29])
cp4 = MS.calm_path([100.0] * 10, tdays("2026-06-01", 10), tdays("2026-06-01", 10)[3])
check("①25日線が無い期間は数えない（first_hit無し）", cp4["ok"] and cp4["first_hit"] is None)
check("①公表日の足が無ければ ok=False", MS.calm_path(cl, ds, "2026-01-05")["ok"] is False)

# ── ② エピソード ──
eps, closed = {}, []
MS.apply_flags(eps, "2026-09-01", {"1234": "003"})
MS.apply_flags(eps, "2026-09-02", {"1234": "003"})
MS.apply_flags(eps, "2026-09-07", {"1234": "004"})      # 9/2→9/7 は3営業日(9/3,9/4,9/7)＝同じ
check("②穴が3公表日以内なら同じエピソード（startは9/1のまま・区分は最新004）", eps["1234"]["start"] == "2026-09-01" and eps["1234"]["cls"] == "004")
MS.prune_episodes(eps, closed, "2026-09-10")             # 9/7→9/10 は3営業日＝まだ残る
check("②最後の公表日から3公表日以内なら残る", "1234" in eps)
MS.prune_episodes(eps, closed, "2026-09-11")             # 4営業日＝終了
check("②4公表日空いたら終了(closedへ)", "1234" not in eps and closed[-1]["code"] == "1234")
MS.apply_flags(eps, "2026-09-14", {"1234": "003"})
check("②空いた後の規制は新しいエピソード(start=9/14)", eps["1234"]["start"] == "2026-09-14")

# ── ⑤ 3日目の日付（祝日またぎ・2026-09-21〜23はシルバーウィーク）──
check("⑤9/18(金)シグナル→翌営業日9/24(木)の寄りで買い", MS.add_trading_days(date(2026, 9, 18), 1) == date(2026, 9, 24))
check("⑤9/18シグナル→3営業日後=9/28(月)の大引けで売り", MS.add_trading_days(date(2026, 9, 18), 3) == date(2026, 9, 28))
check("⑤年末: 12/29シグナル→翌営業日は12/30・3日目は1/5", MS.add_trading_days(date(2026, 12, 29), 1) == date(2026, 12, 30) and MS.add_trading_days(date(2026, 12, 29), 3) == date(2027, 1, 5))


# ── 偽のAPI（一時フォルダで run を通す）──
def bar(d, c, o=None, h=None, l=None, vo=200000.0, adj=1.0, ul="0", ll="0"):
    o = c if o is None else o; h = max(o, c) if h is None else h; l = min(o, c) if l is None else l
    return {"date": d, "O": o, "H": h, "L": l, "C": c, "AdjO": o * adj, "AdjC": c * adj, "Vo": vo, "UL": ul, "LL": ll}


def make_env(n_days=45, first="2026-07-01"):
    days = tdays(first, n_days)
    return days


tmp = tempfile.mkdtemp()
cwd = os.getcwd()
os.chdir(tmp)
try:
    days = tdays("2026-07-01", 45)
    # 銘柄A(1111): 横ばい→3日だけ急騰(25日線+25%超)→最後の急騰日が公表日→翌日から元の水準＝±15%以内が2日→シグナル。
    #   代金は十分（1000円×40万株=4億）。銘柄B(2222): 同じ形だが代金が少ない（1000円×5万株=0.5億）→ filtered
    startA = days[32]
    px = [1000.0] * 30 + [1300.0, 1300.0, 1300.0] + [1000.0] * 12
    def bars_for(code, upto, vol):
        out = []
        for i, d in enumerate(days):
            if d > upto:
                break
            out.append(bar(d, px[i], vo=vol))
        return out
    # シグナル日を計算
    cpA = MS.calm_path(px, days, startA)
    sig_day = cpA["first_hit"]
    check("(前提) 合成データでシグナル日ができる", sig_day is not None)

    def fake_alert(token, d):
        if d > sig_day:                       # シグナル日より先は「未公表」
            return 0, {}
        f = {}
        if d >= startA:
            f = {"1111": "003", "2222": "003"}
        return 300, f

    VOL = {"1111": 400000.0, "2222": 50000.0}
    def fake_bars(token, code, d_from, d_to):
        return bars_for(code, d_to.isoformat(), VOL.get(code, 200000.0))

    def fake_names(token, codes):
        return {c: {"name": f"テスト{c}", "mkt": "グロース", "mrgn": "信用"} for c in codes}

    posted = []
    def fake_post(embed, dry):
        posted.append(embed); return True

    os.environ.pop(MS.WEBHOOK_ENV, None)
    today = date.fromisoformat(sig_day)
    # 状態の初期化は days[20] 以降を読ませる
    MS._save(MS.STATE_FILE, {"last_pubdate": days[20], "episodes": {}, "closed": [], "sent": []})
    out = MS.run(today, token="x", fetch_alert_fn=fake_alert, fetch_bars_fn=fake_bars, fetch_names_fn=fake_names, post_fn=lambda e, d: MS.post_discord(e, d))
    sig = out.get("signals", [])
    check("③④代金十分の1111だけシグナル・2222は代金不足で filtered", [s["code"] for s in sig] == ["1111"]
          and any(r["code"] == "2222" and r["status"] == "filtered" for r in out["rows"]))
    check("⑧webhook未設定でも落ちずにJSON/帳簿を書く", os.path.exists(MS.SIG_FILE) and os.path.exists(MS.BOOK_FILE))
    book = MS._load(MS.BOOK_FILE, [])
    check("帳簿に pending で1件・買い=翌営業日・売り=3営業日後",
          len(book) == 1 and book[0]["status"] == "pending"
          and book[0]["entry_date"] == MS.add_trading_days(today, 1).isoformat()
          and book[0]["exit_date"] == MS.add_trading_days(today, 3).isoformat())
    check("株数: 30万/50万を100株単位（終値1000円→300株/500株）", book[0]["shares"] == {"300000": 300, "500000": 500})
    emb = out["embed"]; d = emb["description"]
    check("⑨文面: 寄りで買い・3日目の大引けで売り・30万/50万の株数・ストップ高見送り",
          "寄りで買い" in d and "大引けで売り" in d and "30万なら **300株**" in d and "50万なら **500株**" in d and "ストップ高に張り付いたら見送り" in d)
    check("⑨タイトルは翌営業日の日付と件数", emb["title"].startswith("🔥【増担 再点火】") and "1件" in emb["title"])
    st = MS._load(MS.STATE_FILE, {})
    check("⑩未公表日の手前で止め、最新の公表日=シグナル日で判定", st["last_pubdate"] == sig_day and out["latest_pubdate"] == sig_day)
    check("⑧webhook未設定でも sent を記録（同じ夜の2回目は何もしない）", today.isoformat() in st["sent"])
    again = MS.run(today, token="x", fetch_alert_fn=fake_alert, fetch_bars_fn=fake_bars, fetch_names_fn=fake_names, post_fn=fake_post)
    check("同じ日の2回目は skipped=sent", again.get("skipped") == "sent")

    # ③ 翌日: 同じエピソードではもう出ない（fired_before）
    nxt = MS.add_trading_days(today, 1)
    def fake_alert2(token, d):
        return (300, {"1111": "003", "2222": "003"}) if d <= nxt.isoformat() and d >= startA else ((300, {}) if d <= nxt.isoformat() else (0, {}))
    px[days.index(nxt.isoformat())] = 1000.0
    out2 = MS.run(nxt, token="x", fetch_alert_fn=fake_alert2, fetch_bars_fn=fake_bars, fetch_names_fn=fake_names, post_fn=fake_post)
    r1 = [r for r in out2["rows"] if r["code"] == "1111"][0]
    check("③エピソード内で2回目のシグナルは出ない（fired_before）", r1["status"] == "fired_before" and not out2["signals"])
    ev = out2["events"]
    check("⑦翌日の夜に建て(entry)が記帳される", any(e["kind"] == "entry" and e["code"] == "1111" for e in ev))
finally:
    os.chdir(cwd)
    shutil.rmtree(tmp, ignore_errors=True)

# ── ⑨' 値がさ株の株数表示（30万で100株に届かない）──
txt = MS._shares_text({"shares": {"300000": 0, "500000": 100}, "close_raw": 4290.0})
check("⑨値がさ株: 30万では100株に届かない（100株=約43万円）・50万なら100株", "30万では100株に届かない（100株=約43万円）" in txt and "50万なら **100株**" in txt)

# ── ⑥⑦ settle_book 単体 ──
bk = [{"code": "3333", "signal_date": "2026-09-01", "entry_date": "2026-09-02", "exit_date": "2026-09-04", "status": "pending",
       "close_sig_raw": 1000.0, "shares": {"300000": 300, "500000": 500}}]
bars_up = [bar("2026-09-01", 1000.0), bar("2026-09-02", 1300.0, o=1300.0, h=1300.0, l=1300.0)]   # 前日1000円の値幅は300円(1,000以上1,500未満)→1300で張り付き
ev = MS.settle_book(bk, {"3333": bars_up}, "2026-09-02")
check("⑥寄りがストップ高張り付き(寄り=高値=前日+値幅)なら見送り(skipped)", bk[0]["status"] == "skipped" and ev and ev[0]["kind"] == "skip")
bk2 = [{"code": "4444", "signal_date": "2026-09-01", "entry_date": "2026-09-02", "exit_date": "2026-09-04", "status": "pending",
        "close_sig_raw": 1000.0, "shares": {"300000": 300, "500000": 500}}]
b4 = [bar("2026-09-01", 1000.0), bar("2026-09-02", 1030.0, o=1010.0), bar("2026-09-03", 1050.0), bar("2026-09-04", 1060.0)]
MS.settle_book(bk2, {"4444": b4[:2]}, "2026-09-02")
check("⑦寄りで建てる(entry_raw=寄り値)", bk2[0]["status"] == "open" and bk2[0]["entry_raw"] == 1010.0)
ev2 = MS.settle_book(bk2, {"4444": b4}, "2026-09-04")
check("⑦3日目の大引けで手仕舞い・損益=(1060/1010-1)", bk2[0]["status"] == "closed" and abs(bk2[0]["pnl_pct"] - (1060 / 1010 - 1) * 100) < 1e-3
      and abs(bk2[0]["yen"]["500000"] - 500 * 1010 * (1060 / 1010 - 1)) <= 1)
bk3 = [{"code": "5555", "signal_date": "2026-09-01", "entry_date": "2026-09-02", "exit_date": "2026-09-04", "status": "open",
        "entry_adj": 1000.0, "entry_raw": 1000.0, "close_sig_raw": 1000.0, "shares": {"300000": 300, "500000": 500}}]
b5 = [bar("2026-09-01", 1000.0), bar("2026-09-02", 1000.0), bar("2026-09-03", 1000.0),
      bar("2026-09-04", 700.0, o=750.0, h=760.0, l=700.0), bar("2026-09-07", 880.0, o=870.0)]   # 前日1000円→値幅300円→700で張り付き
ev3 = MS.settle_book(bk3, {"5555": b5[:4]}, "2026-09-04")
check("⑦3日目の引けがストップ安張り付き→その日は決済しない", bk3[0]["status"] == "open")
ev3 = MS.settle_book(bk3, {"5555": b5}, "2026-09-07")
check("⑦翌日の寄りで処分(870円)", bk3[0]["status"] == "closed" and bk3[0]["exit_raw"] == 870.0 and "ストップ安" in bk3[0].get("exit_note", ""))

print(f"\n{PASS} PASS / {FAIL} FAIL")
sys.exit(1 if FAIL else 0)
