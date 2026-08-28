# -*- coding: utf-8 -*-
"""shadow_exit.py — 出口ボラ正規化の「紙並走」台帳（2026-07-25・本人指示=案2）。

■ 何をするものか
  本番の損切りは今まで通り一律 -3%（配信も帳簿も発注も一切変えない）。
  そのすぐ横で「もし損切りを ATR%×2.0（下限2.0%）にしていたら、この玉はどうなっていたか」
  だけを別台帳に記録し続ける。数ヶ月ぶん貯めてから、実績と突き合わせて採否を決める。

■ 根拠（10年BT・2026-07-25）
  損切りを銘柄の値動きの大きさに比例させると、大100万で10年 +234.5万 → +345.3万、
  最悪3年 -116.2万 → -78.1万。勝ち7/10年。同じ平均幅の「固定%」では逆に現行より悪化する
  ＝効いているのは幅ではなく「銘柄ごとに変えること」自体。検証は _bt_atr_exit_*.py 群。
  ただし2025年は現行の方が良かった（+104.9 vs +87.9万）ため、実物を見てから決める。

■ 非破壊の担保
  ・本ファイルは positions*.json / today_signals*.json を **読むだけ**。書くのは shadow_exit_*.json のみ。
  ・main.py 側は末尾で try/except に包んで呼ぶだけ（例外は握り潰す＝朝の配信を絶対に止めない）。
  ・Discord配信・発注指示・実帳簿には一切関与しない。

■ 限界（結果を読むときの注意）
  記録するのは「本番が実際に採用した玉」だけ。出口が変われば決済日がズレ、空く枠も変わるので
  本来は選ぶ銘柄自体が変わる（10年BTでは2026年で134件→130件）。この台帳が測れるのは
  “同じ玉を違う損切りで持ったらどうだったか”＝**出口単独の効果**であって、BTの差分とは一致しない。

実行:
  python -X utf8 shadow_exit.py --report     # 実績との突き合わせを表示
  python -X utf8 shadow_exit.py --backfill   # 既存 positions から過去分を再構成（初回のみ）
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pandas as pd

import close_decisions

# ── 検証で確定したパラメータ（BT: _bt_atr_exit_kt.py の最終形）───────────────
ATR_MULT      = 2.0     # 損切り幅 = ATR% × これ
STOP_FLOOR    = 2.0     # 下限%（これ未満は板の値幅の中でBTが約定を再現できない）
STOP_CEIL     = 15.0    # 暴走防止のサニティ上限（入口ATRキャップ3.0%なので実質6%が最大）
TAKE_PROFIT   = 5.0     # 利確は現行のまま（BTで「TP据置が最良」と確定）
MAX_HOLD      = 3
ATR_PERIOD    = 14
LIVE_STOP     = 3.0     # 一律損切り（通常版と同じ値）
# 買いの損切りにATR連動を使うか（2026-07-29にFalseへ）。理由は shadow_stop_pct の docstring。
# 要点: 採用検証が枠5・株数丸めなしだった／実運用の枠3では一律-3%に負ける（年-6万・勝ち年-2）／
#       機構はATRが損を「減らす」のでなく損切り箱から期限箱へ「移す」だけで円が完全に閉じる。
# Trueに戻す場合は同時に MAX_SLOTS を5以上にすること（枠3のままだと劣化する）。
USE_ATR_STOP  = False

# 同時保有の上限（2026-07-27 本人決定・_bt_kiwami_size.py の10年検証）:
# 3枠が山。100万×3枠(300万)で10年+270万＝資本比+90%・年利+18.0%・勝ち9/10年・DD-93万。
# 通常版の5枠(500万)は資本比+53%・年利+11.2%・勝ち8/10年なので、同じ資金なら3枠が上。
# 2枠に絞ると勝ち7/10年・DD-40%台に劣化＝絞りすぎは崖。4枠以上は資本効率が落ちる。
# 枠が埋まっている日のシグナルは見送る（＝通常版には出るが極みには入らない）。
MAX_SLOTS     = 3

# ── 極みの1玉サイズ（2026-08-25 本人決定・150万→100万へ戻し / 買い売り両方）─────
# 8/24の150万化は現金バッファ80万に対し攻めすぎ（フェード最悪月-45.5万＋極み最悪月-40.5万の
# 同月直撃-85万>80万）なので1日で撤回。150万時代(8/25分のみ)はシグナル0件＝150万玉は実在しない。
# ★復帰ライン: 現金バッファ120万到達で 1_500_000 へ戻す（2026-08-25 本人合意・戻すのはこの1行。
#   期待値どおりなら11月頃。地雷側は現金40万割れでフェード各70万へ一時縮小＝daytrade_paper側）。
# 買いBTは%ベースでサイズに線形（100万=10年+311万/最悪年-37.8万・150万=+466万/-56.7万）。
# 台帳には記帳時の size を刻む。size の無い旧玉は LEGACY_SIZE=100万で円換算。
# 候補集合はサイズ非連動なので選定・銘柄は一切変わらない。
KIWAMI_SIZE = 1_000_000
LEGACY_SIZE = 1_000_000   # size未記録の旧玉（2026-08-23以前）の円換算用

# ── 値がさカット（2026-08-24 実測で追加・BT公式ベースと本番を一致させる）─────────
# BT公式(dc1.2×1万円)は株価1万円超を除外して測られているが、本番には上限が無く、
# しかも株数計算の max(100株) 床が「100株で予算超過」の買い（例: 4.6万円株=469万）を
# 作り得た。150万実構成で実測: BT公式(≤1万円)+478.4万 / ≤1.5万円+467.5万 /
# 上限なし+100株床=+364.7万(-113.7万・前半マイナス転落・予算超過玉46個/10年)。
# ＝1万円カットが正しい。サイズ非連動の明示定数にする（サイズ変更で帯がずれない）。
KIWAMI_PX_CAP = 10_000


def kiwami_px_cap(key: str) -> int:
    """値がさカット＝100株が1玉サイズに収まる上限（大1万円/中5千円/小3千円）。"""
    return KIWAMI_PX_CAP if key == "main" else TIER_FILES[key][2] // 100

# ── 極みの売り（2026-07-29 実装・_bt_sell_improve.py の8軸グリッド）─────────────
# 踏み上げ損切りを通常版の+3.0%から+2.5%へ。10年・150万×3枠・業種cap2・スコア降順の
# 円シムで PF1.54→1.60・10年+109.5万→+116.9万・勝ち年5/10→7/10、**両期間とも改善**
# （前半+42.9→+46.5万 / 後半+66.6→+70.5万）。
# ただし面は滑らかでない（+3.5%が谷で+2.5%と+4.0%が両方良い）＝効果は年+0.7万と小さく、
# 「採ってよいが期待しすぎない」水準。他7軸（利確TP×保有日数/前日比/RSI/乖離/ATR上限/
# 売買代金/地合いゲート強度）はすべて棄却＝現行がピンポイントで正しい位置にある。
# 特にATR上限2.5→3.0でPF0.91に転落、ゲート撤廃でPF0.97＝この2つは崖。
SELL_STOP_PCT  = 2.5    # 極みだけ。通常版は tracker.STOP_LOSS=3.0 のまま（触らない）
SELL_MAX_SLOTS = 3      # 買いと独立の3枠（BT公式+116.9万は150万×3枠シム＝100万玉は×2/3で年+7.8万）
KIWAMI_SELL_LEDGER = "kiwami_sell.json"
SELL_SIG_FILE      = "today_sell_signals.json"    # 大資金のみ（NOTIFY_KEYS=("main",)と同方針）

TIER_FILES = {
    "main":  ("today_signals.json",        "positions.json",        1_000_000, "大資金"),
    "mid":   ("today_signals_mid.json",    "positions_mid.json",      500_000, "中資金"),
    "small": ("today_signals_small.json",  "positions_small.json",    300_000, "小資金"),
}


def ledger_path(key: str) -> str:
    return f"shadow_exit_{key}.json"


def load_ledger(key: str) -> list[dict]:
    p = ledger_path(key)
    if not os.path.exists(p):
        return []
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_ledger(key: str, rows: list[dict]) -> None:
    with open(ledger_path(key), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def atr_pct_at(df: pd.DataFrame, upto: str) -> float | None:
    """シグナル日(=エントリー前日)までのデータで ATR(14)/終値×100 を返す。screener.calc_atr と同式。"""
    if df is None or df.empty:
        return None
    d = df[df.index.strftime("%Y-%m-%d") <= upto]
    if len(d) < ATR_PERIOD + 2:
        return None
    high, low, close = d["High"], d["Low"], d["Close"]
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    atr = float(tr.rolling(ATR_PERIOD).mean().iloc[-1])
    last = float(close.iloc[-1])
    if not (atr > 0) or not (last > 0):
        return None
    return round(atr / last * 100, 3)


def shadow_stop_pct(atr_pct: float | None) -> float:
    """買いの損切り幅%。

    2026-07-29: **ATR連動を取り下げて一律3.0%へ戻した**（USE_ATR_STOP=False）。
    採用時の検証は枠5・株数丸めなしで回っていたが、実運用の枠3・実株数で測り直すと逆転する:
        3枠（実運用）  一律-3% 年+23.5万・勝ち9/10  ／ ATR×2.0 年+17.6万・勝ち7/10
        5枠（検証時）  一律-3% 年+26.9万・勝ち8/10  ／ ATR×2.0 年+33.3万・勝ち8/10
    枠を1〜10で振るとATRの優劣が符号反転する（2勝ち/3負け/4勝ち/5勝ち/6負け…）＝面が高原でない。

    機構も判明した。ATRは損を減らしておらず、**決済の箱を移しているだけ**（10年・3枠×100万）:
        損切り     -771万 → -413万   +358万（改善）
        RSI/期限   +776万 → +345万   -431万（悪化）← 広い損切りで生き延びた負け玉が期限で沈む
        利確       +344万 → +342万     -2万
        寄りギャップ -114万 →  -97万    +17万
                                    ────────
                                       -58万  ← 実測差59万と一致
    さらに損切り幅の分布は最小2.00/中央4.44/最大6.00%で**92%が-3%より深い**＝
    「銘柄ごとに変える」建て付けだが実態はほぼ一律に広げているのと変わらない。

    復活させるなら USE_ATR_STOP=True に戻すだけ（ATR_MULT等の定数はそのまま残してある）。
    その場合は枠5以上で運用すること＝枠3では上記のとおり負ける。
    """
    if not USE_ATR_STOP or atr_pct is None:
        return LIVE_STOP
    return round(min(max(ATR_MULT * atr_pct, STOP_FLOOR), STOP_CEIL), 2)


def _prev_trading_row_date(df: pd.DataFrame, entry_date_str: str) -> str | None:
    """エントリー日の直前の営業日（＝シグナル日）を価格データから引く。"""
    d = df[df.index.strftime("%Y-%m-%d") < entry_date_str]
    return d.index[-1].strftime("%Y-%m-%d") if len(d) else None


KIWAMI_SIG_FILE = "today_signals_kiwami.json"   # 買残回転1.2で選定した極み専用シグナル（2026-08-02）


def record_signals(key: str, today: date, all_data: dict) -> int:
    """本番が今朝採用したBUYシグナルを影台帳に取り込む（同一(ticker,signal_date)は再登録しない）。"""
    sig_file = TIER_FILES[key][0]
    # 極み(main)は買残回転1.2の専用ファイルを優先（2026-08-02・10年+194万→+311万の採用）。
    # 当日分が無ければ従来の today_signals.json へフォールバック＝移行期・障害時も取りこぼさない。
    # 2026-08-28: 中/小も極み選定を使う（本人指示）。値がさカットだけ資金別。
    if os.path.exists(KIWAMI_SIG_FILE):
        try:
            with open(KIWAMI_SIG_FILE, encoding="utf-8") as f:
                _kp = json.load(f)
            if _kp.get("date") == today.strftime("%Y-%m-%d"):
                sig_file = KIWAMI_SIG_FILE
        except Exception as e:
            print(f"[shadow-{key}] 極みシグナル読込失敗({e}) → 通常版にフォールバック")
    if not os.path.exists(sig_file):
        return 0
    with open(sig_file, encoding="utf-8") as f:
        payload = json.load(f)
    if payload.get("date") != today.strftime("%Y-%m-%d"):
        return 0                                  # 今日のファイルでなければ何もしない

    rows = load_ledger(key)
    seen = {(r["ticker"], r["signal_date"]) for r in rows}
    # 極みは損切りが広い分だけ通常版より長く持つことがある。通常版が先に決済して同じ銘柄に
    # 再シグナルを出しても、極みがまだ保有中なら二重に建ててはいけない（実弾で回す以上、
    # 同一銘柄の重複建ては通常版と同じく禁止・2026-07-26）。
    still_open = {r["ticker"] for r in rows if r.get("status") in ("pending", "open")}
    slots_used = len(still_open)
    added = 0
    skipped: list[str] = []
    for s in payload.get("signals", []):
        if s.get("direction") != "BUY":           # SELLは対象外（年26件で検出力なし）
            continue
        tk = s["ticker"]
        sig_date = today.strftime("%Y-%m-%d")
        # 値がさカット（極みのみ・2026-08-24）: BT公式は1万円超を除外して測っている。
        # 本番のmax(100株)床は予算超過買い(10年-113.7万の実害)を作るのでBTに合わせて見送る。
        if (s.get("prev_close") or 0) > kiwami_px_cap(key):
            print(f"[shadow-{key}] {tk} は値がさ{s.get('prev_close'):,.0f}円>{kiwami_px_cap(key):,}円 → 1玉に収まらず見送り")
            continue
        if (tk, sig_date) in seen:
            continue
        if tk in still_open:
            print(f"[shadow-{key}] {tk} は極みで保有中 → 重複エントリーを回避")
            continue
        if slots_used >= MAX_SLOTS:
            # 枠が埋まっている＝通常版には出るが極みでは建てない。見送った事実は配信に出す
            skipped.append(s.get("name", tk))
            print(f"[shadow-{key}] {tk} は枠満杯({MAX_SLOTS})のため見送り")
            continue
        slots_used += 1
        df = all_data.get(tk)
        atr = atr_pct_at(df, sig_date) if df is not None else None
        rows.append({
            "signal_date": sig_date,
            "entry_date":  sig_date,              # 当日寄り付きエントリー（本番と同じ）
            "ticker":      tk,
            "name":        s.get("name", tk),
            "size":        KIWAMI_SIZE if key == "main" else TIER_FILES[key][2],
            "prev_close":  s.get("prev_close", 0),
            "limit_price": s.get("limit_price"),
            "atr_pct":     atr,
            "stop_pct":    shadow_stop_pct(atr),
            "live_stop":   LIVE_STOP,
            "entry_open":  None,
            "status":      "pending",
            "hold_days":   0,
            "pnl_pct":     None,
            "exit_type":   None,
            "exit_date":   None,
        })
        added += 1
    if added:
        save_ledger(key, rows)
    if skipped:
        with open(f"_shadow_skipped_{key}.json", "w", encoding="utf-8") as f:
            json.dump({"date": today.strftime("%Y-%m-%d"), "names": skipped},
                      f, ensure_ascii=False)
    return added


# ══════════════════════════════════════════════════════════════════════════
#  極みの売り台帳（2026-07-29新設）
#  通常版と入口は完全に同一（同じ today_sell_signals.json を読む）。違うのは
#    ①踏み上げ損切りが +2.5%（通常版+3.0%）
#    ②同時保有3枠まで（通常版は上限なし）
#  の2点だけ。通常版の positions_sell.json は読まないし書かない＝完全に独立。
# ══════════════════════════════════════════════════════════════════════════
def load_sell_ledger() -> list[dict]:
    if not os.path.exists(KIWAMI_SELL_LEDGER):
        return []
    try:
        with open(KIWAMI_SELL_LEDGER, encoding="utf-8") as f:
            rows = json.load(f)
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"[shadow-sell] 台帳読込失敗: {e} → 空で継続")
        return []


def save_sell_ledger(rows: list[dict]) -> None:
    with open(KIWAMI_SELL_LEDGER, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def record_sell_signals(today: date) -> int:
    """本番が今朝出したSELLシグナルを極みの売り台帳へ取り込む。
    SELLは寄指を使わない（tracker側もBUYのみNOFILL判定）ので翌寄り成行エントリー。"""
    if not os.path.exists(SELL_SIG_FILE):
        return 0
    with open(SELL_SIG_FILE, encoding="utf-8") as f:
        payload = json.load(f)
    today_str = today.strftime("%Y-%m-%d")
    if payload.get("date") != today_str:
        return 0

    rows = load_sell_ledger()
    seen = {(r["ticker"], r["signal_date"]) for r in rows}
    still_open = {r["ticker"] for r in rows if r.get("status") in ("pending", "open")}
    slots_used = len(still_open)
    added = 0
    skipped: list[str] = []
    for s in payload.get("signals", []):
        if s.get("direction") != "SELL":
            continue
        tk = s["ticker"]
        if (tk, today_str) in seen:
            continue
        if tk in still_open:
            print(f"[shadow-sell] {tk} は極みで保有中 → 重複エントリーを回避")
            continue
        if slots_used >= SELL_MAX_SLOTS:
            skipped.append(s.get("name", tk))
            print(f"[shadow-sell] {tk} は枠満杯({SELL_MAX_SLOTS})のため見送り")
            continue
        slots_used += 1
        rows.append({
            "signal_date": today_str,
            "entry_date":  today_str,          # 当日寄り付きエントリー（本番と同じ）
            "ticker":      tk,
            "name":        s.get("name", tk),
            "direction":   "SELL",
            "size":        KIWAMI_SIZE,
            "prev_close":  s.get("prev_close", 0),
            "stop_pct":    SELL_STOP_PCT,
            "live_stop":   LIVE_STOP,
            "entry_open":  None,
            "status":      "pending",
            "hold_days":   0,
            "pnl_pct":     None,
            "exit_type":   None,
            "exit_date":   None,
        })
        added += 1
    if added:
        save_sell_ledger(rows)
    if skipped:
        with open("_shadow_skipped_sell.json", "w", encoding="utf-8") as f:
            json.dump({"date": today_str, "names": skipped}, f, ensure_ascii=False)
    return added


def advance_sell(rows: list[dict], today: date, all_data: dict) -> int:
    """極みの売り台帳をその場で進める（保存はしない）。tracker.update_positions の
    SELL分岐と同じ判定順・同じ約定前提で、踏み上げ損切り幅だけ stop_pct を使う。
    週次はdeepcopyに対してこれを呼ぶ＝帳簿を汚さずに当日引けまで反映できる。"""
    from screener import calc_rsi
    _decisions = close_decisions._load()

    today_str = today.strftime("%Y-%m-%d")
    closed = 0
    for pos in rows:
        if pos.get("status") in ("closed", "expired"):
            continue
        df = all_data.get(pos["ticker"])
        if df is None or df.empty:
            continue
        entry_date_str = pos["entry_date"]

        if pos["status"] == "pending":
            # エントリー日が未来なら建てない。all_data に将来の足が混ざっていても
            # 「まだ約定していない玉」を勝手に建玉化しないための防御
            # （entry_date == today は正常＝当日寄りエントリー。週次のドライランで通る）。
            if entry_date_str > today_str:
                continue
            er = df[df.index.strftime("%Y-%m-%d") == entry_date_str]
            if er.empty:
                continue                       # まだエントリー日が来ていない
            # SELLは寄指を使わない＝NOFILL判定は無し（tracker側もBUYのみ）
            pos.update(entry_open=float(er["Open"].iloc[0]), status="open", hold_days=0)

        eo = pos.get("entry_open")
        if not eo or eo <= 0:
            continue
        stop_price = eo * (1 + pos["stop_pct"] / 100)      # 空売りなので上が損切り
        tp_price   = eo * (1 - TAKE_PROFIT / 100)

        post = df[(df.index.strftime("%Y-%m-%d") >= entry_date_str) &
                  (df.index.strftime("%Y-%m-%d") < today_str)]
        pos["hold_days"] = 0
        for dt_idx, row in post.iterrows():
            pos["hold_days"] += 1
            d_str = dt_idx.strftime("%Y-%m-%d")
            hi, lo, cl = float(row["High"]), float(row["Low"]), float(row["Close"])
            if hi >= stop_price:                                  # STOP優先（本番と同順）
                pos.update(pnl_pct=-pos["stop_pct"], exit_type="STOP",
                           exit_date=d_str, status="closed")
                closed += 1
                break
            if lo <= tp_price:
                pos.update(pnl_pct=+TAKE_PROFIT, exit_type="TP",
                           exit_date=d_str, status="closed")
                closed += 1
                break
            rsi_now = calc_rsi(df[df.index <= dt_idx]["Close"].dropna())
            rsi_exit = rsi_now is not None and rsi_now <= 50      # SELLは50以下で手仕舞い
            rsi_exit = close_decisions.apply(rsi_exit, d_str, "kiwami", "SELL",
                                             pos["ticker"], _decisions)   # 14:55判定優先
            if rsi_exit or pos["hold_days"] >= MAX_HOLD:
                pos.update(pnl_pct=round((eo - cl) / eo * 100, 3),
                           exit_type="RSI" if rsi_exit else "MAXHOLD",
                           exit_date=d_str, status="closed")
                closed += 1
                break
    return closed


def update_sell_ledger(today: date, all_data: dict) -> int:
    rows = load_sell_ledger()
    closed = advance_sell(rows, today, all_data)
    save_sell_ledger(rows)
    return closed


def update_ledger(key: str, today: date, all_data: dict) -> tuple[int, int]:
    """極み台帳の未決済を前日までのデータで進めて保存する。戻り値=(決済数, 失効数)。"""
    rows = load_ledger(key)
    closed, expired = advance(rows, today, all_data)
    save_ledger(key, rows)
    return closed, expired


def advance(rows: list[dict], today: date, all_data: dict) -> tuple[int, int]:
    """渡された台帳リストをその場で進める（保存はしない）。tracker.update_positions と
    同じ判定順・同じ約定前提で、損切り幅だけ銘柄ごとの stop_pct を使う。
    週次レポートは deepcopy に対してこれを呼ぶ＝帳簿を汚さずに当日引けまで反映できる
    （report._send_weekly_reports のドライランと同じ手口）。"""
    from screener import calc_rsi
    _decisions = close_decisions._load()

    today_str = today.strftime("%Y-%m-%d")
    closed = expired = 0

    for pos in rows:
        if pos["status"] in ("closed", "expired"):
            continue
        df = all_data.get(pos["ticker"])
        if df is None or df.empty:
            continue
        entry_date_str = pos["entry_date"]

        if pos["status"] == "pending":
            er = df[df.index.strftime("%Y-%m-%d") == entry_date_str]
            if er.empty:
                continue                          # まだエントリー日が来ていない
            eo = float(er["Open"].iloc[0])
            lp = pos.get("limit_price")
            if lp and eo > lp:                    # 寄指不成立（本番と同じ扱い）
                pos.update(entry_open=eo, status="expired", exit_type="NOFILL",
                           exit_date=entry_date_str)
                expired += 1
                continue
            pos.update(entry_open=eo, status="open", hold_days=0)

        eo = pos["entry_open"]
        if not eo or eo <= 0:
            continue
        stop_price = eo * (1 - pos["stop_pct"] / 100)
        tp_price   = eo * (1 + TAKE_PROFIT / 100)

        post = df[(df.index.strftime("%Y-%m-%d") >= entry_date_str) &
                  (df.index.strftime("%Y-%m-%d") < today_str)]
        pos["hold_days"] = 0
        for dt_idx, row in post.iterrows():
            pos["hold_days"] += 1
            d_str = dt_idx.strftime("%Y-%m-%d")
            lo, hi, cl = float(row["Low"]), float(row["High"]), float(row["Close"])
            if lo <= stop_price:                                  # STOP優先（本番と同順）
                pos.update(pnl_pct=-pos["stop_pct"], exit_type="STOP",
                           exit_date=d_str, status="closed")
                closed += 1
                break
            if hi >= tp_price:
                pos.update(pnl_pct=+TAKE_PROFIT, exit_type="TP",
                           exit_date=d_str, status="closed")
                closed += 1
                break
            rsi_now = calc_rsi(df[df.index <= dt_idx]["Close"].dropna())
            rsi_exit = rsi_now is not None and rsi_now >= 50
            rsi_exit = close_decisions.apply(rsi_exit, d_str, "kiwami", "BUY",
                                             pos["ticker"], _decisions)   # 14:55判定優先
            if rsi_exit or pos["hold_days"] >= MAX_HOLD:
                pos.update(pnl_pct=round((cl - eo) / eo * 100, 3),
                           exit_type="RSI" if rsi_exit else "MAXHOLD",
                           exit_date=d_str, status="closed")
                closed += 1
                break

    return closed, expired


def run_shadow(tiers, today: date, get_data) -> None:
    """main.py 末尾から呼ぶ唯一の入口。例外は呼び出し側で握り潰される前提。"""
    keys = [t["key"] for t in tiers if t["key"] in TIER_FILES]
    if not keys:
        return
    if not any(os.path.exists(TIER_FILES[k][0]) or load_ledger(k) for k in keys):
        return
    all_data = get_data() if callable(get_data) else get_data
    if not all_data:
        print("[shadow] 価格データなし → スキップ")
        return
    for k in keys:
        c, e = update_ledger(k, today, all_data)
        a = record_signals(k, today, all_data)
        print(f"[shadow-{TIER_FILES[k][3]}] 新規{a}件 / 影決済{c}件 / 影失効{e}件")

    # 極みの売り台帳（2026-07-29新設・損切り+2.5%／3枠）。買いと同じく更新→記帳の順。
    # ここが落ちても買いの台帳は上で保存済み＝独立。
    try:
        sc = update_sell_ledger(today, all_data)
        sa = record_sell_signals(today)
        print(f"[shadow-売り] 新規{sa}件 / 決済{sc}件")
    except Exception as e:
        print(f"[shadow] 極み売り台帳の更新スキップ（買いと通常版に影響なし）: {e}")

    # 専用チャンネルへの配信。ここが失敗しても台帳は既に保存済みで、本番配信にも影響しない。
    for _k in NOTIFY_KEYS:           # 極み・買い（大/中/小・2026-08-28から3階層）
        try:
            send_discord(today, _k)
        except Exception as e:
            print(f"[shadow-{_k}] 極み買いの配信スキップ（台帳は保存済み・通常版に影響なし）: {e}")
    try:
        send_discord_sell(today)     # 極み・売り（中身は通常版と同一）
    except Exception as e:
        print(f"[shadow] 極み売りの配信スキップ（通常版に影響なし）: {e}")
    try:
        from main import is_month_first_trading_day
        if is_month_first_trading_day(today):    # 月初営業日だけ（通常版と同じタイミング）
            monthly_report(today)
    except Exception as e:
        print(f"[shadow] 極み月次の配信スキップ（通常版に影響なし）: {e}")


# ────────────────────────────── レポート ──────────────────────────────
def _live_closed(key: str) -> dict:
    """本番帳簿の決済済みを {(ticker, signal_date): pos} で返す。"""
    path = TIER_FILES[key][1]
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    return {(p["ticker"], p["signal_date"]): p for p in rows
            if p.get("direction", "BUY") == "BUY" and p["status"] in ("closed", "expired")}


def _pairs(key: str) -> list[tuple]:
    """影と本番の両方で決着した玉を突き合わせて [(影row, 本番row, 本番%, 影%)] を返す。"""
    rows = load_ledger(key)
    if not rows:
        return []
    live = _live_closed(key)
    out = []
    for r in rows:
        lp = live.get((r["ticker"], r["signal_date"]))
        if lp is None or r["status"] not in ("closed", "expired"):
            continue
        lv = 0.0 if lp["status"] == "expired" else (lp.get("pnl_pct") or 0.0)
        sv = 0.0 if r["status"] == "expired" else (r.get("pnl_pct") or 0.0)
        out.append((r, lp, lv, sv))
    return out


def report() -> None:
    print("=" * 96)
    print("出口ボラ正規化 紙並走レポート  ─ 本番(一律-3%) vs 影(ATR%×2.0・下限2.0%)")
    print("  ※記録は本番が実際に採用した玉のみ。銘柄選定の違い(枠の空き方)は含まない＝出口単独の効果")
    print("=" * 96)
    grand = 0.0
    for key, (_, _, size, label) in TIER_FILES.items():
        rows = load_ledger(key)
        if not rows:
            continue
        pairs = _pairs(key)
        openn = sum(1 for r in rows if r["status"] in ("pending", "open"))
        if not pairs:
            print(f"\n  ---- {label} ---- 突き合わせ可能な決済 0件 / 影で保有中 {openn}件")
            continue
        dl = sum(p[2] for p in pairs) / 100 * size
        dsh = sum(p[3] for p in pairs) / 100 * size
        grand += dsh - dl
        print(f"\n  ---- {label}（1件{size//10_000}万）----")
        print(f"    決済 {len(pairs)}件 / 影で保有中 {openn}件")
        print(f"    本番 {sum(p[2] for p in pairs):+7.2f}% = {dl/10_000:+7.2f}万 "
              f"（勝率{sum(1 for p in pairs if p[2] > 0)/len(pairs)*100:4.1f}%）")
        print(f"    影   {sum(p[3] for p in pairs):+7.2f}% = {dsh/10_000:+7.2f}万 "
              f"（勝率{sum(1 for p in pairs if p[3] > 0)/len(pairs)*100:4.1f}%）")
        print(f"    差   {dsh/10_000 - dl/10_000:+7.2f}万")
        diff = sorted([p for p in pairs if abs(p[3] - p[2]) > 0.01],
                      key=lambda p: p[3] - p[2], reverse=True)
        if diff:
            print(f"    判定が割れた玉 {len(diff)}件:")
            for r, lp, lv, sv in diff[:10]:
                print(f"      {r['signal_date']} {r['name'][:14]:<14s} 影の損切り幅{r['stop_pct']:.1f}% | "
                      f"本番 {lv:+6.2f}%({lp.get('exit_type')}) → 影 {sv:+6.2f}%({r['exit_type']}) "
                      f"差{(sv-lv)/100*size/10_000:+.2f}万")
    print(f"\n  === 3階層合計の差: {grand/10_000:+.2f}万 ===")
    print("  ※判断は数ヶ月ぶん貯めてから。10年BTの根拠は memory/project_trading_signal.md 参照")


# ────────────────────────── Discord配信（専用チャンネル）──────────────────────────
# 本番チャンネルとは別のwebhookにだけ送る。ここが落ちても本番配信には一切影響しない
# （run_shadow 内で try/except、さらに main.py 側でも try/except に包まれている）。
# ── 「売買シグナル極み」＝本人専用の改善先行版（2026-07-26 命名）─────────────
#  極み  : 検証を通った改善を先に入れる本人専用版。買い=ATR連動の損切り／売り=通常と同一
#          （SELLのATR連動は2026-07-26に10年検証でt=-0.19＝効果ゼロと確定したため入れない）。
#  通常  : 友達用の安定版。従来のチャンネルへ従来のまま配信（この配信は一切変更しない）。
SHADOW_WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_URL"           # 極み・買い（大資金）
# 中/小資金の極み買い（2026-08-28 本人「中小資金も極みシグナルと同じようにして」）
SHADOW_TIER_WEBHOOK_ENV = {
    "main":  SHADOW_WEBHOOK_ENV,
    "mid":   "DISCORD_WEBHOOK_SHADOW_MID_URL",
    "small": "DISCORD_WEBHOOK_SHADOW_SMALL_URL",
}
SHADOW_SELL_WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_SELL_URL" # 極み・売り
# 週次/月次レポートの専用チャンネル（2026-08-09 本人がwebhook新設・secretsに登録済み）。
# 未設定なら従来どおり買いチャンネルへフォールバック（ローカル等でも壊れない）。
SHADOW_WEEKLY_WEBHOOK_ENV  = "DISCORD_WEBHOOK_SHADOW_WEEKLY_URL"
SHADOW_MONTHLY_WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_MONTHLY_URL"


def _report_env(preferred: str) -> str:
    """専用webhookが設定されていればそれを、無ければ買いチャンネルを使う。"""
    return preferred if os.getenv(preferred, "").strip() else SHADOW_WEBHOOK_ENV
# 配信する階層（2026-07-26 本人指示「資金は大のみ」）。台帳は3階層とも記録し続けるので、
# 後から中/小を出したくなったらこのタプルに足すだけで過去分ごと表示できる。
NOTIFY_KEYS = ("main", "mid", "small")   # 2026-08-28 中/小も極み配信
_COLOR_BUY, _COLOR_WIN, _COLOR_LOSE, _COLOR_INFO = 0x9B59B6, 0x2ECC71, 0xE74C3C, 0x95A5A6


def _shadow_post(embeds: list[dict], env: str = SHADOW_WEBHOOK_ENV) -> bool:
    """極みチャンネルへ送信。未設定/失敗でも例外を投げない（戻り値で成否だけ返す）。"""
    import requests

    url = os.getenv(env, "").strip()
    if not url:
        print(f"[shadow] {env} 未設定 → 配信スキップ（台帳の記録は継続）")
        return False
    verify = os.getenv("DISCORD_VERIFY_SSL", "true").lower() != "false"
    for attempt, wait in enumerate((0, 2, 4)):
        if wait:
            import time
            time.sleep(wait)
        try:
            r = requests.post(url, json={"embeds": embeds}, timeout=10, verify=verify)
            if r.status_code in (200, 204):
                return True
            print(f"[shadow] Discord HTTP {r.status_code} {r.text[:150]}（試行{attempt + 1}）")
        except Exception as e:
            print(f"[shadow] Discord送信失敗: {e}（試行{attempt + 1}）")
    return False


def _price_str(v: float | None) -> str:
    return f"{v:,.1f}".rstrip("0").rstrip(".") if v else "—"


def send_discord(today: date, key: str = "main") -> bool:
    """今朝の極みシグナル・決済・通算スコアを専用チャンネルへ1通で送る。
    送るものが何も無い日は送信しない（ハートビートは本番チャンネル側にあるため不要）。

    【2026-08-04 形式刷新・本人指示】①通常版(notifier._build_buy_embed)と同じ形式に
    ②枠満杯でもシグナルは全部出す（🟢=枠内・台帳が追う ／ ⚪=枠外・建てるなら小玉で自己管理）。
    台帳(答え合わせ)は従来どおり3枠のみ＝⚪を実際に建てた分は台帳に載らない。"""
    today_str = today.strftime("%Y-%m-%d")
    embeds: list[dict] = []

    # ① 今朝の極みシグナル（today_signals_kiwami.json の全件・通常版と同形式）
    sigs: list[dict] = []
    if os.path.exists(KIWAMI_SIG_FILE):
        try:
            with open(KIWAMI_SIG_FILE, encoding="utf-8") as f:
                _kp = json.load(f)
            if _kp.get("date") == today_str:
                sigs = [s for s in _kp.get("signals", []) if s.get("direction") == "BUY"]
        except Exception as e:
            print(f"[shadow] 極みシグナル読込失敗: {e}")
    rows_main = load_ledger(key)
    recorded = {r["ticker"] for r in rows_main if r.get("signal_date") == today_str}
    n_open = sum(1 for r in rows_main if r.get("status") in ("pending", "open"))
    # 昨日以前から保有中の銘柄が今日また候補に入った場合＝再エントリー不可（📌表示）。
    # ⚪(枠外・小玉で建ててよい)と混ざると二重建てを誘発するので分ける（2026-08-04）。
    holding = {r["ticker"] for r in rows_main
               if r.get("status") in ("pending", "open") and r.get("signal_date") != today_str}
    if not sigs:      # フォールバック: ファイル無し/古い日付なら台帳の当日分だけでも出す
        sigs = [{"ticker": r["ticker"], "name": r.get("name", r["ticker"]),
                 "prev_close": r.get("prev_close", 0), "limit_price": r.get("limit_price")}
                for r in rows_main if r.get("signal_date") == today_str]

    # 【2026-08-12 本人指示で全銘柄表示に再変更】8/4の「極みだけの銘柄」差分表示は、
    # 「通常版に出て極みに出ない夜どうするの？」の混乱を生んだ（極みch単体で完結しない）。
    # → 通常版と共通の銘柄も含む「極みの完全な買いリスト」を出す。台帳が枠内に取った玉を
    #   先頭に並べ、ヘッダで「#1〜#Nが枠内」と明示＝このチャンネルだけ見れば発注できる。
    #   8/4に嫌われた🟢⚪📌の行内マークは復活させない（並び順+ヘッダ1行で表現）。
    reg_set: set = set()
    try:
        # 「極み帯」判定は通常版・大の採用リスト基準（中/小の通常版は値がさ除外で欠けるため）
        if os.path.exists(TIER_FILES["main"][0]):
            with open(TIER_FILES["main"][0], encoding="utf-8") as f:
                _rp = json.load(f)
            if _rp.get("date") == today_str:
                reg_set = {s["ticker"] for s in _rp.get("signals", [])}
    except Exception:
        pass
    shown = [s for s in sigs if s["ticker"] not in holding]   # 保有中の再候補は二重建て防止で非表示
    # 台帳が今日枠内に記帳した玉を先頭に（＝実際に買う分）。以降は枠あふれ分。
    extras = ([s for s in shown if s["ticker"] in recorded]
              + [s for s in shown if s["ticker"] not in recorded])
    n_in = sum(1 for s in extras if s["ticker"] in recorded)

    if extras:
        size = TIER_FILES[key][2]
        try:
            from notifier import _nth_trading_day
            exit_str = _nth_trading_day(today, 2).strftime("%m/%d")
        except Exception:
            exit_str = "3営業日後"
        if n_in <= 0:
            waku = "📦 **3枠満杯＝今夜の新規なし**（下は参考・決済で枠が空いたら次回から）"
        elif n_in == len(extras):
            waku = f"📦 **#1〜#{n_in} を買う**（{n_in}銘柄すべて枠内）"
        else:
            waku = f"📦 **#1〜#{n_in} を買う**（枠内{n_in}／#{n_in+1}以降は枠あふれ分＝見送り）"
        lines = [
            f"🎯 **寄指（寄付限定指値）**で発注・1件{size//10000}万円",
            "　 各銘柄の指値↓を指定。寄りがそれ以下なら寄り値で約定／超えたら失効＝その日は見送り",
            f"🛑 損切 寄値×0.97 (-{LIVE_STOP:.0f}%)  ✅ 利確 寄値×1.05 (+{TAKE_PROFIT:.0f}%)",
            f"📅 最大3営業日・RSI≥50で早期決済・処分期限 **{exit_str}**",
            waku,
            "─" * 24,
        ]
        for i, s in enumerate(extras, 1):
            tk = str(s["ticker"]).replace(".T", "")
            pc = s.get("prev_close", 0) or 0
            lp = s.get("limit_price") or 0
            if pc > kiwami_px_cap(key):
                # 値がさ玉は発注情報を出さない（100株でも1玉に収まらない＝買わない）
                head = (f"#{i}（対象外・値がさ{kiwami_px_cap(key):,}円超＝1玉に収まらない・買わない） "
                        f"{s.get('name', tk)} ({tk}) 前日{pc:,.0f}円")
            elif pc > 0:
                shares = max(100, int(size / pc / 100) * 100)
                head = (f"**#{i} {s.get('name', tk)}** ({tk}) 前日{pc:,.0f}円 "
                        f"→ **寄指 {lp:,.0f}円** {shares:,}株/約{shares*pc/1e4:.0f}万")
            else:
                head = f"**#{i} {s.get('name', tk)}** ({tk})"
            parts = []
            if s.get("rsi") is not None:
                parts.append(f"RSI={s['rsi']:.1f}")
            if s.get("deviation") is not None:
                parts.append(f"乖離{s['deviation']:+.1f}%")
            if s.get("vol_ratio") is not None and s.get("vol_ratio", 0) >= 2.0:
                parts.append(f"出来高×{s['vol_ratio']:.1f}")
            elif s.get("range_ratio") is not None:
                parts.append(f"値幅/ATR={s['range_ratio']:.1f}")
            if s.get("turnover"):
                parts.append(f"代金{s['turnover']/1e8:.0f}億")
            if s["ticker"] not in reg_set:
                # 通常版に無い買残0.8-1.2帯の追加銘柄＝目立たせる（2026-08-28 本人指示）
                head = head.replace(f"#{i} ", f"#{i} 🔥【極み帯】", 1)
                parts.append("🔥極み帯＝通常版に出ない極み専用銘柄")
            lines.append(head)
            if parts:
                lines.append("   " + "・".join(parts))
            lines.append("")
        embeds.append({
            "title": f"⚡【スイング極み{_tier_sfx(key)}】{today.strftime('%Y年%m月%d日')} — 買い{len(extras)}銘柄",
            "description": "\n".join(lines).rstrip(),
            "color": _COLOR_BUY,
            "footer": {"text": "極みの完全リスト（通常版と共通の銘柄も含む）。「極み帯」=買残1.2帯の極み専用銘柄"},
        })
    sigs = extras   # 後段の0件判定はこのリスト基準

    # ② 影台帳で今日決済された玉（本番と判定が割れたものを明示）
    settled = []
    for _k in (key,):
        # 判定割れの差額円は階層サイズ換算（サイズ差と判定差を混ぜない・2026-08-24）
        _, _, size, label = TIER_FILES[_k]
        live = _live_closed(_k)
        for r in load_ledger(_k):
            if r.get("exit_date") != today_str or r["status"] not in ("closed", "expired"):
                continue
            lp = live.get((r["ticker"], r["signal_date"]))
            sv = 0.0 if r["status"] == "expired" else (r.get("pnl_pct") or 0.0)
            if lp is None:
                settled.append(f"{label}｜{r['name']} 影 {sv:+.2f}%（{r['exit_type']}）※本番は未決済")
                continue
            lv = 0.0 if lp["status"] == "expired" else (lp.get("pnl_pct") or 0.0)
            mark = "⚠️割れた" if abs(sv - lv) > 0.01 else "一致"
            settled.append(
                f"{label}｜**{r['name']}** {mark}\n"
                f"　本番 {lv:+.2f}%（{lp.get('exit_type')}） → 影 {sv:+.2f}%（{r['exit_type']}）"
                f" 差 **{(sv - lv) / 100 * size / 10_000:+.2f}万**")
    if settled:
        embeds.append({
            "title": "📕 極みの決済（紙の再現・通常版の帳簿とは別)",
            "description": "\n".join(settled[:12]),
            "color": _COLOR_INFO,
        })

    # ③ 通算スコアボード（📊 通常vs極みの合計差）は 2026-08-09 本人指示
    # 「合計差とかの案内いらない」で廃止。差の記録自体は _pairs() で常に再計算できる。

    # 枠満杯の見送りは 2026-08-04 から ⚪ 印で①に直接表示（skip_noteの別枠表示は廃止）

    if not sigs:
        # 実弾で回すので「無音＝故障」と区別できるようシグナル0件の日も必ず出す（通常版と同じ思想）
        embeds.insert(0, {
            "title": f"⚡【スイング極み{_tier_sfx(key)}】{today.strftime('%Y年%m月%d日')} — シグナルなし",
            "description": "**本日の極みの買いシグナルはありません。**（0銘柄＝見送り）",
            "color": _COLOR_INFO,
        })
    if not embeds:
        print(f"[shadow-{key}] 配信対象なし → 送信しない")
        return False
    return _shadow_post(embeds, SHADOW_TIER_WEBHOOK_ENV.get(key, SHADOW_WEBHOOK_ENV))


def _tier_sfx(key: str) -> str:
    return "" if key == "main" else "・" + TIER_FILES[key][3]


def send_discord_sell(today: date) -> bool:
    """極みの売りを専用チャンネルへ。銘柄選定は通常版と同一、**出口だけ違う**。

    2026-07-29: 踏み上げ損切りを +3.0% → **+2.5%** に変更（極みのみ）。
    _bt_sell_improve.py の8軸グリッド（10年・150万×3枠・業種cap2）で
    PF1.54→1.60 / 10年+109.5万→+116.9万 / 勝ち年5/10→7/10、両期間とも改善。
    同時に枠を3に制限（通常版は上限なし）。台帳は kiwami_sell.json で完全に独立。

    ATR連動の損切りは2026-07-26の10年検証で棄却済み（同一232件のreplayで t=-0.19＝効果ゼロ。
    SELL候補のATR%が1.48〜2.50に均質で正規化の余地が無い）ため入れていない。
    """
    today_str = today.strftime("%Y-%m-%d")
    sig_file = "today_sell_signals.json"          # 大資金のみ（NOTIFY_KEYS=("main",)と同方針）
    if not os.path.exists(sig_file):
        return False
    with open(sig_file, encoding="utf-8") as f:
        payload = json.load(f)
    if payload.get("date") != today_str:
        return False
    sigs = [s for s in payload.get("signals", []) if s.get("direction") == "SELL"]
    if not sigs:
        return _shadow_post([{
            "title": f"⚡ 売買シグナル極み（売り）— {today_str}",
            "description": ("**本日の空売りシグナルはありません。**\n"
                            "売りは日経25MA以下のときだけ出る設計で、年26件程度の希少シグナル。"),
            "color": _COLOR_INFO,
        }], env=SHADOW_SELL_WEBHOOK_ENV)

    # 極みは3枠まで。すでに保有中の玉があれば、その分だけ今日は建てられない。
    open_now = [r for r in load_sell_ledger() if r.get("status") in ("pending", "open")]
    free = max(SELL_MAX_SLOTS - len(open_now), 0)
    lines = []
    for i, s in enumerate(sigs):
        pc = s.get("prev_close") or 0
        mark = "" if i < free else "　⏭️ **枠満杯で見送り**"
        lines.append(
            f"**{s.get('name', s['ticker'])}**（{s['ticker'][:4]}）前日終値 {_price_str(pc)}\n"
            f"　翌寄り成行で空売り → 損切り **+{SELL_STOP_PCT}%**"
            f"（{_price_str(pc * (1 + SELL_STOP_PCT / 100))}）/ "
            f"利確 **-5.0%**（{_price_str(pc * 0.95)}）/ RSI50以下 or 最大3日{mark}")
    slot_note = (f"\n\n📊 **枠 {len(open_now)}/{SELL_MAX_SLOTS} 使用中**"
                 f"（あと{free}枠）" if open_now else "")
    return _shadow_post([{
        "title": f"⚡ 売買シグナル極み（売り）— {today_str}",
        "description": ("**俺専用版**。銘柄の選び方は通常版と同一だが、"
                        f"**踏み上げ損切りが +{SELL_STOP_PCT}%**（通常版は+3.0%）。\n"
                        "10年BTで PF1.54→1.60・勝ち年5/10→7/10・両期間改善。"
                        f"同時保有は{SELL_MAX_SLOTS}枠まで。" + slot_note + "\n\n"
                        + "\n\n".join(lines[:10])),
        "color": _COLOR_LOSE,
        "footer": {"text": "貸借区分・在庫はSBIの発注画面で最終確認すること"},
    }], env=SHADOW_SELL_WEBHOOK_ENV)


_EXIT_LABEL = {"TP": "利確", "STOP": "損切", "RSI": "RSI回復", "MAXHOLD": "期限", "NOFILL": "寄指不成立"}


def _md(d: str | None) -> str:
    return f"{d[5:7]}/{d[8:10]}" if d and len(d) >= 10 else "?"


def weekly_report(today: date, all_data: dict | None, sell_positions: list[dict] | None = None) -> bool:
    """金曜引け後の極み週次。**通常版 notifier.send_weekly_report と同一の表示形式**
    （2026-08-09改装・本人「通常版の方が見やすい・売りの損切りが合計に入らない」）。
    明細行＝株数/建値/決済値/損益円、買い＋空売りの2ブロック、週間合計は買売込み。

    帳簿は翌朝 run_shadow が確定するため、金曜15:40時点では当日決済分が open のまま。
    通常版と同じくコピーに対して当日引けまでドライランしてから集計する（保存はしない）。
    sell_positions には極みの売り台帳(kiwami_sell.json・損切り+2.5%)を渡す。
    """
    import copy
    from datetime import timedelta
    from notifier import _pf_str

    size = LEGACY_SIZE                     # size未記録の旧玉の既定（新玉は台帳のsizeを使う）
    rows = copy.deepcopy(load_ledger("main"))
    if all_data:
        advance(rows, today + timedelta(days=1), all_data)   # 当日引けまで反映（非破壊）

    week_start = today - timedelta(days=today.weekday())
    ws, ts = week_start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    buy_week = [r for r in rows if r.get("status") == "closed" and r.get("pnl_pct") is not None
                and ws <= (r.get("exit_date") or "") <= ts]
    sell_week = [p for p in (sell_positions or []) if p.get("status") == "closed"
                 and p.get("pnl_pct") is not None and p.get("direction") == "SELL"
                 and ws <= (p.get("exit_date") or "") <= ts]
    holds = ([r for r in rows if r.get("status") in ("pending", "open")]
             + [p for p in (sell_positions or []) if p.get("status") in ("pending", "open")])

    week_yen_total = 0

    def _shares(p):
        base = p.get("prev_close") or p.get("entry_open") or 0
        sz = p.get("size") or size
        return max(100, int(sz / base / 100) * 100) if base > 0 else 100

    def block(week: list[dict], label: str, emoji: str, *, sell: bool) -> str:
        nonlocal week_yen_total
        if not week:
            return f"{emoji} {label}: 今週は決済なし"
        pnls = [p["pnl_pct"] for p in week]
        wins = sum(1 for x in pnls if x > 0)
        head = (f"{emoji} {label}: {len(week)}件決済 勝率{wins}/{len(week)}"
                f"（{round(wins / len(week) * 100)}%）・PF {_pf_str(pnls)}")
        in_label, out_label = ("売建", "買戻") if sell else ("買", "売")
        out_rows, entry_total, exit_total, pnl_total = [], 0, 0, 0
        for p in sorted(week, key=lambda x: (x.get("exit_date") or "", x["ticker"])):
            sh = _shares(p)
            ep = p.get("entry_open") or 0
            xp = ep * (1 - p["pnl_pct"] / 100 if sell else 1 + p["pnl_pct"] / 100)
            entry_amt = round(sh * ep)
            pnl_yen = round(entry_amt * p["pnl_pct"] / 100)
            exit_amt = entry_amt - pnl_yen if sell else entry_amt + pnl_yen
            entry_total += entry_amt
            exit_total += exit_amt
            pnl_total += pnl_yen
            mark = "✅" if p["pnl_pct"] > 0 else "❌"
            el = _EXIT_LABEL.get(p.get("exit_type") or "", p.get("exit_type") or "?")
            out_rows.append(
                f"{mark} {p['name']} {_md(p.get('entry_date'))}→{_md(p.get('exit_date'))}"
                f" {sh:,}株｜{in_label} {ep:,.0f}円 → {out_label} {xp:,.0f}円"
                f"｜**{pnl_yen:+,}円**（{p['pnl_pct']:+.1f}% {el}）")
        week_yen_total += pnl_total
        in_total, out_total = ("売建合計", "買戻合計") if sell else ("買付合計", "売却合計")
        total_line = (f"💰 {in_total} {entry_total:,}円 → {out_total} {exit_total:,}円"
                      f" ＝ **{pnl_total:+,}円**")
        return "\n".join([head, *out_rows, total_line])

    lines = [block(buy_week, "BUY", "📈", sell=False)]
    if sell_week:
        lines.append(block(sell_week, "空売り", "📉", sell=True))

    if holds:
        def _hold_str(p: dict) -> str:
            tag = "空売り " if p.get("direction") == "SELL" else ""
            if p.get("status") == "pending":
                return f"{p['name']}（{tag}約定未確認）"
            return f"{p['name']}（{tag}損切り-{p.get('stop_pct', LIVE_STOP):.1f}%）"
        names = "、".join(_hold_str(p) for p in holds[:5])
        more = f" ほか{len(holds) - 5}件" if len(holds) > 5 else ""
        lines.append(f"💼 保有中（持ち越し）: {len(holds)}件 — {names}{more}")
    else:
        lines.append("💼 保有中（持ち越し）: なし")

    # 通常版に無い明示の合計行（本人「合計値がわからん」対応＝買い＋空売りの週間損益）
    lines.append(f"\n📊 週間合計（買い＋空売り）: **{week_yen_total:+,}円**")

    rng = f"{week_start.strftime('%m/%d')}–{today.strftime('%m/%d')}"
    return _shadow_post([{
        "title": f"📅【週次レポート】売買シグナル極み｜{rng}",
        "description": "\n".join(lines),
        "color": _COLOR_WIN if week_yen_total >= 0 else _COLOR_LOSE,
        "footer": {"text": f"1件{KIWAMI_SIZE // 10000}万・買い3枠/売り3枠・"
                           "損切り 買い-3%(通常版と同じ)/売り+2.5%・利確+5%"},
    }], env=_report_env(SHADOW_WEEKLY_WEBHOOK_ENV))


def monthly_report(today: date) -> bool:
    """月初営業日に出す極みの月別・年間損益。**通常版 _build_monthly_embed と同一の表示形式**で
    買い・売りを別embedにして1通で送る（2026-08-09改装・本人「通常版の表示に合わせて」）。

    枠は極みの実構成＝買い3枠・売り3枠で集計する（旧版は通常版比較用に
    5枠換算だったが、本人の実弾と一致しない金額になるため実構成へ変更）。
    円は玉単位で正確（台帳のsize・旧玉は100万）。月利%は3枠×KIWAMI_SIZE基準の概算
    （150万時代は8/25の1日だけでシグナル0件＝時代分け不要・全期間100万）。
    台帳は記帳時に3枠制限済みだが、7/25以前のbackfill分は無制限で入っているので
    買いだけ _slot_funded(3枠) で資金枠を再適用する。
    """
    from collections import defaultdict
    from notifier import _slot_funded

    slots = 3
    year = str(today.year)

    def _cap_month(ym: str) -> int:
        return slots * KIWAMI_SIZE

    def _embed(rows: list[dict], *, sell: bool, funded: set | None) -> dict | None:
        monthly = defaultdict(list)
        for r in rows:
            if r.get("status") != "closed" or r.get("pnl_pct") is None:
                continue
            if funded is not None and id(r) not in funded:
                continue
            ym = (r.get("exit_date") or "")[:7]
            if ym:
                monthly[ym].append((r["pnl_pct"], r.get("size") or LEGACY_SIZE))
        ym_year = {k: v for k, v in monthly.items() if k.startswith(year)}
        if not ym_year:
            return None
        lines = []
        ann_yen, ann = 0.0, 0.0
        for ym in sorted(ym_year):
            p = ym_year[ym]
            yen = sum(pct / 100 * sz for pct, sz in p)
            mr = yen / _cap_month(ym) * 100
            ann_yen += yen
            ann += mr
            wins = sum(1 for pct, _ in p if pct > 0)
            sign = "+" if mr >= 0 else ""
            lines.append(f"`{ym}` {len(p)}件 勝率{wins}/{len(p)} "
                         f"**月利{sign}{mr:.1f}%**（{sign}{yen / 10000:.1f}万円）")
        a_sign = "+" if ann >= 0 else ""
        desc = "\n".join(lines)
        desc += f"\n\n**{year}年合計: {a_sign}{ann:.1f}%（{a_sign}{ann_yen / 10000:.1f}万円）**"
        kind = "空売り" if sell else "スイング"
        return {
            "title": f"📉 {year}年 月別・年間損益（極み・{kind}）" if sell
                     else f"📈 {year}年 月別・年間損益（極み・{kind}）",
            "description": desc,
            "color": _COLOR_WIN if ann >= 0 else _COLOR_LOSE,
            "footer": {"text": f"※{slots}枠×1件{KIWAMI_SIZE // 10000}万・"
                               f"年間%は月利の和・資金枠に収まる分のみ集計・"
                               f"損切り{'+2.5%' if sell else '-3%(通常版と同じ)'}"},
        }

    buy_rows = load_ledger("main")
    buy_embed = _embed(buy_rows, sell=False, funded=_slot_funded(buy_rows, slots))
    sell_embed = _embed(load_sell_ledger(), sell=True, funded=None)   # 売り台帳は記帳時3枠制限済み

    if not buy_embed and not sell_embed:
        print("[shadow] 月次: 今年の確定分なし → 送信しない")
        return False

    # 通常版との差の併記（📊通算 通常vs極み）は 2026-08-09 本人指示
    # 「合計差とかの案内いらない」で廃止。必要になれば _pairs("main") から再計算できる。

    return _shadow_post([e for e in (buy_embed, sell_embed) if e],
                        env=_report_env(SHADOW_MONTHLY_WEBHOOK_ENV))


def backfill(days: int = 120) -> None:
    """既存 positions*.json の過去BUYから影台帳を再構成（初回のみ・以後は毎朝の増分）。"""
    from screener import batch_download_jquants, _jquants_id_token, RSI_WARMUP_CAL_DAYS
    today = date.today()
    start = (today - timedelta(days=days + RSI_WARMUP_CAL_DAYS)).strftime("%Y-%m-%d")
    print(f"[backfill] {start}〜{today} の価格データ取得中...")
    all_data = batch_download_jquants(_jquants_id_token(), start=start, end=today.strftime("%Y-%m-%d"))
    if not all_data:
        print("[backfill] データ取得失敗")
        return
    cutoff = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    for key, (_, pos_file, _, label) in TIER_FILES.items():
        if not os.path.exists(pos_file):
            continue
        with open(pos_file, encoding="utf-8") as f:
            live = json.load(f)
        rows = load_ledger(key)
        seen = {(r["ticker"], r["signal_date"]) for r in rows}
        added = 0
        for p in live:
            if p.get("direction", "BUY") != "BUY" or p["signal_date"] < cutoff:
                continue
            k = (p["ticker"], p["signal_date"])
            if k in seen:
                continue
            df = all_data.get(p["ticker"])
            sd = _prev_trading_row_date(df, p["entry_date"]) if df is not None else None
            atr = atr_pct_at(df, sd) if (df is not None and sd) else None
            rows.append({
                "signal_date": p["signal_date"], "entry_date": p["entry_date"],
                "ticker": p["ticker"], "name": p.get("name", p["ticker"]),
                "prev_close": p.get("prev_close", 0), "limit_price": p.get("limit_price"),
                "atr_pct": atr, "stop_pct": shadow_stop_pct(atr), "live_stop": LIVE_STOP,
                "entry_open": None, "status": "pending", "hold_days": 0,
                "pnl_pct": None, "exit_type": None, "exit_date": None,
            })
            added += 1
        save_ledger(key, rows)
        c, e = update_ledger(key, today, all_data)
        print(f"[backfill-{label}] 取込{added}件 → 影決済{c}件 / 影失効{e}件")


if __name__ == "__main__":
    import sys

    from dotenv import load_dotenv   # 単体実行時のみ（main.py経由では既に読み込み済み）
    load_dotenv()

    if "--backfill" in sys.argv:
        n = 120
        for a in sys.argv:
            if a.startswith("--days="):
                n = int(a.split("=")[1])
        backfill(n)
    if "--send" in sys.argv:          # 影チャンネルへ実配信（疎通確認・手動レポート用）
        ok = send_discord(date.today())
        print(f"[shadow] Discord配信: {'成功' if ok else '未送信'}")
        if "--report" not in sys.argv:
            sys.exit(0)
    report()
