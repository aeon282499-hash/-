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

# ── 検証で確定したパラメータ（BT: _bt_atr_exit_kt.py の最終形）───────────────
ATR_MULT      = 2.0     # 損切り幅 = ATR% × これ
STOP_FLOOR    = 2.0     # 下限%（これ未満は板の値幅の中でBTが約定を再現できない）
STOP_CEIL     = 15.0    # 暴走防止のサニティ上限（入口ATRキャップ3.0%なので実質6%が最大）
TAKE_PROFIT   = 5.0     # 利確は現行のまま（BTで「TP据置が最良」と確定）
MAX_HOLD      = 3
ATR_PERIOD    = 14
LIVE_STOP     = 3.0     # 比較対象＝本番の一律損切り

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
    """損切り幅%。ATRが取れない場合は本番と同じ3.0%にフォールバック（＝差分ゼロで安全側）。"""
    if atr_pct is None:
        return LIVE_STOP
    return round(min(max(ATR_MULT * atr_pct, STOP_FLOOR), STOP_CEIL), 2)


def _prev_trading_row_date(df: pd.DataFrame, entry_date_str: str) -> str | None:
    """エントリー日の直前の営業日（＝シグナル日）を価格データから引く。"""
    d = df[df.index.strftime("%Y-%m-%d") < entry_date_str]
    return d.index[-1].strftime("%Y-%m-%d") if len(d) else None


def record_signals(key: str, today: date, all_data: dict) -> int:
    """本番が今朝採用したBUYシグナルを影台帳に取り込む（同一(ticker,signal_date)は再登録しない）。"""
    sig_file = TIER_FILES[key][0]
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
    added = 0
    for s in payload.get("signals", []):
        if s.get("direction") != "BUY":           # SELLは対象外（年26件で検出力なし）
            continue
        tk = s["ticker"]
        sig_date = today.strftime("%Y-%m-%d")
        if (tk, sig_date) in seen:
            continue
        if tk in still_open:
            print(f"[shadow-{key}] {tk} は極みで保有中 → 重複エントリーを回避")
            continue
        df = all_data.get(tk)
        atr = atr_pct_at(df, sig_date) if df is not None else None
        rows.append({
            "signal_date": sig_date,
            "entry_date":  sig_date,              # 当日寄り付きエントリー（本番と同じ）
            "ticker":      tk,
            "name":        s.get("name", tk),
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
    return added


def update_ledger(key: str, today: date, all_data: dict) -> tuple[int, int]:
    """影台帳の未決済を前日までのデータで進める。tracker.update_positions と同じ判定順・同じ約定前提で、
    損切り幅だけ銘柄ごとの stop_pct を使う。戻り値=(決済数, 失効数)。"""
    from screener import calc_rsi

    rows = load_ledger(key)
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
            if rsi_exit or pos["hold_days"] >= MAX_HOLD:
                pos.update(pnl_pct=round((cl - eo) / eo * 100, 3),
                           exit_type="RSI" if rsi_exit else "MAXHOLD",
                           exit_date=d_str, status="closed")
                closed += 1
                break

    save_ledger(key, rows)
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

    # 専用チャンネルへの配信。ここが失敗しても台帳は既に保存済みで、本番配信にも影響しない。
    try:
        send_discord(today)          # 極み・買い（ATR連動の損切り）
    except Exception as e:
        print(f"[shadow] 極み買いの配信スキップ（台帳は保存済み・通常版に影響なし）: {e}")
    try:
        send_discord_sell(today)     # 極み・売り（中身は通常版と同一）
    except Exception as e:
        print(f"[shadow] 極み売りの配信スキップ（通常版に影響なし）: {e}")


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
SHADOW_WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_URL"           # 極み・買い
SHADOW_SELL_WEBHOOK_ENV = "DISCORD_WEBHOOK_SHADOW_SELL_URL" # 極み・売り
# 配信する階層（2026-07-26 本人指示「資金は大のみ」）。台帳は3階層とも記録し続けるので、
# 後から中/小を出したくなったらこのタプルに足すだけで過去分ごと表示できる。
NOTIFY_KEYS = ("main",)
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


def send_discord(today: date) -> bool:
    """今朝の影シグナル・影の決済・通算スコアを専用チャンネルへ1通で送る。
    送るものが何も無い日は送信しない（ハートビートは本番チャンネル側にあるため不要）。"""
    today_str = today.strftime("%Y-%m-%d")
    embeds: list[dict] = []

    # ① 今朝の影シグナル（銘柄は本番と同一・違うのは損切り価格だけ）
    lines = []
    for key in NOTIFY_KEYS:
        _, _, size, label = TIER_FILES[key]
        for r in load_ledger(key):
            if r["signal_date"] != today_str:
                continue
            pc = r.get("prev_close") or 0
            stop_pct = r["stop_pct"]
            atr = r.get("atr_pct")
            ref = r.get("limit_price") or pc          # 寄指の指値を基準に損切り価格の目安を出す
            atr_s = "—" if atr is None else f"{atr:.2f}%"
            lines.append(
                f"**{label}｜{r['name']}**（{r['ticker'][:4]}）\n"
                f"　ATR {atr_s} → **損切り -{stop_pct:.1f}%**（本番 -{LIVE_STOP:.0f}%）\n"
                f"　寄指 {_price_str(r.get('limit_price'))} 以下で約定なら "
                f"損切り目安 **{_price_str(ref * (1 - stop_pct / 100))}** / 利確 "
                f"{_price_str(ref * (1 + TAKE_PROFIT / 100))}（+{TAKE_PROFIT:.0f}%・本番と同じ）"
            )
    if lines:
        embeds.append({
            "title": f"⚡ 売買シグナル極み（買い）— {today_str}",
            "description": ("**俺専用版**。銘柄・寄指・利確は通常版と同一で、**損切りだけ**が銘柄ごとに変わる。\n"
                            "⚠️ 15時の処分チェックは通常ルール(-3%)基準で動くので、"
                            "**極みの損切りは自分でOCOに入れること**。\n\n"
                            + "\n\n".join(lines[:10])),
            "color": _COLOR_BUY,
            "footer": {"text": "根拠=10年BTで大100万+234.5万→+345.3万・最悪3年-116.2→-78.1万（_bt_atr_exit_*.py）"},
        })

    # ② 影台帳で今日決済された玉（本番と判定が割れたものを明示）
    settled = []
    for key in NOTIFY_KEYS:
        _, _, size, label = TIER_FILES[key]
        live = _live_closed(key)
        for r in load_ledger(key):
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

    # ③ 通算スコアボード
    grand, board = 0.0, []
    for key in NOTIFY_KEYS:
        _, _, size, label = TIER_FILES[key]
        pairs = _pairs(key)
        if not pairs:
            continue
        dl = sum(p[2] for p in pairs) / 100 * size
        dsh = sum(p[3] for p in pairs) / 100 * size
        grand += dsh - dl
        split = sum(1 for p in pairs if abs(p[3] - p[2]) > 0.01)
        board.append(
            f"**{label}**（{len(pairs)}件・うち判定が割れた玉{split}件）\n"
            f"　通常 {dl / 10_000:+.2f}万 ／ 極み {dsh / 10_000:+.2f}万 ／ "
            f"差 **{(dsh - dl) / 10_000:+.2f}万**")
    if board:
        embeds.append({
            "title": f"📊 通算 通常 vs 極み　合計差 {grand / 10_000:+.2f}万",
            "description": "\n".join(board),
            "color": _COLOR_WIN if grand >= 0 else _COLOR_LOSE,
            "footer": {"text": "10年BTでは大100万で+234.5万→+345.3万・最悪3年-116.2→-78.1万。"
                               "ただし差が出るのは損切りに触った玉だけ＝数ヶ月貯めないと判断不能"},
        })

    if not embeds:
        print("[shadow] 配信対象なし（新規0・決済0・突合0）→ 送信しない")
        return False
    return _shadow_post(embeds)


def send_discord_sell(today: date) -> bool:
    """極みの売りを専用チャンネルへ。**中身は通常版と完全に同一**。

    SELLのATR連動は2026-07-26の10年検証で棄却（同一232件のreplayで t=-0.19＝効果ゼロ。
    真因はSELL候補のATR%が1.48〜2.50に均質＝入口の急騰条件とATRキャップで散らばりが無く、
    正規化する余地がそもそも無い）。よって極みの売りは通常版のミラーで、改善が見つかった時に
    ここへ先に入れる枠として用意しておく。台帳も持たない（通常版の帳簿がそのまま真）。
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
        print("[shadow] 極みの売り: 本日0件 → 送信しない")
        return False

    lines = []
    for s in sigs:
        pc = s.get("prev_close") or 0
        lines.append(
            f"**{s.get('name', s['ticker'])}**（{s['ticker'][:4]}）前日終値 {_price_str(pc)}\n"
            f"　翌寄り成行で空売り → 損切り **+3.0%**（{_price_str(pc * 1.03)}）/ "
            f"利確 **-5.0%**（{_price_str(pc * 0.95)}）/ RSI50以下 or 最大3日")
    return _shadow_post([{
        "title": f"⚡ 売買シグナル極み（売り）— {today_str}",
        "description": ("**俺専用版**。売りは現時点で**通常版と中身が同一**。\n"
                        "ATR連動の損切りは10年検証で効果ゼロ（t=-0.19）と判明したため入れていない"
                        "＝売り候補はATR%が1.48〜2.50に均質で、正規化する余地が無い。\n"
                        "改善が見つかったらここへ先に入れる。\n\n" + "\n\n".join(lines[:10])),
        "color": _COLOR_LOSE,
        "footer": {"text": "貸借区分・在庫はSBIの発注画面で最終確認すること"},
    }], env=SHADOW_SELL_WEBHOOK_ENV)


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
