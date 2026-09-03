# -*- coding: utf-8 -*-
"""gapfade.py — ギャップアップ・フェード（当日GU+3〜8%を12:30に空売り→引け買戻し）。

■ ルール（_bt_intraday_grand.py で発見・2026-07-26）
    当日 +3〜8% のギャップアップで始まった株（貸借○／株価1,000〜5,000円／前日代金10億+）を
    ギャップの大きい順に上位10まで、**12:30（後場寄り）に空売り** → **大引けで買い戻し**。
  実測（2026-04-28〜07-23・58営業日・コスト0.10%/往復込・50万/枚・実株数ベース）:
    **トップ4 n=221 / PF1.92 / 平均+0.398% / +334,346円**（採用）
    トップ10なら n=475 / PF1.54 / 平均+0.272% / +492,665円
  ⚠️ 2026-07-26に数字を訂正した。旧記載「PF1.75 / +865,367円」は
     ①エントリーが12:30（足ラベルの読み違い＝実際には約定できない時刻）
     ②損益を「50万フル投下」で計算（実際は100株単位で丸まり実稼働76%）
     の2つで43%盛れていた。12:30→12:45の訂正だけでPF1.74→1.54に落ちる。
  月別PF（トップ4）: 5月1.32 / 6月1.44 / 7月5.32 ＝ **7月が突出した外れ月**。
    5-6月は勝率5割前後でコスト込みほぼトントン。7月の利益の4割は7/22の1日。
  ⚠️ 58日=1相場しか見ていない。実弾は小さく始めて紙台帳で答え合わせしながら判断する。

■ なぜ12:30か
    10:00=PF1.36 / 11:00=1.24 / **12:30=1.80** / 13:00=1.37 / 14:00=0.93。
    後場寄りは板が厚く、午前を持ちこたえたギャップ玉が午後に垂れる。14時では遅い。

■ 実行の注意
    ・空売り価格規制は「下落時」の規制なので、跳ねて始まった玉を売る本ルールは原則対象外
    ・日計り（当日決済）なので逆日歩はかからない。一日信用のプレミアム料は別途
    ・+8%超は売り禁/在庫なし/S高張り付きの壁を踏みやすいので**帯から外す**（BTでも牽引していない）
    ・約定したらすぐ引成の返済予約（未決済だと強制決済＋手数料）

実行:
  python -X utf8 gapfade.py            # 取得→過去分の答え合わせ→本日の候補→Discord通知
  python -X utf8 gapfade.py --dry      # Discordに送らない
  python -X utf8 gapfade.py --report   # 取得せず台帳の成績だけ
"""
from __future__ import annotations

import json
import os
import pickle
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

JST = timezone(timedelta(hours=9))
GAP_LO, GAP_HI = 3.0, 8.0
PX_LO, PX_HI = 1000, 5000
TURN_MIN = 1e9
# 上位4本（2026-07-26検証）。10本だと380万拘束して資本比+12.9%だが、4本なら152万で+22.0%
# ＝同じ資金なら4本の方が働く。3本以下は5月がPF0.77で負けるので4が下限。
# 並び順はギャップ降順のまま＝総当たり(VWAP乖離/日中位置/寄りからの騰落/代金/株価/ランダム)で
# 現行が最良(PF1.92 vs ランダム1.74)。gap昇順にするとPF0.92で負ける＝方向性は本物。
TOP_N = 4
# 【重要】Yahooの15分足は「足の開始時刻」ラベル。ラベル"12:30"の足は 12:30〜12:45 を指し、
# その終値＝**12:45時点の価格**。BTもこの値で計測しているので、実際の発注は12:45の成行。
# 実測(59営業日): 12:45成行 PF1.70(前半1.51/後半1.97) > 12:30後場寄り成行 PF1.50(1.34/1.72)
#                > 13:00 PF1.52 > 13:15 PF1.38。前場は9:00がPF1.15(前半0.85)で不合格。
# ＝後場が始まって15分待つのが最良。12:30成行でも合格ラインは通る（差は3ヶ月で約8万円）。
# エントリー＝後場寄り成行（12:30）。昼休み(11:30-12:30)中に成行を仕込めば必ず約定する。
# 2026-07-26に12:45の足終値から変更＝12:45ちょうどに板に張り付く運用は現実的でないため。
# 実測(トップ4・58営業日)では劣化しない: 後場寄り 平均+0.435%/PF1.71/+362,161円 vs
# 12:45 平均+0.398%/PF1.92/+334,346円。PFは下がるが平均と総額は上、しかも月別が
# 1.29/2.32/1.17と均等（12:45は5.32＝7月偏重）＝むしろ頑健。
ENTRY_HM = "12:30"          # 後場寄りの足
ENTRY_LABEL = "後場寄り(12:30)"
COST = 0.10
CAPITAL = 500_000
LEDGER = "gapfade_ledger.json"
UNIV = "_intraday_targets.json"
# 同日の二重配信ガード（2026-07-28）。後場寄り12:30の発注に間に合わせるため
# GitHub cron を複数本＋外部トリガーの多重化にしたので、最初に届いた1本だけ配信して
# 以降はスキップする。投げ売りブースターが同じガード漏れで4連投した前例あり。
SEND_MARKER = "gapfade_last_send.json"

# ── 時刻ガード（2026-07-29 新設）────────────────────────────────────────────
# 実測: この workflow の GitHub cron は **毎日3時間以上遅れて起動する**。
#   予定 9:25→実起動12:46 / 10:10→13:25 / 10:50→13:53 / 11:20→14:32（7/28も同じ）。
# その結果 7/29 は 12:56 に「12:30に撃て」と配信していた＝**26分手遅れの指示**。
# cronを増やしても全部が同じだけずれるので本数では解決しない。
#   ①早すぎ(寄り値が未確定)なら何も送らず終了する＝同日ガードを消費しない
#   ②締切(=エントリー時刻)を過ぎたら、撃てと言わずに「遅延・本日見送り」として送る
# 恒久解は cron-job.org → Cloudflare Worker → workflow_dispatch（他システムでは毎日
# 8:05きっかりに着弾している実績あり）。本人のWorker登録が済むまでの防御がここ。
EARLIEST_HM = "09:46"       # 2026-09-04監査: daily_frames は当日の15分足4本(9:00/9:15/9:30/9:45)が必要。
                            # 9:30便は7/7日で候補0→「該当なし」を送りマーカーを消費し、直後の本物の候補(8/27:4本 8/28:4本 8/31:2本)を
                            # 「配信済み」で握り潰していた。エントリーは12:30なので9:46開始でも十分間に合う。
DEADLINE_HM = "12:20"       # 後場寄り12:30に成行を仕込める最終ライン（10分前）


def _now_hm() -> str:
    return datetime.now(JST).strftime("%H:%M")


def timing_state(now_hm: str | None = None) -> str:
    """'early'（寄り値前）/ 'ok'（間に合う）/ 'late'（エントリー時刻を過ぎた）。"""
    hm = now_hm or _now_hm()
    if hm < EARLIEST_HM:
        return "early"
    if hm > DEADLINE_HM:
        return "late"
    return "ok"


def _load_iss() -> dict:
    """貸借区分 {code4: IssType}。①J-Quants直（CIで動く・鮮度も最良）②ローカルpklフォールバック。

    【2026-08-04 バグ修正】旧実装はローカルpklだけを読んでいたが、pklは.gitignoreの*.pkl
    ルールでCIに存在しない＝本番は毎朝空dictになり、貸借判定で全銘柄が弾かれて
    **稼働開始(7/27)から8営業日ずっと「該当なし」に偽装されていた**（BT発火率は3.8本/日）。
    どちらも失敗した日は {} を返し、呼び出し側が「判定不能」として配信する（該当なしと区別）。"""
    key = (os.getenv("JQUANTS_API_KEY") or "").strip()
    if key:
        try:
            import requests
            d0 = datetime.now(JST).date() - timedelta(days=4)   # 週次公表ラグの安全側
            for _ in range(14):
                if d0.weekday() < 5:
                    r = requests.get("https://api.jquants.com/v2/markets/margin-interest",
                                     headers={"x-api-key": key},
                                     params={"date": d0.strftime("%Y-%m-%d")},
                                     timeout=(10, 50))
                    if r.status_code == 200:
                        rows = r.json().get("data", [])
                        if rows:
                            out = {}
                            for x in rows:
                                c = str(x.get("Code", "")).strip()
                                if len(c) >= 5 and c[4] != "0":
                                    continue    # 優先株(94345等)の4桁衝突ガード（7a64848と同じ）
                                out[c[:4]] = str(x.get("IssType", ""))
                            print(f"[iss] J-Quants {d0} 断面 {len(out)}銘柄")
                            return out
                    time.sleep(1.2)
                d0 -= timedelta(days=1)
            print("[iss] J-Quants 14日遡っても空")
        except Exception as e:
            print(f"[iss] API取得失敗: {e}")
    try:
        d = pickle.load(open("_iss_type_by_year.pkl", "rb"))
        m = d[sorted(d)[-1]]
        print(f"[iss] ローカルpklフォールバック {len(m)}銘柄")
        return m
    except Exception:
        return {}


def fetch(days: str = "5d") -> dict:
    """対象銘柄の15分足を取得。当日の寄り値と前日終値が要るので数日分あれば足りる。"""
    try:
        import truststore
        truststore.inject_into_ssl()          # ローカルAVのSSL傍受を越える（CIでは不要だが無害）
    except Exception:
        pass
    import requests

    targets = json.load(open(UNIV, encoding="utf-8"))
    out, ng, t0 = {}, 0, time.time()
    print(f"[fetch] {len(targets)}銘柄 / {days} / 15分足", flush=True)
    for i, tk in enumerate(targets, 1):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{tk}?range={days}&interval=15m"
        for a in range(3):
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
                if r.status_code == 429:
                    time.sleep(5 * (a + 1)); continue
                res = r.json()["chart"]["result"][0]
                q = res["indicators"]["quote"][0]
                df = pd.DataFrame({"t": res["timestamp"], "o": q["open"], "h": q["high"],
                                   "l": q["low"], "c": q["close"], "v": q["volume"]}).dropna()
                if len(df):
                    df["dt"] = pd.to_datetime(df["t"], unit="s", utc=True).dt.tz_convert("Asia/Tokyo")
                    out[tk] = df
                break
            except Exception:
                time.sleep(1.2 * (a + 1))
        else:
            ng += 1
        if i % 200 == 0:
            print(f"  {i}/{len(targets)} / {time.time()-t0:.0f}秒", flush=True)
        time.sleep(0.3)
    print(f"[fetch] {len(out)}銘柄 取得 / 失敗{ng} / {time.time()-t0:.0f}秒", flush=True)
    return out


def daily_frames(store: dict):
    """{ticker: {date: {open, close, turn, bars}}} に整形。"""
    out = {}
    for tk, df in store.items():
        d = df.copy()
        d["date"] = d["dt"].dt.strftime("%Y-%m-%d")
        d["hm"] = d["dt"].dt.strftime("%H:%M")
        per = {}
        for dt_, sub in d.groupby("date"):
            if len(sub) < 4:
                continue
            per[dt_] = {"open": float(sub["o"].iloc[0]), "close": float(sub["c"].iloc[-1]),
                        "turn": float((sub["c"] * sub["v"]).sum()), "sub": sub}
        out[tk] = per
    return out


def candidates(frames: dict, day: str, iss: dict) -> list[dict]:
    """当日のギャップアップ候補（未来情報なし＝寄り値と前日終値だけで決まる）。"""
    rows = []
    for tk, per in frames.items():
        ds = sorted(per)
        if day not in per:
            continue
        i = ds.index(day)
        if i == 0:
            continue
        prev = per[ds[i - 1]]
        cur = per[day]
        if prev["turn"] < TURN_MIN or not (PX_LO <= cur["open"] <= PX_HI):
            continue
        if iss.get(str(tk)[:4], "?") != "2":
            continue
        gap = (cur["open"] - prev["close"]) / prev["close"] * 100
        if not (GAP_LO <= gap < GAP_HI):
            continue
        rows.append({"ticker": tk, "gap": round(gap, 2), "open": cur["open"],
                     "prev_close": prev["close"], "shares": int(CAPITAL // cur["open"] // 100 * 100)})
    rows.sort(key=lambda r: -r["gap"])
    return rows[:TOP_N]


def price_at(sub: pd.DataFrame, hm: str) -> float | None:
    """後場寄りの約定価格＝12:30足の【始値】。終値(=12:45)ではない点に注意。"""
    idx = np.where(sub["hm"].to_numpy() >= hm)[0]
    return float(sub["o"].iloc[idx[0]]) if len(idx) else None


def settle(frames: dict, ledger: list[dict]) -> int:
    """未決済の記録を、その日の12:30値と終値で確定する。"""
    n = 0
    for r in ledger:
        if r.get("pnl") is not None:
            continue
        per = frames.get(r["ticker"], {})
        cur = per.get(r["date"])
        if cur is None:
            continue
        ep = price_at(cur["sub"], ENTRY_HM)
        if ep is None:
            continue
        xp = cur["close"]
        r["entry_px"], r["exit_px"] = round(ep, 1), round(xp, 1)
        r["pnl"] = round((ep - xp) / ep * 100 - COST, 3)
        r["yen"] = int(r["pnl"] / 100 * CAPITAL)
        n += 1
    return n


def already_sent(day: str) -> bool:
    """当日分を配信済みか（多重トリガーの2本目以降を弾く）。"""
    try:
        with open(SEND_MARKER, encoding="utf-8") as f:
            return json.load(f).get("date") == day
    except Exception:
        return False


def mark_sent(day: str) -> None:
    with open(SEND_MARKER, "w", encoding="utf-8") as f:
        json.dump({"date": day,
                   "sent_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")},
                  f, ensure_ascii=False, indent=2)


def notify(day: str, cand: list[dict], stats: str, dry: bool,
           state: str = "ok", now_hm: str | None = None, no_iss: bool = False,
           no_px: bool = False) -> bool:
    """Discordへ配信。実際に送信できたら True（マーカーを書く条件）。
    state='late' のときは「撃て」と言わず、遅延で見送りになった事実だけを送る。
    no_iss=True は貸借区分が取れなかった日＝「該当なし」でなく「判定不能」を明示する。"""
    # 専用チャンネル（2026-07-26 本人指定）。未設定なら🔻フェードと同じDAY chへフォールバック。
    hook = (os.getenv("DISCORD_WEBHOOK_GAPFADE_URL")
            or os.getenv("DISCORD_WEBHOOK_DAY_URL")
            or os.getenv("DISCORD_WEBHOOK_URL_DAY") or "").strip()
    hm = now_hm or _now_hm()
    if state == "late":
        # エントリー時刻を過ぎてから届いた＝この指示はもう実行できない。
        # 「撃て」と書くと手遅れの発注を誘発するので、銘柄は参考表示に留める。
        head = (f"⚠️ **配信が遅れました（{hm} 着・エントリーは{ENTRY_LABEL}）**\n"
                f"**本日は見送りです。**この時刻から後場寄りの成行は出せません。\n"
                f"原因: GitHub cron の起動遅延（実測で毎日3時間以上）。恒久解は外部トリガー。\n\n")
        if cand:
            ref = "\n".join(f"（参考）{i+1}. {c['ticker']} ギャップ+{c['gap']:.1f}% 寄¥{c['open']:,.0f}"
                            for i, c in enumerate(cand))
            body = head + "本日該当していた銘柄（記録用・撃たない）:\n" + ref
        else:
            body = head + f"なお本日はギャップ+{GAP_LO:.0f}〜{GAP_HI:.0f}%の該当なしでした。"
    elif no_px:
        body = ("⚠️ **価格データが半分以上取れず判定不能**（該当なしではありません）。\n"
                "次のトリガーで自動再試行します。続くようならYahoo側の障害の可能性。")
    elif no_iss:
        # 貸借区分が取れない＝候補ゼロはシステム障害であって相場の「該当なし」ではない。
        # マーカーを立てないので後続の保険トリガーが自動で再試行する。
        body = ("⚠️ **貸借区分が取得できず判定不能**（該当なしではありません）。\n"
                "次のトリガーで自動再試行します。続くようならJ-Quants障害の可能性。")
    elif not cand:
        body = f"本日はギャップ+{GAP_LO:.0f}〜{GAP_HI:.0f}%の該当なし。**撃つ日ではありません。**"
    else:
        lines = [f"**{i+1}. {c['ticker']}** ギャップ **+{c['gap']:.1f}%** ／ 寄¥{c['open']:,.0f} "
                 f"→ {c['shares']}株（約{c['shares']*c['open']/10000:.0f}万）" for i, c in enumerate(cand)]
        body = (f"**{ENTRY_LABEL} に成行で空売り → 大引けで買い戻し**\n"
                f"※いま{hm}。昼休み(11:30-12:30)のうちに後場寄りの成行を仕込んでおく\n"
                "※指値は使わない（刺さらないと見送りになる）。約定したらすぐ引成の返済予約を入れる\n\n"
                + "\n".join(lines))
    payload = {"embeds": [{"title": f"🩳 ギャップフェード {day}",
                           "description": body + f"\n\n{stats}",
                           "color": 0xE4405F,
                           "footer": {"text": "GU+3〜8%・貸借○・1,000〜5,000円・代金10億+ / "
                                              "検証58日 上位4本 平均+0.435%（1相場のみ＝小さく始める）"}}]}
    if dry or not hook:
        print("[notify] 送信スキップ（--dry またはwebhook未設定）\n" + body)
        return False
    import requests
    try:
        r = requests.post(hook, json=payload, timeout=20, verify=False)
    except Exception as e:
        print(f"[notify] Discord送信失敗: {e}（マーカーは立てない）")
        return False
    print(f"[notify] Discord HTTP {r.status_code}")
    # 送信できた時だけ「配信済み」とみなす（webhook失効やタイムアウトで無配信のまま
    # マーカーを立てると、後続の保険トリガーまで黙って死ぬ）。
    return 200 <= r.status_code < 300


def report(ledger: list[dict]) -> str:
    done = [r for r in ledger if r.get("pnl") is not None]
    if not done:
        return "（まだ確定した記録がありません）"
    s = pd.Series([r["pnl"] for r in done])
    gl = -s[s < 0].sum()
    pf = s[s > 0].sum() / gl if gl > 0 else float("inf")
    yen = sum(r["yen"] for r in done)
    dates = sorted({r["date"] for r in done})
    return (f"📒 通算 {len(done)}件 / {len(dates)}日 ・ 勝率{(s>0).mean()*100:.1f}% ・ "
            f"PF{pf:.2f} ・ **{yen:+,}円**（50万/枚）")


if __name__ == "__main__":
    dry = "--dry" in sys.argv
    led = json.load(open(LEDGER, encoding="utf-8")) if os.path.exists(LEDGER) else []
    if "--report" in sys.argv:
        print(report(led))
        for r in [x for x in led if x.get("pnl") is not None][-15:]:
            print(f"  {r['date']} {r['ticker']:<8s} GU+{r['gap']:.1f}% "
                  f"{r['entry_px']:>8,.1f}→{r['exit_px']:>8,.1f} {r['pnl']:+6.2f}% {r['yen']:+8,}円")
        raise SystemExit(0)

    from dotenv import load_dotenv
    load_dotenv()

    # 寄り値が確定する前（9:20 JST未満）に起動した場合は何もしない。
    # ここで空配信すると同日ガードを消費して、間に合う時刻の後続便が黙って死ぬ。
    try:                                            # 2026-09-04監査: 祝日に「該当なし」を送らない
        import jpholiday
        _d = datetime.now(JST).date()
        if not dry and (_d.weekday() >= 5 or jpholiday.is_holiday(_d) or (_d.month == 12 and _d.day == 31) or (_d.month == 1 and _d.day <= 3)):
            print(f"[guard] {_d} は休場日 → 何もせず終了"); raise SystemExit(0)
    except SystemExit:
        raise
    except Exception as _e:
        print(f"[guard] 休場判定スキップ: {_e}")
    state = timing_state()
    if state == "early" and not dry:
        print(f"[guard] {_now_hm()} JST は寄り値の確定前（{EARLIEST_HM}未満）→ 何もせず終了")
        raise SystemExit(0)

    store = fetch()
    frames = daily_frames(store)
    try:
        _n_targets = len(json.load(open(UNIV, encoding="utf-8")))
    except Exception:
        _n_targets = 0
    no_px = bool(_n_targets) and len(store) < 0.5 * _n_targets   # 2026-09-04監査: Yahoo障害で半分以上取れない＝判定不能
    _today_s = datetime.now(JST).strftime("%Y-%m-%d")
    _cov = (sum(1 for _per in frames.values() if _today_s in _per) / len(frames)) if frames else 0.0
    if frames and _cov < 0.5:                       # 当日の足が半分の銘柄にも無い＝早すぎ/データ遅延＝判定不能（マーカー無し）
        print(f"[guard] 当日足のある銘柄 {_cov*100:.0f}% <50% → 判定不能扱い（該当なしとは別）"); no_px = True
    if no_px:
        print(f"[guard] 価格取得 {len(store)}/{_n_targets} 銘柄＝判定不能（該当なしとは別・マーカーを立てない）")
    iss = _load_iss()
    no_iss = not iss          # 貸借区分が取れない＝候補判定不能（該当なしと区別する）
    today = datetime.now(JST).strftime("%Y-%m-%d")

    # 答え合わせ・台帳更新は毎回やる（冪等）。配信だけ1日1回に絞る。
    n = settle(frames, led)
    print(f"[settle] {n}件を確定")

    cand = [] if (no_iss or no_px) else candidates(frames, today, iss)
    seen = {(r["date"], r["ticker"]) for r in led}
    for c in cand:
        if (today, c["ticker"]) not in seen:
            led.append({"date": today, **c, "entry_px": None, "exit_px": None, "pnl": None, "yen": None})
    led.sort(key=lambda r: (r["date"], -r["gap"]))
    json.dump(led, open(LEDGER, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[cand] {today} の候補 {len(cand)}件 / 台帳{len(led)}件" + ("（貸借判定不能）" if no_iss else ""))

    if already_sent(today) and not dry:
        print(f"[notify] 本日({today})は配信済み → スキップ（多重トリガーの2本目）")
    else:
        if state == "late":
            print(f"[guard] {_now_hm()} JST はエントリー({ENTRY_LABEL})を過ぎている"
                  f" → 「撃て」ではなく遅延通知として配信する")
        # no_iss の日はマーカーを立てない＝後続の保険トリガーが自動で再試行する
        if notify(today, cand, report(led), dry, state=state, no_iss=no_iss, no_px=no_px) and not (no_iss or no_px):
            mark_sent(today)
