# -*- coding: utf-8 -*-
"""
daytrade_paper.py — デイトレv2 紙トレ台帳（記帳・決済・通算成績＋信用売り可否チェック）
====================================================================================
【役割】既存の main_day.py / screener_day / screener_sell_day には一切手を加えず、
        「発火の答え合わせ」を自動で積み上げて、実弾投入前に通算成績を可視化する層。

【非破壊設計】
  - main_day.main() の成功パス末尾から run(today, signals) を呼ぶ（例外は握りつぶす想定）。
  - 単体実行も可: `python daytrade_paper.py [--dry] [--test]`（day_signals.json から当日発火を読む）。
  - 台帳は positions_day_paper.json（CIでコミットして永続化）。

【決済ロジック（v2は当日完結：寄り→引け）】
  - 記帳時: basis_date = シグナル算出の最終確定足（＝前営業日）。エントリー実セッション = basis_dateの翌取引日。
  - 決済時: そのティッカーの basis_date より後の最初の足を取り、Open/Close で損益確定。
      BUY : 寄り > MAX指値 → 見送り(SKIP) / それ以外 pnl=(引-寄)/寄
      SELL: 寄り < MIN指値 → 見送り(SKIP) / それ以外 pnl=(寄-引)/寄
  - 当日足はまだ無い（寄り前実行）ため、決済は翌営業日以降の実行で自然に確定する。
  - basis_date が14暦日超過しても足が取れない（売買停止/上場廃止）→ expired で台帳から退避。

【信用売り可否】J-Quants /markets/margin-interest の IssType（"2"=貸借銘柄＝空売り可）を利用。
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone
import zoneinfo

import jpholiday
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")
BOOK_FILE = "positions_day_paper.json"
DAY_SIGNALS_FILE = "day_signals.json"
# 1建玉サイズ（株数・通算損益円・値がさカット(1単元≤この額)が連動）。
# 2026-07-23に100万→50万、2026-07-28に100万へ戻した。本人目標「年100万」に対し、
# 10年検証(乖離+ATR順・上位1本)で 50万=年+41.5万 / 100万=年+94.0万 / 150万=年+146.9万。
# 100万は10年すべて勝ち年（最悪年 +29.7万）で、日中100万しか使わない＝夜またぎゼロ。
# 代償: 1件の最悪が -21.8万 → -43.8万 に倍増する。
CAPITAL_PER_TRADE = 1_000_000
EXPIRE_DAYS = 14
# フェードのGO閾値（前日上昇率）。2026-07-28に +12% → +6% へ。
# 「ほぼ毎日撃ちたい」という本人要望に対し、10年検証で約定日率 50%→92% に上がり、
# かつ並び順を乖離+ATRにしたことで年+73.5万→+94.0万・勝ち11/11年と質も落ちない。
# （旧コメントの「+12%が総額最良」は _iss_type_by_year.pkl に2019/2024/2025が無く
#  その3年を落として測った値だった＝2026-07-28に欠年を補完して再測定し訂正）
# ライブ実弾の screener_sell_day は別系統で +25% 据置。
DAILY_PICK_GAIN_MIN = 6.0


# ------------------------------------------------------------------ util
def _today_jst_date():
    return datetime.now(JST).date()


def is_trading_day(d) -> bool:
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def _code4(ticker: str) -> str:
    return ticker.split(".")[0][:4]


def load_book() -> dict:
    if os.path.exists(BOOK_FILE):
        with open(BOOK_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"positions": [], "expired": [], "last_report_date": None}


def save_book(book: dict) -> None:
    with open(BOOK_FILE, "w", encoding="utf-8") as f:
        json.dump(book, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------------ 信用売り可否
def fetch_iss_map(token) -> dict:
    """{code4: IssType} を直近公表週の /markets/margin-interest から。取れなければ {}。"""
    try:
        from screener import _jquants_get
        cur = _today_jst_date() - timedelta(days=4)
        for _ in range(14):
            if cur.weekday() < 5:
                d = _jquants_get("/markets/margin-interest", token,
                                 {"date": cur.strftime("%Y-%m-%d")})
                rows = d.get("data", [])
                if rows:
                    return {str(r.get("Code", ""))[:4]: str(r.get("IssType", "")) for r in rows}
            cur -= timedelta(days=1)
    except Exception as e:
        print(f"[paper] iss_map取得失敗: {e}")
    return {}


def fetch_ratio_map(token) -> dict:
    """{code4: 信用倍率(買残/売残)} を直近週の margin-interest から。売残0は99(=借りやすい)。取れなければ {}。"""
    try:
        from screener import _jquants_get
        cur = _today_jst_date() - timedelta(days=4)
        for _ in range(14):
            if cur.weekday() < 5:
                d = _jquants_get("/markets/margin-interest", token, {"date": cur.strftime("%Y-%m-%d")})
                rows = d.get("data", [])
                if rows:
                    out = {}
                    for r in rows:
                        sv = float(r.get("ShrtVol") or 0)
                        lv = float(r.get("LongVol") or 0)
                        out[str(r.get("Code", ""))[:4]] = (lv / sv) if sv > 0 else 99.0
                    return out
            cur -= timedelta(days=1)
    except Exception as e:
        print(f"[paper] ratio_map取得失敗: {e}")
    return {}


def fetch_alert_map(token) -> dict:
    """{code4: {"jsf_stop","jsf_warn","tse_reg","daily_pub"}} を /markets/margin-alert（日々公表・
    規制銘柄情報）の直近公表日から。**jsf_stop=RestrictedByJSF=日証金の貸借取引申込停止＝売り禁**
    （制度信用の新規売り不可）。jsf_warn=注意喚起（売れるが逆日歩警戒）・tse_reg=取引所規制（増担保等）・
    daily_pub=日々公表。夕方公表なので朝runの最新は前営業日分（当日朝指定の新規売り禁は
    拾えないことがある→配信の「最終確認はSBI」注記でカバー）。取れなければ {}＝フェイルオープン。"""
    try:
        from screener import _jquants_get
        cur = _today_jst_date()
        for _ in range(8):
            if cur.weekday() < 5:
                d = _jquants_get("/markets/margin-alert", token,
                                 {"date": cur.strftime("%Y-%m-%d")})
                rows = d.get("data", [])
                if rows:
                    out = {}
                    for r in rows:
                        pr = r.get("PubReason") or {}
                        out[str(r.get("Code", ""))[:4]] = {
                            "jsf_stop":  pr.get("RestrictedByJSF") == "1",
                            "jsf_warn":  pr.get("PrecautionByJSF") == "1",
                            "tse_reg":   pr.get("Restricted") == "1",
                            "daily_pub": pr.get("DailyPublication") == "1",
                        }
                    print(f"[paper] alert_map: {cur} 公表分 {len(out)}銘柄"
                          f"（売り禁{sum(1 for v in out.values() if v['jsf_stop'])}）")
                    return out
            cur -= timedelta(days=1)
    except Exception as e:
        print(f"[paper] alert_map取得失敗: {e}")
    return {}


def borrow_grade(ratio) -> str:
    """信用倍率から借りやすさ+フェード強度グレード。10年BT: 売残少(>=10)は借り易くPF1.6-1.9・
    売り長(<1)は最強PF4.9だが借りにくい・1-10は普通。"""
    if ratio is None:
        return "貸株?"
    if ratio < 1:
        return "⭐売り長(最強だが借りにくい・逆日歩注意)"
    if ratio >= 10:
        return "◎売残少(空売り楽・よく落ちる)"
    return "○普通"


def shortability(ticker: str, iss_map: dict) -> dict:
    """信用売り可否の判定を返す。○=貸借銘柄(空売り可) / ×=信用銘柄(制度信用売り不可) / ?=不明。"""
    it = iss_map.get(_code4(ticker))
    if it == "2":
        return {"mark": "○", "iss": it,
                "note": "貸借銘柄＝制度信用で空売り可。ただし増担保・日々公表・逆日歩は当日板で要確認。"}
    if it:
        return {"mark": "×", "iss": it,
                "note": "信用銘柄（貸借でない）＝制度信用の空売り不可。一般信用（売り）在庫があれば可、無ければ見送り。"}
    return {"mark": "?", "iss": None,
            "note": "貸借区分データ無し（新興/新規上場など）。SBIで一般信用売り在庫の有無を要確認。"}


# ------------------------------------------------------------------ データ
def _fetch_all(today):
    """J-Quantsを日付ベースで一括取得（batch_downloadは全銘柄を返す）。決済＋紙SELLスキャン共用。"""
    from screener import batch_download_jquants, _jquants_id_token
    token = _jquants_id_token()
    # 45暦日≒31営業日。当日を除いても20日平均に足る履歴を確保（30日だと不足でスキップ）。
    start = (today - timedelta(days=45)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    return batch_download_jquants(token, start=start, end=end)


# 後方互換（テスト等が参照）: 全銘柄取得に委譲
def _fetch_data(tickers, today):
    return _fetch_all(today)


FADE_CAND_MIN = 5.0        # 候補プールの下限（GO閾値 DAILY_PICK_GAIN_MIN より緩く取り、表示用に残す）
FADE_TOV_MIN = 3e8         # 流動性フロア（20日代金中央値3億・BTと同一）
STICKY_RANGE_MIN = 0.05    # 張り付き除外: 信号日レンジ(高-安)/終値がこれ以下=ロックS高=踏み上げ危険で除外

# 出来高比の上限（2026-07-28検証）。20日平均のこの倍数を超えた急騰は候補から外す。
# 出来高が爆発しすぎた玉は「本物の材料が入った」可能性が高く、翌日も買われ続けてフェードが効かない。
# 帯別（10年・上位1本・100万）: 〜3倍PF1.24 / 3-6倍PF1.34 / **6-12倍PF0.81（年-8.0万）**。
# 除外の効果: 年+94.0万→+98.2万・最悪年+29.7万→**+49.9万**・両期間でPF改善(前1.20→1.22/後1.25→1.27)。
# 除外すると次点が繰り上がるので件数はほぼ減らない（2,230→2,202）＝毎日撃てるのは不変。
# 無効化は None。
FADE_VOL_RATIO_MAX = 6.0

# プレミアム料（SBI一日信用/HYPERの空売り・円/株/日）の損益分岐。10年の gross 取り分から算出:
#   上寄りの日に約定した玉（＝寄指で通る本体）: 平均 +0.509% ＝ 100万建玉で 5,091円
#   下寄りの日に追加される玉                : 平均 +0.254% ＝ 100万建玉で 2,537円
# プレミアム料がこれを食い切ると期待値が消えるので、発注画面の総額と見比べる線として配信に出す。
#   総額 〜2,500円     → 下寄りでも撃ってよい
#   総額 2,500〜5,000円 → 上寄り（寄指が通る日）だけ
#   総額 5,000円超      → その銘柄は撃たない
# 貸借銘柄を制度信用で売る場合はプレミアム料が無く、貸株料 年1.15%＝1日0.003% だけなので
# 実質ゼロ＝この判定は不要（売り禁でHYPER/一般信用を使う時だけ効く）。
FADE_ENTRY_MARKET = True       # True=寄付成行で撃つ（下寄りも建てる）/ False=寄指（下寄りは見送り）
FADE_EDGE_PCT_MAIN = 0.509     # 上寄りの玉の gross 期待値%
FADE_EDGE_PCT_GAPDN = 0.254    # 下寄りの玉の gross 期待値%

# 寄りのギャップアップ下限（2026-07-27検証・_bt_fade_size.py系の10年分析で発見）:
# 旧ルールは「寄り≥前日終値」＝ギャップ0%以上なら何でも建てていたが、**ほぼフラットで寄る玉は
# 両期間ともPF1未満の負け筋**（GU 0〜1%: 前半PF0.80 / 後半PF0.87・n=295）。前日+12%も上げた
# のに翌朝上に寄らない＝もう熱が冷めていて、そこから売っても落ちない。
# 下限1%にすると 10年 PF1.45→1.72・+347万→+376万・前半PF1.46→1.72・後半1.45→1.71 と
# 両期間で改善し、総額のピークもここ（0.5%:+368万 / 1.5%:+362万 / 2%:+342万）。
# スイング買いの寄指（前日終値×1.01）と同じ形。無効化は FADE_MIN_GAP_UP_PCT = 0.0。
# 2026-07-28 撤回して 0.0 に戻した。前日 GU≥1% を入れたが、上位8本・10年でしか
# 良く見えておらず、実運用の上位1〜3本・5年で測ると総額が下がり最悪年が2〜5倍悪化した
# （+12%×上位1: 最悪年 -5.9万 → -16.9万 / +8%×上位1: -9.3万 → -32.6万）。
# 件数が減って集中度が上がる分テールが太る。寄指は「下寄りは見送る」(GU≥0)のみが正しい。
FADE_MIN_GAP_UP_PCT = 0.0
                           # 10年BT: 除外でPF1.44→1.62・11年全プラス。7月は-36万→+75万に逆転。
# 配信・記帳する本数。2026-07-28に 8 → 2。順位別の期待値を10年で測った結果、
# エッジは1番にほぼ全部集中しており、3番以降は撃つと損になるため（1玉100万）:
#   1番 勝率56.1% 平均+0.46% PF1.22 年+94.0万 最悪年+29.7万 勝ち11/11年
#   2番 勝率53.3% 平均+0.18% PF1.11 年+32.7万 最悪年-38.8万 勝ち 8/11年
#   3番 勝率51.4% 平均-0.01% PF0.99 年 -1.1万 最悪年-105万  勝ち 6/11年 ← 期待値ゼロ以下
#   4番以降はすべてマイナス。1+2で年+126.7万、3番を足すと+125.6万で逆に減る。
# 運用ルール: **撃つのは1番。1番が売り禁/在庫なし/寄指不成立の時だけ2番。3番以降は撃たない。**
# 8本出していた頃は「予備」として4番以降も見せていたが、迷いの元なので表示ごと落とした。
PAPER_MAX_PICKS = 2


def daily_top_fades(data: dict, today, iss_map: dict, n: int = PAPER_MAX_PICKS,
                    ratio_map: dict | None = None, alert_map: dict | None = None,
                    excluded_out: list | None = None) -> list[dict]:
    """毎日『フェード上位N銘柄』を上昇率降順で返す（各GO/NO-GO判定付き・空なら[]）。
    選定＝貸借○ × 前日+5%以上 × 張り付き除外(信号日レンジ>5%)。
    判定: 前日+12%以上(DAILY_PICK_GAIN_MIN) → GO（撃つ／紙）。それ未満は薄い → NO-GO（見送り）。
    売り禁(日証金申込停止)は**除外せず🚫バッジ表示**（2026-07-23本人指示「ハイカラで売れた」＝
    制度信用✕でもSBI一日信用HYPER/一般信用は自社在庫の別枠で売れることがある。jsf_stopフラグ付与・
    紙台帳にも記録し在庫依存分を後で分離分析できるようにする）。7/22の完全除外仕様は1日で廃止。
    注意喚起/増担保/日々公表も reg_note ⚠️注記のみ。excluded_out は旧互換で常に空。
    10年検証: 張り付き除外+上位3で+12%が総額最良(+26.4M・11年全プラス)。"""
    if not data:
        return []
    today_str = today.strftime("%Y-%m-%d")

    # 直近の市場営業日（データ全体の最終足）。売買停止中の銘柄が「古い+12%」のまま
    # 候補化して停止明けギャップに突っ込むのを防ぐ（鮮度ガード・2026-07-22実弾前監査で追加）
    last_mkt = None
    for df in data.values():
        if df is None or df.empty:
            continue
        mx = df.index.max()
        ds = mx.strftime("%Y-%m-%d")
        if ds >= today_str:
            older = df.index[df.index.strftime("%Y-%m-%d") < today_str]
            if len(older) == 0:
                continue
            ds = older.max().strftime("%Y-%m-%d")
        if last_mkt is None or ds > last_mkt:
            last_mkt = ds

    cands = []
    for tk, df in data.items():
        if df is None or df.empty:
            continue
        if iss_map.get(_code4(tk)) != "2":                  # 貸借○のみ（売れる玉だけ）
            continue
        d = df[df.index.strftime("%Y-%m-%d") < today_str]   # 前日までの確定足
        if len(d) < 21:
            continue
        if last_mkt and d.index[-1].strftime("%Y-%m-%d") != last_mkt:
            continue                                        # 最終足が直近営業日でない=停止/古い→除外
        c = d["Close"].astype(float)
        v = d["Volume"].astype(float)
        h = d["High"].astype(float)
        lo = d["Low"].astype(float)
        last_c = float(c.iloc[-1]); prev_c = float(c.iloc[-2])
        if last_c < 300 or prev_c <= 0:
            continue
        if last_c * 100 > CAPITAL_PER_TRADE:   # 1単元(100株)が予算超=値がさで建てられない→除外
            continue
        vol_avg = float(v.iloc[:-1].tail(20).mean())
        if vol_avg < 100_000:
            continue
        tov20 = float((c * v).tail(20).median())
        if tov20 < FADE_TOV_MIN:
            continue
        gain = (last_c - prev_c) / prev_c * 100
        if gain < FADE_CAND_MIN:
            continue
        rng = (float(h.iloc[-1]) - float(lo.iloc[-1])) / last_c
        if rng <= STICKY_RANGE_MIN:                         # 張り付きS高を除外
            continue
        _vr = float(v.iloc[-1]) / vol_avg if vol_avg > 0 else 0.0
        if FADE_VOL_RATIO_MAX is not None and _vr >= FADE_VOL_RATIO_MAX:
            continue                                        # 出来高爆発=本物の材料で翌日も買われる
        # ── 選定に使う2軸（2026-07-28・10年検証で並び順を上昇率→この2軸に変更）──
        # 25MA乖離: どれだけ伸びきっているか / ATR%: どれだけ荒い銘柄か。
        # 両方とも「大きいほど翌日よく落ちる」で、前半後半の両期間で強さが一致した。
        ma25 = float(c.tail(25).mean()) if len(c) >= 25 else 0.0
        dev = (last_c / ma25 - 1) * 100 if ma25 > 0 else 0.0
        pc_s = c.shift(1)
        tr = pd.concat([h - lo, (h - pc_s).abs(), (lo - pc_s).abs()], axis=1).max(axis=1)
        atr_pct = float(tr.tail(14).mean()) / last_c * 100 if last_c > 0 else 0.0
        cands.append({
            "ticker": tk, "name": tk, "direction": "SELL",
            "daily_gain": round(gain, 2),
            "prev_close": round(last_c, 1),
            # 2026-07-28: 成売りに変更したので約定判定には使わない。
            # プレミアム料が高い日に寄指へ切り替えるための「参考価格」として残す。
            "min_entry_price": fade_min_entry_price(last_c),
            "vol_ratio": round(float(v.iloc[-1]) / vol_avg if vol_avg > 0 else 0, 1),
            "range_pct": round(rng * 100, 1),
            "dev25": round(dev, 1),
            "atr_pct": round(atr_pct, 2),
        })
    if not cands:
        return []
    # 25MA乖離とATR%の「順位の平均」で並べる（どちらも降順＝大きいほど上位）。
    # 単位の違う2軸を足すため生値でなく順位で正規化する。10年検証:
    #   上昇率降順(旧) 前半PF1.15/後半1.15・10年+293万・最悪年-8.6万・勝ち8/11年
    #   乖離+ATR(新)   前半PF1.20/後半1.25・10年+415万・最悪年+11.9万・**勝ち11/11年**
    # 配合を乖離0%〜100%のどこに振っても年+89〜97万/勝ち11年で崩れない＝針でなく高原。
    _n = len(cands)
    _rank_dev = {id(x): i for i, x in enumerate(sorted(cands, key=lambda z: -z["dev25"]))}
    _rank_atr = {id(x): i for i, x in enumerate(sorted(cands, key=lambda z: -z["atr_pct"]))}
    for x in cands:
        x["pick_score"] = round((_rank_dev[id(x)] + _rank_atr[id(x)]) / 2 / max(_n, 1), 4)
    # GO基準(上昇率≥DAILY_PICK_GAIN_MIN)を満たす玉を必ず先に並べ、その中を pick_score 順にする。
    # BTは「上昇率≥閾値に絞ってから乖離+ATRで並べた上位1本」を撃つ想定なので、閾値未満を
    # 混ぜて並べると画面の「1番」がBTの1番と食い違う（2026-07-28: 上昇率5.6%のインフォマートが
    # 乖離20.9%で5番に入り、GO対象のフリー(7番)/カバー(8番)より上に表示されていた）。
    # 閾値未満は候補として残すが必ず後ろ＝「1番＝撃つ玉」が常に成立する。
    cands.sort(key=lambda x: (x["daily_gain"] < DAILY_PICK_GAIN_MIN, x["pick_score"]))
    picks = cands[:max(1, n)]
    try:  # 銘柄名補完（上位数件のみ・軽量）
        from screener import fetch_tse_universe
        nm = {t: n2 for t, n2 in fetch_tse_universe()}
        for p in picks:
            p["name"] = nm.get(p["ticker"], p["ticker"])
    except Exception:
        pass
    for i, p in enumerate(picks, 1):
        sh = shortability(p["ticker"], iss_map)
        p["short"] = sh
        p["rank"] = i
        rt = (ratio_map or {}).get(_code4(p["ticker"]))
        p["ratio"] = round(rt, 1) if rt is not None else None
        p["borrow"] = borrow_grade(rt)
        # 規制注記（売り禁も表示する=2026-07-23本人指示「ハイカラで売れた」。
        # 制度信用の新規売りは不可だがSBI一日信用HYPER/一般信用は自社在庫の別枠で売れることがある）
        al = (alert_map or {}).get(_code4(p["ticker"])) or {}
        p["jsf_stop"] = bool(al.get("jsf_stop"))
        regs = []
        if p["jsf_stop"]:
            regs.append("🚫売り禁(制度✕・ハイカラ/一般信用の在庫があれば可)")
        if al.get("jsf_warn"):
            regs.append("⚠️日証金注意喚起(逆日歩警戒)")
        if al.get("tse_reg"):
            regs.append("⚠️増担保等規制中")
        elif al.get("daily_pub"):
            regs.append("📢日々公表(規制近接)")
        p["reg_note"] = "・".join(regs)
        go = p["daily_gain"] >= DAILY_PICK_GAIN_MIN and sh["mark"] == "○"
        p["verdict"] = "GO" if go else "NOGO"
        if not go:
            p["nogo_reason"] = f"前日+{p['daily_gain']:.0f}%<{DAILY_PICK_GAIN_MIN:.0f}%＝薄い(コスト後トントン帯)"
    return picks


def fade_min_entry_price(prev_close: float) -> float:
    """フェードの寄指MIN価格 = 前日終値×(1+FADE_MIN_GAP_UP_PCT%) を呼値単位で**切り上げ**。

    切り上げる理由: これは「この価格以上で寄らなければ建てない」下限なので、丸めで下振れすると
    弾きたい GU<1% の玉を拾ってしまう。安全側＝厳しい側に倒す（スイング買いの寄指は上限なので
    逆に切り下げ＝どちらも「条件を緩めない向き」に丸める）。
    呼値は普通銘柄の刻み（〜3,000円:1円 / 〜5,000円:5円 / 〜30,000円:10円）。
    """
    if not prev_close or prev_close <= 0:
        return 0.0
    raw = prev_close * (1 + FADE_MIN_GAP_UP_PCT / 100)
    tick = 1 if raw <= 3000 else (5 if raw <= 5000 else 10)
    return float(-(-raw // tick) * tick)          # ceil を整数演算で


def _shares_for(limit_price: float) -> int:
    if not limit_price or limit_price <= 0:
        return 0
    return max(100, int(CAPITAL_PER_TRADE / limit_price / 100) * 100)


# ------------------------------------------------------------------ 決済
def settle(book: dict, data: dict, today) -> list[dict]:
    """pending を決済確定。確定した建玉リストを返す。"""
    today_str = today.strftime("%Y-%m-%d")
    just_closed = []
    still_pending = []

    for p in book["positions"]:
        if p.get("status") != "pending":
            still_pending.append(p)
            continue

        df = data.get(p["ticker"])
        basis = p["basis_date"]
        entry_row = None
        if df is not None and not df.empty:
            after = df[df.index.strftime("%Y-%m-%d") > basis]
            if not after.empty:
                entry_row = after.iloc[0]
                entry_date = after.index[0].strftime("%Y-%m-%d")

        # まだエントリーセッションの足が無い / 当日足（未確定）→ pending維持
        if entry_row is None or entry_date >= today_str:
            # 期限切れ（売買停止・上場廃止で永久に取れない）チェック
            basis_dt = datetime.strptime(basis, "%Y-%m-%d").date()
            if (today - basis_dt).days > EXPIRE_DAYS:
                p["status"] = "expired"
                book["expired"].append(p)
            else:
                still_pending.append(p)
            continue

        o = float(entry_row["Open"])
        c = float(entry_row["Close"])
        direction = p["direction"]
        limit = p.get("limit_price")

        # 約定日はシグナル当日のみ有効。当日売買停止等で足が無い場合、実弾の寄指は
        # 不成立なので紙も SKIP（翌日以降の足で約定扱いにしない・2026-07-22実弾前監査で追加）
        if p.get("signal_date") and entry_date != p["signal_date"]:
            exit_type, pnl = "SKIP", 0.0
            p["skip_reason"] = "当日約定なし(売買停止/寄らず)"
        elif direction == "BUY":
            if limit is not None and o > limit:
                exit_type, pnl = "SKIP", 0.0
            else:
                exit_type, pnl = "CLOSE", (c - o) / o * 100
        else:  # SELL
            # 2026-07-28: 寄指→成売りに変更。下寄りでも撃つので指値による見送りをしない。
            # 下寄りの玉も10年両期間でPF1超（前1.08/後1.21・平均+0.254%）で、成行の方が
            # 年+4.4万・最悪年-2.0万→+28.2万・勝ち10/11→11/11年。
            # 旧挙動に戻すなら FADE_ENTRY_MARKET=False（limitで見送り判定に戻る）。
            if (not FADE_ENTRY_MARKET) and limit is not None and o < limit:
                exit_type, pnl = "SKIP", 0.0
            else:
                exit_type, pnl = "CLOSE", (o - c) / o * 100

        shares = _shares_for(limit or o)
        pnl_yen = int(round(shares * o * pnl / 100)) if exit_type == "CLOSE" else 0

        p.update({
            "status": "closed",
            "entry_session": entry_date,
            "entry_open": round(o, 1),
            "entry_close": round(c, 1),
            "exit_type": exit_type,
            "pnl_pct": round(pnl, 3),
            "pnl_yen": pnl_yen,
            "win": bool(pnl > 0),
        })
        just_closed.append(p)
        still_pending.append(p)

    book["positions"] = still_pending
    return just_closed


# ------------------------------------------------------------------ 記帳
def record(book: dict, signals: list[dict], data: dict, iss_map: dict, today) -> list[dict]:
    """当日発火を pending として記帳（重複は無視）。新規記帳リストを返す。"""
    today_str = today.strftime("%Y-%m-%d")
    existing = {(p["ticker"], p["signal_date"]) for p in book["positions"] + book["expired"]}
    added = []

    for s in signals:
        tk = s["ticker"]
        key = (tk, today_str)
        if key in existing:
            continue

        # basis_date = そのティッカーの当日より前の最終確定足
        df = data.get(tk)
        basis = None
        if df is not None and not df.empty:
            before = df[df.index.strftime("%Y-%m-%d") < today_str]
            if not before.empty:
                basis = before.index[-1].strftime("%Y-%m-%d")
        if basis is None:
            print(f"[paper] {tk} basis足なし → 記帳スキップ")
            continue

        direction = s.get("direction", "BUY")
        limit = s.get("max_entry_price") if direction == "BUY" else s.get("min_entry_price")
        rec = {
            "ticker": tk,
            "name": s.get("name", tk),
            "direction": direction,
            "signal_date": today_str,
            "basis_date": basis,
            "prev_close": s.get("prev_close"),
            "limit_price": limit,
            "status": "pending",
        }
        if direction == "BUY":
            rec["high_20"] = s.get("high_20")
        else:
            rec["daily_gain"] = s.get("daily_gain")
            rec["short"] = shortability(tk, iss_map)
            rec["jsf_stop"] = bool(s.get("jsf_stop"))   # 売り禁=ハイカラ在庫依存の紙。後で分離分析用
            rec["rank"] = s.get("rank")                 # 1-3=本命/4-8=予備。帯別成績の分離分析用
        book["positions"].append(rec)
        added.append(rec)

    return added


# ------------------------------------------------------------------ 通算成績
def cumulative_stats(book: dict) -> dict:
    closed = [p for p in book["positions"] if p.get("status") == "closed"]
    executed = [p for p in closed if p.get("exit_type") == "CLOSE"]
    skipped = [p for p in closed if p.get("exit_type") == "SKIP"]

    def agg(rows):
        if not rows:
            return dict(n=0, win=0.0, avg=0.0, pf=0.0, yen=0)
        wins = sum(1 for r in rows if r["pnl_pct"] > 0)
        gain = sum(r["pnl_pct"] for r in rows if r["pnl_pct"] > 0)
        loss = -sum(r["pnl_pct"] for r in rows if r["pnl_pct"] < 0)
        pf = (gain / loss) if loss > 0 else (float("inf") if gain > 0 else 0.0)
        return dict(n=len(rows), win=wins / len(rows) * 100,
                    avg=sum(r["pnl_pct"] for r in rows) / len(rows),
                    pf=pf, yen=sum(r.get("pnl_yen", 0) for r in rows))

    return {
        "all": agg(executed),
        "buy": agg([r for r in executed if r["direction"] == "BUY"]),
        "sell": agg([r for r in executed if r["direction"] == "SELL"]),
        "skipped": len(skipped),
        "pending": sum(1 for p in book["positions"] if p.get("status") == "pending"),
        "expired": len(book["expired"]),
    }


def _fmt_pf(pf):
    return "∞" if pf == float("inf") else f"{pf:.2f}"


# ------------------------------------------------------------------ Discord
def send_report(just_closed, buy_fires, picks, stats, today, dry=False, banned=None):
    date_str = today.strftime("%Y年%m月%d日")
    lines = []
    if picks is None:
        picks = []
    go_picks = [p for p in picks if p.get("verdict") == "GO"]

    # ── 🎯 今日のデイトレ 上位N（フェード・毎営業日） ──
    if go_picks:
        lines.append(f"**🎯 今日のデイトレ 上位{len(go_picks)}（フェード＝上がりすぎを空売り・成行→引成）**")
        lines.append("　＝25MA乖離とATR%の順位平均で並べた順（伸びきって荒い銘柄ほど翌日よく落ちる）")
        for p in go_picks:
            sh = p.get("short") or shortability(p["ticker"], _LAST_ISS)
            shares = _shares_for(p["min_entry_price"])
            amt = shares * p["min_entry_price"]
            reg = f" ／ {p['reg_note']}" if p.get("reg_note") else ""
            rk = p.get("rank", 1)
            tag = "" if rk <= 3 else "（予備）"
            lines.append(f"**{rk}番{tag}** 🔴 **{p.get('name', p['ticker'])}**（{p['ticker']}）"
                         f"前日+{p['daily_gain']:.0f}% ／ 出来高{p.get('vol_ratio', 0):.0f}倍 ／ "
                         f"レンジ{p.get('range_pct', 0):.0f}% ／ 貸借{sh['mark']}{reg}")
            _cap_all = int(FADE_EDGE_PCT_GAPDN / 100 * CAPITAL_PER_TRADE)
            _cap_up = int(FADE_EDGE_PCT_MAIN / 100 * CAPITAL_PER_TRADE)
            lines.append(f"　→ **寄付 成行で売り {shares:,}株**（約{amt/1e4:.0f}万円）"
                         f"→ 当日 **引成 買戻し** ／ 信用: {p.get('borrow', '')}")
            lines.append(f"　　💰プレミアム料の上限（発注画面の総額と見比べる）: "
                         f"**〜{_cap_all:,}円なら成行でOK** ／ {_cap_all:,}〜{_cap_up:,}円なら"
                         f"**寄指¥{p['prev_close']:,.0f}以上**に切替（下寄りなら見送り）／ "
                         f"{_cap_up:,}円超は**撃たない**")
        if len(go_picks) >= 2:
            lines.append("　※**撃つのは1番だけ**。2番は1番が売り禁/在庫なし/寄指不成立の時の代替"
                         "（期待値は1番の39%）。3番以降は10年でPF0.99＝撃つと損なので出していません")
        lines.append("　※◎売残少=空売り楽で優先／⭐売り長=最強だが要在庫確認・逆日歩")
        lines.append("　※**下寄りでも撃つ**（2026-07-28変更）。下寄りの玉も10年両期間でPF1超"
                     "（前1.08/後1.21・平均+0.254%）＝成行の方が年+4.4万・最悪年-2.0万→+28.2万・"
                     "勝ち10/11→**11/11年**。ただしプレミアム料が上記を超える日は寄指に切替")
        lines.append("　※約定した分だけ・当日決済必須・持ち越し禁止・損切りなし(引けまで保持)")
        lines.append("　※実弾: SBI一日信用売り(手数料0)・約定確認後すぐ**引成返済を予約**"
                     "(未決済のまま大引けだと強制決済+手数料)・在庫無し/プレミアム高は見送り")
        lines.append("　※🚫売り禁=制度信用の新規売り停止中。**ハイカラ(HYPER)/一般信用の在庫があれば売れる**"
                     "＝発注画面で在庫とプレミアム料を確認してから")
        lines.append("")
    else:
        # GO無し（薄い候補のみ/候補ゼロ）は銘柄名を出さず「撃つ銘柄なし」だけ（紛らわしさ回避）
        lines.append("**🎯 今日は撃つ銘柄なし（見送り）**")
        lines.append("")
    # (旧)売り禁除外の可視化行は廃止=2026-07-23から売り禁も🚫バッジ付きで銘柄行に表示

    # ── 🟢 ライブ買いシグナル（レア） ──
    if buy_fires:
        lines.append(f"**🟢 買いシグナル {len(buy_fires)}件（実弾基準・出来高10倍ブレイク）**")
        for s in buy_fires:
            lines.append(f"・{s.get('name', s['ticker'])}（{s['ticker']}）MAX指値¥{s.get('max_entry_price', 0):,.0f}で寄成買い→引け")
        lines.append("")

    # ── 📓 答え合わせ ──
    if just_closed:
        lines.append("**📓 答え合わせ（前回の当日結果）**")
        for p in just_closed:
            de = "🟢買" if p["direction"] == "BUY" else "🔴売"
            if p["exit_type"] == "SKIP":
                lines.append(f"⏭️{de} {p['name']}（{p['ticker']}）見送り（{p.get('skip_reason', '指値条件外')}）")
            else:
                mk = "✅" if p["pnl_pct"] > 0 else "❌"
                lines.append(f"{mk}{de} {p['name']}（{p['ticker']}）"
                             f"寄{p['entry_open']:,.0f}→引{p['entry_close']:,.0f} "
                             f"**{p['pnl_pct']:+.2f}%**（{p['pnl_yen']:+,}円）")
        lines.append("")

    a, b, se = stats["all"], stats["buy"], stats["sell"]
    lines.append("**📈 紙トレ通算成績（v2・答え合わせベース）**")
    lines.append(f"執行 **{a['n']}件** / 勝率 **{a['win']:.0f}%** / 平均 **{a['avg']:+.2f}%** / "
                 f"PF **{_fmt_pf(a['pf'])}** / 損益 **{a['yen']:+,}円**")
    if b["n"]:
        lines.append(f"　🟢買 {b['n']}件 勝率{b['win']:.0f}% 平均{b['avg']:+.2f}% PF{_fmt_pf(b['pf'])}")
    if se["n"]:
        lines.append(f"　🔴売 {se['n']}件 勝率{se['win']:.0f}% 平均{se['avg']:+.2f}% PF{_fmt_pf(se['pf'])}")
    tail = f"見送り{stats['skipped']} / 保有中{stats['pending']}"
    if stats["expired"]:
        tail += f" / 失効{stats['expired']}"
    lines.append("　" + tail)

    color = 0x43A047 if a["yen"] > 0 else (0xE53935 if a["yen"] < 0 else 0x757575)
    payload = {"embeds": [{
        "title": f"🩳【デイトレ売りシグナル（紙）】{date_str}",
        "description": "\n".join(lines),
        "color": color,
        "footer": {"text": "台帳は紙の理論値。実弾はSBI約定に従い、紙との差＝摩擦(在庫/プレミアム/滑り)を測る。"},
    }]}

    if dry:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    url = (os.getenv("DISCORD_WEBHOOK_DAY_URL") or os.getenv("DISCORD_WEBHOOK_URL_DAY")
           or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()
    if not url:
        print("[paper] webhook未設定 → 通知スキップ")
        return
    import requests
    r = requests.post(url, json=payload, timeout=15)
    print(f"[paper] Discord通知 HTTP {r.status_code}")


_LAST_ISS = {}


# ------------------------------------------------------------------ orchestration
def run(today=None, signals=None, dry=False):
    """main_day末尾から呼ぶ想定。毎営業日『フェード上位3』＋ライブBUYを紙で回す。
    紙記帳するのは GO（前日+12%以上×貸借○×張り付き除外）の上位3と、ライブBUY発火のみ。
    signals未指定（単体実行）なら day_signals.json のBUYを読む。"""
    global _LAST_ISS
    if today is None:
        today = _today_jst_date()
    if not is_trading_day(today):
        print("[paper] 休場 → スキップ")
        return
    today_str = today.strftime("%Y-%m-%d")

    # ライブBUYの取り込み元
    if signals is None:
        signals = []
        if os.path.exists(DAY_SIGNALS_FILE):
            with open(DAY_SIGNALS_FILE, "r", encoding="utf-8") as f:
                signals = [s for s in json.load(f) if s.get("signal_date") == today_str]
    buy_fires = [s for s in signals if s.get("direction", "BUY") == "BUY"]

    book = load_book()

    # 決済＆1番選定に全銘柄を一括取得（失敗時は無取得で決済のみ試行）
    try:
        data = _fetch_all(today)
    except Exception as e:
        print(f"[paper] データ取得失敗（{e}）→ 決済のみ試行")
        data = {}

    just_closed = settle(book, data, today)

    tok = _jq_token() if data else None
    _LAST_ISS = fetch_iss_map(tok) if data else {}
    ratio_map = fetch_ratio_map(tok) if data else {}
    alert_map = fetch_alert_map(tok) if data else {}
    banned: list = []   # 売り禁(日証金申込停止)で除外した銘柄（配信で可視化）
    picks = daily_top_fades(data, today, _LAST_ISS, ratio_map=ratio_map,
                            alert_map=alert_map, excluded_out=banned)   # 上位3（各GO/NO-GO+借りやすさ）
    go_picks = [p for p in picks if p.get("verdict") == "GO"]

    # 紙記帳＝GOの上位3 ＋ ライブBUY発火のみ（見送りは記帳しない）
    to_record = list(buy_fires) + go_picks
    added = record(book, to_record, data, _LAST_ISS, today)

    stats = cumulative_stats(book)
    print(f"[paper] 決済{len(just_closed)}件 / 記帳{len(added)}件（買{len(buy_fires)}/GO売{len(go_picks)}）/ "
          f"通算執行{stats['all']['n']}件 PF{_fmt_pf(stats['all']['pf'])} 損益{stats['all']['yen']:+,}円")

    # 毎営業日1回だけ配信（上位3まで。二重送信は日付ガード）
    if book.get("last_report_date") != today_str:
        send_report(just_closed, buy_fires, picks, stats, today, dry=dry, banned=banned)
        if not dry:
            book["last_report_date"] = today_str

    if not dry:
        save_book(book)


def _jq_token():
    from screener import _jquants_id_token
    return _jquants_id_token()


def main():
    dry = "--dry" in sys.argv
    if "--test" in sys.argv:
        import _test_daytrade_paper  # noqa
        return
    run(dry=dry)


if __name__ == "__main__":
    main()
