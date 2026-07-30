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
# 2026-07-28に 100万→50万（本人「40万くらうと立ち直れない」＝月の傷の許容から逆算）。
# 10年検証（最終形＝乖離+ATR順・上位1本・出来高6倍未満・乖離80%未満）:
#   100万 年+108.7万 / 最悪月-72.4万 / 最悪1件-39.2万
#    70万 年 +74.1万 / 最悪月-48.2万 / 最悪1件-27.2万
#  **50万 年 +52.1万 / 最悪月-36.1万 / 最悪1件-19.2万**  ← 採用
# いずれも勝ち11/11年・PF1.31で、選ばれる銘柄も件数もほぼ同じ（2,199 vs 2,197）＝
# サイズは「同じ玉にいくら張るか」だけの違い。株価上限も自動連動（50万なら5,000円以下）だが、
# 上限を1万円に広げても1番に選ばれるのは10年で119件だけで年-6.6万と微減するため実害なし
# （乖離+ATR順では値がさ株が上位に来にくい＝5,000円上限は制約でなく結果的に最適）。
CAPITAL_PER_TRADE = 500_000
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

# 25MA乖離の上限（2026-07-28検証・テールリスク対策）。ここを超えた玉は候補から外す。
# 乖離は「大きいほどよく落ちる」ので選定の主軸に使っているが、**80%を超えると別の生き物**＝
# 仕手化して制御不能な上昇が続く領域（2026-03のジャパンディスプレイ 乖離+105%と+152%で
# -43.8万/-28.6万、寄57→引82の日中+44%踏み上げ）。
# 除外効果（10年・上位1本・100万）: 年+95.9万→**+104.0万**（利益は増える）・
# **最悪月DD -91.1万→-57.7万**・両期間PF改善(前1.21→1.26 / 後1.26→1.28)・
# 2026年3月 -18万→**+46万** / 4月 -41万→**-4万**。除外されるのは10年でわずか3件。
# ※株価下限(500円)の併用は後半PFが1.26→1.22に劣化するため入れない（低位株は当たりも消える）。
# 無効化は None。
FADE_DEV25_MAX = 80.0

# ── ATR%と25MA乖離の「下限」（2026-07-31検証・_bt_fade_atrdev.py / _bt_fade_final.py）──
# 発端は本人の「もう少し勝率が欲しい。小型株を除くと良くならないか」。
# **小型株除外は逆効果だった**（_bt_fade_smallcap.py）: 代金フロアを3億→10億にすると
# 勝率55.4%→53.5%・年+59.1万→+20.2万・勝ち11→8年。株価1,000円フロアも年+28.0万・勝ち8年。
# フェードは過熱の反動を取る戦略なので、厚くて動かない玉は+6%上げても垂れない。
# 効いたのは逆方向＝**よく動く玉だけ残す**（残る側の株価中央は1,350円で現行1,548円より安い）。
#   現行                勝率55.4% PF1.22 年+59.1万 勝ち11/11 前半+312.6万 後半+338.0万 撃つ日2,211
#   ATR5%以上×乖離12%以上  勝率59.6% PF1.45 年+67.4万 勝ち11/11 前半+340.5万 後半+401.4万 撃つ日1,350
# 【2026-07-31 監査による訂正】上の「年+67.4万」はBT側の数字で、**本番が実際に取れる額ではない**。
#   本番だけが持つ制約（株価300円下限・出来高10万株下限・ETF除外・株数は前日終値基準）を
#   BTに入れ、同点順位をticker昇順に固定して測り直すと **年+53.3万**（勝率60.1%/PF1.39/
#   勝ち11-11年/最悪年+9.2万/最悪月-45.4万/撃つ日1,239）。BTは年+11.6万＝22%過大だった。
#   → その後 FADE_PX_MIN=0（株価下限の撤廃）を入れたので、**現在の期待値は年+66.7万**
#     （勝率59.9%/PF1.43/勝ち11-11年/最悪月-35.2万/2026年1-7月+73.9万）。下の下限のコメント参照。
#   ※ここでETF除外は「銘柄マスタに無い＝真のETF」だけを落とす（本番と同じ）。screener の
#     is_etf_ticker で落とすと実在株10銘柄まで消えて年+54.7万と過大に出るので使わない。
#   最大の要因は株価300円下限で、BTは20〜300円の低位株229玉を撃っている（1ティックが数%＝
#   [[feedback_bt_intraday_traps]]の「低位株の幻」）。この下限は本番が正しいのでBT側を直す。
#   2026年(1-7月)も +69.2万 → **+35.8万** に下がる（1月は-38.9万）。詳細は _audit_fade_parity.py。
# 高原であることの確認: ATR4.0〜5.5 × 乖離10〜15 の全セルで両期間改善。往復0.15%のコストを
# 乗せるとATR4〜6×乖離0〜18まで面が広がる（件数が減るぶんコスト総額も減るので差が開く：
# 0.20%コストで 現行+27.4万/年 に対し新条件+48.4万/年）。上位3日・3銘柄を除いても93%/87%残る。
# 捨てる側が本当に捨てて良いかの直接確認: 落とす玉だけで組むと年-5.8万・勝ち5/11年
# （ATR5%未満だけなら年-11.2万・勝ち4/11年・後半-117万）＝エッジがあるのは残す側だけ。
# 【利益がどこから来るかの内訳＝「撃つ日が減るから勝てる」ではない】撃たなくなる2,240玉は
# 合計では勝率51.7%・-54.1万だが、分解すると符号が違う:
#   ・0件になる日(861日・1,485玉)は **+47.4万** ＝プラスの日を捨てている（年-4.3万の機会損失）
#   ・撃つ日の中で消える755玉が **-101.5万** ＝ここが本体（質の低い2枚目や条件未達の玉）
#   ・新しく入る162玉が +37.2万
#   差し引き年+8.3万。**効いているのは「日を減らしたこと」でなく「撃つ日の中身が入れ替わったこと」**。
#   1玉平均 +1,586円→+3,667円（2.3倍）・1撃つ日あたり +2,943円→+5,495円。
# 代償（承知の上で採る）: 撃つ日が年201日→123日に減る（2本ある日673/1,350日）。
#   最悪年 +20.4万→+4.4万（2018）・最悪月 -30.1万→-32.8万（2026-01）。全年プラスは維持。
#   最悪年が薄くなる直接の原因は、捨てる日が2018年に+38.7万あったこと（捨てる日の年別は
#   -46.2万〜+38.7万とブレが大きい＝「撃たなかったせいで取り逃す年」は確実に来る）。
#   日計りなので撃たない日の資金コストはゼロ＝回数でなく総額で見るのが正しい
#   （枠を埋め続けるのが生命線の決算持ち越しとは逆の性質）。
# 【年別で見ると金額は5勝6敗＝毎年増える改善ではない】(2026-07-31 _bt_fade_2026.py)
#   勝率は10/11年・PFも10/11年で上がる（例外は2018だけ）＝ここは安定。金額で新が勝つのは5/11年で、
#   10年計+91万は勝つ年が大きいから（2021+71.6万/2024+61.3万/2023+34.2万）。玉数が半分になるぶん
#   金額のブレ自体が大きくなるのは構造上不可避。「年+67.4万」は11年平均であって毎年の期待値ではない。
#   2026年(1/1-7/27)実測: 新+71.3万＝過去10年の同期間で1位だが、2026単体では現行に-8.2万負ける
#   （最大の取りこぼし=6/24 JDI 6740 前日+22.4%/ATR11.5%だが乖離8.2%で足切り→+13.2万逃した）。
#   ※2026は撃つ日が134→121日とほぼ減らない（過去9年平均は126→80日）＝候補が総じてボラ高・乖離大の
#     相場ではこのフィルタは空振りする。「撃つ日が4割減る」も年平均であって毎年そうなるわけではない。
# 無効化は None。
FADE_ATR_MIN = 5.0
FADE_DEV25_MIN = 12.0

# ── 株価下限を撤廃（2026-07-31・旧300円）────────────────────────────────────
# 監査で「BTには無く本番にだけある制約」として出てきた最大の乖離（10年-124万）。
# 由来の記録が無く、検証して入れた形跡もない（スクリーナー由来の慣習値と思われる）。
# measure: 300→200→100→撤廃 で 年+53.3/+55.5/+57.9/**+66.7万** と単調。撤廃版は
#   勝率59.9% PF1.43 勝ち11-11年・**両期間とも改善**(前半+304.5→+336.9万/後半+281.4→+396.2万)・
#   **最悪月まで改善**(-45.4万→-35.2万)。2026年(1-7月)は +35.8万→**+73.9万**。
# 「そもそも低位株を空売りできるのか」を先に潰した（本人の指摘）:
#   ①貸借○は元から条件 ②建てる株数は20日平均出来高の中央値0.03%・最大0.14%＝板は余裕
#   ③**寄成/引成は板寄せ**なので参加者全員が同じ値で約定＝呼値のスプレッドは payしない
#     （BTのo1/c1はその板寄せ価格そのもの。「低位株は1ティックが数%だから不利」は
#       指値で入る場合の話で、この執行方式には当たらない＝前言を訂正した）
#   ④残る唯一の未知が一日信用の売り在庫で、これは履歴が無くBTで測れない
#     → 本人に確認し「だいたい建てられる」との回答を得たので撤廃に踏み切った。
# ⚠️正直な弱点: 撤廃分の利益は**〜100円帯に集中**（74玉で+93.6万）。しかも後半偏重
#   （前半+10.1万/後半+83.5万）・9銘柄だけ・上位3銘柄で66%（JDI 6740が17玉+32.1万）。
#   上位3玉を除いても+67.5万は残るのでテール1発ではないが、薄い帯であることは変わらない。
#   低位株の踏み上げも太い（99%点 +24.7% vs 300円以上+17.6%・最悪+39.4%）。
# 無効化（元に戻す）は FADE_PX_MIN = 300。
FADE_PX_MIN = 0

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
# 2026-07-29 再測定（1玉50万・データを2026-07-27まで更新・実株数）。
# 「何本まで撃つか」の答えは **2本**。3本目から崩れる:
#   上位1本 n=2,211 勝率56.3% PF1.28 年+43.3万 勝ち11/11年 最悪年+19.7万 最悪月-22.6万
#   上位2本 n=4,101 勝率55.4% PF1.22 年+59.1万 勝ち11/11年 最悪年+20.4万 最悪月-30.1万 ←最良
#   上位3本 n=5,667 勝率54.0% PF1.15 年+54.4万 勝ち10/11年 最悪年 -1.1万 最悪月-39.5万
# 順位を単独で撃った場合の実力:
#   1番だけ PF1.28 年+43.3万 勝ち11/11年
#   2番だけ PF1.13 年+15.9万 勝ち 8/11年  ← 単独でもプラス＝「代替」でなく本命の2枚目
#   3番だけ PF0.96 年 -4.8万 勝ち 4/11年  ← ここから期待値マイナス
# 運用ルール: **1番と2番の両方を撃つ。3番以降は撃たない。**
# （2026-07-28までは「撃つのは1番だけ・2番は代替」と配信していたが、上位1本の数字だけを見た
#   誤りだった。2番を足すと年+15.8万増えて勝ち年11/11は変わらない＝2本が正しい。）
PAPER_MAX_PICKS = 2


def fade_nogo_reason(gain: float, atr_pct: float, dev25: float) -> str | None:
    """撃てる玉か判定し、撃てないなら理由を返す（撃てるなら None）。

    BTは「この3条件を満たす玉だけに絞ってから乖離+ATRで並べた上位2本」を撃つ想定なので、
    ここを通らない玉は必ず順位の後ろへ回す（＝画面の1番＝BTの1番を常に成立させる）。
    候補プール自体からは落とさない: 落とすと「今日は何も出ない」日に理由が見えなくなるため、
    上昇率トップは NO-GO の理由付きで必ず表示する（売り禁を除外せずバッジ表示にしたのと同じ方針）。
    """
    # 小数1桁で出す: 5.96%を「+6%<6%」と表示すると条件が矛盾して見える（境界の玉は毎日出る）
    if gain < DAILY_PICK_GAIN_MIN:
        return f"前日+{gain:.1f}%<{DAILY_PICK_GAIN_MIN:.0f}%＝薄い(コスト後トントン帯)"
    if FADE_ATR_MIN is not None and atr_pct < FADE_ATR_MIN:
        return (f"ATR{atr_pct:.1f}%<{FADE_ATR_MIN:.0f}%＝普段動かない玉の急騰"
                "(本物の材料で翌日も買われる・10年勝率50.5%)")
    if FADE_DEV25_MIN is not None and dev25 < FADE_DEV25_MIN:
        return (f"25MA乖離{dev25:.1f}%<{FADE_DEV25_MIN:.0f}%＝まだ伸びきっていない"
                "(戻す余地が小さい)")
    return None


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

    # ── ETF/ETN除外（2026-07-31 監査で発覚した実害バグ）──────────────────────
    # BTは name_map が引けない銘柄（＝ETF/ETN）を丸ごと落としていたが、本番には
    # 一切のETF判定が無かった。10年で22件のETFが入口を全部通り、しかも
    # **2020-03-12(コロナ暴落)と2025-04-09(関税暴落)は通常株の候補がゼロ**＝
    # その日は「ベア2倍ETFを空売りしろ」という指示だけが出ていた（下げ相場で
    # 上がり続ける玉を空売りする＝戦略の前提と真逆）。直近も2026-05/06に3日該当。
    # fetch_tse_universe はプライム/スタンダード/グロースのみを返すのでETFは入らない。
    # ※screener.is_etf_ticker は使わない: 名称キーワード"ブル"/"ベア"/"ダブル"が
    #   ブルボン・ミネベアミツミ・ダブル・スコープ等の**実在株10銘柄を誤判定**するため。
    equities: set[str] | None = None
    names: dict[str, str] = {}
    try:
        from screener import fetch_tse_universe
        names = dict(fetch_tse_universe())      # 銘柄名の補完にも再利用（API呼び出しは1回）
        _u = set(names)
        # 取得失敗時は日経225のフォールバックが返る。それで絞ると候補がほぼ消えて
        # 「毎日シグナルなし」で黙って死ぬので、少数しか返らなければガードを諦める。
        equities = _u if len(_u) >= 1000 else None
        if equities is None:
            print(f"[fade] 銘柄マスタが{len(_u)}件しか取れずETF除外を見送り（フェイルオープン）")
    except Exception as e:
        print(f"[fade] 銘柄マスタ取得失敗({e}) → ETF除外を見送り（フェイルオープン）")

    cands = []
    for tk, df in data.items():
        if df is None or df.empty:
            continue
        if equities is not None and tk not in equities:     # ETF/ETN/REITを除外
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
        if last_c <= 0 or prev_c <= 0 or last_c < FADE_PX_MIN:
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
        _ma25 = float(c.tail(25).mean()) if len(c) >= 25 else 0.0
        if (FADE_DEV25_MAX is not None and _ma25 > 0
                and (last_c / _ma25 - 1) * 100 >= FADE_DEV25_MAX):
            continue                                        # 過熱の異常値=仕手化して踏み上げが止まらない
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
            # 閾値判定と順位付けは**丸める前**の値で行う（2026-07-31監査）。
            # 表示用に丸めた dev25/atr_pct をそのまま使うと、①ATR4.996%が5.00%に化けて
            # 下限を通る ②同点が増えて「どちらを撃つか」が並び順で決まる玉が増える。
            "_dev_raw": dev,
            "_atr_raw": atr_pct,
        })
    if not cands:
        return []
    # 25MA乖離とATR%の「順位の平均」で並べる（どちらも降順＝大きいほど上位）。
    # 単位の違う2軸を足すため生値でなく順位で正規化する。10年検証:
    #   上昇率降順(旧) 前半PF1.15/後半1.15・10年+293万・最悪年-8.6万・勝ち8/11年
    #   乖離+ATR(新)   前半PF1.20/後半1.25・10年+415万・最悪年+11.9万・**勝ち11/11年**
    # 配合を乖離0%〜100%のどこに振っても年+89〜97万/勝ち11年で崩れない＝針でなく高原。
    # 同点は必ず ticker 昇順で決める（2026-07-31監査）。従来は sorted の安定ソート任せ＝
    # J-Quantsが返す辞書順で「どちらを撃つか」が決まっていた。10年BTでは2位と3位が同点に
    # なる日が93日/1,350日(6.9%)あり、同点の決め方だけで10年の総額が±53.5万(7.5%)動く。
    # 恣意的でも再現可能な規則を置かないと、BTの数字と本番の挙動が構造的に一致しない。
    _n = len(cands)
    _rank_dev = {id(x): i for i, x in
                 enumerate(sorted(cands, key=lambda z: (-z["_dev_raw"], z["ticker"])))}
    _rank_atr = {id(x): i for i, x in
                 enumerate(sorted(cands, key=lambda z: (-z["_atr_raw"], z["ticker"])))}
    for x in cands:
        x["pick_score"] = round((_rank_dev[id(x)] + _rank_atr[id(x)]) / 2 / max(_n, 1), 4)
    # GO基準(上昇率≥DAILY_PICK_GAIN_MIN)を満たす玉を必ず先に並べ、その中を pick_score 順にする。
    # BTは「上昇率≥閾値に絞ってから乖離+ATRで並べた上位1本」を撃つ想定なので、閾値未満を
    # 混ぜて並べると画面の「1番」がBTの1番と食い違う（2026-07-28: 上昇率5.6%のインフォマートが
    # 乖離20.9%で5番に入り、GO対象のフリー(7番)/カバー(8番)より上に表示されていた）。
    # 閾値未満は候補として残すが必ず後ろ＝「1番＝撃つ玉」が常に成立する。
    # 2026-07-31: 後ろに回す条件を「上昇率」だけからATR%下限・乖離下限も含む3条件に拡張。
    for x in cands:
        x["_nogo"] = fade_nogo_reason(x["daily_gain"], x["_atr_raw"], x["_dev_raw"])
    # pick_score も4桁に丸めてあるので、最後は ticker で決着させる（同点を残さない）
    cands.sort(key=lambda x: (x["_nogo"] is not None, x["pick_score"], x["ticker"]))
    picks = cands[:max(1, n)]
    for p in picks:      # 銘柄名補完（上の ETF ガードで取った names を使い回す）
        p["name"] = names.get(p["ticker"], p["ticker"])
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
        reason = p.pop("_nogo", None)
        p.pop("_dev_raw", None); p.pop("_atr_raw", None)   # 内部用（JSONに出さない）
        go = reason is None and sh["mark"] == "○"
        p["verdict"] = "GO" if go else "NOGO"
        if not go:
            p["nogo_reason"] = reason or "貸借✕＝売れない玉"
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
        _cap_all = int(FADE_EDGE_PCT_GAPDN / 100 * CAPITAL_PER_TRADE)
        _cap_up = int(FADE_EDGE_PCT_MAIN / 100 * CAPITAL_PER_TRADE)
        n_shoot = min(len(go_picks), PAPER_MAX_PICKS)
        total = sum(_shares_for(p["min_entry_price"]) * p["min_entry_price"]
                    for p in go_picks[:n_shoot])

        # ── 手順（毎日同じ・迷わないように最初に固定表示）──
        lines.append("**🩳 デイトレ売り（フェード）**")
        lines.append("```")
        lines.append("① 9:00 寄付 “成行” で空売り（下寄りでも撃つ）")
        lines.append("② 約定したらすぐ「引成」で買戻しを予約")
        lines.append("③ 大引けで自動決済。持ち越し禁止・損切りなし")
        lines.append("```")
        lines.append(f"**下の{n_shoot}銘柄を撃つ**（両方とも本命／合計 約{total/1e4:.0f}万円）")
        lines.append("")

        for i, p in enumerate(go_picks):
            sh = p.get("short") or shortability(p["ticker"], _LAST_ISS)
            shares = _shares_for(p["min_entry_price"])
            amt = shares * p["min_entry_price"]
            reg = f" {p['reg_note']}" if p.get("reg_note") else ""
            rk = p.get("rank", i + 1)
            if i < n_shoot:
                head = f"**{rk}番** 🔴 **{p.get('name', p['ticker'])}**（{p['ticker']}）"
            else:
                head = (f"{rk}番（予備・撃たない） {p.get('name', p['ticker'])}"
                        f"（{p['ticker']}）")
            lines.append(head)
            qty = (f"**{shares:,}株 空売り**（約{amt/1e4:.0f}万円）" if i < n_shoot
                   else f"{shares:,}株（約{amt/1e4:.0f}万円）")
            lines.append(f"　{qty}／ 貸借{sh['mark']}{reg}"
                         + (f" ／ {p['borrow']}" if p.get("borrow") else ""))
            lines.append(f"　前日+{p['daily_gain']:.0f}% ・ 出来高{p.get('vol_ratio', 0):.0f}倍 ・ "
                         f"レンジ{p.get('range_pct', 0):.0f}%")
            if i < n_shoot:
                lines.append(f"　💰プレミアム料 〜{_cap_all:,}円→成行OK ／ "
                             f"〜{_cap_up:,}円→寄指¥{p['prev_close']:,.0f}以上に切替 ／ "
                             f"超えたら撃たない")
            lines.append("")

        lines.append("　※**1番と2番の両方を撃つ**（2026-07-29修正）。10年BTで上位2本が最良＝"
                     "年+43.3万→**+59.1万**・勝ち11/11年。2番単独でもPF1.13でプラス。"
                     "3番以降はPF0.96で撃つと損なので出していない")
        lines.append("　※2026-07-31から**ATR5%以上×25MA乖離12%以上**の玉だけ撃つ（勝率55→**60%**・"
                     "PF1.22→**1.45**・年+59.1万→**+67.4万**）。そのぶん撃たない日が増える")
        lines.append("　※在庫が無い/プレミアム料が上限超なら、その銘柄だけ見送る（もう片方は撃つ）")
        # 2026-07-31に株価下限(300円)を撤廃した。板と呼値は問題ない（板寄せ約定・出来高比0.03%）が、
        # 一日信用の売り在庫だけはBTで測れない唯一の未知なので、低位株の日だけ注意を促す。
        if any(p["prev_close"] < 300 for p in go_picks[:n_shoot]):
            lines.append("　※**低位株あり**（300円未満）。板・呼値は問題ないが一日信用の売り在庫だけ"
                         "要確認。建てられなければその銘柄は見送り")
        lines.append("　※🚫売り禁=制度信用の新規売り停止中。**ハイカラ(HYPER)/一般信用に在庫があれば売れる**")
        lines.append("　※◎売残少=空売り楽／⭐売り長=最強だが要在庫確認・逆日歩")
        lines.append("　※実弾はSBI一日信用売り(手数料0)。未決済のまま大引けだと強制決済+手数料")
        lines.append("")
    else:
        # GO無し（条件未達の候補のみ/候補ゼロ）は銘柄名を出さず「撃つ銘柄なし」だけ（紛らわしさ回避）。
        # 2026-07-31にATR/乖離の下限を入れて見送り日が約4割に増えたので、なぜ撃たないかは書く
        # （理由が見えないと「システムが止まったのか」と区別できない）。
        lines.append("**🎯 今日は撃つ銘柄なし（見送り）**")
        if picks and picks[0].get("nogo_reason"):
            lines.append(f"　理由: 1番手が {picks[0]['nogo_reason']}")
        lines.append("　※撃たない日は10年BTで4割前後ある（条件を満たす玉だけ撃つ設計）")
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
