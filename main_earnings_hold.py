# -*- coding: utf-8 -*-
"""main_earnings_hold.py — 決算持ち越しシグナル（2026-07-10新設・BT根拠は _bt_earnings_hold*.py）。

毎営業日14:55 JST（schedule_close.yml相乗り）:
  1. JPX決算発表予定を更新（失敗時は手元のJSONで続行）＋発表時刻履歴の鮮度維持
  2. 大100万/中50万/小30万の3階層それぞれ（2026-07-10分割・各8枠・専用Webhook）:
     - 昨日エントリー分を決済記帳（entry=シグナル日終値[J-Quants公式]/exit=翌営業日寄り）
     - 本日発表予定 × ルールA（RSI≤45・5日騰落<-3%・売買代金20日中央値≥10億・株価≤階層上限）
       → RSI昇順に最大8枠 → Discord配信「大引け成行買いリスト」
  3. positions_earnings*.json 保存（翌日の決済記帳用）

出口: 原則翌寄り成行売り（1晩・オーバーナイトはSTOP無効）。
例外=PEAD延長: 寄りがエントリー比+8%超なら5営業日目の大引けまでホールド。
BT(4.5年・8枠・PEAD+流動性7.5億): 大PF1.53/+922万・中PF1.60/+473万・小PF1.72/+263万・全5年プラス。

実行: python main_earnings_hold.py [--force](時間ガード無視) [--dry](配信/保存なし)
      [--test](フォーマット確認用のサンプル配信のみ)
"""
from __future__ import annotations

import json
import os
import re
import sys
import zoneinfo
from datetime import date, datetime, timedelta

import jpholiday
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

JST = zoneinfo.ZoneInfo("Asia/Tokyo")
SCHEDULE_FILE = "jpx_earnings_schedule.json"
TIMES_FILE = "earnings_times.json"

# ── ルールA（BT確定値・むやみに変えない） ──
SLOTS = 8
# RSI上限を45→55に緩和（2026-07-29・_bt_earnings_ultra.py・10年・大100万×8枠）。
# 【前提】この判断は「価格キャッシュの穴バグ」を直した後のクリーンなデータで出したもの。
#   旧データは iloc[p+1] を翌営業日として扱っていたため数年の穴をまたいだ偽ギャップが混入し、
#   10年計が37%水増しされていた（詳細は _bt_earnings_rich.py のコメント）。
# 効果: 10年 +1,001万→+1,315万（年+91万→+120万）・勝ち年9/11維持・PF1.30→1.28。
# 頑健性: 上位20玉を除去しても +161万→+330万で改善が残る＝大当たり1発の偶然ではない。
#   追加される帯が単独でプラスかつ両期間プラス（45-50帯 PF1.30/+373万・50-55帯 +155万）。
#   年別では9/11年で改善（悪化は2018年-94万と2024年-40万）。
# 面: 45→50→55→60 で +1,001/+1,192/+1,315/+1,359万と単調。55→60は147件で+41万・PF1.10と
#   薄くなるので55で止める（高原の端）。
# 機構: 入口にはもう1つ「直近5日騰落<-3%」があるので、RSI55でも「直近で下げた銘柄」である
#   ことは変わらない。RSIという別指標を重ねて絞る必要が薄かった、という読み。
# 代償: 最悪年が-132万→-221万に深くなる（2018年が沈む年で玉数が増える分だけ傷も深い）。
# 頻度: シグナルが出る日が年73→89日（+22%）。1日あたり本数は2.80→2.98本でほぼ不変、
#   8枠フルの日は4%→5%なので夜の資金ピークは786万のまま変わらない（平均は300→325万）。
# 【同時に入れてはいけない】PEAD閾値の+10%化は単独では+29万だがRSI緩和と併用すると
#   -131万で逆効果（RSI≤55×PEAD>10% = +1,184万 < +1,315万）。PEADは+8%のまま。
RSI_MAX = 55.0
RUNUP_MAX = -3.0      # 直前5営業日騰落% がこれ未満
# 流動性フロア: 10億→7.5億に引き下げ(2026-07-11・_bt_earnings_tovfloor.py)。
# 閑散月の枠遊びを解消: 大+770→+922万/中+384→+473万・PF維持〜改善・DD改善(大)・
# 全5年プラス維持。5億以下はPF/DD劣化=7.5億が内側最適。
TOV_MIN = 7.5e8       # 20日中央値売買代金
HIST_DAYS = 60        # J-Quants取得窓（暦日）

# ── 決算ボラゲート（2026-07-31採用・_bt_earnings_vol_axis.py / _bt_earnings_tail_check.py）──
# 決算ボラ = その銘柄の過去の決算翌寄りギャップ絶対値の中央値。
# 「決算でどれだけ動く体質か」であって「上がるかどうか」ではない点が肝。
# 本システムのエッジは方向でなく非対称（勝ち平均+4.96% / 負け平均-3.73%）にあるので、
# 振れない銘柄ではそもそもエッジが発生しない。実測でも最も動かない20%は10年+8万＝ゼロ。
# 10年BT（8枠・1玉100万）: +1,418万/DD-448万 → +1,750万/DD-298万・陽性9→10/11年・
#   勝率49.0→50.5%。前半281→541万・後半1,136→1,209万で両期間改善。
#   閾値2.0〜4.5%が高原・枠5〜16すべてでプラス・大勝ち玉トップ20の利益94%が残る。
# 直近5年半でも +1,274→+1,300万 / DD-118→-99万＝レジームに賭けていない。
# 値は build_earnings_vol.py が earnings_vol.json に事前生成（実行時計算は窓60日では不可）。
# データが無い銘柄は必ず「買う」側に倒す＝完全フェイルオープン。
EARNINGS_VOL_MIN = 2.0          # None にすると無効化

# ── 場中発表の除外（2026-08-01・本人指示）──
# 詳細と実測値は disc_time_pass() のdocstring。False で従来動作に戻る。
EXCLUDE_INTRADAY_DISC = True
# 15:00は場中発表の最大バケット（[15:00,15:30)帯の90.7%・15:05までで92.3%）。
# 14:55照会では原理的に見えないため、TDnet照会と配信を15:06まで待つ
# （本人了承「配信は15:05でもいいよ」2026-08-01）。
# BT: 場中すり抜け9.3%→0.9% / 引け後の取りこぼし5.8%→0.6%（_bt_earnings_intraday_v2.py）
# None で待たない（従来の即時配信に戻す）。
TDNET_WAIT_MIN = 15 * 60 + 6
EARNINGS_VOL_FILE = "earnings_vol.json"
_EVOL: dict[str, float] | None = None
_EVOL_BUILT: str = "?"

# ── PEAD延長（2026-07-11採用・_bt_earnings_pead*.py） ──
# 翌朝の寄りがエントリー比+8%超の爆勝ちギャップだけ売らず、エントリー5営業日目の
# 大引けまでホールド（決算後ドリフト）。8枠シムで大+540→+770万/中+272→+384万/
# 小+159→+224万・PF全階層改善・全5年プラス・閾値+4〜+12%の台地で頑健。
# 負け玉は従来どおり翌寄り売り＝最悪テール不変。
PEAD_EXT_GAP = 8.0    # 延長発動のギャップ閾値%
PEAD_EXT_DAYS = 5     # エントリー日からの営業日数（この日の大引けで売却）

# 決済価格がこの暦日数を超えて取得不能（売買停止/上場廃止など）なら失効記帳して
# 枠を解放する（2026-07-15追加・放置すると枠1つが永久占有＋毎朝リマインダーが鳴り続ける）
EXPIRE_CAL_DAYS = 14

# 【2026-07-26】本人指示「決算も大だけでいい」→ 大資金1階層のみに縮小。
# 切替時点で中/小はともに未決済ゼロ＝管理されない建玉を残さずに外せることを確認済み
# （大は 4063.T 信越化学工業 が pending で残るが、これは継続する側）。
# 中/小の positions_earnings_{mid,small}.json は削除せず残置＝下の2行を戻すだけで復活できる。
#
# 【2026-08-01】本人決定「50万がいいな」→ 1玉100万→50万・価格上限1万→5千円。
# 数字(10年BT): 100万=+1,191万/DD-237万 → 50万=約+580万/DD-170万＝夜間名目800→400万・
# 口座800万比のDDは-30%→約-21%で生存線の内側に。5千円超の帯の寄与は10年+13万＝ゼロ同然
# (157件×平均+800円)だが、第一工業製薬(8,830円)型の単発ロケットは出なくなる=説明済みの代償。
# 狙いはサイズを落として「配信されたものを全部撃てる」状態にすること(テール依存＝取り逃しが最大の敵)。
# 切替前の建玉(第一工業製薬100株など)は記録済みsharesで決済されるため影響なし。
# 戻すときは size=1_000_000 / price_cap=10000 の2箇所。
TIERS = [
    {"label": "50万×8枠", "size": 500_000, "price_cap": 5000,
     "webhook_env": "DISCORD_WEBHOOK_EARNINGS_URL",
     "positions_file": "positions_earnings.json"},
    # {"label": "中資金", "size": 500_000, "price_cap": 5000,
    #  "webhook_env": "DISCORD_WEBHOOK_EARNINGS_MID_URL",
    #  "positions_file": "positions_earnings_mid.json"},
    # {"label": "小資金", "size": 300_000, "price_cap": 3000,
    #  "webhook_env": "DISCORD_WEBHOOK_EARNINGS_SMALL_URL",
    #  "positions_file": "positions_earnings_small.json"},
]
MAX_PRICE_CAP = max(t["price_cap"] for t in TIERS)


# ================================================================
# 共通ユーティリティ
# ================================================================

def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        return False
    if d.month == 12 and d.day == 31:
        return False
    if d.month == 1 and d.day <= 3:
        return False
    return True


def next_trading_day(d: date) -> date:
    n = d + timedelta(days=1)
    while not is_trading_day(n):
        n += timedelta(days=1)
    return n


def nth_trading_day(d: date, n: int) -> date:
    """dからn営業日後（PEAD延長の売却日=シグナル日+5営業日）。"""
    cur = d
    for _ in range(n):
        cur = next_trading_day(cur)
    return cur


def time_bucket(t: str | None) -> str:
    """DiscTime('15:30:00')→時間帯。場中組は買う時点で発表済みの可能性が高い。
    大引けは15:30（2024-11東証延長）＝15:00〜15:29発表も場中扱い。"""
    if not t:
        return "履歴なし"
    try:
        hm = int(t[:2]) * 60 + int(t[3:5])
    except Exception:
        return "履歴なし"
    if hm < 9 * 60:
        return "寄り前"
    if hm < 15 * 60 + 30:
        return "場中"
    return "引け後"


def load_times() -> dict:
    if os.path.exists(TIMES_FILE):
        with open(TIMES_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def last_disc_time(times: dict, ticker: str, before: str) -> str | None:
    """beforeより前の直近の発表時刻。"""
    dm = times.get(ticker, {})
    prev = [d for d in dm if d < before]
    return dm[max(prev)] if prev else None


def disc_time_pass(last_time: str | None, announced_today: bool = False,
                   tdnet_ok: bool = True, now_min: int = 15 * 60) -> bool:
    """場中発表の銘柄を除外する（2026-08-01 本人指示→同日v2→v2.1）。

    戦略の前提は「引け後に決算が出る → 大引けで仕込んで翌寄りのギャップを取る」。
    本人は実運用で「買う直前に、もう発表済みかを見る」で場中組を一切買っていない。
    初版(1eb2c62)は «前回の発表時刻が場中なら弾く» の習性代用だったが、これは
    前回14:00→今回16:00の第一工業製薬(7/29 gap+16.65%の爆益玉)を弾いてしまう。

    v2.1 = 本人の実手順の機械化（now_min=TDnetを照会した時刻・分）:
      ①当日すでに発表済み（TDnetで観測・announced_today）→ 弾く   ※事実ベース
      ②未発表でも前回時刻が[now_min,15:30)＝習性の発表枠がこれから大引けまでに残っている
        → 弾く（この帯だけは事実確認が構造的に不可能）
      ③前回時刻がnow_minより前なのに当日未発表 → 通す（発表枠を引け後に移した公算。
        15:00発表組の救済はTDNET_WAIT_MINで15:06まで待つことで①の事実判定に変わる）

    BT実測（_bt_earnings_intraday_v2.py・2022-26・時刻カバー100%・本番同一構成）:
      場中すり抜け  初版11.3% → v2@14:55 9.3% → v2.1@15:06 0.9%
      引け後の取りこぼし 初版7.4% → 5.8% → 0.6%
      ＝15:06まで待つだけで両面ほぼ消滅（[15:00,15:30)帯の90.7%が15:00ちょうど発表のため）。
      残る漏れ＝当日15:07以降の駆け込み発表(帯の7.7%・年数件)。本人の直前チェックが最後の網。

    フェイルセーフ:
      TDnet取得失敗(tdnet_ok=False)は初版の習性ルールに退行（前回が場中なら全部弾く＝保守側）。
      時刻履歴が無い銘柄は買う側に倒す（フェイルオープン）。
    戻すときは EXCLUDE_INTRADAY_DISC=False の1行。
    """
    if not EXCLUDE_INTRADAY_DISC:
        return True
    if announced_today:
        return False
    if not tdnet_ok:
        return time_bucket(last_time) in ("引け後", "履歴なし")
    if not last_time:
        return True
    try:
        hm = int(last_time[:2]) * 60 + int(last_time[3:5])
    except Exception:
        return True
    return not (now_min <= hm < 15 * 60 + 30)


# ── TDnet適時開示のリアルタイム照会（2026-08-01・disc_time_pass v2の①） ──
# release.tdnet.info は当日の開示を日別一覧(I_list_NNN_YYYYMMDD.html・100行/頁)で公開。
# 14:55時点でここに決算短信が載っている＝「今日もう発表済み」の事実が取れる。
# 実測(7/30)でJ-Quants /fins/summaryのDiscTimeと7/7銘柄で時刻完全一致を確認済み。
TDNET_LIST_URL = "https://www.release.tdnet.info/inbs/I_list_{page:03d}_{ymd}.html"
_TDNET_ROW_RE = None  # コンパイル済み正規表現のキャッシュ


def _parse_tdnet_page(html: str) -> list[tuple[str, str, str]]:
    """一覧HTMLから(時刻'HH:MM', 4桁コード, 表題)を抽出。構造が変わったら空を返す。"""
    global _TDNET_ROW_RE
    if _TDNET_ROW_RE is None:
        _TDNET_ROW_RE = re.compile(
            r'kjTime[^>]*>([^<]+)<.*?kjCode[^>]*>([^<]+)<.*?'
            r'kjName[^>]*>[^<]*<.*?kjTitle[^>]*>.*?>([^<]+)<', re.S)
    out = []
    for t, code, title in _TDNET_ROW_RE.findall(html):
        out.append((t.strip()[:5], code.strip()[:4], title.strip()))
    return out


def wait_until_min(target_min: int, max_wait_min: int = 40) -> None:
    """JSTでtarget_min(0:00からの分)まで待つ。15:00ちょうど発表組がTDnetに載るのを待つため。
    すでに過ぎていれば即返る。極端に早い起動(max_wait超)も待たずに返る（ハング防止）。"""
    import time as _time

    while True:
        now = datetime.now(JST)
        remain = target_min - (now.hour * 60 + now.minute + now.second / 60)
        if remain <= 0 or remain > max_wait_min:
            return
        _time.sleep(min(remain * 60 + 1, 30))


def _codes_announced_before_close(rows: list[tuple[str, str, str]]) -> tuple[set[str], int]:
    """行リスト→「大引け(15:30)より前に決算短信を出した」4桁コード集合と時刻パース成功数。

    15:30以降の発表は引け後＝買ってよい側なので数えない（遅延起動や--force再実行で
    引け後銘柄を「発表済み」と誤除外しないため。通常の15:06照会では差は出ない）。"""
    codes: set[str] = set()
    parsed = 0
    for t, code, title in rows:
        try:
            hm = int(t[:2]) * 60 + int(t[3:5])
        except Exception:
            continue
        parsed += 1
        if hm < 15 * 60 + 30 and "決算短信" in title:
            codes.add(code)
    return codes, parsed


def fetch_tdnet_announced(ymd: str, max_pages: int = 30,
                          deadline_sec: float = 120.0) -> set[str] | None:
    """当日「大引け前に」決算短信を発表した銘柄の4桁コード集合。

    取得不能はNone＝呼び出し側で習性ルール(前回場中は全部弾く)に退行する（保守側）。
    平日午後のTDnetに開示ゼロはあり得ないため、1ページ目が非200/0行/全行時刻パース不能
    の場合も「故障」とみなしNoneを返す（空集合=正常・発表なし、と区別する）。
    deadline超過も同様（遅いだけの成功で配信を遅らせない）。"""
    import time as _time

    import urllib3
    urllib3.disable_warnings()
    t0 = _time.monotonic()
    try:
        codes: set[str] = set()
        parsed_total = 0
        for page in range(1, max_pages + 1):
            if _time.monotonic() - t0 > deadline_sec:
                print(f"  [tdnet] {deadline_sec:.0f}秒超過(page{page}) → 退行")
                return None
            r = requests.get(TDNET_LIST_URL.format(page=page, ymd=ymd),
                             headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                             verify=False, timeout=20)
            if r.status_code != 200:
                if page == 1:
                    print(f"  [tdnet] 1ページ目 HTTP {r.status_code} → 退行")
                    return None
                break
            rows = _parse_tdnet_page(r.content.decode("utf-8", errors="replace"))
            if not rows:
                if page == 1:
                    print("  [tdnet] 1ページ目パース0行(構造変更?) → 退行")
                    return None
                break
            c, p = _codes_announced_before_close(rows)
            codes |= c
            parsed_total += p
            if len(rows) < 100:
                break
        if parsed_total == 0:
            print("  [tdnet] 時刻が1件もパースできない(形式変更?) → 退行")
            return None
        return codes
    except Exception as e:
        print(f"  [tdnet] 取得失敗: {e} → 習性ルールに退行")
        return None


def refresh_earnings_times(today: date, days: int = 10) -> None:
    """直近N営業日の/fins/summaryを取得しearnings_times.jsonへマージ（時刻の鮮度維持）。"""
    import time as _time

    import urllib3
    urllib3.disable_warnings()
    key = os.getenv("JQUANTS_API_KEY", "").strip()
    if not key:
        print("[times] JQUANTS_API_KEY なし → スキップ")
        return
    times = load_times()
    d = today
    fetched = 0
    while fetched < days:
        d -= timedelta(days=1)
        if not is_trading_day(d):
            continue
        fetched += 1
        ds = d.strftime("%Y-%m-%d")
        try:
            r = requests.get("https://api.jquants.com/v2/fins/summary",
                             headers={"x-api-key": key}, params={"date": ds},
                             timeout=60, verify=False)
            if r.status_code != 200:
                continue
            for x in r.json().get("data", []):
                code = str(x.get("Code", ""))[:4]
                dd, tt = x.get("DiscDate"), x.get("DiscTime")
                if code and dd and tt:
                    times.setdefault(code + ".T", {})[dd] = tt
        except Exception as e:
            print(f"[times] {ds} 取得失敗: {e}")
        _time.sleep(1.1)
    with open(TIMES_FILE, "w", encoding="utf-8") as f:
        json.dump(times, f, ensure_ascii=False)
    print(f"[times] 直近{days}営業日をマージ（銘柄{len(times)}）")


def rule_pass(rsi: float | None, runup5: float | None,
              tov20: float | None, price: float | None,
              price_cap: float = MAX_PRICE_CAP) -> bool:
    if rsi is None or runup5 is None or tov20 is None or price is None:
        return False
    if any(pd.isna(x) for x in (rsi, runup5, tov20, price)):
        return False
    return (rsi <= RSI_MAX and runup5 < RUNUP_MAX
            and tov20 >= TOV_MIN and price <= price_cap)


def load_earnings_vol() -> dict[str, float]:
    """earnings_vol.json を読む。無い/壊れている場合は空＝全銘柄フェイルオープン。"""
    global _EVOL, _EVOL_BUILT
    if _EVOL is not None:
        return _EVOL
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), EARNINGS_VOL_FILE)
    if not os.path.exists(path):
        print(f"[earnings_hold] {EARNINGS_VOL_FILE} が無い → 決算ボラゲートOFF（全部買う）")
        _EVOL = {}
        return _EVOL
    try:
        with open(path, encoding="utf-8") as f:
            blob = json.load(f)
        _EVOL = {k: float(v["vol"]) for k, v in (blob.get("vol") or {}).items()
                 if isinstance(v, dict) and v.get("vol") is not None}
        _EVOL_BUILT = str(blob.get("built", "?"))
    except Exception as e:
        print(f"[earnings_hold] {EARNINGS_VOL_FILE} 読込失敗: {e} → 決算ボラゲートOFF")
        _EVOL = {}
        return _EVOL

    print(f"[earnings_hold] 決算ボラ: {len(_EVOL):,}銘柄（生成 {_EVOL_BUILT}・"
          f"閾値{EARNINGS_VOL_MIN}%）")
    try:
        age = (date.today() - datetime.strptime(_EVOL_BUILT, "%Y-%m-%d").date()).days
        if age > 120:
            print(f"[earnings_hold] ⚠️ 決算ボラが{age}日前のまま "
                  f"→ build_earnings_vol.py の定期実行を確認")
    except Exception:
        pass
    return _EVOL


def vol_pass(ticker: str) -> tuple[bool, float | None]:
    """決算ボラゲート。(通すか, その銘柄の決算ボラ)。

    データが無い銘柄は必ず True（買う）＝フェイルオープン。ゲートで «買わない» 判断を
    するのは、実測値があってそれが閾値未満のときだけ。
    """
    if EARNINGS_VOL_MIN is None:
        return True, None
    v = load_earnings_vol().get(ticker)
    if v is None:
        return True, None
    return v >= EARNINGS_VOL_MIN, v


VOL_REJECT_FILE = "earnings_vol_rejected.json"


def _log_vol_rejected(day: str, rows: list[dict]) -> None:
    """ゲートで弾いた銘柄を残す。«弾かなければどうだったか» を後で検証するため。

    帳簿(positions_earnings*.json)には一切触らない。失敗しても本処理は止めない。
    """
    try:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), VOL_REJECT_FILE)
        log = {}
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                log = json.load(f)
        log[day] = rows
        with open(path, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=1)
    except Exception as e:
        print(f"[earnings_hold] 除外ログの保存失敗: {e}（本処理は継続）")


def calc_shares(price: float, size: int) -> int:
    return max(100, int(size / price / 100) * 100)


def load_positions(path: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"last_signal_date": None, "positions": []}


def save_positions(store: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=1)


def send_discord(embeds: list[dict], webhook: str, label: str,
                 dry: bool = False) -> None:
    if dry:
        print(f"[dry-{label}] Discord送信スキップ:")
        print(json.dumps(embeds, ensure_ascii=False, indent=1)[:1500])
        return
    if not webhook:
        print(f"[warn-{label}] Webhook未設定 → 送信不可")
        return
    import time as _time
    for attempt in range(3):
        try:
            r = requests.post(webhook, json={"embeds": embeds}, timeout=15)
            if r.status_code in (200, 204):
                print(f"[discord-{label}] 送信OK ({len(embeds)} embeds)")
                return
            print(f"[discord-{label}] HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[discord-{label}] attempt{attempt + 1} 失敗: {e}")
        if attempt < 2:
            _time.sleep(2 * (attempt + 1))  # 429レート制限の即時再失敗を防ぐ
    print(f"[discord-{label}] 3回失敗 → 断念")


# ================================================================
# 決済記帳（昨日エントリー分）
# ================================================================

def _px_of(tk: str, all_data: dict, day_str: str, col: str) -> float | None:
    """J-Quants窓→なければyfinanceで day_str の col(Open/Close) を返す。"""
    df = all_data.get(tk)
    if df is not None and len(df):
        idx = df.index.strftime("%Y-%m-%d")
        m = idx == day_str
        if m.any():
            v = float(df[col][m].iloc[0])
            if v > 0:
                return v
    try:
        import yfinance as yf
        h = yf.Ticker(tk).history(period="1mo")
        hi = h.index.strftime("%Y-%m-%d")
        m = hi == day_str
        if m.any():
            v = float(h[col][m].iloc[0])  # 位置[0]はpandas3でKeyError
            if v > 0:
                return v
    except Exception as e:
        print(f"  [settle] {tk} yfinance失敗: {e}")
    return None


def _close_position(pos: dict, entry: float, exit_px: float, exit_d: date,
                    size: int, kind: str) -> None:
    shares = pos.get("shares") or calc_shares(entry, size)
    pos.update({
        "status": "closed", "entry": round(entry, 1),
        "exit_date": exit_d.strftime("%Y-%m-%d"), "exit": round(exit_px, 1),
        "exit_kind": kind,
        "pnl_pct": round((exit_px - entry) / entry * 100, 2),
        "pnl_yen": int((exit_px - entry) * shares),
    })
    pos.pop("note", None)


def _expire_position(pos: dict, today: date, tk: str) -> None:
    """決済価格が長期間取得不能な建玉を失効記帳（売買停止/上場廃止想定・枠を解放）。"""
    pos.update({
        "status": "expired", "expired_date": today.strftime("%Y-%m-%d"),
        "note": f"決済価格が{EXPIRE_CAL_DAYS}日超取得不能（売買停止/上場廃止の可能性）"
                "→失効・実際に約定していた場合は手動で処分と記帳が必要",
    })
    print(f"  [settle] {tk} {pos.get('name', '')} {EXPIRE_CAL_DAYS}日超データなし → 失効記帳")


def settle_pendings(store: dict, today: date, all_data: dict,
                    size: int) -> tuple[list[dict], list[dict], list[dict]]:
    """記帳処理。pending=翌寄りで決済（ただし寄りがエントリー比+PEAD_EXT_GAP%超なら
    extendedに昇格＝5営業日目の大引けまでホールド）。extended=売却日の翌日以降に
    公式終値で決済。決済価格がEXPIRE_CAL_DAYS超取得不能なら失効（枠解放）。
    戻り値: (今回closed明細, 今回extended昇格明細, 今回expired失効明細)。"""
    settled: list[dict] = []
    extended: list[dict] = []
    expired: list[dict] = []
    for pos in store["positions"]:
        st = pos.get("status")
        tk = pos.get("ticker")
        if st == "pending":
            sig_d = datetime.strptime(pos["date"], "%Y-%m-%d").date()
            exit_d = next_trading_day(sig_d)
            if exit_d > today:
                continue  # まだ決済日が来ていない（安全側）
            entry = _px_of(tk, all_data, pos["date"], "Close")
            open_px = _px_of(tk, all_data, exit_d.strftime("%Y-%m-%d"), "Open")
            if entry is None or open_px is None:
                if (today - exit_d).days > EXPIRE_CAL_DAYS:
                    _expire_position(pos, today, tk)
                    expired.append(pos)
                    continue
                pos["note"] = f"決済価格未取得(entry={entry}, exit={open_px})・翌日再試行"
                print(f"  [settle] {tk} 価格未取得 → pending維持")
                continue
            gap = (open_px - entry) / entry * 100
            if gap > PEAD_EXT_GAP:
                ext_d = nth_trading_day(sig_d, PEAD_EXT_DAYS)
                pos.update({
                    "status": "extended", "entry": round(entry, 1),
                    "gap_pct": round(gap, 2),
                    "ext_exit_date": ext_d.strftime("%Y-%m-%d"),
                })
                pos.pop("note", None)
                extended.append(pos)
                print(f"  [settle] {tk} {pos['name']} 寄り{gap:+.1f}% > +{PEAD_EXT_GAP:.0f}%"
                      f" → PEAD延長（{ext_d}大引け売り）")
                continue
            _close_position(pos, entry, open_px, exit_d, size, "翌寄り")
            settled.append(pos)
            print(f"  [settle] {tk} {pos['name']} {entry:,.0f}→{open_px:,.0f} "
                  f"{pos['pnl_pct']:+.2f}% ({pos['pnl_yen']:+,}円)")
        elif st == "extended":
            ext_d = datetime.strptime(pos["ext_exit_date"], "%Y-%m-%d").date()
            if ext_d >= today:
                continue  # 売却日の大引け値は翌営業日に確定
            close_px = _px_of(tk, all_data, pos["ext_exit_date"], "Close")
            entry = pos.get("entry")
            if close_px is None or entry is None:
                if (today - ext_d).days > EXPIRE_CAL_DAYS:
                    _expire_position(pos, today, tk)
                    expired.append(pos)
                    continue
                print(f"  [settle] {tk} 延長分の終値未取得 → extended維持")
                continue
            _close_position(pos, float(entry), close_px, ext_d, size, "PEAD延長")
            settled.append(pos)
            print(f"  [settle] {tk} {pos['name']} 延長決済 {entry:,.0f}→{close_px:,.0f} "
                  f"{pos['pnl_pct']:+.2f}% ({pos['pnl_yen']:+,}円)")
    return settled, extended, expired


# ================================================================
# 当日シグナル（候補生成は全階層共用・価格帯カットは階層側）
# ================================================================

def build_candidates(codes: list[dict], all_data: dict,
                     times: dict | None = None,
                     announced: set[str] | None = None,
                     now_min: int = 15 * 60) -> list[dict]:
    """発表予定銘柄にルールA(価格帯は最大キャップ)を適用。現在値はyfinance(15分遅延)。

    announced = 当日すでに決算短信を出した4桁コード集合(TDnet)。
                None=取得不能→場中除外は初版の習性ルールに退行(保守側)。
    now_min   = TDnetを照会した時刻(0:00からの分)。習性除外帯[now_min,15:30)の起点。
    """
    import yfinance as yf
    from screener import calc_rsi

    times = times or {}
    tdnet_ok = announced is not None
    announced = announced or set()
    today_str = date.today().strftime("%Y-%m-%d")
    out: list[dict] = []
    rejected: list[dict] = []
    for x in codes:
        tk = x["code"] + ".T"
        df = all_data.get(tk)
        if df is None or len(df) < 30:
            continue
        closes = df["Close"].dropna()
        vols = df["Volume"].reindex(closes.index).fillna(0)
        tov20 = float((closes * vols).tail(20).median())
        if tov20 < TOV_MIN:      # 現在値を取りに行く前に足切り（yfinanceコール節約）
            continue
        try:
            intraday = yf.Ticker(tk).history(period="1d", interval="5m")
            price = float(intraday["Close"].iloc[-1]) if not intraday.empty else None
        except Exception as e:
            print(f"  [yfinance] {tk} 現在値失敗: {e}")
            price = None
        if price is None or price <= 0:
            continue
        seq = closes.tolist() + [price]
        rsi = calc_rsi(pd.Series(seq))
        runup5 = (price - seq[-6]) / seq[-6] * 100 if len(seq) >= 6 and seq[-6] > 0 else None
        if rsi is None or runup5 is None:
            continue
        if rule_pass(rsi, runup5, tov20, price, MAX_PRICE_CAP):
            ok, evol = vol_pass(tk)
            lt = last_disc_time(times, tk, today_str)
            is_announced = x["code"] in announced
            if not ok or not disc_time_pass(lt, announced_today=is_announced,
                                            tdnet_ok=tdnet_ok, now_min=now_min):
                # 弾いた銘柄は必ず残す。後から「弾かなければどうだったか」を検証するため。
                why = "vol" if not ok else ("announced" if is_announced else "preclose")
                rejected.append({"ticker": tk, "name": x["name"], "vol": evol,
                                 "why": why,
                                 "last_time": lt[:5] if lt else None,
                                 "bucket": time_bucket(lt),
                                 "rsi": round(float(rsi), 1), "runup5": round(runup5, 1),
                                 "price": price})
                continue
            out.append({"ticker": tk, "code": x["code"], "name": x["name"],
                        "type": x.get("type", ""), "price": price,
                        "rsi": round(float(rsi), 1), "runup5": round(runup5, 1),
                        "tov20": tov20, "evol": evol,
                        "last_time": lt[:5] if lt else None,
                        "last_bucket": time_bucket(lt)})
    if rejected:
        v = [r for r in rejected if r["why"] == "vol"]
        a = [r for r in rejected if r["why"] == "announced"]
        i = [r for r in rejected if r["why"] == "preclose"]
        if v:
            print(f"  [SKIP VOL] 決算ボラ<{EARNINGS_VOL_MIN}%で除外 {len(v)}件: "
                  + " ".join(f"{r['ticker']}({r['vol']:.1f}%)" for r in v))
        if a:
            print(f"  [SKIP ANNOUNCED] 本日すでに発表済み(TDnet)のため除外 {len(a)}件: "
                  + " ".join(r["ticker"] for r in a))
        if i:
            print(f"  [SKIP PRECLOSE] 前回が引け直前[15:00-15:29]型のため除外 {len(i)}件: "
                  + " ".join(f"{r['ticker']}({r['last_time']})" for r in i))
        _log_vol_rejected(today_str, rejected)
    return sorted(out, key=lambda r: r["rsi"])


# ================================================================
# Discord embeds
# ================================================================

def embed_results(settled: list[dict], tier: dict) -> dict:
    total = sum(p["pnl_yen"] for p in settled)
    lines = []
    for p in settled:
        tag = "｜🚀延長分" if p.get("exit_kind") == "PEAD延長" else ""
        lines.append(f"{'🟢' if p['pnl_yen'] > 0 else '🔴'} **{p['name']}**（{p['ticker'].replace('.T', '')}）"
                     f" 買{p['entry']:,.0f}円→売{p['exit']:,.0f}円｜"
                     f"{p['pnl_pct']:+.2f}%｜**{p['pnl_yen']:+,}円**{tag}")
    wins = sum(1 for p in settled if p["pnl_yen"] > 0)
    return {
        "title": f"✅ 決算持ち越し【{tier['label']}】｜決済結果（{wins}勝{len(settled) - wins}敗）",
        "description": "\n".join(lines) + f"\n\n**合計 {total:+,}円**",
        "color": 0x2ECC71 if total >= 0 else 0xE74C3C,
    }


def embed_extended(extended: list[dict], tier: dict) -> dict:
    """PEAD延長の通知: 爆勝ちギャップ玉はまだ売らない。"""
    lines = []
    for p in extended:
        lines.append(f"🚀 **{p['name']}**（{p['ticker'].replace('.T', '')}）"
                     f" 寄り**{p['gap_pct']:+.1f}%** → 売らずホールド"
                     f"（**{p['ext_exit_date']} 大引けで売却**）")
    return {
        "title": f"🚀 決算持ち越し【{tier['label']}】｜PEAD延長 {len(extended)}件",
        "description": "\n".join(lines) +
                       f"\n\n寄りが+{PEAD_EXT_GAP:.0f}%超の爆勝ちは決算後ドリフトを"
                       "追加で取る（BT: 延長分は平均+3.8%上乗せ・全5年プラス）",
        "color": 0x9B59B6,
    }


def embed_expired(expired: list[dict], tier: dict) -> dict:
    """失効記帳の通知: 決済価格が長期間取れない建玉（売買停止/上場廃止想定）。"""
    lines = []
    for p in expired:
        lines.append(f"・**{p['name']}**（{p['ticker'].replace('.T', '')}）"
                     f" {p.get('shares', 0):,}株（{p['date']}買い）")
    return {
        "title": f"⚠️ 決算持ち越し【{tier['label']}】｜失効 {len(expired)}件（要手動確認）",
        "description": "\n".join(lines) +
                       f"\n\n決済価格が{EXPIRE_CAL_DAYS}日を超えて取得できないため帳簿から"
                       "失効させました（売買停止・上場廃止などの可能性）。"
                       "**実際に約定していた場合は口座を確認し手動で処分してください**",
        "color": 0xE74C3C,
    }


def embed_ext_exit_today(exits: list[dict], tier: dict) -> dict:
    """延長玉の売却日当日: 本日の大引け成行で売る指示。"""
    lines = []
    for p in exits:
        lines.append(f"・**{p['name']}**（{p['ticker'].replace('.T', '')}）"
                     f" {p.get('shares', 0):,}株（{p['date']}買い・延長中）")
    return {
        "title": f"⏰ 決算持ち越し【{tier['label']}】｜延長分を本日大引けで売却 {len(exits)}件",
        "description": "\n".join(lines) + "\n\n**15:25までに大引け成行の売り注文**を入れてください",
        "color": 0xE67E22,
    }


def embed_reminder(pends: list[dict], tier: dict) -> dict:
    """朝の売り忘れ防止。昨日(以前)の大引けで買った建玉を今日の寄りで売る確認。
    例外=PEAD延長: 寄り気配がエントリー比+8%超なら売らずホールド。"""
    lines = []
    for p in pends:
        base = p.get("prev_close") or p.get("signal_price")
        hold = f"（🚀気配 {base * (1 + PEAD_EXT_GAP / 100):,.0f}円 以上なら売らずホールド）" if base else ""
        lines.append(f"・**{p['name']}**（{p['ticker'].replace('.T', '')}）"
                     f" {p['shares']:,}株（{p['date']}買い）{hold}")
    return {
        "title": f"⏰ 決算持ち越し【{tier['label']}】｜本日寄りで売る建玉 {len(pends)}件",
        "description": "\n".join(lines) +
                       f"\n\n**原則9:00の寄り成行で売却**。ただし寄り気配が+{PEAD_EXT_GAP:.0f}%超の"
                       "爆勝ちだけは売らずホールド（15時の通知が売却日を案内します）",
        "color": 0xE67E22,
    }


def is_week_last_trading_day(d: date) -> bool:
    """dがその週の最終営業日か（金曜祝日なら木曜がTrue・report.pyと同じISO週比較）。"""
    n = next_trading_day(d)
    return (n.isocalendar()[0], n.isocalendar()[1]) != (d.isocalendar()[0], d.isocalendar()[1])


def embed_weekly(store: dict, today: date, tier: dict) -> dict:
    """今週(月〜当日)に決済確定した分の週次サマリー＋通算。"""
    monday = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
    today_s = today.strftime("%Y-%m-%d")
    closed_all = [p for p in store["positions"] if p.get("status") == "closed"]
    wk = [p for p in closed_all if monday <= p.get("exit_date", "") <= today_s]
    total = sum(p["pnl_yen"] for p in wk)
    md = f"{today.month}/{today.day}"
    if not wk:
        desc = "今週の決済 0件（シーズン外は普通です）"
    else:
        wins = [p for p in wk if p["pnl_yen"] > 0]
        pos = sum(p["pnl_pct"] for p in wk if p["pnl_pct"] > 0)
        neg = abs(sum(p["pnl_pct"] for p in wk if p["pnl_pct"] <= 0))
        pf = (pos / neg) if neg > 0 else float("inf")
        pf_s = "∞" if pf == float("inf") else f"{pf:.2f}"
        best = max(wk, key=lambda p: p["pnl_yen"])
        worst = min(wk, key=lambda p: p["pnl_yen"])
        desc = (f"決済 {len(wk)}件 勝率{len(wins)}/{len(wk)} PF {pf_s}\n"
                f"**週間 {total:+,}円**\n"
                f"🏆 {best['name']} {best['pnl_yen']:+,}円 / "
                f"🥶 {worst['name']} {worst['pnl_yen']:+,}円")
    grand = sum(p["pnl_yen"] for p in closed_all)
    desc += f"\n\n通算（稼働開始から）: {len(closed_all)}件 **{grand:+,}円**"
    return {
        "title": f"📅 決算持ち越し【{tier['label']}】｜週次サマリー（〜{md}）",
        "description": desc,
        "color": 0x2ECC71 if total >= 0 else 0xE74C3C,
        "footer": {"text": "金曜買い分は月曜朝決済＝翌週の週次に計上"},
    }


def embed_signals(picks: list[dict], n_scheduled: int, today: date, tier: dict,
                  note: str | None = None) -> dict:
    md = today.strftime("%m/%d")
    if not picks:
        return {
            "title": f"📊 決算持ち越し【{tier['label']}】｜対象なし（{md}）",
            "description": f"本日の発表予定 {n_scheduled}件 → 売られすぎ条件の該当なし。\n"
                           "（決算シーズン外はゼロが続くのが正常です）"
                           + (f"\n⚠️ {note}" if note else ""),
            "color": 0x95A5A6,
        }
    lines = []
    for i, r in enumerate(picks, 1):
        shares = calc_shares(r["price"], tier["size"])
        amount = shares * r["price"] / 10000
        lb, lt = r.get("last_bucket", "履歴なし"), r.get("last_time")
        if lb == "場中":
            t_note = f"⚠️ 前回発表 {lt}（場中型＝本日すでに発表済みの可能性）"
        elif lt:
            t_note = f"前回発表 {lt}（今夜型）"
        else:
            t_note = "前回発表時刻 不明"
        lines.append(
            f"**{i}. {r['name']}**（{r['code']}）{r['type']}\n"
            f"　現在値 {r['price']:,.0f}円 → **大引け成行 {shares:,}株**（約{amount:,.0f}万円）\n"
            f"　RSI {r['rsi']} / 5日騰落 {r['runup5']:+.1f}% / {t_note}")
    return {
        "title": f"📊 決算持ち越し【{tier['label']}】｜本日の買いリスト {len(picks)}件（{md}）",
        "description": "\n".join(lines) + (f"\n⚠️ {note}" if note else ""),
        "color": 0x3498DB,
        "footer": {"text": "今夜決算発表→明日寄り成行で売り（夜のうちに売り予約推奨）｜"
                           f"{SLOTS}枠×{tier['size'] // 10000}万｜オーバーナイトはSTOP無効・最悪-25%級あり"},
    }


# ================================================================
# 朝の売り忘れリマインダー（schedule.yml相乗り・8時台）
# ================================================================

def remind(force: bool = False, dry: bool = False) -> None:
    now = datetime.now(JST)
    today = now.date()
    print(f"[remind] 実行: {now.strftime('%Y-%m-%d %H:%M JST')}")
    if not force:
        if not is_trading_day(today):
            print("[remind] 休場日 → スキップ")
            return
        hm = now.hour * 60 + now.minute
        if not (7 * 60 <= hm <= 8 * 60 + 55):
            print(f"[remind] 時間外({now.strftime('%H:%M')}) → スキップ（7:00-8:55のみ）")
            return
    today_s = today.strftime("%Y-%m-%d")
    for tier in TIERS:
        label = tier["label"]
        store = load_positions(tier["positions_file"])
        if store.get("last_reminder_date") == today_s and not force:
            print(f"[remind-{label}] 本日送信済み → スキップ")
            continue
        pends = [p for p in store["positions"]
                 if p.get("status") == "pending" and p.get("date", "") < today_s]
        if not pends:
            print(f"[remind-{label}] 売る建玉なし → 無送信")
            continue
        # PEAD判定ライン用に前日終値を取得（失敗時はsignal_price近似）
        for p in pends:
            try:
                import yfinance as yf
                h = yf.Ticker(p["ticker"]).history(period="5d")
                hi = h.index.strftime("%Y-%m-%d")
                m = hi == p["date"]
                if m.any():
                    p["prev_close"] = float(h["Close"][m].iloc[0])
            except Exception as e:
                print(f"[remind-{label}] {p['ticker']} 終値取得失敗: {e}")
        send_discord([embed_reminder(pends, tier)],
                     os.getenv(tier["webhook_env"], ""), label, dry=dry)
        if not dry:
            store["last_reminder_date"] = today_s
            save_positions(store, tier["positions_file"])


# ================================================================
# main
# ================================================================

def main() -> None:
    force = "--force" in sys.argv
    dry = "--dry" in sys.argv

    if "--remind" in sys.argv:
        remind(force=force, dry=dry)
        return

    if "--test" in sys.argv:
        for tier in TIERS:
            picks = [{"ticker": "0000.T", "code": "0000", "name": "テスト銘柄",
                      "type": "第１四半期", "price": 2340.0, "rsi": 32.5,
                      "runup5": -6.2, "tov20": 2.5e9,
                      "last_time": "15:30", "last_bucket": "引け後"}]
            e = embed_signals(picks, 42, date.today(), tier)
            e["title"] = "🧪【テスト配信】" + e["title"]
            send_discord([e], os.getenv(tier["webhook_env"], ""),
                         tier["label"], dry=dry)
        return

    now = datetime.now(JST)
    today = now.date()
    print(f"[earnings_hold] 実行: {now.strftime('%Y-%m-%d %H:%M JST')}")

    if not force:
        if not is_trading_day(today):
            print("[earnings_hold] 休場日 → スキップ")
            return
        hm = now.hour * 60 + now.minute
        if not (14 * 60 + 30 <= hm <= 15 * 60 + 18):
            print(f"[earnings_hold] 時間外({now.strftime('%H:%M')}) → スキップ"
                  "（発注が間に合う14:30-15:18のみ配信）")
            return

    # 二重発火ガード（先頭階層の記録で代表判定）
    head_store = load_positions(TIERS[0]["positions_file"])
    if head_store.get("last_signal_date") == today.strftime("%Y-%m-%d") and not force:
        print("[earnings_hold] 本日配信済み → スキップ（二重発火ガード）")
        return

    # ── 1. JPX予定表更新（失敗しても手元JSONで続行） ──
    note = None
    try:
        import fetch_jpx_earnings_schedule as fjx
        fjx.main()
    except SystemExit as e:  # fjxはリンク0本でsys.exit(1)＝Exceptionでは捕まらない
        print(f"[earnings_hold] JPX更新中止(exit {e.code}) → 手元のJSONで続行")
        note = "JPX予定表の更新に失敗（前回取得分で判定）"
    except Exception as e:
        print(f"[earnings_hold] JPX更新失敗: {e} → 手元のJSONで続行")
        note = "JPX予定表の更新に失敗（前回取得分で判定）"
    if not os.path.exists(SCHEDULE_FILE):
        print("[earnings_hold] 予定表なし → 中止")
        return
    sched = json.load(open(SCHEDULE_FILE, encoding="utf-8"))
    fetched = sched.get("fetched", "?")
    todays = sched.get("schedule", {}).get(today.strftime("%Y-%m-%d"), [])
    print(f"[earnings_hold] 予定表({fetched}取得) 本日{len(todays)}件")

    # ── 1b. 発表時刻履歴の鮮度維持（表示用・失敗しても続行） ──
    try:
        refresh_earnings_times(today)
    except Exception as e:
        print(f"[earnings_hold] 時刻履歴の更新失敗: {e} → 手元データで続行")
    times = load_times()

    # ── 2. J-Quants履歴（昨日まで・決済記帳と判定に全階層共用） ──
    from screener import _jquants_id_token, batch_download_jquants
    start = (today - timedelta(days=HIST_DAYS)).strftime("%Y-%m-%d")
    end = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    token = _jquants_id_token()
    all_data = batch_download_jquants(token, start=start, end=end)
    print(f"[earnings_hold] J-Quants {start}〜{end}: {len(all_data)}銘柄")

    # ── 3. 候補生成（yfinanceは1回だけ・価格帯カットは階層側） ──
    # 15:00ちょうどの発表（場中の最大バケット）がTDnetに載るまで待ってから照会する。
    # 重い前処理（JPX/J-Quants）は済んでいるので、ここから配信までは1〜2分。
    if TDNET_WAIT_MIN and not dry:
        wait_until_min(TDNET_WAIT_MIN)
    chk = datetime.now(JST)
    chk_min = chk.hour * 60 + chk.minute
    announced = fetch_tdnet_announced(today.strftime("%Y%m%d"))
    if announced is None:
        print(f"[earnings_hold] TDnet取得不能 → 習性ルール(前回場中は除外)に退行")
    else:
        print(f"[earnings_hold] TDnet({chk.strftime('%H:%M')}時点): "
              f"本日ここまでに決算短信 {len(announced)}銘柄")
    cands_all = build_candidates(todays, all_data, times,
                                 announced=announced, now_min=chk_min)
    print(f"[earnings_hold] 全候補 {len(cands_all)}件（価格帯カット前）")

    # ── 4. 階層ループ: 決済記帳 → 当日シグナル → 配信 → 保存 ──
    for tier in TIERS:
        label = tier["label"]
        store = load_positions(tier["positions_file"])
        embeds = []

        settled, extended, expired = settle_pendings(store, today, all_data, tier["size"])
        if settled:
            embeds.append(embed_results(settled, tier))
        if extended:
            embeds.append(embed_extended(extended, tier))
        if expired:
            embeds.append(embed_expired(expired, tier))

        # 延長玉の売却日当日 → 大引け売り指示（14:55=発注にちょうど間に合う）
        today_s = today.strftime("%Y-%m-%d")
        ext_today = [p for p in store["positions"]
                     if p.get("status") == "extended"
                     and p.get("ext_exit_date") == today_s]
        if ext_today:
            embeds.append(embed_ext_exit_today(ext_today, tier))

        # pending(今夜またぎ待ち)と extended(延長ホールド中)の両方が枠を占有
        holding_now = {p["ticker"] for p in store["positions"]
                       if p.get("status") in ("pending", "extended")}
        free_slots = max(0, SLOTS - len(holding_now))
        picks = [c for c in cands_all
                 if c["price"] <= tier["price_cap"]
                 and c["ticker"] not in holding_now][:free_slots]
        print(f"[earnings_hold-{label}] 配信{len(picks)}件（空き枠{free_slots}）")
        embeds.append(embed_signals(picks, len(todays), today, tier, note=note))

        # 週の最終営業日は週次サマリーを同梱（決済は翌朝確定のため金曜買いは翌週計上）
        if is_week_last_trading_day(today):
            embeds.append(embed_weekly(store, today, tier))

        send_discord(embeds, os.getenv(tier["webhook_env"], ""), label, dry=dry)

        if not dry:
            for p in picks:
                store["positions"].append({
                    "ticker": p["ticker"], "name": p["name"], "type": p["type"],
                    "date": today.strftime("%Y-%m-%d"),
                    "signal_price": round(p["price"], 1),
                    "shares": calc_shares(p["price"], tier["size"]),
                    "rsi": p["rsi"], "runup5": p["runup5"],
                    "status": "pending",
                })
            store["last_signal_date"] = today.strftime("%Y-%m-%d")
            save_positions(store, tier["positions_file"])
            print(f"[earnings_hold-{label}] {tier['positions_file']} 保存"
                  f"（pending {len(picks)}件追加）")


if __name__ == "__main__":
    main()
