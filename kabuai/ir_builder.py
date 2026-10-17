# -*- coding: utf-8 -*-
"""ir_builder.py — 📰 IR（適時開示）の好材料・悪材料ピックアップ（2026-10-17・チンパン v7.5 の核）

本人の依頼（10/17）: 「株探の好材料・悪材料のピックアップ」→「brisk（timely-disclosure.brisk.jp）基準のIRの取得＋
その内容の精査 → 株が上がる/下がるかを君（Claude）が判断。既出も含めて」。

元データ: TDnet（https://www.release.tdnet.info/inbs/I_list_NNN_YYYYMMDD.html）を直接読む。
  brisk の利用規約は「データの加工・第三者提供・私的目的以外の使用を禁止」なので、公開ページ（GitHub Pages）に
  brisk のデータは載せない。brisk は「分類の基準（タグの体系）」としてだけ使う。
  TDnet は GitHub Actions からも取れる（arena_watchlist.fetch_tdnet_all で実績あり）。

やること:
  1) 直近 WINDOW_DAYS 営業日の TDnet 開示を全件取る（時刻・コード・社名・表題・PDF URL）
  2) brisk のタグ体系（決算短信/予想修正/配当/自社株買い/優待/分割・併合/エクイティ/提携/公開買付け/訴訟/行政処分/
     事故/災害/上場廃止/破産・再生/月次/経営計画/人事/訂正/定款/株主総会/CG報告書/統合報告書/有価証券報告書/英文資料/
     説明会資料/IPO/その他）に表題から分類する
  3) ルール判定: 表題の語で 好材料(+1〜+3)/悪材料(−1〜−3)/中立(0)/要精査(0・中身を読まないと分からない)
  4) Claude 判定: ir_judge.py が PDF の冒頭を読んで判定した結果（kabuai/ir_judged.json・開示IDで引く）を重ねる。
     キーが無い回は ルール判定だけで出る（止まらない）。
  5) 答え合わせ: 開示が 場中(15:30まで)なら「既出」＝その日の終値の反応を付ける。引け後なら翌営業日の
     寄りギャップ・終値（データが来た回に埋まる）。直近 STATS_DAYS 営業日の 好/悪 ごとの平均と勝率を出す
     ＝「判定が当たっているか」を毎日表に出す（勝てると書かない・数字で見せる）。

出力: latest.json["ir"] = {date, window:[...日付], rows:[...], stats:{...}, tags_count:{}, llm:{...}, rules:[...]}
"""
from __future__ import annotations

import html as _html
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
JST = timezone(timedelta(hours=9))
TDNET_LIST_URL = "https://www.release.tdnet.info/inbs/I_list_{page:03d}_{ymd}.html"
TDNET_PDF_BASE = "https://www.release.tdnet.info/inbs/"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"}
JUDGED_PATH = HERE / "ir_judged.json"      # ir_judge.py が書く（PC から gh api でコミット／Actions 内でも生成）

WINDOW_DAYS = int(os.environ.get("KABUAI_IR_WINDOW", "3"))     # 画面に出す営業日数（当日＋既出2日）
STATS_DAYS = int(os.environ.get("KABUAI_IR_STATS", "15"))      # 答え合わせの営業日数（TDnet は約1ヶ月分しか残らない）
DEADLINE_SEC = float(os.environ.get("KABUAI_IR_DEADLINE", "240"))
SESSION_END = "15:30"                                           # これより前の開示＝場中（既出）。15:30ちょうどは引け後

_ROW_RE = re.compile(
    r'<tr[^>]*>\s*<td[^>]*kjTime[^>]*>(.*?)</td>\s*<td[^>]*kjCode[^>]*>(.*?)</td>\s*<td[^>]*kjName[^>]*>(.*?)</td>'
    r'\s*<td[^>]*kjTitle[^>]*>(.*?)</td>\s*<td[^>]*kjXbrl[^>]*>(.*?)</td>\s*<td[^>]*kjPlace[^>]*>(.*?)</td>', re.S)
_A_RE = re.compile(r'href="([^"]+)"[^>]*>(.*?)</a>', re.S)


def _clean(s: str) -> str:
    return _html.unescape(re.sub(r"<[^>]+>", "", s or "")).replace("　", " ").strip()


# ── 営業日 ────────────────────────────────────────────────────────
def is_trading_day(d: date) -> bool:
    try:
        import jpholiday
        if jpholiday.is_holiday(d):
            return False
    except Exception:
        pass
    if d.weekday() >= 5:
        return False
    return not ((d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3))


def prev_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while not is_trading_day(p):
        p -= timedelta(days=1)
    return p


def next_trading_day(d: date) -> date:
    n = d + timedelta(days=1)
    while not is_trading_day(n):
        n += timedelta(days=1)
    return n


# ── TDnet 取得 ────────────────────────────────────────────────────
def fetch_tdnet_day(ymd: str, max_pages: int = 40, timeout: float = 20.0) -> list[dict] | None:
    """1日分の TDnet 開示を全件。[{id,time,code,code5,name,title,pdf,xbrl,place}]。取れない時は None。"""
    out: list[dict] = []
    for page in range(1, max_pages + 1):
        try:
            r = requests.get(TDNET_LIST_URL.format(page=page, ymd=ymd), headers=UA, timeout=timeout)
        except Exception as e:
            if page == 1:
                print(f"  [ir] TDnet {ymd} 取得失敗: {e}")
                return None
            break
        if r.status_code != 200:
            if page == 1:
                print(f"  [ir] TDnet {ymd} 1ページ目 HTTP {r.status_code}")
                return None
            break
        t = r.content.decode("utf-8", errors="replace")
        rows = _ROW_RE.findall(t)
        if not rows:
            if page == 1:
                # 休日などで0件のページは「全0件」。取れないのとは区別する
                return []
            break
        for tm, code, name, title, xbrl, place in rows:
            m = _A_RE.search(title)
            pdf = (m.group(1) if m else "").strip()
            ttl = _clean(m.group(2) if m else title)
            code5 = _clean(code)
            did = re.sub(r"\.pdf$", "", pdf)
            out.append({"id": did or f"{ymd}-{code5}-{len(out)}", "time": _clean(tm)[:5], "code": code5[:4], "code5": code5,
                        "name": _clean(name), "title": ttl, "pdf": (TDNET_PDF_BASE + pdf) if pdf and not pdf.startswith("http") else pdf,
                        "xbrl": bool(_A_RE.search(xbrl)), "place": _clean(place)})
        if len(rows) < 100:
            break
    return out


# ── brisk 基準のタグ付け（表題 → タグ） ──────────────────────────────
# 順序に意味がある: 先に当たったものから複数付く（brisk も複数タグ）。
TAG_RULES: list[tuple[str, str]] = [
    (r"訂正", "訂正"),
    (r"決算短信", "決算短信"),
    (r"決算説明|説明会資料|説明資料|決算補足|補足説明|補足資料|プレゼンテーション資料", "説明会資料"),
    (r"業績予想.*修正|予想.*修正|上方修正|下方修正|予想(値|数値)?の(修正|変更)|予想との差異|差異に関する", "予想修正"),
    (r"配当", "配当"),
    (r"自己株式.*(取得|買付|買い付け)|自社株買い|自己株式立会外|自己株式の消却|自己株式取得", "自社株買い"),
    (r"株主優待", "優待"),
    (r"株式分割|株式併合", "分割・併合"),
    (r"公募|第三者割当|新株予約権|新株式発行|売出し|売出|転換社債|ＣＢ|CB|MSCB|行使価額|株式の発行|資本金の額の減少|減資|自己株式の処分", "エクイティ"),
    (r"業務提携|資本提携|資本業務提携|協業|提携|共同開発|共同事業|パートナーシップ", "提携"),
    (r"公開買付|ＴＯＢ|TOB|株式交換|完全子会社化|経営統合|合併|株式売渡し請求", "公開買付け"),
    (r"訴訟|提訴|判決|仲裁|和解", "訴訟"),
    (r"行政処分|業務改善命令|業務停止|課徴金|勧告|指名停止|是正", "行政処分"),
    (r"不正アクセス|情報漏えい|情報漏洩|ランサム|サイバー|火災|爆発|事故|不祥事|不適切|過年度|リコール|回収", "事故"),
    (r"地震|台風|豪雨|災害|被害状況", "災害"),
    (r"上場廃止|監理銘柄|特設注意|上場維持基準|整理銘柄|上場契約違約金", "上場廃止"),
    (r"民事再生|破産|会社更生|債務超過|継続企業|特別清算|私的整理|事業再生ADR", "破産・再生"),
    (r"月次|売上高速報|売上速報|既存店|月間|受注高(速報|状況)|出荷実績|客数", "月次"),
    (r"中期経営計画|中期計画|経営計画|中計|長期ビジョン|経営方針|事業計画", "経営計画"),
    (r"代表取締役の異動|役員の異動|人事|役員人事|社長交代|取締役候補|執行役員", "人事"),
    (r"定款", "定款"),
    (r"株主総会|招集ご?通知|招集通知", "株主総会"),
    (r"新規上場|上場承認|上場に関する", "IPO"),
    (r"コーポレート・ガバナンス|コーポレートガバナンス|ガバナンスに関する報告書", "CG報告書"),
    (r"統合報告書|統合レポート|Integrated\s*Report|サステナビリティレポート|ESG", "統合報告書"),
    (r"有価証券報告書|四半期報告書|半期報告書|内部統制報告書|臨時報告書", "有価証券報告書"),
    (r"特別損失|減損|貸倒|引当金|損失の(発生|計上)", "特損"),
    (r"特別利益|固定資産の譲渡|売却益|受取補償金", "特益"),
    (r"子会社|関連会社|持分", "子会社"),
    (r"受注|契約締結|採用|納入|導入|ライセンス|承認|特許|新製品|新サービス|出店|開設|竣工|参入|開始|発売|開発", "事業"),
]
_ASCII_RE = re.compile(r"^[\x20-\x7e -ÿ\s]+$")

# 事務開示（画面の既定では隠す。全部タブでは出す）
ADMIN_TAGS = {"CG報告書", "定款", "株主総会", "有価証券報告書", "統合報告書", "英文資料", "説明会資料", "人事"}
# 中身を読まないと方向が決まらない（Claude 判定の対象）
MATERIAL_TAGS = {"決算短信", "予想修正", "配当", "自社株買い", "優待", "分割・併合", "エクイティ", "提携", "公開買付け", "訴訟",
                 "行政処分", "事故", "災害", "上場廃止", "破産・再生", "月次", "経営計画", "特損", "特益", "IPO"}


def tag_title(title: str) -> list[str]:
    t = title or ""
    tags = [lab for pat, lab in TAG_RULES if re.search(pat, t)]
    if _ASCII_RE.match(t):
        tags = ["英文資料"] + [x for x in tags if x != "英文資料"]
    if not tags:
        tags = ["その他"]
    # 自己株式の処分は報酬目的が大半＝エクイティではなく事務。譲渡制限付株式報酬も事務。
    if re.search(r"市場区分|市場変更|市場への変更", t):
        tags = ["市場変更"] + [x for x in tags if x not in ("上場廃止", "事業", "IPO")]
    if re.search(r"譲渡制限付株式|株式報酬|ストック・?オプション|取得状況|処分の払込完了|払込完了|失権|行使状況|買付終了|取得結果|取得の状況|公認会計士|監査法人|配当金(の)?受領|譲渡(の)?承認|株式交換比率の決定|合併比率の決定", t):
        tags = [x for x in tags if x not in ("自社株買い", "エクイティ")] or ["その他"]
        if "事務" not in tags:
            tags.append("事務")
    return list(dict.fromkeys(tags))


# ── ルール判定（表題だけで分かる範囲） ──────────────────────────────
def _pct_in(title: str) -> float | None:
    """月次などの『前年同月比 112.3%』『+8.2%』『▲3.1%』を数字に。"""
    m = re.search(r"前年(同月)?比\s*([▲△\-−+＋]?)\s*(\d+(?:\.\d+)?)\s*[%％]", title)
    if not m:
        return None
    sign, v = m.group(2), float(m.group(3))
    if sign in ("▲", "△", "-", "−"):
        return -v
    if sign in ("+", "＋"):
        return v
    return v - 100.0 if v > 50 else v     # 「112.3%」は水準・「8.2%」は増減として読む


def _nfkc(s: str) -> str:
    import unicodedata
    return unicodedata.normalize("NFKC", s or "").replace("株式会社", "").replace("（株）", "").replace("(株)", "").replace(" ", "").lower()


def rule_verdict(title: str, tags: list[str], name: str = "") -> tuple[int, str, str]:
    """(score −3..+3, label 好材料/悪材料/中立/要精査, reason)。表題の語だけで決める。迷う物は要精査。"""
    t = title or ""
    if "市場変更" in tags:
        return 0, "中立", "市場区分の変更（プライム→スタンダード等）"
    if "訂正" in tags and not any(x in tags for x in ("予想修正", "配当", "決算短信")):
        return 0, "中立", "訂正（数値の訂正でなければ材料ではない）"
    if "事務" in tags:
        return 0, "中立", "事務開示（報酬目的の自己株処分・取得状況など）"
    if "上場廃止" in tags:
        if re.search(r"に向けた計画|計画の進捗|計画に基づく|計画の撤回|計画書", t):
            return 0, "中立", "上場維持基準の計画・進捗（適合ではない）"
        if re.search(r"適合|解除|回復", t):
            return 1, "好材料", "上場維持基準に適合／監理解除"
        return -3, "悪材料", "上場廃止・監理・特設注意"
    if "破産・再生" in tags:
        return -3, "悪材料", "破産・再生・債務超過・継続企業の疑義"
    if "予想修正" in tags:
        if re.search(r"一転.*黒字|黒字転換|黒字に", t):
            return 3, "好材料", "一転黒字の上方修正"
        if re.search(r"一転.*赤字|赤字転落|赤字に", t):
            return -3, "悪材料", "一転赤字の下方修正"
        if "上方修正" in t or re.search(r"赤字縮小|増額", t):
            return 2, "好材料", "業績予想の上方修正"
        if "下方修正" in t or re.search(r"赤字拡大|減額", t):
            return -2, "悪材料", "業績予想の下方修正"
        if "配当" in tags:
            if re.search(r"増配|復配|記念配|特別配当", t):
                return 2, "好材料", "増配・復配"
            if re.search(r"減配|無配", t):
                return -2, "悪材料", "減配・無配"
        return 0, "要精査", "修正の方向が表題から分からない（中身を読む）"
    if "配当" in tags:
        if re.search(r"増配|復配|記念配|特別配当|配当方針の変更.*引き?上げ|累進配当", t):
            return 2, "好材料", "増配・復配・特別配当"
        if re.search(r"減配|無配|見送り", t):
            return -2, "悪材料", "減配・無配"
        if re.search(r"剰余金の配当|配当金支払", t):
            return 0, "中立", "予定どおりの配当決議"
        return 0, "要精査", "配当の変更の方向が表題から分からない"
    if "自社株買い" in tags:
        if "消却" in t:
            return 1, "好材料", "自己株式の消却"
        return 2, "好材料", "自社株買い"
    if "優待" in tags:
        if re.search(r"新設|導入|拡充|変更.*拡充|開始", t):
            return 1, "好材料", "株主優待の新設・拡充"
        if re.search(r"廃止|縮小|中止|変更", t):
            return -1, "悪材料", "株主優待の廃止・縮小・変更"
        return 0, "要精査", "優待の内容次第"
    if "分割・併合" in tags:
        if "分割" in t:
            return 1, "好材料", "株式分割（買いやすくなる）"
        return 0, "中立", "株式併合"
    if "公開買付け" in tags:
        if re.search(r"連結子会社の吸収合併|完全子会社の吸収合併|子会社間の|簡易合併|略式合併|孫会社", t):
            return 0, "中立", "グループ内の再編（簡易合併など）"
        if re.search(r"当社株(式|券)に対する|当社株式等に対する|に対する賛同|賛同の意見|応募を推奨|による当社|当社を完全子会社", t):
            return 3, "好材料", "TOB・完全子会社化の対象（プレミアム）"
        m = re.search(r"による(.+?)(の|を)(完全子会社化|株式交換|公開買付|買収)", t)
        if m and name:
            tgt = _nfkc(m.group(1)); me = _nfkc(name)
            if me and (me[:3] in tgt or tgt[:3] in me):
                return 3, "好材料", "完全子会社化・TOBの対象（プレミアム）"
            return 0, "中立", "買収する側（対象は相手）"
        if re.search(r"結果|終了|買付条件等の変更|撤回|比率の決定", t):
            return 0, "中立", "TOBの結果・経過"
        return 0, "要精査", "TOB・統合の当事者がどちらか中身で確認"
    if "エクイティ" in tags:
        if re.search(r"減資|資本金の額の減少", t):
            return 0, "中立", "減資（税務目的が多い）"
        if re.search(r"結果|完了|払込|失権|行使状況|権利行使", t):
            return 0, "中立", "増資・行使の結果報告"
        if "提携" in tags:
            return 0, "要精査", "資本業務提携に伴う割当（相手と条件次第）"
        return -2, "悪材料", "増資・新株予約権・売出し（希薄化）"
    if "提携" in tags:
        if re.search(r"解消|終了|中止", t):
            return -1, "悪材料", "提携の解消"
        return 1, "好材料", "業務・資本提携"
    if "行政処分" in tags:
        return -2, "悪材料", "行政処分・業務改善命令・課徴金"
    if "訴訟" in tags:
        if re.search(r"勝訴|和解|取下げ|棄却", t):
            return 1, "好材料", "勝訴・和解・取下げ"
        return -1, "悪材料", "訴訟の提起・判決"
    if "事故" in tags:
        return -1, "悪材料", "事故・不正アクセス・不適切事案"
    if "災害" in tags:
        return -1, "悪材料", "災害の被害"
    if "特損" in tags:
        return -1, "悪材料", "特別損失・減損"
    if "特益" in tags:
        return 1, "好材料", "特別利益・資産売却"
    if "月次" in tags:
        p = _pct_in(t)
        if p is not None:
            if p >= 5:
                return 1, "好材料", f"月次 前年比 {p:+.1f}%"
            if p <= -5:
                return -1, "悪材料", f"月次 前年比 {p:+.1f}%"
            return 0, "中立", f"月次 前年比 {p:+.1f}%"
        return 0, "要精査", "月次（数字は中身）"
    if "決算短信" in tags:
        return 0, "要精査", "決算短信（数字は中身・Claude判定）"
    if "経営計画" in tags:
        return 0, "要精査", "中期経営計画（目標の高さは中身）"
    if "IPO" in tags:
        return 0, "中立", "上場関連"
    if "子会社" in tags and re.search(r"設立|取得|買収", t):
        return 0, "要精査", "子会社の設立・取得（規模は中身）"
    if "事業" in tags:
        if re.search(r"大型|大口|過去最大|最大級", t):
            return 1, "好材料", "大型受注・大口契約"
        return 0, "要精査", "事業ニュース（規模は中身）"
    if any(x in ADMIN_TAGS for x in tags):
        return 0, "中立", "事務開示"
    return 0, "中立", "材料性が薄い"


# ── 答え合わせ（価格反応） ────────────────────────────────────────
def _px(data: dict, code: str):
    for k in (code + ".T", code):
        df = data.get(k)
        if df is not None and len(df):
            return df
    return None


def reaction(data: dict, code: str, ymd: date, intraday: bool) -> dict:
    """既出(場中)ならその日の終値の反応、引け後なら翌営業日の反応。データが無い所は None。"""
    import pandas as pd
    df = _px(data, code)
    out = {"day_chg": None, "next_gap": None, "next_chg": None, "next_oc": None, "next_date": None}
    if df is None:
        return out
    try:
        idx = df.index
        d0 = pd.Timestamp(ymd)
        pos = idx.searchsorted(d0)
        if pos < len(idx) and idx[pos] == d0 and pos >= 1:
            c0, cp = float(df["Close"].iloc[pos]), float(df["Close"].iloc[pos - 1])
            if cp > 0:
                out["day_chg"] = round((c0 / cp - 1) * 100, 2)
            base_pos = pos
        else:
            base_pos = pos - 1 if pos >= 1 else None     # 当日の足が無い（休日開示など）→ 直前の足を基準
        if base_pos is not None and base_pos + 1 < len(idx):
            cb = float(df["Close"].iloc[base_pos])
            o1, c1 = float(df["Open"].iloc[base_pos + 1]), float(df["Close"].iloc[base_pos + 1])
            if cb > 0 and o1 > 0:
                out["next_gap"] = round((o1 / cb - 1) * 100, 2)
                out["next_chg"] = round((c1 / cb - 1) * 100, 2)
                out["next_oc"] = round((c1 / o1 - 1) * 100, 2)
                out["next_date"] = idx[base_pos + 1].strftime("%Y-%m-%d")
    except Exception:
        pass
    return out


def _mean(xs: list[float]) -> float | None:
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 2) if xs else None


def _hit(xs: list[float]) -> float | None:
    xs = [x for x in xs if x is not None]
    return round(100.0 * sum(1 for x in xs if x > 0) / len(xs), 1) if xs else None


def summarize(rows: list[dict], key: str) -> dict:
    """key='rule' or 'llm'。好/悪ごとの翌日ギャップ・終値の平均と勝率（引け後開示＝翌日・場中開示＝その日の終値）。"""
    out = {}
    for lab, sign in (("好材料", 1), ("悪材料", -1)):
        sel = [r for r in rows if (r.get(key) or {}).get("label") == lab]
        gaps = [r["react"]["next_gap"] for r in sel if r["react"].get("next_gap") is not None]
        chgs = [r["react"]["next_chg"] for r in sel if r["react"].get("next_chg") is not None]
        ocs = [r["react"]["next_oc"] for r in sel if r["react"].get("next_oc") is not None]
        days = [r["react"]["day_chg"] for r in sel if r["intraday"] and r["react"].get("day_chg") is not None]
        out[lab] = {"n": len(sel), "n_scored": len(gaps),
                    "gap": _mean(gaps), "gap_hit": _hit([g * sign for g in gaps]),
                    "chg": _mean(chgs), "chg_hit": _hit([c * sign for c in chgs]),
                    "oc": _mean(ocs), "oc_hit": _hit([c * sign for c in ocs]),
                    "day": _mean(days), "day_hit": _hit([c * sign for c in days]), "n_day": len(days)}
    return out


# ── Claude 判定（ir_judge.py の結果ファイル）を重ねる ───────────────
def load_judged() -> dict:
    try:
        with open(JUDGED_PATH, encoding="utf-8") as f:
            j = json.load(f)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


# ── 本体 ──────────────────────────────────────────────────────────
def build_ir(data: dict, name_map: dict, data_date: str, sec_of: dict | None = None, today: date | None = None,
             judge_inline: bool | None = None) -> dict:
    t0 = time.monotonic()
    sec_of = sec_of or {}
    today = today or datetime.now(JST).date()
    # 画面の窓: 今日（営業日なら）とその前。データ日(data_date)より未来の開示も当日分として出す
    days: list[date] = []
    d = today if is_trading_day(today) else prev_trading_day(today)
    while len(days) < max(WINDOW_DAYS, STATS_DAYS):
        days.append(d)
        d = prev_trading_day(d)
    window = days[:WINDOW_DAYS]
    fetched: dict[str, list[dict] | None] = {}
    n_pages_fail = 0
    for dd in days:
        if time.monotonic() - t0 > DEADLINE_SEC:
            print(f"  [ir] {DEADLINE_SEC:.0f}秒超過 → {dd} 以前は取らない")
            break
        ymd = dd.strftime("%Y%m%d")
        rows = fetch_tdnet_day(ymd)
        fetched[dd.isoformat()] = rows
        if rows is None:
            n_pages_fail += 1
            if dd in window and n_pages_fail >= 2:
                break
    judged = load_judged()
    jmap = judged.get("items") or {}
    rows_all: list[dict] = []
    for dkey, lst in fetched.items():
        if not lst:
            continue
        dd = date.fromisoformat(dkey)
        for r in lst:
            code = r["code"]
            if not re.match(r"^[0-9][0-9A-Z]{3}$", code):
                continue
            tags = tag_title(r["title"])
            score, label, reason = rule_verdict(r["title"], tags, r["name"])
            intraday = bool(r["time"]) and r["time"] < SESSION_END   # 15:30ちょうどは大引け後＝翌日で答え合わせ(2026-10-17 本人「好材料なのに下がる」→313件が15:30開示だった)
            react = reaction(data, code, dd, intraday)
            j = jmap.get(r["id"])
            llm = None
            if j:
                llm = {"label": j.get("label"), "score": j.get("score"), "reason": j.get("reason"), "priced": j.get("priced"),
                       "conf": j.get("conf"), "model": j.get("model"), "facts": j.get("facts")}
            df = _px(data, code)
            close = float(df["Close"].iloc[-1]) if df is not None and len(df) else None
            tov = None
            if df is not None and len(df) >= 20:
                try:
                    tov = round(float((df["Close"] * df["Volume"]).tail(20).median()) / 1e8, 1)
                except Exception:
                    tov = None
            rows_all.append({
                "id": r["id"], "date": dkey, "time": r["time"], "intraday": intraday, "code": code, "name": r["name"] or name_map.get(code, code),
                "sector": sec_of.get(code + ".T") or sec_of.get(code) or "", "title": r["title"], "pdf": r["pdf"], "xbrl": r["xbrl"],
                "tags": tags, "material": any(t in MATERIAL_TAGS for t in tags), "admin": all(t in ADMIN_TAGS or t in ("その他", "事務", "訂正") for t in tags),
                "rule": {"label": label, "score": score, "reason": reason}, "llm": llm,
                "final": (llm if llm and llm.get("label") in ("好材料", "悪材料", "中立") else {"label": label, "score": score, "reason": reason, "model": "rule"}),
                "react": react, "close": close, "tov20_oku": tov,
            })
    # 任意: この場で Claude 判定を走らせる（キーがある時だけ・ir_judge.judge_rows が結果ファイルを更新）
    llm_meta = {"available": False, "judged": 0, "model": None, "note": ""}
    want_inline = (os.environ.get("KABUAI_IR_LLM", "1") == "1") if judge_inline is None else judge_inline
    if want_inline and os.environ.get("ANTHROPIC_API_KEY"):
        try:
            import ir_judge
            n, model, note = ir_judge.judge_rows([r for r in rows_all if r["date"] in {w.isoformat() for w in window}], data)
            llm_meta.update({"available": True, "judged": n, "model": model, "note": note})
            jmap = (load_judged().get("items") or {})
            for r in rows_all:
                j = jmap.get(r["id"])
                if j:
                    r["llm"] = {"label": j.get("label"), "score": j.get("score"), "reason": j.get("reason"), "priced": j.get("priced"),
                                "conf": j.get("conf"), "model": j.get("model"), "facts": j.get("facts")}
                    if r["llm"]["label"] in ("好材料", "悪材料", "中立"):
                        r["final"] = dict(r["llm"])
        except Exception as e:
            llm_meta["note"] = f"Claude判定でエラー: {e}"[:200]
            print(f"  [ir] Claude判定スキップ: {e}")
    elif jmap:
        llm_meta.update({"available": True, "judged": sum(1 for r in rows_all if r.get("llm")), "model": judged.get("model"), "note": f"結果ファイル {judged.get('updated_at', '')}"})
    else:
        llm_meta["note"] = "ANTHROPIC_API_KEY が無いのでルール判定だけ（Claude判定は ir_judge.py）"

    rows_all.sort(key=lambda r: (r["date"], r["time"], r["code"]), reverse=True)
    wkeys = {w.isoformat() for w in window}
    today_key = window[0].isoformat() if window else ""
    # 画面に渡すのは 当日=全件、それ以前（既出）=好/悪/要精査だけ（中立・事務は落として JSON を軽くする）
    # 🔍開示検索（v7.6・2026-10-17）: 全開示の PDF 本文冒頭を取って data/ir_text/<日付>.json に。latest.json には入れない。非致命。
    search_meta = None
    try:
        import ir_text
        search_meta = ir_text.build_search(rows_all)
        run = search_meta.get("run") or {}
        print(f"  [ir] 開示検索: {search_meta['n']}件中 本文あり{search_meta['n_text']}（今回 取得{run.get('ok')}/失敗{run.get('ng')}/{run.get('elapsed_s')}s・未取得{search_meta['pending']}）")
    except Exception as e:
        print(f"  [ir] 開示検索はスキップ（非致命）: {e}")
        search_meta = {"error": str(e)[:200]}
    rows_win = [r for r in rows_all if r["date"] == today_key or (r["date"] in wkeys and r["final"]["label"] != "中立")]
    for r in rows_win:
        r["title"] = r["title"][:140]
        r.pop("xbrl", None)
    tags_count: dict[str, int] = {}
    for r in rows_win:
        for t in r["tags"]:
            tags_count[t] = tags_count.get(t, 0) + 1
    stats_days = [k for k, v in fetched.items() if v]
    stats = {"days": len(stats_days), "from": min(stats_days) if stats_days else None, "to": max(stats_days) if stats_days else None,
             "rule": summarize(rows_all, "rule"), "llm": summarize([r for r in rows_all if r.get("llm")], "llm")}
    n_win = len(rows_win)
    print(f"  [ir] 窓{len(window)}日 {n_win}件（材料{sum(1 for r in rows_win if r['material'])}・好{sum(1 for r in rows_win if r['final']['label']=='好材料')}・"
          f"悪{sum(1 for r in rows_win if r['final']['label']=='悪材料')}・要精査{sum(1 for r in rows_win if r['final']['label']=='要精査')}）"
          f"／答え合わせ{stats['days']}日 {len(rows_all)}件・Claude判定{llm_meta['judged']} {time.monotonic()-t0:.0f}s")
    # 株探 人気テーマ【ベスト30】（kabutan_hot_pc.py が PC から取って main に置く kabuai/kabutan_hot.json・2026-10-17 本人「株たんみながら人気テーマを取得」）
    hot = None
    try:
        with open(HERE / "kabutan_hot.json", encoding="utf-8") as f:
            hj = json.load(f)
        if isinstance(hj, dict) and isinstance(hj.get("themes"), list) and hj["themes"]:
            hot = {"fetched_at": hj.get("fetched_at"), "page_time": hj.get("page_time"), "period": hj.get("period"), "src": hj.get("src"),
                   "themes": [{"rank": t.get("rank"), "name": t.get("name"), "stocks": (t.get("stocks") or [])[:3]} for t in hj["themes"][:30]]}
            print(f"  [ir] 株探 人気テーマ {len(hot['themes'])}件（{hot['page_time']}）")
    except FileNotFoundError:
        print("  [ir] 株探 人気テーマ: kabutan_hot.json が無い（PC の kabutan_hot_pc.py が置く）")
    except Exception as e:
        print(f"  [ir] 株探 人気テーマ 読込失敗: {e}")
    return {
        "date": data_date, "generated_at": datetime.now(JST).strftime("%Y-%m-%d %H:%M"), "window": [w.isoformat() for w in window],
        "hot_themes": hot,
        "fetched": {k: (len(v) if v is not None else None) for k, v in fetched.items()},
        "rows": rows_win, "tags_count": tags_count, "stats": stats, "llm": llm_meta, "search": search_meta,
        "basis": "brisk（timely-disclosure.brisk.jp）のタグ体系を基準に、元データは TDnet を直接。brisk のデータは載せない（規約: 加工・第三者提供の禁止）。",
        "rules": [
            "好/悪はまず表題の語で（上方修正・増配・自社株買い・TOB対象＝好／下方修正・減配・増資・行政処分・上場廃止＝悪）",
            "決算短信・月次・中計・資本提携の割当など方向が表題で決まらない物は🔍要精査＝Claudeが中身（PDF冒頭）を読んで判定",
            f"{SESSION_END}より前の開示＝📌既出（その日の終値の反応＝開示前の値動きも含む）／引け後の開示＝翌営業日の寄りギャップと終値で答え合わせ",
            "上がる/下がるの判定であって、翌日買えば儲かるという意味ではない（答え合わせの数字が全て）",
        ],
    }


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(HERE))
    ymd = sys.argv[1] if len(sys.argv) > 1 else datetime.now(JST).strftime("%Y%m%d")
    rows = fetch_tdnet_day(ymd) or []
    print(f"{ymd}: {len(rows)}件")
    from collections import Counter
    c = Counter()
    for r in rows:
        tags = tag_title(r["title"]); s, lab, why = rule_verdict(r["title"], tags, r["name"])
        c[lab] += 1
        if lab != "中立":
            print(f"  {r['time']} {r['code']} {r['name'][:10]:10s} {lab} {s:+d} [{'/'.join(tags)}] {r['title'][:60]} ← {why}")
    print(dict(c))
