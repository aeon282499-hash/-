"""
競馬データスクレイパー (netkeiba.com)

取得データ:
  - レース結果 (着順・タイム・着差・上がり3F)
  - 馬・騎手・調教師の基本情報
  - 各馬の過去5走成績
  - オッズ (単勝・複勝・馬連等)
"""

import re
import time
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup

from keiba_ai.config import (
    NETKEIBA_BASE, REQUEST_INTERVAL, REQUEST_TIMEOUT,
    USER_AGENT, DATA_DIR,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP セッション
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


_SESSION = _make_session()


def _get(url: str, params: dict | None = None) -> BeautifulSoup:
    """GETして BeautifulSoup を返す。レート制限・リトライ付き。"""
    for attempt in range(3):
        try:
            resp = _SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            time.sleep(REQUEST_INTERVAL)
            return BeautifulSoup(resp.content, "html.parser")
        except requests.RequestException as e:
            logger.warning(f"HTTP error ({attempt+1}/3): {e}")
            time.sleep(REQUEST_INTERVAL * (attempt + 1))
    raise RuntimeError(f"Failed to fetch: {url}")


# ---------------------------------------------------------------------------
# レース一覧取得
# ---------------------------------------------------------------------------

def get_race_id_list(year: int, venue_code: str, kai: int, day: int) -> list[str]:
    """
    指定開催のレースIDリストを返す。
    レースID形式: YYYYVVKKDDРР (12桁)
      YYYY=年, VV=場コード, KK=回, DD=日, RR=レース番号
    """
    ids = []
    for race_no in range(1, 13):
        rid = f"{year}{venue_code:0>2}{kai:0>2}{day:0>2}{race_no:0>2}"
        ids.append(rid)
    return ids


def get_race_list_from_calendar(target_date: date) -> list[str]:
    """
    指定日のネットkeiba開催カレンダーからレースIDを収集する。
    """
    url = f"{NETKEIBA_BASE}/race/list/{target_date.strftime('%Y%m%d')}/"
    try:
        soup = _get(url)
    except RuntimeError:
        return []

    ids = []
    for a in soup.find_all("a", href=re.compile(r"/race/\d{12}/")):
        rid = re.search(r"(\d{12})", a["href"])
        if rid:
            ids.append(rid.group(1))
    return list(set(ids))


# ---------------------------------------------------------------------------
# レース結果スクレイピング
# ---------------------------------------------------------------------------

def scrape_race_result(race_id: str) -> Optional[pd.DataFrame]:
    """
    1レースの着順結果を DataFrame で返す。

    返却カラム:
      race_id, order, frame_no, horse_no, horse_name, horse_id,
      sex_age, weight_carried, jockey, time, margin, last3f,
      horse_weight, horse_weight_diff, odds, fav_rank,
      trainer, owner
    """
    url = f"{NETKEIBA_BASE}/race/{race_id}/"
    try:
        soup = _get(url)
    except RuntimeError:
        logger.error(f"Failed to fetch race: {race_id}")
        return None

    # レース情報
    race_info = _parse_race_info(soup, race_id)

    # 着順テーブル
    table = soup.find("table", class_="race_table_01")
    if table is None:
        logger.warning(f"No result table: {race_id}")
        return None

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 18:
            continue
        try:
            row = {
                "race_id": race_id,
                "order": _safe_int(tds[0].text),
                "frame_no": _safe_int(tds[1].text),
                "horse_no": _safe_int(tds[2].text),
                "horse_name": tds[3].text.strip(),
                "horse_id": _extract_id(tds[3], "horse"),
                "sex_age": tds[4].text.strip(),
                "weight_carried": _safe_float(tds[5].text),
                "jockey": tds[6].text.strip(),
                "jockey_id": _extract_id(tds[6], "jockey"),
                "time": _parse_time(tds[7].text),
                "margin": tds[8].text.strip(),
                "fav_rank": _safe_int(tds[9].text),
                "odds": _safe_float(tds[10].text),
                "last3f": _safe_float(tds[11].text),
                "horse_weight": _parse_horse_weight(tds[14].text)[0],
                "horse_weight_diff": _parse_horse_weight(tds[14].text)[1],
                "trainer": tds[17].text.strip() if len(tds) > 17 else "",
                "trainer_id": _extract_id(tds[17], "trainer") if len(tds) > 17 else "",
            }
            row.update(race_info)
            rows.append(row)
        except Exception as e:
            logger.debug(f"Row parse error ({race_id}): {e}")
            continue

    if not rows:
        return None
    return pd.DataFrame(rows)


def _parse_race_info(soup: BeautifulSoup, race_id: str) -> dict:
    info = {
        "year": int(race_id[:4]),
        "venue_code": race_id[4:6],
        "race_no": int(race_id[10:12]),
        "race_name": "",
        "course_type": "",   # 芝 or ダート or 障害
        "distance": 0,
        "direction": "",     # 右 or 左 or 直線
        "weather": "",
        "track_cond": "",    # 良・稍重・重・不良
        "race_date": "",
        "race_class": "",
        "head_count": 0,
    }

    try:
        title = soup.find("div", class_="race_name")
        if title:
            info["race_name"] = title.text.strip()

        data_intro = soup.find("div", class_="data_intro")
        if data_intro:
            spans = data_intro.find_all("span")
            for span in spans:
                text = span.text.strip()
                m = re.search(r"([芝ダ障])(\d+)m", text)
                if m:
                    info["course_type"] = "芝" if m.group(1) == "芝" else \
                                          "障害" if m.group(1) == "障" else "ダート"
                    info["distance"] = int(m.group(2))
                m2 = re.search(r"(右|左|直線)", text)
                if m2:
                    info["direction"] = m2.group(1)

        p_info = soup.find("p", class_="smalltxt")
        if p_info:
            text = p_info.text
            m_date = re.search(r"(\d{4})年(\d+)月(\d+)日", text)
            if m_date:
                info["race_date"] = f"{m_date.group(1)}-{int(m_date.group(2)):02d}-{int(m_date.group(3)):02d}"
            for cond in ["良", "稍重", "重", "不良"]:
                if cond in text:
                    info["track_cond"] = cond
                    break
            for weather in ["晴", "曇", "雨", "雪", "小雪", "小雨"]:
                if weather in text:
                    info["weather"] = weather
                    break

        # 頭数
        result_table = soup.find("table", class_="race_table_01")
        if result_table:
            rows = result_table.find_all("tr")[1:]
            info["head_count"] = len([r for r in rows if len(r.find_all("td")) > 5])
    except Exception as e:
        logger.debug(f"Race info parse error: {e}")

    return info


# ---------------------------------------------------------------------------
# 馬の過去成績
# ---------------------------------------------------------------------------

def scrape_horse_history(horse_id: str, limit: int = 10) -> Optional[pd.DataFrame]:
    """
    馬の過去レース成績を取得する (直近 limit 走)。
    """
    url = f"{NETKEIBA_BASE}/horse/{horse_id}/"
    try:
        soup = _get(url)
    except RuntimeError:
        return None

    table = soup.find("table", class_="db_h_race_results")
    if table is None:
        return None

    rows = []
    for tr in table.find_all("tr")[1:limit + 1]:
        tds = tr.find_all("td")
        if len(tds) < 20:
            continue
        try:
            row = {
                "horse_id": horse_id,
                "date": tds[0].text.strip(),
                "venue": tds[1].text.strip(),
                "weather": tds[2].text.strip(),
                "race_no_hist": _safe_int(tds[3].text),
                "race_name_hist": tds[4].text.strip(),
                "head_count_hist": _safe_int(tds[6].text),
                "frame_no_hist": _safe_int(tds[7].text),
                "horse_no_hist": _safe_int(tds[8].text),
                "odds_hist": _safe_float(tds[9].text),
                "fav_rank_hist": _safe_int(tds[10].text),
                "order_hist": _safe_int(tds[11].text),
                "jockey_hist": tds[12].text.strip(),
                "weight_carried_hist": _safe_float(tds[13].text),
                "distance_hist": _parse_distance(tds[14].text),
                "course_type_hist": _parse_course_type(tds[14].text),
                "track_cond_hist": tds[15].text.strip(),
                "time_hist": _parse_time(tds[17].text),
                "margin_hist": tds[18].text.strip(),
                "last3f_hist": _safe_float(tds[20].text) if len(tds) > 20 else None,
                "horse_weight_hist": _parse_horse_weight(tds[22].text)[0] if len(tds) > 22 else None,
            }
            rows.append(row)
        except Exception:
            continue

    return pd.DataFrame(rows) if rows else None


# ---------------------------------------------------------------------------
# オッズ取得
# ---------------------------------------------------------------------------

def scrape_odds(race_id: str) -> dict:
    """
    単勝・複勝オッズを取得して dict で返す。
    {horse_no: {"win": float, "place_min": float, "place_max": float}}
    """
    url = f"{NETKEIBA_BASE}/odds/{race_id}/"
    try:
        soup = _get(url)
    except RuntimeError:
        return {}

    result = {}
    table = soup.find("table", id="odds_tan_table")
    if table:
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            horse_no = _safe_int(tds[1].text)
            win = _safe_float(tds[4].text)
            if horse_no:
                result[horse_no] = {"win": win, "place_min": None, "place_max": None}

    # 複勝
    table_fuku = soup.find("table", id="odds_fuku_table")
    if table_fuku:
        for tr in table_fuku.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 6:
                continue
            horse_no = _safe_int(tds[1].text)
            txt = tds[4].text.strip()
            parts = txt.split(" - ")
            if horse_no and horse_no in result:
                result[horse_no]["place_min"] = _safe_float(parts[0]) if parts else None
                result[horse_no]["place_max"] = _safe_float(parts[1]) if len(parts) > 1 else None

    return result


# ---------------------------------------------------------------------------
# 騎手・調教師成績
# ---------------------------------------------------------------------------

def scrape_jockey_stats(jockey_id: str, year: int) -> dict:
    """年間騎手成績を取得。"""
    url = f"{NETKEIBA_BASE}/jockey/result/{jockey_id}/?year={year}"
    try:
        soup = _get(url)
    except RuntimeError:
        return {}

    return _parse_stats_table(soup)


def scrape_trainer_stats(trainer_id: str, year: int) -> dict:
    """年間調教師成績を取得。"""
    url = f"{NETKEIBA_BASE}/trainer/result/{trainer_id}/?year={year}"
    try:
        soup = _get(url)
    except RuntimeError:
        return {}

    return _parse_stats_table(soup)


def _parse_stats_table(soup: BeautifulSoup) -> dict:
    table = soup.find("table", class_="race_table_01")
    if not table:
        return {}
    rows = table.find_all("tr")
    # 通常1行目がヘッダ、2行目が通算
    if len(rows) < 2:
        return {}
    tds = rows[1].find_all("td")
    if len(tds) < 10:
        return {}
    try:
        wins = _safe_int(tds[1].text)
        seconds = _safe_int(tds[2].text)
        thirds = _safe_int(tds[3].text)
        total = _safe_int(tds[4].text)
        win_rate = wins / total if total else 0
        top3_rate = (wins + seconds + thirds) / total if total else 0
        return {
            "wins": wins,
            "seconds": seconds,
            "thirds": thirds,
            "total_races": total,
            "win_rate": win_rate,
            "top3_rate": top3_rate,
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# バルクデータ収集 (期間指定)
# ---------------------------------------------------------------------------

def collect_race_results(
    start_date: date,
    end_date: date,
    save_path: Path | None = None,
    jra_only: bool = True,
) -> pd.DataFrame:
    """
    期間内の全レース結果を収集して DataFrame で返す。
    save_path が指定されれば CSV に保存する。
    """
    from datetime import timedelta

    all_rows = []
    current = start_date
    while current <= end_date:
        logger.info(f"Collecting: {current}")
        ids = get_race_list_from_calendar(current)
        if jra_only:
            ids = [r for r in ids if r[4:6] in ("01","02","03","04","05","06","07","08","09","10")]
        for rid in ids:
            df = scrape_race_result(rid)
            if df is not None:
                all_rows.append(df)
        current += timedelta(days=1)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    if save_path:
        result.to_csv(save_path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved {len(result)} rows → {save_path}")
    return result


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _safe_int(text: str) -> Optional[int]:
    try:
        return int(re.sub(r"[^\d]", "", text))
    except Exception:
        return None


def _safe_float(text: str) -> Optional[float]:
    try:
        clean = re.sub(r"[^\d.]", "", text.strip())
        return float(clean) if clean else None
    except Exception:
        return None


def _parse_time(text: str) -> Optional[float]:
    """'1:23.4' → 83.4 (秒)"""
    text = text.strip()
    m = re.match(r"(\d+):(\d+)\.(\d+)", text)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2)) + int(m.group(3)) / 10
    m2 = re.match(r"(\d+)\.(\d+)", text)
    if m2:
        return float(f"{m2.group(1)}.{m2.group(2)}")
    return None


def _parse_horse_weight(text: str) -> tuple[Optional[int], Optional[int]]:
    """'480(+2)' → (480, 2)"""
    m = re.match(r"(\d+)\(([+-]?\d+)\)", text.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.match(r"(\d+)", text.strip())
    if m2:
        return int(m2.group(1)), None
    return None, None


def _parse_distance(text: str) -> Optional[int]:
    m = re.search(r"(\d{3,4})", text)
    return int(m.group(1)) if m else None


def _parse_course_type(text: str) -> str:
    if "芝" in text:
        return "芝"
    if "ダ" in text or "ダート" in text:
        return "ダート"
    return "障害"


def _extract_id(tag, id_type: str) -> str:
    a = tag.find("a", href=re.compile(f"/{id_type}/"))
    if a:
        m = re.search(rf"/{id_type}/(\w+)/", a["href"])
        if m:
            return m.group(1)
    return ""
