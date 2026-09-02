# -*- coding: utf-8 -*-
"""立花 e支店API の「その日しか取れない」情報を毎日スナップショット保存して履歴化する。

J-Quants には無い（＝今まで取れなかった）もの:
  - 日証金 貸借取引残高（日次）: 融資残/貸株残/差引残/貸借倍率/回転日数/新規・返済/前日比/速報・確報
  - 逆日歩（日次・品貸料）
  - 銘柄別規制（制度・一般の売建/買建 停止区分、一極集中、即日入金規制）と 増担保の保証金率
  - 銘柄詳細（予想PER/PBR/ROE/配当利回り/年初来高安と更新日/権利落日）
  - 前日終値・値幅制限・貸借区分（銘柄市場マスタ）
API は当日値しか返さないので、毎日貯めて初めてBTに使える。→ tachibana_snapshots/YYYY-MM-DD_{am|pm}.pkl

実行: python -X utf8 tachibana_daily_snapshot.py [--slot am|pm] [--limit N]
      1回 約130要求・2分弱（3,750銘柄を120銘柄ずつ）。18:25(pm)=当日速報、11:30(am)=確報＋逆日歩（Windowsタスク TachibanaSnapshot_am/_pm）。
読込: from tachibana_daily_snapshot import load_panel; p = load_panel("syoukin")  # 日付×銘柄のロング形式
"""
from __future__ import annotations

import argparse
import logging
import os
import pickle
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
from tachibana import TachibanaClient, TachibanaError  # noqa: E402

OUT_DIR = ROOT / "tachibana_snapshots"
LOG = ROOT / "logs" / "tachibana_snapshot.log"

SYOUKIN_COLS = {"pSFD": "証金更新日", "pSFKS": "速報確報", "pSFF6": "融資残", "pSFG6": "融資前日比", "pSFL6": "融資新規",
                "pSFP6": "融資返済", "pSFS6": "貸株残", "pSSG6": "貸株前日比", "pSSL6": "貸株新規", "pSSP6": "貸株返済",
                "pSFN6": "差引残", "pSFC6": "差引残前日比", "pSFR6": "貸借倍率", "pSFD6": "回転日数"}
SHINYOU_COLS = {"pMBD": "信用残日付", "pMBBQ": "買残", "pMBSQ": "売残", "pMBRQ": "信用倍率", "pMBNQ": "買残前週比", "pMBCQ": "売残前週比",
                "pMBB6": "買残制度", "pMBS6": "売残制度", "pMBR6": "信用倍率制度", "pMBB3": "買残一般", "pMBS3": "売残一般", "pMBR3": "信用倍率一般"}
DETAIL_COLS = {"pRPER": "予想PER", "pSPBR": "PBR", "pROEL": "予想ROE", "pSYIE": "予想配当利回り", "pEPSF": "予想EPS", "pBPSB": "BPS",
               "pSPRO": "益回り", "pYHPR": "年初来高値", "pYHPD": "年初来高値日", "pYLPR": "年初来安値", "pYLPD": "年初来安値日",
               "pCLOE": "本決算権利落日", "pIDVE": "中間権利落日", "pEXRD": "最終落日"}


def universe_codes() -> list[str]:
    d = pickle.load(open(ROOT / "jquants_cache.pkl", "rb"))
    return [t.removesuffix(".T") for t, _ in d["universe"]]


def _num(df: pd.DataFrame, skip: tuple[str, ...] = ()) -> pd.DataFrame:
    for c in df.columns:
        if c in skip or c == "code":
            continue
        if not pd.api.types.is_numeric_dtype(df[c]):
            raw = df[c].astype(str).str.replace(",", "")
            conv = pd.to_numeric(raw, errors="coerce")
            nonempty = int((raw != "").sum())
            if nonempty and conv.notna().sum() >= 0.5 * nonempty:
                df[c] = conv
    return df


def take_snapshot(slot: str, limit: int = 0, log=print) -> Path:
    codes = universe_codes()
    if limit:
        codes = codes[:limit]
    tc = TachibanaClient()
    tc.MIN_INTERVAL = 0.5
    tc.ensure_session()
    t0 = time.time()
    snap: dict = {"taken_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "slot": slot, "date_info": tc.date_info()}

    def frame(rows, colmap, drop_empty=True):
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.rename(columns={"sIssueCode": "code", **colmap})
        keep = ["code"] + [v for v in colmap.values() if v in df.columns]
        df = df[keep]
        if drop_empty:
            df = df[(df.drop(columns="code").astype(str) != "").any(axis=1)]
        return _num(df.reset_index(drop=True))

    snap["syoukin"] = frame(tc.syoukin_zan(codes), SYOUKIN_COLS)
    log(f"  証金残 {len(snap['syoukin'])}件 ({time.time()-t0:.0f}s)")
    snap["hibu"] = frame(tc.hibu_info(codes), {"pBWRQ": "逆日歩"})
    log(f"  逆日歩 {len(snap['hibu'])}件（付いている銘柄のみ） ({time.time()-t0:.0f}s)")
    snap["shinyou"] = frame(tc.shinyou_zan(codes), SHINYOU_COLS)
    log(f"  信用残(週次) {len(snap['shinyou'])}件 ({time.time()-t0:.0f}s)")
    snap["detail"] = frame(tc.issue_detail(codes), DETAIL_COLS)
    log(f"  銘柄詳細 {len(snap['detail'])}件 ({time.time()-t0:.0f}s)")

    reg = pd.DataFrame(tc.regulation_master())
    if not reg.empty:
        reg = reg.rename(columns={"sIssueCode": "code"})
    snap["regulation"] = reg
    hm = pd.DataFrame(tc.hosyoukin_master())
    if not hm.empty:
        hm = _num(hm.rename(columns={"sIssueCode": "code", "sHenkouDay": "変更日", "sDaiyoHosyokinRitu": "代用保証金率", "sGenkinHosyokinRitu": "現金保証金率"}), skip=("変更日",))
    snap["hosyoukin"] = hm
    mk = pd.DataFrame(tc.issue_market_master())
    if not mk.empty:
        mk = mk.rename(columns={"sIssueCode": "code", "sSinyouC": "信用C", "sZenzituOwarine": "前日終値", "sNehabaMin": "値幅下限",
                                "sNehabaMax": "値幅上限", "sZyouzyouKubun": "上場区分", "sIssueKubunC": "銘柄区分"})
        mk = _num(mk[[c for c in ("code", "信用C", "前日終値", "値幅下限", "値幅上限", "上場区分", "銘柄区分") if c in mk.columns]], skip=("信用C", "上場区分", "銘柄区分"))
    snap["market"] = mk
    log(f"  規制 {len(reg)} / 増担保等 {len(hm)} / 市場マスタ {len(mk)} ({time.time()-t0:.0f}s)")

    OUT_DIR.mkdir(exist_ok=True)
    day = snap["date_info"].get("sTheDay") or datetime.now().strftime("%Y%m%d")
    out = OUT_DIR / f"{day[:4]}-{day[4:6]}-{day[6:]}_{slot}.pkl"
    pickle.dump(snap, open(out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    log(f"保存 {out} ({out.stat().st_size/1e6:.1f} MB)")
    return out


def load_panel(table: str = "syoukin", slot: str | None = None) -> pd.DataFrame:
    """貯めたスナップショットを縦持ちに結合（列 snap_date, slot, code, ...）。"""
    frames = []
    for p in sorted(OUT_DIR.glob("*.pkl")):
        d, s = p.stem.split("_")
        if slot and s != slot:
            continue
        snap = pickle.load(open(p, "rb"))
        df = snap.get(table)
        if df is None or len(df) == 0:
            continue
        df = df.copy()
        df.insert(0, "slot", s)
        df.insert(0, "snap_date", d)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slot", default="pm" if datetime.now().hour >= 12 else "am")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    LOG.parent.mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(LOG, encoding="utf-8"), logging.StreamHandler(sys.stdout)])
    log = logging.getLogger("snapshot").info
    log(f"=== snapshot start slot={a.slot}")
    try:
        take_snapshot(a.slot, a.limit, log)
    except TachibanaError as e:
        logging.getLogger("snapshot").error(f"失敗: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
