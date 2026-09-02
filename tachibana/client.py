# -*- coding: utf-8 -*-
"""立花証券 e支店・API (v4r10) クライアント本体。

仕様の要点（公式リファレンス + 公式サンプル e_api_login_pubkey.py 準拠）:
- 要求は「URL?{JSON}」形式。JSONに共通項目 p_no / p_sd_date / sCLMID / sJsonOfmt を含める。
- 認証は公開鍵方式。ログイン応答の仮想URL(sUrlRequest/Master/Price/Event/EventWebSocket)は
  利用設定画面で登録した公開鍵で暗号化されて返るので、対の秘密鍵で RSA-OAEP(SHA-256) 復号する。
- 応答は Shift_JIS。共通応答項目 p_errno(0=正常, 2=仮想URL無効, -2=パラメータ誤り, 9=サービス停止中)。
- 業務応答は sResultCode(0=正常) / sResultText、警告は sWarningCode / sWarningText。
- 仮想URLは「1日券」（夜間の閉局まで有効）。同日中は .tachibana/session.json を再利用する。
- p_no はログインで 1 に戻し、以降は要求ごとに +1（.tachibana/p_no.json に永続化）。

安全:
- 秘密鍵・認証ID・セッション・第二暗証番号はすべて .tachibana/ 配下（gitignore済み）か環境変数。
- 注文系メソッドは allow_orders=True（または環境変数 TACHIBANA_ALLOW_ORDERS=1）が無いと例外で止める。
"""
from __future__ import annotations

import base64
import datetime as _dt
import json
import logging
import os
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("tachibana")

JST = ZoneInfo("Asia/Tokyo")
PROD_BASE = "https://kabuka.e-shiten.jp/e_api_v4r10/"
DEMO_BASE = "https://demo-kabuka.e-shiten.jp/e_api_v4r10/"
DEFAULT_STATE_DIR = Path(__file__).resolve().parent.parent / ".tachibana"

# 時価情報問合取得(CLMMfdsGetMarketPrice)で指定できる情報コード（公式サンプルより）
PRICE_COLUMNS: dict[str, str] = {
    "pDPP": "現在値", "tDPP:T": "現在値時刻", "pDPG": "現値前値比較",
    "pDYWP": "前日比", "pDYRP": "騰落率",
    "pDOP": "始値", "tDOP:T": "始値時刻", "pDHP": "高値", "tDHP:T": "高値時刻",
    "pDLP": "安値", "tDLP:T": "安値時刻", "pDV": "出来高", "pDJ": "売買代金",
    "pQAS": "売気配値種類", "pQAP": "売気配値", "pAV": "売気配数量",
    "pQBS": "買気配値種類", "pQBP": "買気配値", "pBV": "買気配数量",
    "pAAV": "売数量(成行)", "pABV": "買数量(成行)", "pQOV": "売-OVER", "pQUV": "買-UNDER",
    "xDVES": "配当落銘柄区分", "xDCFS": "不連続要因銘柄区分",
    "pDHF": "日通し高値フラグ", "pDLF": "日通し安値フラグ",
    "pVWAP": "VWAP", "pPRP": "前日終値",
}
for _i in range(1, 11):
    PRICE_COLUMNS[f"pGAV{_i}"] = f"売-{_i}-数量"
    PRICE_COLUMNS[f"pGAP{_i}"] = f"売-{_i}-値段"
    PRICE_COLUMNS[f"pGBV{_i}"] = f"買-{_i}-数量"
    PRICE_COLUMNS[f"pGBP{_i}"] = f"買-{_i}-値段"

DEFAULT_PRICE_COLUMNS = ("pDPP", "tDPP:T", "pDYWP", "pDYRP", "pDOP", "pDHP", "pDLP",
                         "pDV", "pDJ", "pQAP", "pAV", "pQBP", "pBV", "pVWAP", "pPRP")
BOARD_COLUMNS = tuple(
    ["pQOV"] + [f"pGA{k}{i}" for i in range(10, 0, -1) for k in ("P", "V")]
    + [f"pGB{k}{i}" for i in range(1, 11) for k in ("P", "V")] + ["pQUV"]
)

# 停止区分（CLMStkGetIssueSizyouKiseiKabu.sTeisiKubun）
TEISI_KUBUN = {"0": "通常", "1": "取引禁止", "2": "成行禁止", "3": "端株禁止", "": "通常"}
SINYOU_SYUTYU = {"0": "なし", "1": "あり", "2": "日々公表銘柄", "": "-"}
SINYOU_C = {"1": "貸借銘柄", "2": "信用制度銘柄", "3": "一般信用銘柄", "": "-"}

# 仮想URLの振り分け
_MASTER_CLMIDS = {
    "CLMMfdsGetNews", "CLMMfdsGetIssueDetail", "CLMMfdsGetSyoukinZan",
    "CLMMfdsGetShinyouZan", "CLMMfdsGetHibuInfo",
}
_PRICE_CLMIDS = {"CLMMfdsGetMarketPrice", "CLMMfdsGetMarketPriceHistory"}
_ORDER_CLMIDS = {"CLMKabuNewOrder", "CLMKabuCorrectOrder", "CLMKabuCancelOrder", "CLMKabuCancelOrderAll"}
MAX_CODES_PER_REQUEST = 120


class TachibanaError(Exception):
    """立花APIクライアントの基底例外。"""


class TachibanaTransportError(TachibanaError):
    """HTTP到達不可・タイムアウト・JSONでない応答。"""


class TachibanaProtocolError(TachibanaError):
    """共通応答 p_errno != 0。"""

    def __init__(self, p_errno: str, p_err: str, response: dict | None = None):
        self.p_errno, self.p_err, self.response = p_errno, p_err, response or {}
        super().__init__(f"p_errno={p_errno} p_err={p_err}")


class TachibanaSessionError(TachibanaProtocolError):
    """仮想URL（1日券）が無効。再ログインが必要。"""


class TachibanaResultError(TachibanaError):
    """業務応答 sResultCode != 0。"""

    def __init__(self, code: str, text: str, response: dict | None = None):
        self.code, self.text, self.response = code, text, response or {}
        super().__init__(f"sResultCode={code} {text}")


# ----------------------------------------------------------------------------
# 復号
# ----------------------------------------------------------------------------
def load_private_key(pem_or_der: bytes):
    from cryptography.hazmat.primitives import serialization

    data = pem_or_der
    if b"-----BEGIN" in data:
        return serialization.load_pem_private_key(data, password=None)
    return serialization.load_der_private_key(data, password=None)


def decrypt_virtual_url(encoded: str, private_key) -> str:
    """ログイン応答の暗号化仮想URLを復号する。

    規定は RSA-OAEP / SHA-256（公式サンプル準拠）。念のため SHA-1 OAEP と PKCS1v15 も試す。
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    raw = base64.b64decode(encoded.strip().replace('"', ""))
    attempts = [
        ("OAEP-SHA256", padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None)),
        ("OAEP-SHA1", padding.OAEP(mgf=padding.MGF1(hashes.SHA1()), algorithm=hashes.SHA1(), label=None)),
        ("PKCS1v15", padding.PKCS1v15()),
    ]
    last: Exception | None = None
    for name, pad in attempts:
        try:
            out = private_key.decrypt(raw, pad)
            if name != "OAEP-SHA256":
                log.warning("仮想URL復号: %s で成功（規定のOAEP-SHA256ではない）", name)
            return out.decode("utf-8-sig").strip()
        except Exception as e:  # noqa: BLE001
            last = e
    raise TachibanaError(f"仮想URLの復号に失敗（秘密鍵が公開鍵と対でない可能性）: {last}")


# ----------------------------------------------------------------------------
# クライアント
# ----------------------------------------------------------------------------
class TachibanaClient:
    """立花証券 e支店・API クライアント（情報取得中心・注文は明示許可制）。"""

    TIMEOUT = 20.0
    MAX_RETRY = 3
    RETRY_INTERVAL = 5.0
    MIN_INTERVAL = 0.35  # 共用システムへの配慮（注文上限は秒10件だが情報系はそれ未満で）

    def __init__(
        self,
        base_url: str | None = None,
        *,
        demo: bool = False,
        state_dir: str | Path | None = None,
        auth_id: str | None = None,
        private_key_pem: str | bytes | None = None,
        allow_orders: bool | None = None,
        json_ofmt: str = "5",
        session: requests.Session | None = None,
    ):
        self.state_dir = Path(state_dir or os.environ.get("TACHIBANA_DIR") or DEFAULT_STATE_DIR)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if demo:
            base_url = DEMO_BASE
        self.base_url = (base_url or os.environ.get("TACHIBANA_API_BASE") or PROD_BASE).rstrip("/") + "/"
        self._auth_id = auth_id
        self._private_key_pem = private_key_pem
        self._private_key = None
        self.json_ofmt = json_ofmt
        self.allow_orders = (
            allow_orders if allow_orders is not None
            else os.environ.get("TACHIBANA_ALLOW_ORDERS", "") in ("1", "true", "TRUE", "yes")
        )
        self.http = session or requests.Session()
        self._lock = threading.RLock()
        self._last_call = 0.0
        self._p_no = self._load_p_no()
        self.session_info: dict[str, Any] = self._load_session()
        self.last_response: dict[str, Any] = {}

    # ---- 認証情報 ----------------------------------------------------------
    @property
    def session_file(self) -> Path:
        return self.state_dir / "session.json"

    @property
    def p_no_file(self) -> Path:
        return self.state_dir / "p_no.json"

    def _read_state_text(self, *names: str) -> str | None:
        for n in names:
            p = self.state_dir / n
            if p.exists():
                return p.read_text(encoding="utf-8-sig").strip()
        return None

    @property
    def auth_id(self) -> str:
        if not self._auth_id:
            self._auth_id = os.environ.get("TACHIBANA_AUTH_ID") or self._read_state_text("e_api_authid.txt", "authid.txt")
        if not self._auth_id:
            raise TachibanaError(
                f"認証IDが見つかりません。{self.state_dir / 'e_api_authid.txt'} に置くか TACHIBANA_AUTH_ID を設定してください。"
            )
        return self._auth_id

    @property
    def private_key(self):
        if self._private_key is None:
            data: bytes | None = None
            if self._private_key_pem:
                data = self._private_key_pem.encode() if isinstance(self._private_key_pem, str) else self._private_key_pem
            elif os.environ.get("TACHIBANA_PRIVATE_KEY_PEM"):
                data = os.environ["TACHIBANA_PRIVATE_KEY_PEM"].encode()
            else:
                for n in ("e_api_private_key.pem", "private_key.pem", "e_api_private_key.der", "private_key.der"):
                    p = self.state_dir / n
                    if p.exists():
                        data = p.read_bytes()
                        break
            if not data:
                raise TachibanaError(
                    f"秘密鍵が見つかりません。{self.state_dir / 'e_api_private_key.pem'} に置くか TACHIBANA_PRIVATE_KEY_PEM を設定してください。"
                )
            self._private_key = load_private_key(data)
        return self._private_key

    def second_password(self) -> str:
        pw = os.environ.get("TACHIBANA_SECOND_PASSWORD") or self._read_state_text("file_pwd2.txt", "second_password.txt")
        if not pw:
            raise TachibanaError("第二暗証番号が未設定です（TACHIBANA_SECOND_PASSWORD か .tachibana/file_pwd2.txt）。")
        return pw

    # ---- 永続状態 ----------------------------------------------------------
    def _load_p_no(self) -> int:
        try:
            return int(json.loads(self.p_no_file.read_text(encoding="utf-8-sig"))["p_no"])
        except Exception:  # noqa: BLE001
            return 1

    def _save_p_no(self) -> None:
        try:
            self.p_no_file.write_text(json.dumps({"p_no": str(self._p_no)}), encoding="utf-8")
        except OSError as e:
            log.warning("p_no保存失敗: %s", e)

    def _load_session(self) -> dict[str, Any]:
        try:
            return json.loads(self.session_file.read_text(encoding="utf-8-sig"))
        except Exception:  # noqa: BLE001
            return {}

    def _save_session(self) -> None:
        self.session_file.write_text(json.dumps(self.session_info, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            os.chmod(self.session_file, 0o600)
        except OSError:
            pass

    def clear_session(self) -> None:
        self.session_info = {}
        try:
            self.session_file.unlink()
        except FileNotFoundError:
            pass

    @staticmethod
    def now_jst() -> _dt.datetime:
        return _dt.datetime.now(JST)

    @staticmethod
    def p_sd_date(now: _dt.datetime | None = None) -> str:
        now = now or _dt.datetime.now(JST)
        return now.strftime("%Y.%m.%d-%H:%M:%S.") + f"{now.microsecond:06d}"[:3]

    def has_valid_session(self) -> bool:
        s = self.session_info
        if not s or s.get("base_url") != self.base_url or not s.get("urls", {}).get("sUrlRequest"):
            return False
        try:
            login_day = _dt.datetime.fromisoformat(s["login_at"]).astimezone(JST).date()
        except Exception:  # noqa: BLE001
            return False
        return login_day == self.now_jst().date()

    # ---- 通信の基礎 --------------------------------------------------------
    def build_url(self, target: str, params: dict[str, Any]) -> str:
        body = json.dumps(params, ensure_ascii=False, separators=(",", ":"))
        return f"{target}?{urllib.parse.quote(body, safe='')}"

    def _throttle(self) -> None:
        wait = self.MIN_INTERVAL - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _http_call(self, url: str, method: str = "POST") -> bytes:
        last: Exception | None = None
        for attempt in range(1, self.MAX_RETRY + 1):
            try:
                if attempt > 1:
                    time.sleep(self.RETRY_INTERVAL)
                self._throttle()
                r = self.http.request(method, url, timeout=self.TIMEOUT)
                return r.content
            except (requests.ConnectionError, requests.Timeout) as e:
                last = e
                log.warning("通信エラー (%d/%d): %s", attempt, self.MAX_RETRY, e)
        raise TachibanaTransportError(f"APIサーバーに接続できません（{self.MAX_RETRY}回失敗）: {last}")

    @staticmethod
    def decode_response(raw: bytes) -> dict[str, Any]:
        text = raw.decode("cp932", errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise TachibanaTransportError(f"JSONでない応答: {text[:300]!r}") from e

    def _next_p_no(self) -> int:
        self._p_no += 1
        self._save_p_no()
        return self._p_no

    def _target_for(self, clmid: str) -> str:
        urls = self.session_info.get("urls", {})
        if clmid in _PRICE_CLMIDS:
            key = "sUrlPrice"
        elif clmid.startswith("CLMStk") or clmid in _MASTER_CLMIDS:
            key = "sUrlMaster"
        else:
            key = "sUrlRequest"
        url = urls.get(key)
        if not url:
            raise TachibanaSessionError("2", f"仮想URL({key})がありません。login() してください。")
        return url

    def call(self, clmid: str, params: dict[str, Any] | None = None, *, raise_on_result: bool = True,
             _retry_login: bool = True) -> dict[str, Any]:
        """任意の機能ID（sCLMID）を仮想URLへ送る。共通項目は自動付与。"""
        if clmid in _ORDER_CLMIDS and not self.allow_orders:
            raise TachibanaError(
                f"{clmid} は注文系のため実行を拒否しました。本当に発注する場合は TachibanaClient(allow_orders=True) か "
                "環境変数 TACHIBANA_ALLOW_ORDERS=1 を明示してください。"
            )
        self.ensure_session()
        with self._lock:
            body = {"p_no": str(self._next_p_no()), "p_sd_date": self.p_sd_date(), "sCLMID": clmid}
            body.update(params or {})
            body["sJsonOfmt"] = self.json_ofmt
            url = self.build_url(self._target_for(clmid), body)
            resp = self.decode_response(self._http_call(url))
        self.last_response = resp
        p_errno = str(resp.get("p_errno", "0"))
        if p_errno == "2":
            self.clear_session()
            if _retry_login:
                log.info("仮想URLが無効（p_errno=2）→ 再ログインして1回だけ再試行")
                self.login()
                return self.call(clmid, params, raise_on_result=raise_on_result, _retry_login=False)
            raise TachibanaSessionError(p_errno, str(resp.get("p_err", "")), resp)
        if p_errno not in ("0", ""):
            raise TachibanaProtocolError(p_errno, str(resp.get("p_err", "")), resp)
        rc = str(resp.get("sResultCode", "0"))
        if raise_on_result and rc not in ("0", ""):
            raise TachibanaResultError(rc, str(resp.get("sResultText", "")), resp)
        return resp

    # ---- 認証 ----------------------------------------------------------------
    def login(self) -> dict[str, Any]:
        """公開鍵認証でログインし、仮想URLを復号して session.json に保存する。"""
        with self._lock:
            self._p_no = 1
            self._save_p_no()
            body = {"p_no": "1", "p_sd_date": self.p_sd_date(), "sCLMID": "CLMAuthLoginRequest",
                    "sAuthId": self.auth_id, "sJsonOfmt": self.json_ofmt}
            url = self.build_url(self.base_url + "auth/", body)
            resp = self.decode_response(self._http_call(url))
        self.last_response = resp
        p_errno = str(resp.get("p_errno", "0"))
        if p_errno not in ("0", ""):
            raise TachibanaProtocolError(p_errno, str(resp.get("p_err", "")), resp)
        rc = str(resp.get("sResultCode", "0"))
        if rc not in ("0", ""):
            raise TachibanaResultError(rc, str(resp.get("sResultText", "")), resp)
        if not resp.get("sUrlRequest"):
            raise TachibanaError(
                "ログインは通ったが仮想URLが空です。金商法交付書面が未読（sKinsyouhouMidokuFlg=1）の可能性。"
                "標準Webにログインして書面を確認してください。"
            )
        urls = {}
        for k in ("sUrlRequest", "sUrlMaster", "sUrlPrice", "sUrlEvent", "sUrlEventWebSocket"):
            if resp.get(k):
                urls[k] = decrypt_virtual_url(resp[k], self.private_key)
        masked = {k: v for k, v in resp.items() if not k.startswith("sUrl")}
        self.session_info = {
            "base_url": self.base_url,
            "login_at": self.now_jst().isoformat(),
            "urls": urls,
            "login_response": masked,
        }
        self._save_session()
        log.info("ログイン成功。仮想URL(1日券)を保存: %s", self.session_file)
        return self.session_info

    def ensure_session(self) -> dict[str, Any]:
        if not self.has_valid_session():
            return self.login()
        return self.session_info

    def logout(self) -> dict[str, Any]:
        try:
            return self.call("CLMAuthLogoutRequest", raise_on_result=False, _retry_login=False)
        finally:
            self.clear_session()

    def login_notices(self) -> dict[str, str]:
        """交付書面更新予定日・APIリリース予定日（ログイン応答から）。"""
        r = self.session_info.get("login_response", {})
        return {
            "交付書面更新予定日": r.get("sUpdateInformWebDocument", ""),
            "APIリリース予定日": r.get("sUpdateInformAPISpecFunction", ""),
            "最終ログイン": r.get("sLastLoginDate", ""),
        }

    # ---- マスタ ----------------------------------------------------------------
    def date_info(self, day_key: str = "001") -> dict[str, Any]:
        """日付情報（001=当日基準, 002=翌日基準/夕場）。翌1〜10営業日・受渡日を含む。"""
        r = self.call("CLMStkGetDateZyouhou")
        rows = r.get("aCLMDateZyouhou") or []
        for row in rows:
            if row.get("sDayKey") == day_key:
                return row
        return rows[0] if rows else {}

    def issue_master(self) -> list[dict[str, Any]]:
        return self.call("CLMStkGetIssueMstKabu").get("aCLMStkIssueMstKabu") or []

    def issue_market_master(self) -> list[dict[str, Any]]:
        return self.call("CLMStkGetIssueSizyouMstKabu").get("aCLMStkIssueSizyouMstKabu") or []

    def regulation_master(self) -> list[dict[str, Any]]:
        """株式銘柄別・市場別規制（取引禁止/成行禁止/一極集中/即日入金規制など・当日と翌営業日）。"""
        return self.call("CLMStkGetIssueSizyouKiseiKabu").get("aCLMStkIssueSizyouKiseiKabu") or []

    def hosyoukin_master(self) -> list[dict[str, Any]]:
        """保証金マスタ（増担保規制銘柄の代用/現金保証金率）。"""
        return self.call("CLMStkGetHosyoukinMst").get("aCLMStkHosyoukinMst") or []

    def daiyou_kakeme(self) -> list[dict[str, Any]]:
        return self.call("CLMStkGetDaiyouKakeme").get("aCLMStkDaiyouKakeme") or []

    def yobine(self) -> list[dict[str, Any]]:
        return self.call("CLMStkGetYobine").get("aCLMStkYobine") or []

    def order_error_reasons(self) -> dict[str, str]:
        rows = self.call("CLMStkGetOrderErrReason").get("aCLMStkOrderErrReason") or []
        return {r.get("sErrReasonCode", ""): r.get("sErrReasonText", "") for r in rows}

    def index_master(self) -> list[dict[str, Any]]:
        return self.call("CLMStkGetIssueMstIndex").get("aCLMStkIssueMstIndex") or []

    def news(self, date: str | None = None) -> list[dict[str, Any]]:
        """指定日(YYYYMMDD)のニュース。見出し/本文は Shift_JIS→BASE64 なのでデコードして返す。"""
        date = date or self.now_jst().strftime("%Y%m%d")
        rows = self.call("CLMMfdsGetNews", {"p_DT": date}).get("aCLMMfdsNews") or []
        for r in rows:
            for k in ("p_HDL", "p_TX"):
                v = r.get(k)
                if v:
                    try:
                        r[k] = base64.b64decode(v).decode("cp932", errors="replace")
                    except Exception:  # noqa: BLE001
                        pass
        return rows

    # ---- 銘柄別付帯情報（最大120銘柄/要求） ------------------------------------
    @staticmethod
    def _norm_codes(codes: Iterable[str] | str) -> list[str]:
        if isinstance(codes, str):
            codes = codes.replace("，", ",").split(",")
        out = []
        for c in codes:
            c = str(c).strip().upper()
            if c.endswith(".T"):
                c = c[:-2]
            if c:
                out.append(c)
        return out

    def _by_codes(self, clmid: str, list_key: str, codes: Iterable[str] | str) -> list[dict[str, Any]]:
        codes = self._norm_codes(codes)
        out: list[dict[str, Any]] = []
        for i in range(0, len(codes), MAX_CODES_PER_REQUEST):
            chunk = codes[i:i + MAX_CODES_PER_REQUEST]
            r = self.call(clmid, {"sTargetIssueCode": ",".join(chunk)})
            out.extend(r.get(list_key) or [])
        return out

    def issue_detail(self, codes: Iterable[str] | str) -> list[dict[str, Any]]:
        """PER/PBR/配当利回り/年初来高安値/権利落日など。"""
        return self._by_codes("CLMMfdsGetIssueDetail", "aCLMMfdsIssueDetail", codes)

    def syoukin_zan(self, codes: Iterable[str] | str) -> list[dict[str, Any]]:
        """証金残（融資残・貸株残・貸借倍率・回転日数・速報/確報）。"""
        return self._by_codes("CLMMfdsGetSyoukinZan", "aCLMMfdsSyoukinZan", codes)

    def shinyou_zan(self, codes: Iterable[str] | str) -> list[dict[str, Any]]:
        """信用残（週次・買残/売残/信用倍率、一般/制度/合算）。"""
        return self._by_codes("CLMMfdsGetShinyouZan", "aCLMMfdsShinyouZan", codes)

    def hibu_info(self, codes: Iterable[str] | str) -> list[dict[str, Any]]:
        """逆日歩（pBWRQ）。"""
        return self._by_codes("CLMMfdsGetHibuInfo", "aCLMMfdsHibuInfo", codes)

    # ---- 時価 ------------------------------------------------------------------
    def market_price(self, codes: Iterable[str] | str, columns: Sequence[str] = DEFAULT_PRICE_COLUMNS) -> list[dict[str, Any]]:
        """時価情報問合取得（スナップショット）。columns は PRICE_COLUMNS のキー。"""
        codes = self._norm_codes(codes)
        out: list[dict[str, Any]] = []
        for i in range(0, len(codes), MAX_CODES_PER_REQUEST):
            chunk = codes[i:i + MAX_CODES_PER_REQUEST]
            r = self.call("CLMMfdsGetMarketPrice",
                          {"sTargetIssueCode": ",".join(chunk), "sTargetColumn": ",".join(columns)})
            out.extend(r.get("aCLMMfdsMarketPrice") or [])
        return out

    def board(self, code: str) -> dict[str, Any]:
        """板（売買10本気配＋OVER/UNDER）。"""
        rows = self.market_price([code], ("pDPP", "tDPP:T") + BOARD_COLUMNS)
        return rows[0] if rows else {}

    def price_history_raw(self, code: str, market: str = "00") -> list[dict[str, Any]]:
        """蓄積情報問合取得（最大約20年の日足、日付昇順）。AM0:00-0:59は前日分反映処理のため避ける。"""
        code = self._norm_codes([code])[0]
        r = self.call("CLMMfdsGetMarketPriceHistory", {"sIssueCode": code, "sSizyouC": market})
        return r.get("aCLMMfdsMarketPriceHistory") or []

    def price_history(self, code: str, market: str = "00"):
        """20年日足を DataFrame で返す（Open/High/Low/Close/Volume は分割調整済み、Raw* は生値）。"""
        return history_to_frame(self.price_history_raw(code, market))

    # ---- 口座・注文照会 ----------------------------------------------------------
    def genbutu_list(self, code: str = "") -> dict[str, Any]:
        return self.call("CLMGenbutuKabuList", {"sIssueCode": code})

    def shinyou_tategyoku_list(self, code: str = "") -> dict[str, Any]:
        return self.call("CLMShinyouTategyokuList", {"sIssueCode": code})

    def order_list(self, code: str = "", sikkou_day: str = "", status: str = "") -> list[dict[str, Any]]:
        """注文一覧。status: ''全部 1未約定 2全部約定 3一部約定 4訂正取消可能 5未約定+一部約定"""
        r = self.call("CLMOrderList", {"sIssueCode": code, "sSikkouDay": sikkou_day, "sOrderSyoukaiStatus": status})
        return r.get("aOrderList") or []

    def order_detail(self, order_number: str, eigyou_day: str) -> dict[str, Any]:
        return self.call("CLMOrderListDetail", {"sOrderNumber": order_number, "sEigyouDay": eigyou_day})

    def zan_summary(self) -> dict[str, Any]:
        """可能額サマリー（買付可能額・新規建可能額・保証金率・追証/立替金フラグ・本日の約定件数など）。"""
        return self.call("CLMZanKaiSummary")

    def buy_power(self) -> dict[str, Any]:
        return self.call("CLMZanKaiKanougaku", {"sIssueCode": "", "sSizyouC": ""})

    def margin_power(self) -> dict[str, Any]:
        return self.call("CLMZanShinkiKanoIjiritu", {"sIssueCode": "", "sSizyouC": ""})

    def sellable_qty(self, code: str) -> dict[str, Any]:
        return self.call("CLMZanUriKanousuu", {"sIssueCode": self._norm_codes([code])[0]})

    def kanougaku_suii(self) -> dict[str, Any]:
        return self.call("CLMZanKaiKanougakuSuii")

    def real_hosyoukin_ritu(self) -> dict[str, Any]:
        return self.call("CLMZanRealHosyoukinRitu")

    # ---- 注文（明示許可が必要） ----------------------------------------------------
    def new_order(
        self,
        code: str,
        side: str,               # "1"売 "3"買 "5"現渡 "7"現引
        qty: int,
        price: str | int | float = "0",  # "0"成行 / 値段 / "*"指定なし（逆指値のみ等）
        *,
        genkin_shinyou: str = "0",       # 0現物 2新規(制度6ヶ月) 4返済(制度) 6新規(一般) 8返済(一般)
        condition: str = "0",            # 0指定なし 2寄付 4引け 6不成
        expire_day: str = "0",           # 0当日 / YYYYMMDD
        zyoutoeki_kazei: str = "1",      # 1特定 3一般 5NISA 6N成長
        gyakusasi_type: str = "0",       # 0通常 1逆指値 2通常+逆指値
        gyakusasi_zyouken: str = "0",
        gyakusasi_price: str = "*",
        tatebi_type: str = "*",          # 返済時: 1個別 2建日順 3単価益順 4単価損順
        tategyoku_kazei: str = "*",
        hensai_list: list[dict[str, str]] | None = None,
        second_password: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "sZyoutoekiKazeiC": zyoutoeki_kazei,
            "sIssueCode": self._norm_codes([code])[0],
            "sSizyouC": "00",
            "sBaibaiKubun": side,
            "sCondition": condition,
            "sOrderPrice": str(price),
            "sOrderSuryou": str(qty),
            "sGenkinShinyouKubun": genkin_shinyou,
            "sOrderExpireDay": expire_day,
            "sGyakusasiOrderType": gyakusasi_type,
            "sGyakusasiZyouken": gyakusasi_zyouken,
            "sGyakusasiPrice": gyakusasi_price,
            "sTatebiType": tatebi_type,
            "sTategyokuZyoutoekiKazeiC": tategyoku_kazei,
            "sSecondPassword": second_password or self.second_password(),
        }
        if hensai_list:
            params["aCLMKabuHensaiData"] = hensai_list
        return self.call("CLMKabuNewOrder", params)

    def correct_order(self, order_number: str, eigyou_day: str, *, price: str = "*", qty: str = "*",
                      condition: str = "*", expire_day: str = "*", gyakusasi_zyouken: str = "*",
                      gyakusasi_price: str = "*", second_password: str | None = None) -> dict[str, Any]:
        return self.call("CLMKabuCorrectOrder", {
            "sOrderNumber": order_number, "sEigyouDay": eigyou_day, "sCondition": condition,
            "sOrderPrice": str(price), "sOrderSuryou": str(qty), "sOrderExpireDay": expire_day,
            "sGyakusasiZyouken": gyakusasi_zyouken, "sGyakusasiPrice": gyakusasi_price,
            "sSecondPassword": second_password or self.second_password(),
        })

    def cancel_order(self, order_number: str, eigyou_day: str, second_password: str | None = None) -> dict[str, Any]:
        return self.call("CLMKabuCancelOrder", {
            "sOrderNumber": order_number, "sEigyouDay": eigyou_day,
            "sSecondPassword": second_password or self.second_password(),
        })

    def cancel_all_orders(self, second_password: str | None = None) -> dict[str, Any]:
        return self.call("CLMKabuCancelOrderAll", {"sSecondPassword": second_password or self.second_password()})

    # ---- 複合ヘルパ（このプロジェクト向け） ------------------------------------------
    def short_sell_status(self, codes: Iterable[str] | str) -> list[dict[str, Any]]:
        """空売り可否の一覧: 規制(制度信用売建/翌日)・一極集中・貸借区分・逆日歩・証金貸株残。

        フェード（前夜18:50配信）の在庫確認・売り禁判定の材料。
        """
        codes = self._norm_codes(codes)
        want = set(codes)
        reg = {r["sIssueCode"]: r for r in self.regulation_master() if r.get("sIssueCode") in want}
        mkt = {r["sIssueCode"]: r for r in self.issue_market_master() if r.get("sIssueCode") in want}
        hibu = {r["sIssueCode"]: r for r in self.hibu_info(codes)}
        syk = {r["sIssueCode"]: r for r in self.syoukin_zan(codes)}
        out = []
        for c in codes:
            r, m, h, s = reg.get(c, {}), mkt.get(c, {}), hibu.get(c, {}), syk.get(c, {})
            out.append({
                "code": c,
                "貸借区分": SINYOU_C.get(m.get("sSinyouC", ""), m.get("sSinyouC", "")),
                "停止区分": TEISI_KUBUN.get(r.get("sTeisiKubun", ""), r.get("sTeisiKubun", "")),
                "制度信用売建": TEISI_KUBUN.get(r.get("sSeidoSinyouSinkiUritate", ""), r.get("sSeidoSinyouSinkiUritate", "")),
                "制度信用売建(翌)": TEISI_KUBUN.get(r.get("sSeidoSinyouSinkiUritateYoku", ""), r.get("sSeidoSinyouSinkiUritateYoku", "")),
                "一般信用売建": TEISI_KUBUN.get(r.get("sIppanSinyouSinkiUritate", ""), r.get("sIppanSinyouSinkiUritate", "")),
                "一極集中": SINYOU_SYUTYU.get(r.get("sSinyouSyutyuKubun", ""), r.get("sSinyouSyutyuKubun", "")),
                "一極集中(翌)": SINYOU_SYUTYU.get(r.get("sSinyouSyutyuKubunYoku", ""), r.get("sSinyouSyutyuKubunYoku", "")),
                "即日入金規制": r.get("sSokuzituNyukinC", ""),
                "逆日歩": h.get("pBWRQ", ""),
                "証金貸株残": s.get("pSFS6", ""),
                "証金貸株前日比": s.get("pSSG6", ""),
                "貸借倍率": s.get("pSFR6", ""),
                "証金更新日": s.get("pSFD", ""),
                "速報確報": s.get("pSFKS", ""),
                "前日終値": m.get("sZenzituOwarine", ""),
                "値幅下限": m.get("sNehabaMin", ""),
                "値幅上限": m.get("sNehabaMax", ""),
            })
        return out


# ----------------------------------------------------------------------------
# 日足履歴の変換
# ----------------------------------------------------------------------------
def _f(v: Any) -> float | None:
    try:
        if v in ("", None):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def history_to_frame(rows: list[dict[str, Any]]):
    """蓄積情報の配列を DataFrame へ。Open..Volume=分割調整済み(xK)、Raw*=生値。"""
    import pandas as pd

    cols = ["Date", "Open", "High", "Low", "Close", "Volume", "RawOpen", "RawHigh",
            "RawLow", "RawClose", "RawVolume", "SplitBefore", "SplitAfter", "SplitK"]
    recs = []
    for r in rows:
        d = str(r.get("sDate", ""))
        if len(d) != 8:
            continue
        recs.append({
            "Date": pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:]}"),
            "Open": _f(r.get("pDOPxK")), "High": _f(r.get("pDHPxK")), "Low": _f(r.get("pDLPxK")),
            "Close": _f(r.get("pDPPxK")), "Volume": _f(r.get("pDVxK")),
            "RawOpen": _f(r.get("pDOP")), "RawHigh": _f(r.get("pDHP")), "RawLow": _f(r.get("pDLP")),
            "RawClose": _f(r.get("pDPP")), "RawVolume": _f(r.get("pDV")),
            "SplitBefore": _f(r.get("pSPUO")), "SplitAfter": _f(r.get("pSPUC")), "SplitK": _f(r.get("pSPUK")),
        })
    df = pd.DataFrame(recs, columns=cols)
    if df.empty:
        return df.set_index("Date")
    df = df.sort_values("Date").drop_duplicates("Date", keep="last").set_index("Date")
    # 分割係数が付いていない（xK 列が空）行は生値で埋める
    for c in ("Open", "High", "Low", "Close", "Volume"):
        df[c] = df[c].fillna(df["Raw" + c])
    return df


def history_to_cache_frame(df):
    """jquants_cache.pkl の all_data と同じ形（Open/High/Low/Close/Volume・Date index・float）に揃える。"""
    out = df[["Open", "High", "Low", "Close", "Volume"]].astype("float64").dropna(subset=["Close"])
    out.index.name = "Date"
    return out
