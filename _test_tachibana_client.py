# -*- coding: utf-8 -*-
"""立花APIクライアントのオフライン検証（通信をモックし、鍵・URL組立・復号・エラー処理・日足変換を確認）。

    python _test_tachibana_client.py
"""
from __future__ import annotations

import base64
import json
import tempfile
import urllib.parse
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from tachibana import (TachibanaClient, TachibanaError, TachibanaProtocolError, TachibanaResultError,
                       TachibanaSessionError, history_to_frame, history_to_cache_frame)

REQ = "https://kabuka.e-shiten.jp/e_api_v4r10/request/abc123/"
MST = "https://kabuka.e-shiten.jp/e_api_v4r10/master/abc123/"
PRC = "https://kabuka.e-shiten.jp/e_api_v4r10/price/abc123/"


class FakeHTTP:
    """requests.Session の代役。送られたURLを記録し、用意した応答(Shift_JIS bytes)を返す。"""

    def __init__(self, pub):
        self.pub = pub
        self.calls: list[str] = []
        self.queue: list[dict] = []
        self.login_count = 0

    def enc(self, s: str) -> str:
        ct = self.pub.encrypt(s.encode(), padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
        return base64.b64encode(ct).decode()

    def request(self, method, url, timeout=None):
        self.calls.append(url)
        body = json.loads(urllib.parse.unquote(url.split("?", 1)[1]))
        if body["sCLMID"] == "CLMAuthLoginRequest":
            self.login_count += 1
            resp = {"p_no": body["p_no"], "p_errno": "0", "p_err": "", "sCLMID": "CLMAuthLoginAck", "sResultCode": "0",
                    "sResultText": "", "sKinsyouhouMidokuFlg": "0", "sUrlRequest": self.enc(REQ), "sUrlMaster": self.enc(MST),
                    "sUrlPrice": self.enc(PRC), "sUrlEvent": "", "sUrlEventWebSocket": "",
                    "sUpdateInformWebDocument": "20261001", "sUpdateInformAPISpecFunction": "20260531", "sLastLoginDate": "20260902100000"}
        else:
            resp = self.queue.pop(0) if self.queue else {"p_errno": "0", "sResultCode": "0"}
            resp = {"p_no": body["p_no"], "p_errno": "0", "p_err": "", "sCLMID": body["sCLMID"], **resp}

        class R:
            content = json.dumps(resp, ensure_ascii=False).encode("cp932")
        return R()


def make_client(tmp: str):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    d = Path(tmp)
    (d / "e_api_authid.txt").write_text("﻿AUTHID-TEST-123\n", encoding="utf-8")   # BOM付きでも読めること
    (d / "e_api_private_key.pem").write_bytes(pem)
    fake = FakeHTTP(key.public_key())
    tc = TachibanaClient(state_dir=d, session=fake)
    tc.MIN_INTERVAL = 0
    return tc, fake


def test_login_and_routing():
    with tempfile.TemporaryDirectory() as tmp:
        tc, fake = make_client(tmp)
        info = tc.login()
        assert info["urls"] == {"sUrlRequest": REQ, "sUrlMaster": MST, "sUrlPrice": PRC}, info["urls"]
        assert (Path(tmp) / "session.json").exists()
        assert "sUrlRequest" not in json.dumps(info["login_response"])  # 暗号文を保存しない
        login_body = json.loads(urllib.parse.unquote(fake.calls[0].split("?", 1)[1]))
        assert fake.calls[0].startswith("https://kabuka.e-shiten.jp/e_api_v4r10/auth/?")
        assert login_body["p_no"] == "1" and login_body["sAuthId"] == "AUTHID-TEST-123" and login_body["sJsonOfmt"] == "5"
        assert len(login_body["p_sd_date"]) == len("2026.09.02-23:59:59.123")

        fake.queue.append({"aCLMDateZyouhou": [{"sDayKey": "001", "sTheDay": "20260902"}, {"sDayKey": "002", "sTheDay": "20260903"}]})
        assert tc.date_info()["sTheDay"] == "20260902"
        assert fake.calls[-1].startswith(MST + "?")
        b = json.loads(urllib.parse.unquote(fake.calls[-1].split("?", 1)[1]))
        assert b["p_no"] == "2" and b["sCLMID"] == "CLMStkGetDateZyouhou"

        fake.queue.append({"aCLMMfdsMarketPrice": [{"sIssueCode": "7203", "pDPP": "3000"}]})
        rows = tc.market_price(["7203.T"], ("pDPP",))
        assert rows[0]["pDPP"] == "3000" and fake.calls[-1].startswith(PRC + "?")
        b = json.loads(urllib.parse.unquote(fake.calls[-1].split("?", 1)[1]))
        assert b["sTargetIssueCode"] == "7203" and b["sTargetColumn"] == "pDPP" and b["p_no"] == "3"

        fake.queue.append({"sIssueCode": "", "aGenbutuKabuList": []})
        tc.genbutu_list()
        assert fake.calls[-1].startswith(REQ + "?")

        # 同日セッションは再利用され、再ログインしない
        tc2 = TachibanaClient(state_dir=tmp, session=fake); tc2.MIN_INTERVAL = 0
        assert tc2.has_valid_session()
        fake.queue.append({"aCLMDateZyouhou": []})
        tc2.date_info()
        assert fake.login_count == 1
        assert tc2._p_no == 5  # p_no はファイル経由で継続
    print("ok test_login_and_routing")


def test_errors():
    with tempfile.TemporaryDirectory() as tmp:
        tc, fake = make_client(tmp)
        tc.login()
        fake.queue.append({"sResultCode": "11304", "sResultText": "第二暗証番号が誤っています"})
        try:
            tc.genbutu_list(); raise AssertionError("should raise")
        except TachibanaResultError as e:
            assert e.code == "11304" and "第二暗証番号" in e.text
        fake.queue.append({"p_errno": "-2", "p_err": "パラメータ誤り"})
        try:
            tc.genbutu_list(); raise AssertionError("should raise")
        except TachibanaProtocolError as e:
            assert e.p_errno == "-2" and not isinstance(e, TachibanaSessionError)
        # 仮想URL無効(p_errno=2) → 自動再ログイン1回 → 成功
        fake.queue.append({"p_errno": "2", "p_err": "invalid url"})
        fake.queue.append({"aGenbutuKabuList": [{"sUriOrderIssueCode": "7203"}]})
        r = tc.genbutu_list()
        assert fake.login_count == 2 and r["aGenbutuKabuList"][0]["sUriOrderIssueCode"] == "7203"
        # 注文系は明示許可なしで拒否（通信しない）
        n = len(fake.calls)
        try:
            tc.new_order("7203", "3", 100, second_password="x"); raise AssertionError("should raise")
        except TachibanaError as e:
            assert "注文系" in str(e) and len(fake.calls) == n
        # 交付書面未読（仮想URL空）
        fake.enc_backup = fake.enc
        fake.enc = lambda s: ""
        tc.clear_session()
        try:
            tc.login(); raise AssertionError("should raise")
        except TachibanaError as e:
            assert "交付書面" in str(e)
        fake.enc = fake.enc_backup
    print("ok test_errors")


def test_history_conversion():
    rows = [
        {"sDate": "20240102", "pDOP": "1000", "pDHP": "1100", "pDLP": "990", "pDPP": "1050", "pDV": "1000",
         "pDOPxK": "500", "pDHPxK": "550", "pDLPxK": "495", "pDPPxK": "525", "pDVxK": "2000", "pSPUO": "", "pSPUC": "", "pSPUK": ""},
        {"sDate": "20240104", "pDOP": "520", "pDHP": "530", "pDLP": "510", "pDPP": "525", "pDV": "3000",
         "pDOPxK": "", "pDHPxK": "", "pDLPxK": "", "pDPPxK": "", "pDVxK": "", "pSPUO": "1", "pSPUC": "2", "pSPUK": "0.5"},
        {"sDate": "20240103", "pDOP": "", "pDHP": "", "pDLP": "", "pDPP": "", "pDV": "0",
         "pDOPxK": "", "pDHPxK": "", "pDLPxK": "", "pDPPxK": "", "pDVxK": ""},
    ]
    df = history_to_frame(rows)
    assert list(df.index.strftime("%Y%m%d")) == ["20240102", "20240103", "20240104"]
    assert df.loc["2024-01-02", "Close"] == 525 and df.loc["2024-01-02", "RawClose"] == 1050
    assert df.loc["2024-01-04", "Close"] == 525 and df.loc["2024-01-04", "SplitK"] == 0.5
    c = history_to_cache_frame(df)
    assert list(c.columns) == ["Open", "High", "Low", "Close", "Volume"] and len(c) == 2 and c.index.name == "Date"
    assert history_to_frame([]).empty
    print("ok test_history_conversion")


def test_short_status_and_news():
    with tempfile.TemporaryDirectory() as tmp:
        tc, fake = make_client(tmp)
        tc.login()
        fake.queue.append({"aCLMStkIssueSizyouKiseiKabu": [{"sIssueCode": "7203", "sTeisiKubun": "0", "sSeidoSinyouSinkiUritate": "1",
                                                            "sSeidoSinyouSinkiUritateYoku": "1", "sIppanSinyouSinkiUritate": "0",
                                                            "sSinyouSyutyuKubun": "2", "sSinyouSyutyuKubunYoku": "2", "sSokuzituNyukinC": "0"}]})
        fake.queue.append({"aCLMStkIssueSizyouMstKabu": [{"sIssueCode": "7203", "sSinyouC": "1", "sZenzituOwarine": "3000.000000",
                                                          "sNehabaMin": "2500", "sNehabaMax": "3500"}]})
        fake.queue.append({"aCLMMfdsHibuInfo": [{"sIssueCode": "7203", "pBWRQ": "0.05"}]})
        fake.queue.append({"aCLMMfdsSyoukinZan": [{"sIssueCode": "7203", "pSFS6": "3700", "pSSG6": "-3200", "pSFR6": "64.75", "pSFD": "2026/09/01", "pSFKS": "1"}]})
        r = tc.short_sell_status("7203")[0]
        assert r["制度信用売建"] == "取引禁止" and r["一極集中"] == "日々公表銘柄" and r["逆日歩"] == "0.05" and r["貸借区分"] == "貸借銘柄"
        hdl = base64.b64encode("トヨタ 上方修正".encode("cp932")).decode()
        fake.queue.append({"aCLMMfdsNews": [{"p_ID": "x", "p_TM": "1530", "p_ISL": "7203", "p_HDL": hdl, "p_TX": ""}]})
        n = tc.news("20260902")
        assert n[0]["p_HDL"] == "トヨタ 上方修正"
        b = json.loads(urllib.parse.unquote(fake.calls[-1].split("?", 1)[1]))
        assert b["p_DT"] == "20260902" and fake.calls[-1].startswith(MST)
    print("ok test_short_status_and_news")


if __name__ == "__main__":
    test_login_and_routing()
    test_errors()
    test_history_conversion()
    test_short_status_and_news()
    print("ALL OK")
