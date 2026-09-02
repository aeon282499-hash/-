# 立花証券 e支店・API (v4r10) 連携

`tachibana/` = クライアント本体。公式リファレンス（e_api reference manual v4r10）と
公式サンプル（github.com/e-shiten-jp/e_api_login_pubkey.py 等）に準拠。

## 初回セットアップ（本人作業・1回だけ）

1. e支店 標準Web にパスキーでログイン → **お客様情報 → 設定情報 → e支店・API利用設定**
2. 鍵ペアを作って公開鍵を登録し、**登録直後の画面で** 認証ID と秘密鍵をダウンロード
   （後から再DLは不可。逃したら再発行）
3. ダウンロードした2ファイルをこのリポジトリ直下の `.tachibana/`（gitignore済み）に置く
   ```
   .tachibana/e_api_authid.txt        ← 認証ID（1行）
   .tachibana/e_api_private_key.pem   ← 秘密鍵（.der でも可）
   ```
4. 標準Web で **第二暗証番号の省略を「無効」**にする（API仕様上の必須設定。情報取得だけなら不要）
5. 金商法交付書面が未読だと仮想URLが発行されない → 標準Webで書面を既読にしておく
6. 本番と デモ環境は認証ID/鍵が別物。デモは `--demo` か `TACHIBANA_API_BASE=https://demo-kabuka.e-shiten.jp/e_api_v4r10/`

## 毎日のログイン（移行期間中は電話認証つき）

```
（登録電話から API用認証電話番号へ発信 → 3分以内に）
python -m tachibana.cli login
```
成功すると仮想URL（1日券・夜間閉局まで有効）が `.tachibana/session.json` に保存され、
同日中の他コマンドは自動でそれを使う（再ログイン不要）。翌日は再ログイン。

## よく使うコマンド

| 目的 | コマンド |
|---|---|
| 口座サマリ＋営業日 | `python -m tachibana.cli status` |
| 時価スナップショット | `python -m tachibana.cli price 7203 6501` |
| 板10本 | `python -m tachibana.cli board 7203` |
| 20年日足 | `python -m tachibana.cli history 7203 --csv 7203.csv` |
| **空売り可否**（規制/一極集中/逆日歩/証金貸株残） | `python -m tachibana.cli short 7203 6501` |
| 証金残・信用残・逆日歩・PER等まとめ | `python -m tachibana.cli margin 7203` |
| ニュース | `python -m tachibana.cli news 20260902 --code 7203` |
| 現物＋建玉 | `python -m tachibana.cli positions` |
| 注文一覧 | `python -m tachibana.cli orders --status 1` |
| 全銘柄マスタ結合CSV | `python -m tachibana.cli master --out issue_master.csv` |
| 全銘柄20年日足→pkl | `python tachibana_fetch_history.py --universe` |

`--json` で生JSON。`-v` で送受信ログ。

## Python から

```python
from tachibana import TachibanaClient
tc = TachibanaClient()
tc.ensure_session()
tc.short_sell_status(["7203", "6501"])       # フェード在庫チェック用
tc.price_history("7203")                     # DataFrame（Open..Volume=分割調整済み, Raw*=生値）
tc.call("CLMZanKaiSummary")                  # 任意の sCLMID を直接
```

## 安全設計

- 注文系（`new_order` / `correct_order` / `cancel_order` / `cancel_all_orders`）は
  `TachibanaClient(allow_orders=True)` か環境変数 `TACHIBANA_ALLOW_ORDERS=1` が無いと **通信前に例外で止まる**。
- 第二暗証番号は `TACHIBANA_SECOND_PASSWORD` か `.tachibana/file_pwd2.txt`（注文時のみ必要）。
- 秘密鍵・認証ID・session.json・p_no.json は全部 `.tachibana/`。このリポジトリは public なので絶対にコミットしない。
- 公式推奨: 接続元の固定IP制限を利用設定で有効にする。→ GitHub Actions からは使わず、ローカルPC専用。
- 要求間隔は 0.35 秒以上（共用システム配慮）。時価の高頻度ポーリングは禁止（公式README）。

## API仕様メモ（実装根拠）

- 要求 = `仮想URL?{JSON}`（URLエンコード）。共通項目 `p_no`（ログインで1、以後+1）, `p_sd_date`（JST `YYYY.MM.DD-hh:mm:ss.sss`）, `sCLMID`, `sJsonOfmt:"5"`。
- 応答は Shift_JIS。`p_errno` 0=正常 / 2=仮想URL無効（→自動で1回再ログイン） / -2=パラメータ誤り / 9=サービス停止中。
- 仮想URLは登録公開鍵で暗号化 → 秘密鍵で **RSA-OAEP(SHA-256)** 復号（保険で SHA-1 OAEP, PKCS1v15 も試す）。
- 振り分け: `CLMStk*`・ニュース/銘柄詳細/証金残/信用残/逆日歩 → sUrlMaster、時価/蓄積 → sUrlPrice、他 → sUrlRequest。
- 銘柄指定系は 1要求 120 銘柄まで（自動分割）。蓄積情報は 1要求1銘柄・AM0:00-0:59 は避ける。
- ログイン応答の `sUpdateInformWebDocument` / `sUpdateInformAPISpecFunction` は `login_notices()` で確認（書面更新・API改定の予告日）。

## オフラインテスト

```
python _test_tachibana_client.py
```
通信をモックして、URL組立・p_no・RSA復号・仮想URL振り分け・エラー変換・日足変換を検証。
