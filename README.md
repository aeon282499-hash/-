# 日本株 自動売買シグナル配信システム

毎営業日 **8:30 JST** に日経225採用銘柄をスクリーニングし、
売買シグナルを **Discord** に自動送信する Python プロジェクトです。

---

## ファイル構成

```
.
├── main.py              # エントリーポイント・休場日チェック
├── screener.py          # 銘柄選定・テクニカルロジック
├── notifier.py          # Discord Webhook 通知
├── requirements.txt
├── .env.example         # 環境変数テンプレート
└── .github/
    └── workflows/
        └── schedule.yml # GitHub Actions ワークフロー
```

---

## セットアップ手順

### 1. リポジトリをクローン

```bash
git clone https://github.com/<あなたのユーザー名>/<リポジトリ名>.git
cd <リポジトリ名>
```

### 2. Python 仮想環境を作成・依存パッケージをインストール

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Discord Webhook URL を取得する

1. Discord サーバーを開く
2. **サーバー設定** → **連携サービス** → **ウェブフック** を開く
3. 「**新しいウェブフック**」を作成し、通知を送りたいチャンネルを選択
4. 「**ウェブフック URL をコピー**」をクリック

### 4. `.env` ファイルを作成する

```bash
cp .env.example .env
```

`.env` を開き、取得した Webhook URL を貼り付ける：

```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxxxxxxxx/xxxxxxxxx
```

> ⚠️ `.env` は **絶対に Git にコミットしないでください**。
> `.gitignore` に `.env` を追加しておくことを推奨します。

---

## ローカルでの動作確認

```bash
# 仮想環境を有効化した状態で実行
python main.py
```

- 今日が**休場日**の場合はスキップメッセージが出て終了します。
- 営業日の場合はスクリーニングが走り、結果が Discord に送信されます。
- シグナルが 0 件でも「0件通知」が Discord に届きます（正常動作）。

---

## GitHub Actions による自動実行のセットアップ

### 5. GitHub Secrets に Webhook URL を登録する

GitHub Actions から Discord に通知するために、Webhook URL を Secrets に登録します。

1. GitHub のリポジトリページを開く
2. **Settings** → **Secrets and variables** → **Actions** を開く
3. **「New repository secret」** をクリック
4. 以下を入力して保存：
   - **Name**: `DISCORD_WEBHOOK_URL`
   - **Secret**: Discord の Webhook URL（`.env` に書いたものと同じ）

### 6. リポジトリに push する

```bash
git add .
git commit -m "feat: 自動売買シグナル配信システムを追加"
git push origin main
```

push 後、**Actions タブ**でワークフローが登録されたことを確認できます。

### 自動実行スケジュール

| JST（日本時間） | UTC              | 実行日         |
|----------------|------------------|----------------|
| 毎日 8:30      | 前日 23:30       | 月〜金（営業日のみ有効） |

> GitHub Actions の cron は UTC 基準です。
> `30 23 * * 0-4`（UTC日曜〜木曜）＝ JST 月〜金の 8:30 に相当します。

### 手動実行（テスト用）

GitHub の **Actions** タブ → ワークフロー名 → **「Run workflow」** ボタンから手動実行できます。

---

## トレードロジックのカスタマイズ

`screener.py` の冒頭にある **閾値設定ブロック** を編集するだけでロジックが変わります。

```python
RSI_BUY_MAX   = 30     # RSI がこの値以下 → 買い候補
RSI_SELL_MIN  = 70     # RSI がこの値以上 → 売り候補
DEV_BUY_MAX   = -5.0   # 乖離率がこの値(%)以下 → 買い候補
DEV_SELL_MIN  = +5.0   # 乖離率がこの値(%)以上 → 売り候補
RANGE_MULT    = 1.5    # 前日値幅が ATR の何倍以上か
VOL_MULT      = 1.5    # 前日出来高が 20日平均の何倍以上か
MAX_SIGNALS   = 10     # 最大抽出銘柄数
```

独自の指標（MACD、ボリンジャーバンドなど）を追加したい場合は、
`screener.py` の `judge_signal()` 関数内にコメントに沿って追記してください。

---

## 注意事項

- このシステムは**情報提供のみ**を目的としています。
- 実際の投資判断・売買はご自身の責任で行ってください。
- 東証の取引時間は **9:00〜15:30**（2024年11月より延長）です。
- 空売りには証券会社の信用取引口座が必要です。
