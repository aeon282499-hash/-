# close-signal-trigger デプロイ手順

GitHub Actions cron が遅延して大引けに通知が間に合わない事故（2026-04-27, 2026-04-30）の対策として、
朝シグナル配信と同じく **cron-job.org → Cloudflare Worker → workflow_dispatch** に切替える。

## 1. Cloudflare Worker を作成

1. Cloudflare ダッシュボード → Workers & Pages → Create Worker
2. 名前: `close-signal-trigger`
3. デフォルトの "Hello World" Worker をデプロイ後、**Edit code** で `close-signal-trigger.js` の中身を全コピペして貼付 → Save and Deploy
4. デプロイ後の URL をメモ（例: `https://close-signal-trigger.aeon282499.workers.dev`）

## 2. 環境変数を設定

ワーカーの Settings → Variables and Secrets で以下を追加:

| Name | Value | 種別 |
|---|---|---|
| `GITHUB_PAT` | 既存 swing-signal-trigger と同じ fine-grained PAT | Secret |
| `GITHUB_OWNER` | `aeon282499-hash` | Plaintext |
| `GITHUB_REPO` | `-` | Plaintext |
| `WORKFLOW_FILE` | `schedule_close.yml` | Plaintext |
| `GIT_REF` | `main` | Plaintext |

PAT は新規発行不要・swing-signal-triggerに設定済のものを Copy → Paste で再利用できる。
（Cloudflareでは Secret は値を表示できないので、GitHubで再発行するか別管理しているメモから貼付）

## 3. cron-job.org に新規ジョブを追加

- Title: `close-signal-trigger`
- URL: 手順1でメモしたWorker URL（例: `https://close-signal-trigger.aeon282499.workers.dev`）
- Schedule:
  - Days of week: Mon〜Fri
  - Time: **14:55 JST** = **05:55 UTC**
  - 毎月毎日: every / every
- Request method: GET（Workerはfetchハンドラなのでメソッド不問）
- Notifications: お好み

理由: 14:55 JSTに発火 → close_check.pyが yfinance intraday取得 → ~14:57通知
→ 15:00頃にスマホでDiscord確認 → 15:25-15:30大引けに余裕で間に合う。

## 4. 動作確認

- Worker URL をブラウザで叩く → `OK: dispatched schedule_close.yml on main` が表示されるはず
- GitHub Actions の "大引け前RSI判定" タブに即座に手動Run履歴が出ればOK
- 数分後にDiscordへ大引け処分通知（or「保有銘柄なし」スキップログ）が来る

## 5. GitHub Actions cron はバックアップとして残す

`schedule_close.yml` の `cron: '0 6 * * 1-5'`（15:00 JST）はそのまま維持。
外部cron経由のworkflow_dispatchが失敗した場合のフェイルセーフ。
重複実行は schedule_close.yml の `concurrency:` グループで安全。

ただし「外部cronとGitHub cronで5分しかズレない」ので近接重複が気になるなら
GitHub cron 側を `cron: '30 6 * * 1-5'`（15:30 JST = 大引け直前）に後ろ倒し推奨。
