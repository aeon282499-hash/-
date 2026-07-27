# gapfade-signal-trigger デプロイ手順（所要5分）

## なぜ必要か

ギャップフェード初日（2026-07-27）は `gapfade.yml` の GitHub cron が **9:25 JST 設計に対して実際の起動 13:10 JST**（3時間45分遅延）。
エントリーは**後場寄り 12:30 の成行**なので完全に手遅れだった（本人「ひるの13時じゃ間に合わん・12時には必ず出して」）。

同じリポジトリの実測（2026-07-28 時点）:

| 経路 | 設計時刻 | 実際の起動 |
|---|---|---|
| GitHub cron（schedule.yml） | 8:20 | 9:08〜9:13（**常時40〜50分遅延**） |
| GitHub cron（gapfade.yml） | 9:25 | **13:10**（3時間45分遅延） |
| cron-job.org → Cloudflare Worker | 8:05 | **8:05（毎日1分もズレない）** |

＝「12時までに必ず出す」には外部トリガー経路が要る。GitHub cron は保険として4本に増やしてあるが、**本命はこれ**。

## 1. Cloudflare Worker を作成

1. Cloudflare ダッシュボード → Workers & Pages → Create Worker
2. 名前: `gapfade-signal-trigger`
3. デフォルトの "Hello World" をデプロイ後、**Edit code** で `gapfade-signal-trigger.js` の中身を全コピペ → Save and Deploy
4. デプロイ後の URL をメモ（例: `https://gapfade-signal-trigger.aeon282499.workers.dev`）

※ CLIから入れるなら `cd cloudflare && npx wrangler deploy -c wrangler.gapfade.toml`（要 `wrangler login`）。
　その場合も `GITHUB_PAT` だけは `npx wrangler secret put GITHUB_PAT -c wrangler.gapfade.toml` で別途投入する。

## 2. 環境変数を設定

Worker の Settings → Variables and Secrets:

| Name | Value | 種別 |
|---|---|---|
| `GITHUB_PAT` | 既存 swing-signal-trigger と同じ fine-grained PAT | Secret |
| `GITHUB_OWNER` | `aeon282499-hash` | Plaintext |
| `GITHUB_REPO` | `-` | Plaintext |
| `WORKFLOW_FILE` | `gapfade.yml` | Plaintext |
| `GIT_REF` | `main` | Plaintext |

PAT は新規発行不要（swing/close と同じものを再利用）。Cloudflare は Secret の値を表示できないので、
分からなければ GitHub で fine-grained PAT を再発行して3つのWorkerすべてに貼り直す。

## 3. cron-job.org に新規ジョブを追加

- Title: `gapfade-signal-trigger`
- URL: 手順1でメモした Worker URL
- Schedule:
  - Days of week: **Mon〜Fri**
  - Time: **11:00 JST = 02:00 UTC**
  - 毎月毎日: every / every
- Request method: GET（Worker は fetch ハンドラなのでメソッド不問）

**なぜ11:00か**: 判定に必要な寄り値は9:15の15分足で確定済みなので、11:00でも中身は9:25と同じ。
配信からエントリー（後場寄り12:30）まで90分あり、昼休み（11:30〜12:30）に入る前に手元へ届く。
Worker実行は数秒＋Actions側の処理が5〜10分なので、**11:10には着弾**する。

## 4. 動作確認

- Worker URL をブラウザで叩く → `OK: dispatched gapfade.yml on main`
- GitHub Actions の "ギャップフェード 朝の候補配信" に手動Run履歴が出る
- 数分後に Discord の🩳チャンネルへ着弾（場が引けた後に叩くと「該当なし」になる）

## 5. 二重配信の心配は不要

`gapfade.py` に同日配信ガード（`gapfade_last_send.json`）を入れてあるので、
GitHub cron 4本 + 外部トリガーの何本が当たっても **配信は1日1回**。
最初に届いた1本が配信し、以降は「配信済み→スキップ」でログだけ残して終わる。
Discord へ実際に送信できた（HTTP 2xx）時だけマーカーを立てるので、webhook失効時は後続の保険が再挑戦する。
