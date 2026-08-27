# evening-signal-trigger デプロイ手順（所要5分）

## なぜ必要か

2026-08-27夜、`schedule_evening.yml`（スイング前夜配信＝通常版＋極み＋売りフェードの3系統）の
GitHub cron が**全本とも黙ってスキップ**され、前夜配信が丸ごと欠落した（23:38に手動dispatchで復旧）。

さらに 2026-08-28 に着弾を 21:00 → **18:50** へ前倒し（本人依頼「ハイカラが19時スタートだから取りたい」）。
「18:50必達」は実測40〜50分遅延のGitHub cronでは不可能＝朝8:05と同じ外部トリガー経路が必須。

| 経路 | 設計時刻 | 実際の起動 |
|---|---|---|
| GitHub cron（schedule_evening.yml 旧19:30/20:05） | 19:30 | 19:56〜20:37（毎晩40〜90分遅延・**8/27は無発火**） |
| cron-job.org → Cloudflare Worker（朝8:05実績） | 8:05 | **8:05（毎日1分もズレない）** |

## 1. Cloudflare Worker を作成

1. Cloudflare ダッシュボード → Workers & Pages → Create Worker
2. 名前: `evening-signal-trigger`
3. デフォルトの "Hello World" をデプロイ後、**Edit code** で `evening-signal-trigger.js` の中身を全コピペ → Save and Deploy
4. デプロイ後の URL をメモ（例: `https://evening-signal-trigger.aeon282499.workers.dev`）

※ CLIから入れるなら `cd cloudflare && npx wrangler deploy -c wrangler.evening.toml`（要 `wrangler login`）。
　その場合も `GITHUB_PAT` だけは `npx wrangler secret put GITHUB_PAT -c wrangler.evening.toml` で別途投入する。

## 2. 環境変数を設定

Worker の Settings → Variables and Secrets:

| Name | Value | 種別 |
|---|---|---|
| `GITHUB_PAT` | 既存 swing-signal-trigger と同じ fine-grained PAT | Secret |
| `GITHUB_OWNER` | `aeon282499-hash` | Plaintext |
| `GITHUB_REPO` | `-` | Plaintext |
| `WORKFLOW_FILE` | `schedule_evening.yml` | Plaintext |
| `GIT_REF` | `main` | Plaintext |

PAT は新規発行不要（swing/close/gapfade と同じものを再利用）。Cloudflare は Secret の値を表示できないので、
分からなければ GitHub で fine-grained PAT を再発行して全Workerに貼り直す。

## 3. cron-job.org に新規ジョブを追加

- Title: `evening-signal-trigger`
- URL: 手順1でメモした Worker URL
- Schedule:
  - Days of week: **Mon〜Fri**
  - Time: **18:05 JST = 09:05 UTC**
  - 毎月毎日: every / every
- Request method: GET（Worker は fetch ハンドラなのでメソッド不問）

**なぜ18:05か**: 判定材料（四本値）は当日16:30頃にJ-Quants公開済み（公式仕様）＝18時台でも
朝ラン・21時ランと同一銘柄。18:05起動→スイング着弾≒18:35→フェード着弾≒18:45-50で、
**19:00のSBIハイカラ(HYPER)在庫解禁**に間に合う。
唯一の注意: 売り禁(margin-alert)の当日公表分だけは18時台に未反映の可能性があり、その場合は
前日分へ自動フォールバック（🚫バッジとプレミアム料行のみ影響・配信は止まらない）。
反映状況はランのログ `alert_map: <日付> 公表分` が当日日付かどうかで確認できる。

## 4. 動作確認

- Worker URL をブラウザで叩く → `OK: dispatched schedule_evening.yml on main`
- GitHub Actions の "スイング前夜配信" に手動Run履歴が出る
- 18時台に叩けば待機なしで実行、昼間に叩くと18:05まで待機してから実行（main.py側にも17時ガードあり）

## 5. 二重配信の心配は不要

GitHub cron保険3本（18:10/19:00/21:15 JST設計）＋外部トリガーの何本が当たっても、
concurrency直列化＋today_signals.jsonの送信済みガードで**配信は1日1回**。
最初に届いた1本が配信し、以降は「配信済み→スキップ」でログだけ残して終わる。
