# データカタログ（2026-08-03作成・J-Quantsスタンダード¥3,300）

ローカルデータ資産とAPIの棚卸し。**J-Quantsの履歴は10年ローリング窓＝毎日1日ずつ古い側が消える**ので、
「再取得不可」欄のファイルはこのPCにしか存在しない。PCバックアップ（デスクトップ★バックアップ実行.cmd）が
`Documents\my-first-project` を含むことを定期確認すること。*.pkl と大CSVは .gitignore＝GitHubには無い。

## 1. 再取得不可（消えたら終わり・バックアップ最優先）

| ファイル | サイズ | 中身 | 備考 |
|---|---|---|---|
| `jquants_cache_2016_2021.pkl` | 225MB | 全銘柄日足 2016-07-19〜2021-10-01 | 窓の外＝再取得不可。優先株修復済(2593は2016-08-10以降)。`.bak_prefstock`=修復前 |
| `_margin_10y_full.pkl` / `_margin_10y.pkl` | 49/30MB | 週次信用残10年(LongVol/ShrtVol/IssType) | 古い側から窓外に消えていく |
| `_indices_10y.pkl` | 11MB | 79指数10年(TOPIX=0000/33業種=0040系) | 同上。日経225は全プラン提供外 |
| `_short_ratio_10y.pkl` | 4MB | 33業種別空売り比率・日次10年 | 同上 |
| `_iss_type_by_year.pkl` | 小 | 貸借区分の年別 | **欠年あり(2019/2024/2025)→使う時は必ず近傍年補完(iss_for)** |
| `_intraday_cache_wide.pkl` / `_intraday_cache.pkl` | 97/13MB | yfinance分足(2026-05〜07) | yfinanceは60日しか遡れない＝実質再取得不可 |
| `market_calendar.csv` | 60KB | 東証公式営業日 2016-08-02〜2027-12-31 | git管理。HolDiv: 1=営業日/0=休場/3=祝日取引日 |
| `_short_positions_10y.pkl` / `_short_positions_fade.pkl` | 計約40MB | 空売り残高報告10年（決算907銘柄+フェード364銘柄・69万+18万報告・報告者別） | 2026-08-03取得。再取得は`fetch_short_positions.py <銘柄リスト> <出力>`（約30分/900銘柄） |

## 2. 再取得可能だが重い（スナップ済み・壊れたら作り直せる）

| ファイル | サイズ | 再生成 |
|---|---|---|
| `jquants_cache.pkl` | 225MB | 2021-10-04〜現在。`cache_jquants_update.py`が日次追記。優先株修復済(8/1)・`.bak_prefstock`=修復前 |
| `_fins_history.pkl` / `_fins_history_nodiv.pkl` | 82/71MB | /fins/summary 全銘柄履歴（決算システム用） |
| `_crashshort_events.csv` | **419MB** | 崩壊ショートのイベントスキャン（紙運用GO判断待ちの間は保持） |
| `_earnings_events_rich2.csv` ほか決算系CSV | 40MB級 | 決算BTイベント群（キャッシュから再生成可） |

## 3. BT派生プール（スクリプトで再生成可・使い捨てに近い）

- `_fade_pool_v5.pkl`（現行正・修復済キャッシュ由来）← `_bt_fade_pool_v5.py`。v2〜v4は旧世代
- `_bt10y_pool_wide.csv`（買い入口グリッド用・68,302件）← `_bt10y_pool_wide.py`
- `_probe_shortdc_atr.csv` / `_bt10y_candidates*.csv`（極み系の基準プール。**修復前キャッシュ由来＝
  優先株6銘柄の行が約23行汚染だが結論への影響は測定済みでゼロ。次の本格BTで再生成**）
- `_sell_wide2.pkl`（売り極みプール）・`_fade_deep.pkl`（旧フェード）ほか

## 4. 本番が毎日読む/書くもの（git管理・CIがコミット）

positions系（大/中/小×買売）・shadow_exit_*.json・kiwami_sell.json・today_signals*（+_kiwami）・
day_signals.json・positions_day_paper.json・gapfade_ledger.json・earnings_calendar.json・
jpx_earnings_schedule.json・sector33_map.json・trade_history系

## 5. API棚卸し（2026-08-03実測プローブ）

**開通（スタンダード）:**
- `/equities/master`・`/equities/bars/daily`（調整済OHLCV）・`/equities/earnings-calendar`
- `/fins/summary`
- `/markets/margin-interest`（週次信用残）・`/markets/short-ratio`（業種別空売り比率・日次）
- `/markets/margin-alert` ＝ **日々公表信用取引残高**。規制フラグ(PubReason)だけでなく
  **ShrtOut/LongOut等の残高列あり**＝「日々公表信用残」フロンティアはここに入っている（未活用）
- `/markets/short-sale-report` ＝ **空売り残高報告（0.5%以上の大口・報告者名/ファンド名つき）**。
  params: code / disc_date / calc_date。全銘柄10年の一括スナップはコード別×約4千×1.2s≒2時間
  ＝**使う検証を始める時に取る**（未スナップ）
- `/markets/calendar` ＝ 公式営業日（2027年末まで先も入る）→ market_calendar.csv にスナップ済み
- `/indices/bars/daily`（79指数）

**プレミアム専用（403 "not available on your subscription"）:**
`/markets/breakdown`（売買内訳）・`/fins/details`・`/fins/dividend`・前場/先物/オプション系

## 6. 運用ルール

1. 新しい10年データを使い始めたら**必ず即pklスナップ**（窓は毎日進む）
2. `.bak_prefstock` 2本(450MB)は優先株修復(2026-08-01)の修復前バックアップ。数週間問題なければ消してよい
3. プール(§3)は「どのキャッシュから作ったか」をスクリプト冒頭に書く。土台(キャッシュ)を直したら再生成
4. 検証で否定されたデータ軸は捨てない（短期で消えるのはスクリプトだけ・データは再利用される）
