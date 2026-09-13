# XM自動売買の復旧手順（PCが壊れた/買い替えた時）

前提: 口座番号 70674546・サーバー XMTrading-MT5 3・パスワードは本人が保管（このリポジトリには無い）。
建玉はXMのサーバー側にあるので、PCが死んでも玉は消えない。急ぎなら XMアプリ/MT5モバイルで手動決済できる。

1. XM公式サイトから MT5 をインストール → ファイル→口座を開く→「XMTrading」で検索→Tradexfin Limited→サーバー「XMTrading-MT5 3」を登録→ログイン
2. このリポジトリを `Documents\my-first-project` に clone
3. PowerShell で `mt5\_deploy_combo.ps1` を実行（EAをコンパイル→.set生成→_start.ini生成→MT5を/config付きで再起動）
   - MetaEditorのパスは `C:\Program Files\XM Trading MT5\MetaEditor64.exe`、データフォルダIDは環境ごとに変わるので `$d` を新PCの `%APPDATA%\MetaQuotes\Terminal\<ID>` に直す
4. Python: `pip install MetaTrader5 pandas` → `python -X utf8 mt5\_fix_report.py --days 7` で接続確認
5. タスク: `mt5\tasks\*.xml` を `Register-ScheduledTask -Xml (Get-Content x.xml -Raw) -TaskName <名前>` で登録（Wake×4・Report×3）
6. スタートアップに `terminal64.exe /config:"...\mt5\_start.ini"` のショートカット
7. 電源: `powercfg /change standby-timeout-ac 0` `powercfg /change hibernate-timeout-ac 0`
8. MT5のログ `MQL5\Logs\<日付>.log` に「XMCombo 起動: ...」が出れば完了
