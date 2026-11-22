# 📉BB売り（GU株のBB40上抜け売り・前場）の場中通知と紙の台帳（bb_sell_live.py --live）を平日9:00に動かす Windows タスクを登録する（2026-11-22）
# FiboDaytrade と同じ書き方（cmd /c python ... >> ログ・対話ログオン時のみ・権限は標準）。本人が確認してから実行すること。
# 実行: powershell -ExecutionPolicy Bypass -File _register_bb_sell_task.ps1
$py = "C:\Users\星野\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$root = "C:\Users\星野\Documents\my-first-project"
$arg = "/c `"`"$py`" -X utf8 `"$root\bb_sell_live.py`" --live >> `"$root\live_flow\bb_sell_live.log`" 2>&1`""
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 9:00
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 7) -StartWhenAvailable
Register-ScheduledTask -TaskName "BBSellLive" -Action $action -Trigger $trigger -Settings $settings -Description "📉BB売りの場中通知（発注なし・紙）" -Force
Write-Host "登録しました: BBSellLive（平日 9:00）"
