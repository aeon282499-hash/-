# 🌙 キオクシア打法（kioxia_overnight.py・紙・発注なし）の Windows タスク2本を登録する（2026-10-08）
# 実行: powershell -ExecutionPolicy Bypass -File _register_kioxia_tasks.ps1
$py = "C:\Users\星野\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$root = "C:\Users\星野\Documents\my-first-project"
$days = @("Monday","Tuesday","Wednesday","Thursday","Friday")
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -StartWhenAvailable

$argJ = "/c `"`"$py`" -X utf8 `"$root\kioxia_overnight.py`" --judge >> `"$root\live_flow\kioxia_overnight.log`" 2>&1`""
$actJ = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $argJ -WorkingDirectory $root
$trgJ = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At 15:20
Register-ScheduledTask -TaskName "KioxiaOvernightJudge" -Action $actJ -Trigger $trgJ -Settings $settings -Description "🌙 キオクシア打法 15:20判定（紙・発注なし）" -Force | Out-Null
Write-Host "登録しました: KioxiaOvernightJudge（平日 15:20）"

$argS = "/c `"`"$py`" -X utf8 `"$root\kioxia_overnight.py`" --settle >> `"$root\live_flow\kioxia_overnight.log`" 2>&1`""
$actS = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $argS -WorkingDirectory $root
$trgS = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At 9:12
Register-ScheduledTask -TaskName "KioxiaOvernightSettle" -Action $actS -Trigger $trgS -Settings $settings -Description "🌙 キオクシア打法 翌朝9:12 寄りで紙決済" -Force | Out-Null
Write-Host "登録しました: KioxiaOvernightSettle（平日 9:12）"
