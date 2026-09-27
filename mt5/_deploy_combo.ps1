# _deploy_combo.ps1 — 本番(9/28 02:37 配備・本人「儲かる軸をふやして実装して」): 9/27の本番＋金のドル衝撃(G3・InpDsMode=2・買いだけ・0.01lot固定・SL$30・30分保有・起動時の自己テスト1440本=Pythonの双子と差5e-7で一致)。_deploy_combo_pending.ps1 と同一内容。
# 統合EA XMCombo をコンパイルし、_start.ini の[StartUp]を XMCombo に切替えて正常終了→再起動→起動確認

$code = @'

using System; using System.Runtime.InteropServices;

public class GW2 { [DllImport("user32.dll")] public static extern bool PostMessage(IntPtr h, uint m, IntPtr w, IntPtr l); }

'@

Add-Type -TypeDefinition $code

$d = "$env:APPDATA\MetaQuotes\Terminal\C4171FD2B38378D6406D5C84412B5F20"

$src = $PSScriptRoot

Copy-Item "$src\XMCombo.mq5" "$d\MQL5\Experts\Hoshino\" -Force

$log = "$src\compile_combo.log"

if (Test-Path $log) { Remove-Item $log }

Start-Process "C:\Program Files\XM Trading MT5\MetaEditor64.exe" -ArgumentList "/compile:`"$d\MQL5\Experts\Hoshino\XMCombo.mq5`"", "/log:`"$log`"" -Wait

Get-Content $log -Encoding Unicode | Select-String "error|Result"

@("InpDemoOnly=false","InpStopBelowBalance=25000","InpGoldOn=true","InpGoldSymbol=GOLD.","InpGoldJpyPer001=12500","InpGoldLotMax=1.00","InpGoldStopUsd=10.0","InpGoldCostUsd=0.593","InpGoldMaxSpread=40","InpGoldHourLon=10","InpGoldMinLon=19","InpGoldHoldMin=13","InpGoldMagic=20260908","InpGoldGateN=40","InpGoldGateThr=0.00","InpGoldPmGateThr=0.00","InpGoldVolOrThr=22","InpGoldPmVolOrThr=25","InpGoldPmUpSkipPct=0.5","InpGoldMonMult=2.0","InpGoldFriMult=0.5","InpSkipUkHoliday=true","InpGoldPmOn=true","InpGoldPmHourLon=14","InpGoldPmMinLon=53","InpGoldPmHoldMin=10","InpGoldPmMagic=20260919","InpScale1Bal=100000","InpScale1=1.25","InpScale2Bal=300000","InpScale2=1.5","InpScale3Bal=0","InpScale3=2.0",

  "InpJpOn=true","InpJpSymbol=JP225Cash","InpJpJpyPerLot=8750","InpJpLotMax=500.0","InpJpEntryHour=15","InpJpExitHour=9","InpJpHoldWeekend=false","InpJpPrevNightFilter=true","InpJpPrevNightMax=0.0","InpJpMondayFree=true","InpJpMaxSpread=20","InpJpMagic=20260909","InpJpAddHour=1","InpJpAddPct=-0.5","InpJpAddMult=1.0","InpUsOn=true","InpUsSymbol=US100Cash","InpUsJpyPer01=50000","InpUsLotMax=50.0","InpUsMaxSpread=600","InpUsEntryHour=21","InpUsExitHour=19","InpUsMagic=20260924","InpDeOn=true","InpDeSymbol=GER40Cash","InpDeJpyPer01=65000","InpDeLotMax=50.0","InpDeMaxSpread=400","InpDeEntryHour=1","InpDeExitHour=16","InpDeMagic=20260914","InpDePrevWeekend=true","InpGdOn=false","InpGdJpyPer001=120000","InpGdLotMax=1.00","InpGdMaxSpread=40","InpGdEntryHour=4","InpGdExitHour=22","InpGdMagic=20260915","InpGxMode=0","InpGxMinBalance=200000","InpGxEntryHourSrv=1","InpGxEntryMinSrv=5","InpGxExitHourSrv=3","InpGxMaxSpread=25","InpGxJpyPer001=20000","InpGxLotMax=0.10","InpGxMagic=20260910","InpRnOn=true","InpRnStep=100.0","InpRnDelta=1.0","InpRnArmMin=60","InpRnHoldMin=15","InpRnStopUsd=10.0","InpRnLot=0.02","InpRnMaxSpread=40","InpRnStartSrvMin=120","InpRnEndSrvMin=1364","InpRnMagic=20260927","InpDsMode=2","InpDsSide=1","InpDsLot=0.01","InpDsK=4.0","InpDsLookMin=15","InpDsHoldMin=30","InpDsDeclMin=15","InpDsWinMin=28800","InpDsMinN=3000","InpDsStopUsd=30.0","InpDsMaxSpread=40","InpDsValidStartMin=120","InpDsValidEndMin=1379","InpDsTradeEndMin=1349","InpDsSelfTestMin=1440","InpDsEurUsd=EURUSD.","InpDsUsdChf=USDCHF.","InpDsUsdJpy=USDJPY.","InpDsMagic=20260928") | Set-Content "$d\MQL5\Presets\XMCombo.set" -Encoding Unicode

$ini = "$src\_start.ini"

@("[Common]", "Login=70674546", "Server=XMTrading-MT5 3", "NewsEnable=0",

  "[Experts]", "AllowLiveTrading=1", "AllowDllImport=0", "Enabled=1", "Account=1", "Profile=1",

  "[StartUp]", "Symbol=GOLD.", "Period=M15", "Expert=Hoshino\XMCombo", "ExpertParameters=XMCombo.set") | Set-Content $ini -Encoding ASCII

$p = Get-Process terminal64 -ErrorAction SilentlyContinue

if ($p) { [GW2]::PostMessage($p.MainWindowHandle, 0x10, [IntPtr]::Zero, [IntPtr]::Zero) | Out-Null; $p.WaitForExit(30000) | Out-Null; if (-not $p.HasExited) { $p | Stop-Process -Force } }

Start-Sleep 4

Start-Process "C:\Program Files\XM Trading MT5\terminal64.exe" -ArgumentList "/config:`"$ini`""

Start-Sleep 60

"TITLE: " + (Get-Process terminal64 | Select-Object -Expand MainWindowTitle)

Get-ChildItem "$d\logs" | Where-Object { $_.Name -match '^\d{8}\.log$' } | Sort-Object LastWriteTime | Select-Object -Last 1 | ForEach-Object { Get-Content $_.FullName -Encoding Unicode | Select-String "loaded successfully|removed|Startup" | Select-Object -Last 4 }

"--- experts ---"

Get-ChildItem "$d\MQL5\Logs" | Sort-Object LastWriteTime | Select-Object -Last 1 | ForEach-Object { Get-Content $_.FullName -Encoding Unicode | Select-String "XMCombo" | Select-Object -Last 2 }

