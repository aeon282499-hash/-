# _deploy_combo.ps1 — 統合EA XMCombo をコンパイルし、_start.ini の[StartUp]を XMCombo に切替えて正常終了→再起動→起動確認
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
@("InpDemoOnly=false","InpStopBelowBalance=25000","InpGoldOn=true","InpGoldSymbol=GOLD.","InpGoldJpyPer001=50000","InpGoldLotMax=1.00","InpGoldStopUsd=10.0","InpGoldMaxSpread=40","InpGoldHourLon=10","InpGoldMinLon=15","InpGoldHoldMin=15","InpGoldMagic=20260908",
  "InpJpOn=true","InpJpSymbol=JP225Cash","InpJpJpyPerLot=7000","InpJpLotMax=500.0","InpJpEntryHour=15","InpJpExitHour=9","InpJpHoldWeekend=false","InpJpPrevNightFilter=true","InpJpPrevNightMax=0.0","InpJpMaxSpread=20","InpJpMagic=20260909","InpUsOn=true","InpUsSymbol=US500Cash","InpUsJpyPer01=40000","InpUsLotMax=50.0","InpUsMaxSpread=150","InpUsMagic=20260913","InpGxMode=1","InpGxMinBalance=200000","InpGxEntryHourSrv=1","InpGxEntryMinSrv=5","InpGxExitHourSrv=3","InpGxMaxSpread=25","InpGxJpyPer001=20000","InpGxLotMax=0.10","InpGxMagic=20260910") | Set-Content "$d\MQL5\Presets\XMCombo.set" -Encoding Unicode
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
