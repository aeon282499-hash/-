@echo off
rem Windows task launcher for fade_notify.py (ASCII only: cmd reads this file in the OEM codepage).
rem usage: _fade_notify_task.cmd evening|score|auto
rem PC is the backup of the GitHub run (main / fade_notify.yml sends at 18:45 JST):
rem take the ledger from origin/main first, so a day GitHub already sent is skipped by the sent-guard.
set FADE_NOTIFY_DOTENV=%USERPROFILE%\Documents\my-first-project\.env
set FADE_NOTIFY_CACHE_DIR=%LOCALAPPDATA%\fade_notify_cache
cd /d "%~dp0"
set PY=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe
if not exist "%PY%" set PY=python
echo ---- %DATE% %TIME% mode=%1 >> "%~dp0_fade_notify.log"
git fetch -q origin main >> "%~dp0_fade_notify.log" 2>&1
git show origin/main:kabuai/fade_notify_ledger.json > "%~dp0_ledger_remote.tmp" 2>> "%~dp0_fade_notify.log"
if %ERRORLEVEL%==0 (
  move /y "%~dp0_ledger_remote.tmp" "%~dp0fade_notify_ledger.json" > nul
  echo ledger: took origin/main >> "%~dp0_fade_notify.log"
) else (
  del "%~dp0_ledger_remote.tmp" 2> nul
  echo ledger: origin/main not readable, using local >> "%~dp0_fade_notify.log"
)
"%PY%" -X utf8 fade_notify.py --mode %1 >> "%~dp0_fade_notify.log" 2>&1
