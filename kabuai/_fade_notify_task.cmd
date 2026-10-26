@echo off
rem Windows task launcher for fade_notify.py (ASCII only: cmd reads this file in the OEM codepage).
rem usage: _fade_notify_task.cmd evening|score|auto
set FADE_NOTIFY_DOTENV=%USERPROFILE%\Documents\my-first-project\.env
set FADE_NOTIFY_CACHE_DIR=%LOCALAPPDATA%\fade_notify_cache
cd /d "%~dp0"
set PY=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe
if not exist "%PY%" set PY=python
echo ---- %DATE% %TIME% mode=%1 >> "%~dp0_fade_notify.log"
"%PY%" -X utf8 fade_notify.py --mode %1 >> "%~dp0_fade_notify.log" 2>&1
