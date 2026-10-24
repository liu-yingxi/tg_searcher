@echo off
rem One-click launcher for TG Searcher on Windows.
rem Uses -ExecutionPolicy Bypass for this invocation only.
setlocal
set "PS1=%~dp0start-bot.ps1"
if not exist "%PS1%" (
    echo ERROR: start-bot.ps1 was not found next to this file.
    pause
    exit /b 1
)
set "PS=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
if not exist "%PS%" set "PS=powershell.exe"
"%PS%" -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
exit /b %ERRORLEVEL%
