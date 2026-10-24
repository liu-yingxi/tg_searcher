@echo off
rem Wrapper for package-windows.ps1 that bypasses the current execution policy
rem for this single invocation only (no system-wide policy change).
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0package-windows.ps1" %*
exit /b %ERRORLEVEL%
