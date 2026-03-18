@echo off
REM ============================================================
REM  One-time setup: registers the ETL as a Windows Scheduled Task.
REM  Run this script once as Administrator.
REM  The task runs daily at 05:00 AM using your current login.
REM ============================================================

set TASK_NAME=Tableau ETL - Workday Daily
set SCRIPT_PATH=%~dp0run_etl.bat

echo Creating scheduled task: "%TASK_NAME%"
echo Script: %SCRIPT_PATH%

schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%SCRIPT_PATH%\"" ^
  /sc daily ^
  /st 05:00 ^
  /ru "%USERDOMAIN%\%USERNAME%" ^
  /it ^
  /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Task created successfully.
    echo.
    echo Next steps in Task Scheduler ^(taskschd.msc^):
    echo   1. Open the task and go to Conditions tab.
    echo   2. Check "Start only if the following network connection is available".
    echo   3. Go to Settings tab:
    echo      - Set "Stop the task if it runs longer than: 2 hours"
    echo      - Set "If the task is already running: Do not start a new instance"
    echo   4. Go to Settings ^> "If the task fails, restart every: 30 minutes, up to 2 times"
    echo.
    echo To test immediately:
    echo   schtasks /run /tn "%TASK_NAME%"
) else (
    echo.
    echo ERROR: Failed to create scheduled task. Try running as Administrator.
)

pause
