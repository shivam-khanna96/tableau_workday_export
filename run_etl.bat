@echo off
REM ============================================================
REM  Workday ETL Pipeline Runner
REM  Called by Windows Task Scheduler.
REM  Exit code mirrors Python: 0=success, 1=failure, 3=config error
REM ============================================================

REM Activate the virtual environment
call "%~dp0venv\Scripts\activate.bat"

REM Ensure working directory is the project root
cd /d "%~dp0"

REM Run the pipeline
python main.py
set ETL_EXIT=%ERRORLEVEL%

call deactivate

exit /b %ETL_EXIT%
