@echo off
REM Foxfuel Lead Generation System - GUI Launcher
REM Double-click this file to start the GUI application

cd /d "%~dp0"
python -m src.gui.main_window

if errorlevel 1 (
    echo.
    echo Error: Failed to start GUI application.
    echo Please ensure Python is installed and dependencies are installed.
    echo Run: pip install -r requirements.txt
    pause
)

