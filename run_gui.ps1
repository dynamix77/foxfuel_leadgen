# Foxfuel Lead Generation System - GUI Launcher (PowerShell)
# Right-click and select "Run with PowerShell" to start the GUI

Set-Location $PSScriptRoot
python -m src.gui.main_window

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to start GUI application." -ForegroundColor Red
    Write-Host "Please ensure Python is installed and dependencies are installed." -ForegroundColor Yellow
    Write-Host "Run: pip install -r requirements.txt" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
}

