@echo off
REM Build the Crypto Daytrading Arena executable for Windows.
REM Requires: pip install pyinstaller
REM Output: dist\arena.exe

echo Building Crypto Daytrading Arena for Windows...

pip install pyinstaller 2>nul
pyinstaller launcher.spec --noconfirm

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Build successful! Executable at: dist\arena.exe
    echo.
    echo Usage:
    echo   dist\arena.exe                              # interactive wizard
    echo   dist\arena.exe --config arena_config.json   # headless launch
    echo   dist\arena.exe --teardown                   # stop Kafka
) else (
    echo Build failed. Check the output above for errors.
)
