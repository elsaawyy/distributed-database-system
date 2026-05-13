@echo off
title Distributed Database Cluster Manager
echo ========================================
echo   Starting Distributed Database Cluster
echo ========================================
echo.

REM Check if MySQL is running
echo [1/6] Checking MySQL...
mysql -u root -P 3309 -e "SELECT 1" > nul 2>&1
if errorlevel 1 (
    echo ERROR: MySQL is not running on port 3309
    echo Please start MySQL in XAMPP first
    pause
    exit /b 1
)
echo MySQL is running OK
echo.

REM Start Reducer
echo [2/6] Starting Reducer on port 8090...
start "Reducer" cmd /k "cd /d \"C:\Users\target\Desktop\8th semester\DDB\new and last one\distributed-db\reducer\" && go run main.go"
timeout /t 3 > nul

REM Start Primary Master (will acquire lock)
echo [3/6] Starting Primary Master on port 8080...
start "Master-Primary" cmd /k "cd /d \"C:\Users\target\Desktop\8th semester\DDB\new and last one\distributed-db\master\" && go run main.go"
timeout /t 5 > nul

REM Start Backup Master (will be standby)
echo [4/6] Starting Backup Master on port 8081...
start "Master-Backup" cmd /k "cd /d \"C:\Users\target\Desktop\8th semester\DDB\new and last one\distributed-db\master\" && set PORT=8081 && go run main.go"
timeout /t 3 > nul

REM Start Go Worker
echo [5/6] Starting Go Worker on port 8081...
start "Worker-Go" cmd /k "cd /d \"C:\Users\target\Desktop\8th semester\DDB\new and last one\distributed-db\worker-go\" && go run main.go"
timeout /t 2 > nul

REM Start Python Worker
echo [6/6] Starting Python Worker on port 8082...
start "Worker-Python" cmd /k "cd /d \"C:\Users\target\Desktop\8th semester\DDB\new and last one\distributed-db\worker-py\" && python app.py"

echo.
echo ========================================
echo   ALL COMPONENTS STARTED
echo ========================================
echo Master Primary:    http://localhost:8080
echo Master Backup:     http://localhost:8081
echo Reducer:           http://localhost:8090
echo Go Worker:         http://localhost:8081
echo Python Worker:     http://localhost:8082
echo.
echo Note: Backup master will be in standby mode
echo       It will take over if primary fails
echo.
pause