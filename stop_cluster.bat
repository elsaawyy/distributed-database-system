@echo off
title Stopping Distributed Database Cluster
echo Stopping all cluster components...
echo.

taskkill /F /FI "WINDOWTITLE eq Reducer" > nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Master-Primary" > nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Master-Backup" > nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Worker-Go" > nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Worker-Python" > nul 2>&1

echo All components stopped successfully!
echo.
pause