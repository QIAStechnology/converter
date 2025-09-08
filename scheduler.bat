@echo off
REM Go to project folder
cd /d "C:\Users\bello\Desktop\QIAS\neccesary files mx business\converter\converter"

REM Run Python script and log output
"C:/Users/bello/AppData/Local/Programs/Python/Python310/python.exe" "csv_to_mx_business.py" >> sync_log.txt 2>&1
