@echo off
setlocal

set PROJECT_DIR=c:\Users\Michael\python_projects\Masha_laba
set LOG=%PROJECT_DIR%\dbt_run.log

cd /d "%PROJECT_DIR%"
call venv\Scripts\activate.bat

echo. >> "%LOG%"
echo ====== dbt run started: %DATE% %TIME% ====== >> "%LOG%"

cd jaffle_shop

dbt seed --profiles-dir . >> "%LOG%" 2>&1
dbt run  --profiles-dir . >> "%LOG%" 2>&1
dbt test --profiles-dir . >> "%LOG%" 2>&1

echo ====== dbt run finished: %DATE% %TIME% ====== >> "%LOG%"
endlocal
