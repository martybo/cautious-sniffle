@echo off
setlocal ENABLEEXTENSIONS

REM === Base dir (this BAT’s folder) ===
set "BASEDIR=%~dp0"
pushd "%BASEDIR%" 1>nul 2>nul

echo ===============================================================
echo  TRUE Shortage Report
echo  Started   : %DATE% %TIME%
echo  Working   : %CD%
echo ===============================================================

REM === Ensure output folder exists ===
if not exist "output" mkdir "output"

REM === Pick Python ===
set "PYEXE="
where py >nul 2>&1 && set "PYEXE=py -3"
if not defined PYEXE (
  where python >nul 2>&1 && set "PYEXE=python"
)
if not defined PYEXE (
  echo [ERROR] Python not found on PATH.
  pause
  exit /b 1
)

REM === Inputs ===
set "ORDERS=Orders Report Generator.csv"
set "PRODUCT=ExportFullProductList.csv"
set "SUBS=Substitutions.csv"
set "ALIAS=BranchAlias.csv"
set "DISP=disp report.csv"

REM === Script ===
set "SCRIPT=warehouse_shortage_report_v2g.py"

REM === Lists ===
set "WH_OL=Warehouse;Medicare Warehouse;Xmas Warehouse;Perfumes;Warehouse CDs;Warehouse - CD Products"
set "NC_OL=Warehouse;Warehouse - CD Products"

REM === Log file (simple timestamp) ===
for /f "tokens=1-3 delims=/ " %%a in ("%DATE%") do set "D=%%c-%%b-%%a"
for /f "tokens=1-2 delims=:." %%h in ("%TIME%") do set "T=%%h-%%i"
set "LOG=output\run_%D%_%T%.log"

echo Python    : %PYEXE%
echo Script    : %SCRIPT%
echo Inputs    : "%ORDERS%" + "%PRODUCT%" + "%SUBS%" + "%DISP%"
echo WH-OLs    : %WH_OL%
echo NC-OLs    : %NC_OL%
echo Alias CSV : %ALIAS%
echo Disp CSV  : %DISP%
echo Log file  : %LOG%
echo.

if not exist "%SCRIPT%" echo [ERROR] Missing script "%SCRIPT%" & pause & exit /b 2
if not exist "%ORDERS%" echo [ERROR] Missing "%ORDERS%" & pause & exit /b 3
if not exist "%PRODUCT%" echo [ERROR] Missing "%PRODUCT%" & pause & exit /b 4
if not exist "%SUBS%" echo [WARN] Missing "%SUBS%" (continuing without substitutions)
if not exist "%DISP%" echo [WARN] Missing "%DISP%" (continuing without dispensary report lookups)

REM === Build command (NO PowerShell piping; pure cmd redirection) ===
set "CMD=%PYEXE% "%SCRIPT%" --orders "%ORDERS%" --product-list "%PRODUCT%" --warehouse-orderlists "%WH_OL%" --nc-orderlists "%NC_OL%" --out "output\Shortage_Report.xlsx""

if exist "%SUBS%" set "CMD=%CMD% --subs "%SUBS%""
if exist "%ALIAS%" set "CMD=%CMD% --branch-alias-csv "%ALIAS%""
if exist "%DISP%" set "CMD=%CMD% --disp-report "%DISP%""

echo Running:
echo   %CMD%
echo.

REM Write both console and file logs
%CMD% 1>>"%LOG%" 2>>&1
set "EC=%ERRORLEVEL%"

echo.
echo ExitCode=%EC%
if not "%EC%"=="0" (
  echo [ERROR] Script returned non-zero exit code. See: "%LOG%"
  pause
  exit /b %EC%
)

REM Open newest Shortage_Report*.xlsx
for /f "delims=" %%F in ('dir /b /a:-d /o:-d "output\Shortage_Report*.xlsx" 2^>nul') do (
  set "LATEST=output\%%F"
  goto :Found
)

echo [INFO] No Excel file found in output\
goto :End

:Found
echo Opening "%LATEST%" ...
start "" "%LATEST%"

:End
echo.
pause
endlocal
