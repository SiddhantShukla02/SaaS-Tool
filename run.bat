@echo off

set CELL=%1

if "%CELL%"=="" (
    echo Please provide a cell number. Example: run 03
    exit /b
)

for %%f in (stages\cells\cell_%CELL%_*.py) do (
    set FILE=%%~nf
    goto runfile
)

echo No matching file found for cell %CELL%
exit /b

:runfile
python -m stages.cells.%FILE%