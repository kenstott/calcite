@echo off
rem Copyright (c) 2026 Kenneth Stott
rem Licensed under the Business Source License 1.1 (see LICENSE, NOTICE).
rem
rem pgwire-@ADAPTER@ pack launcher. The runtime it requires, @RUNTIME@, is installed beside it
rem under the same root (runtimes\@RUNTIME@). The caller's arguments follow the pack's flags and win.
setlocal
for %%i in ("%~dp0..") do set "HERE=%%~fi"
for %%i in ("%HERE%\..\..\..") do set "ROOT=%%~fi"
set "RUNTIME=%ROOT%\runtimes\@RUNTIME@"
if not exist "%RUNTIME%\runtime.json" (
  echo pgwire-@ADAPTER@ @VERSION@ requires pgwire-calcite runtime @RUNTIME@, which is not installed ^(expected at %RUNTIME%^). 1>&2
  exit /b 2
)
if not defined PGWIRE_CALCITE_STATE_DIR set "PGWIRE_CALCITE_STATE_DIR=%LOCALAPPDATA%\pgwire-@ADAPTER@"
call "%RUNTIME%\bin\pgwire-calcite-runtime.bat" --pack "%HERE%" %*
exit /b %ERRORLEVEL%
