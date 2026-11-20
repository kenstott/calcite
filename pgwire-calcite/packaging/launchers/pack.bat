@echo off
rem Copyright (c) 2026 Kenneth Stott
rem Licensed under the Business Source License 1.1 (see LICENSE, NOTICE).
rem
rem pgwire-@ADAPTER@ pack launcher. It starts the newest pgwire-calcite runtime installed on
rem runtime line @LINE@ under runtimes\ beside the packs. The caller's arguments follow the pack's
rem flags and win.
setlocal EnableDelayedExpansion
for %%i in ("%~dp0..") do set "HERE=%%~fi"
rem A package manager that installs the runtime in its own prefix names its runtimes directory
rem (REQ-2254, amended 2026-11-20: one runtime formula plus one formula per pack).
if defined PGWIRE_CALCITE_RUNTIMES (
  set "RUNTIMES=!PGWIRE_CALCITE_RUNTIMES!"
) else (
  for %%i in ("%HERE%\..\..\..") do set "RUNTIMES=%%~fi\runtimes"
)
set "NEWEST="
set /a NEWEST_KEY=-1
for /d %%d in ("%RUNTIMES%\*") do (
  if exist "%%d\runtime.json" (
    findstr /L /C:"\"line\": @LINE@," "%%d\runtime.json" >nul 2>&1
    if not errorlevel 1 (
      for /f "tokens=1-3 delims=." %%a in ("%%~nxd") do set /a KEY=%%a*1000000+%%b*1000+%%c
      if !KEY! gtr !NEWEST_KEY! (
        set /a NEWEST_KEY=!KEY!
        set "NEWEST=%%d"
      )
    )
  )
)
if not defined NEWEST (
  echo pgwire-@ADAPTER@ @VERSION@ requires a pgwire-calcite runtime on line @LINE@, and none is installed under !RUNTIMES!. 1>&2
  exit /b 2
)
rem The caller's arguments reach the runtime as given: a "!" in one is not expanded.
setlocal DisableDelayedExpansion
if not defined PGWIRE_CALCITE_STATE_DIR set "PGWIRE_CALCITE_STATE_DIR=%LOCALAPPDATA%\pgwire-@ADAPTER@"
call "%NEWEST%\bin\pgwire-calcite-runtime.bat" --pack "%HERE%" %*
exit /b %ERRORLEVEL%
