@echo off
rem Copyright (c) 2026 Kenneth Stott
rem Licensed under the Business Source License 1.1 (see LICENSE, NOTICE).
rem
rem pgwire-calcite runtime launcher (runtimes\<version>\bin\pgwire-calcite-runtime.bat). A pack's
rem launcher starts it with --pack <pack-dir>; it runs the server from this runtime's JRE and CPython.
setlocal
for %%i in ("%~dp0..") do set "HERE=%%~fi"
set "PGWIRE_CALCITE_RUNTIME_HOME=%HERE%"
set "PGWIRE_CALCITE_JAVA_HOME=%HERE%\jre"
rem The venv's launcher stub (cpython\Scripts\python.exe) reads pyvenv.cfg's `home` line at every
rem invocation to find the interpreter. The runtime ships a placeholder there; resolve it to this
rem install's path before every launch, since that varies per machine.
for /d %%d in ("%HERE%\python\cpython-*") do set "PYHOME=%%~fd"
if defined PYHOME powershell -NoProfile -Command "(Get-Content -Raw '%HERE%\cpython\pyvenv.cfg') -replace [regex]::Escape('__PGWIRE_PYTHON_HOME__'), '%PYHOME%' | Set-Content -NoNewline '%HERE%\cpython\pyvenv.cfg'"
"%HERE%\jre\bin\java.exe" "-Dpgwire.python=%HERE%\cpython\Scripts\python.exe" -jar "%HERE%\pgwire-launcher.jar" %*
exit /b %ERRORLEVEL%
