@echo off
setlocal
set APP_HOME=%~dp0
set GRADLE_VERSION=9.5.1
set GRADLE_BIN=%APP_HOME%.gradle\bootstrap\gradle-%GRADLE_VERSION%\bin\gradle.bat
if exist "%GRADLE_BIN%" (
  call "%GRADLE_BIN%" %*
  exit /b %ERRORLEVEL%
)
where gradle >NUL 2>NUL
if %ERRORLEVEL%==0 (
  gradle %*
  exit /b %ERRORLEVEL%
)
set ZIP=%APP_HOME%.gradle\bootstrap\gradle-%GRADLE_VERSION%-bin.zip
if not exist "%APP_HOME%.gradle\bootstrap" mkdir "%APP_HOME%.gradle\bootstrap"
powershell -NoProfile -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://services.gradle.org/distributions/gradle-%GRADLE_VERSION%-bin.zip' -OutFile '%ZIP%'"
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Force '%ZIP%' '%APP_HOME%.gradle\bootstrap'"
call "%GRADLE_BIN%" %*
exit /b %ERRORLEVEL%
