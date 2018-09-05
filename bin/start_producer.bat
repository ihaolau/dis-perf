@echo off
chcp 936
mode 120,35
for /F "delims=" %%X in ('where powershell') DO set PSHELL=%%X
if not "%PSHELL%" == "" (
    powershell -command "&{$H=get-host;$W=$H.ui.rawui;$B=$W.buffersize;$B.width=120;$B.height=999;$W.buffersize=$B;}"
)
title DIS Producer

cd /d "%~dp0"
cd ..

set MAIN_CLASS=com.bigdata.dis.sdk.demo.producer.AppProducer

set JAVACMD=java
set JAVA_START_HEAP=2048m
set JAVA_MAX_HEAP=2048m

for /F "delims=" %%X in ('where java') DO set JAVA_PATH=%%X
if "%JAVA_PATH%" == "" (
    echo "No java found."
    pause
    exit 1
)

set LIB_DIR=.\lib
set CONFIG_DIR=.
set JVM_ARGS=-Xms%JAVA_START_HEAP% -Xmx%JAVA_MAX_HEAP% -Djava.io.tmpdir=%LIB_DIR% %JVM_ARGS%

%JAVACMD% %JVM_ARGS% %JVM_DBG_OPTS% -Dfile.encoding="UTF-8" -cp "%LIB_DIR%\*;%CONFIG_DIR%" %MAIN_CLASS% %MAIN_CLASS_ARGS%
pause
