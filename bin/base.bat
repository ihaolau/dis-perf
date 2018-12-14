@echo off
chcp 936
mode 120,35
for /F "delims=" %%X in ('where powershell') DO set PSHELL=%%X
if not "%PSHELL%" == "" (
    powershell -command "&{$H=get-host;$W=$H.ui.rawui;$B=$W.buffersize;$B.width=120;$B.height=999;$W.buffersize=$B;}"
)
set name=%1
set start_heap=%2
set max_heap=%3

if not defined start_heap (
    set start_heap=256m
)

if not defined max_heap (
    set max_heap=512m
)

title DIS %name%

cd /d "%~dp0"
cd ..

set MAIN_CLASS=%name:"=%

set JAVACMD=java
set JAVA_START_HEAP=%start_heap%
set JAVA_MAX_HEAP=%max_heap%

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
