@echo off
echo Continue to delete all streams? [yes or no]
set/p confirm=

echo %confirm%

if "%confirm%"=="yes" (
    call base.bat com.bigdata.dis.sdk.demo.manager.DeleteAllApps
)