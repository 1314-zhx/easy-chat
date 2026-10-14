@echo off
REM 下载 Jaeger MongoDB Storage Plugin (Windows)

set PLUGIN_DIR=%~dp0jaeger-plugin
mkdir "%PLUGIN_DIR%" 2>nul

echo 下载 Jaeger MongoDB Storage Plugin...

REM Windows 下需要手动下载，请访问：
REM https://github.com/jaegertracing/jaeger-storage-mongodb/releases

echo.
echo 请手动下载并解压到 %PLUGIN_DIR% 目录
echo 下载地址: https://github.com/jaegertracing/jaeger-storage-mongodb/releases
echo.
echo Linux/Mac 用户可以运行: bash scripts/download-jaeger-plugin.sh
pause
