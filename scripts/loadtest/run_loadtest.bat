@echo off
REM Easy-Chat Load Test Script

echo ====================================
echo   Easy-Chat Load Test
echo ====================================

REM 检查 Go 是否安装
where go >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Go is not installed
    exit /b 1
)

REM 检查 Docker 是否安装
where docker >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Docker is not installed
    exit /b 1
)

REM 设置参数
set BASE_URL=http://localhost:12112
set REQUESTS=10000
set CONCURRENCY=100

REM 解析参数
if not "%1"=="" set REQUESTS=%1
if not "%2"=="" set CONCURRENCY=%2

echo Target URL: %BASE_URL%
echo Total Requests: %REQUESTS%
echo Concurrency: %CONCURRENCY%
echo.

REM 初始化依赖
echo [1/3] Initializing dependencies...
cd /d %~dp0
go mod tidy
if %errorlevel% neq 0 (
    echo Error: Failed to initialize dependencies
    exit /b 1
)

REM 运行压测
echo [2/3] Running load test...
echo --------------------------------
go run http_loadtest.go -url %BASE_URL% -n %REQUESTS% -c %CONCURRENCY%

echo.
echo [3/3] Done!
echo.
echo Check metrics at: http://localhost:9091/metrics
echo Check Prometheus at: http://localhost:9090
echo Check Grafana at: http://localhost:3000 (admin/admin123)
