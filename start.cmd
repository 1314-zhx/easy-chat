@echo off
cd /d D:\easy-chat

echo ========================================
echo   Starting Easy-Chat Services
echo ========================================
echo.

echo [Step 1] Starting User RPC (base service)...
start "user-rpc" cmd /c "cd /d D:\easy-chat\apps\user\rpc && echo === user-rpc === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 2] Starting User API...
start "user-api" cmd /c "cd /d D:\easy-chat\apps\user\api && echo === user-api === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 3] Starting Social RPC...
start "social-rpc" cmd /c "cd /d D:\easy-chat\apps\social\rpc && echo === social-rpc === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 4] Starting Social API...
start "social-api" cmd /c "cd /d D:\easy-chat\apps\social\api && echo === social-api === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 5] Starting IM RPC...
start "im-rpc" cmd /c "cd /d D:\easy-chat\apps\im\rpc && echo === im-rpc === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 6] Starting IM WS...
start "im-ws" cmd /c "cd /d D:\easy-chat\apps\im\ws && echo === im-ws === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 7] Starting AI RPC...
start "ai-rpc" cmd /c "cd /d D:\easy-chat\apps\ai\rpc && echo === ai-rpc === && go run ."
timeout /t 3 /nobreak >nul

echo [Step 8] Starting Task MQ (depends on all above)...
start "task-mq" cmd /c "cd /d D:\easy-chat\apps\task\mq && echo === task-mq === && go run ."

echo.
echo ========================================
echo   All services started!
echo   Note: Some services may retry connections
echo ========================================
echo.
pause
