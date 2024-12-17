@echo off
echo Levantando el contenedor central...
docker-compose build central
docker-compose stop central --timeout 0
docker-compose up -d central
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor central.
    pause
    exit /b
)
echo Entrando a la consola del contenedor central...
docker exec -it easycab-central-1 /bin/bash -c "python ec_central.py kafka:9093 8000"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor central.
    pause
    exit /b
)
pause
