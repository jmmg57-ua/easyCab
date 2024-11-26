@echo off
echo Levantando el contenedor sensor...
docker-compose stop sensor1 --timeout 0
docker-compose up -d sensor1
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor sensor1.
    pause
    exit /b
)
echo Entrando a la consola del contenedor central...
docker exec -it easycab-sensor1-1 /bin/bash -c "python ec_s.py dengine1 9001"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor central.
    pause
    exit /b
)
pause

