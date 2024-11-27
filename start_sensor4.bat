@echo off
echo Levantando el contenedor sensor4...
docker-compose stop sensor4 --timeout 0
docker-compose up -d sensor4
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor sensor4.
    pause
    exit /b
)
echo Entrando a la consola del contenedor sensor4...
docker exec -it easycab-sensor4-1 /bin/bash -c "python ec_s.py dengine4 9004"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor sensor.
    pause
    exit /b
)
pause

