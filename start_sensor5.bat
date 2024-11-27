@echo off
echo Levantando el contenedor sensor5...
docker-compose stop sensor5 --timeout 0
docker-compose up -d sensor5
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor sensor5.
    pause
    exit /b
)
echo Entrando a la consola del contenedor sensor5...
docker exec -it easycab-sensor5-1 /bin/bash -c "python ec_s.py dengine5 9005"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor central.
    pause
    exit /b
)
pause

