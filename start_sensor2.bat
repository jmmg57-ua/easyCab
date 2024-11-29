@echo off
echo Levantando el contenedor sensor...
docker-compose stop sensor2 --timeout 0
docker-compose up -d sensor2
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor sensor2.
    pause
    exit /b
)
echo Entrando a la consola del contenedor sensor2...
docker exec -it easycab-sensor2-1 /bin/bash -c "python ec_s.py dengine2 9002"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor central.
    pause
    exit /b
)
pause

