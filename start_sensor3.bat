@echo off
echo Levantando el contenedor sensor3...
docker-compose stop sensor3 --timeout 0
docker-compose up -d sensor3
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor sensor3.
    pause
    exit /b
)
echo Entrando a la consola del contenedor central...
docker exec -it easycab-sensor3-1 /bin/bash -c "python ec_s.py dengine3 9003"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor central.
    pause
    exit /b
)
pause

