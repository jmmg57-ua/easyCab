@echo off
echo Levantando el contenedor dengine...
docker-compose stop dengine4 --timeout 0
docker-compose up -d dengine4
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor dengine4.
    pause
    exit /b
)
echo Entrando a la consola del contenedor dengine...
docker exec -it easycab-dengine4-1 /bin/bash -c "python ec_de.py central 8000 kafka:9093 sensor4 9004 4 10000"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor dengine.
    pause
    exit /b
)
pause

