@echo off
echo Levantando el contenedor dengine...
docker-compose stop dengine3 --timeout 0
docker-compose up -d dengine3
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor dengine3.
    pause
    exit /b
)
echo Entrando a la consola del contenedor dengine...
docker exec -it easycab-dengine3-1 /bin/bash -c "python ec_de.py central 8000 kafka:9092 sensor3 9003 3 10000"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor dengine.
    pause
    exit /b
)
pause

