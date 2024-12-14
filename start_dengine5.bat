@echo off
echo Levantando el contenedor dengine...
docker-compose stop dengine5 --timeout 0
docker-compose up -d dengine5
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor dengine5.
    pause
    exit /b
)
echo Entrando a la consola del contenedor dengine...
docker exec -it easycab-dengine5-1 /bin/bash -c "python ec_de.py central 8000 kafka:9093 sensor5 9005 5 10000"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor dengine.
    pause
    exit /b
)
pause

