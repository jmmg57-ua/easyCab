@echo off
echo Levantando el contenedor dengine...
docker-compose stop dengine1 --timeout 0
docker-compose up -d dengine1
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor dengine1.
    pause
    exit /b
)
echo Entrando a la consola del contenedor central...
docker exec -it easycab-dengine1-1 /bin/bash -c "ec_de.py central 8000 kafka:9092 sensor1 9001 1 registry 10000"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor dengine.
    pause
    exit /b
)
pause

