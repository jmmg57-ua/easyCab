@echo off
echo Levantando el contenedor CTC...
docker-compose stop ctc --timeout 0
docker-compose up -d ctc
if %errorlevel% neq 0 (
    echo Error al levantar el contenedor CTC.
    pause
    exit /b
)
echo Entrando a la consola del contenedor CTC...
docker exec -it easycab-ctc-1 /bin/bash -c "python ec_ctc.py"
if %errorlevel% neq 0 (
    echo Error al entrar a la consola del contenedor CTC.
    pause
    exit /b
)
pause