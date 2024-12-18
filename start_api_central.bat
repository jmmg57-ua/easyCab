@echo off
echo Levantando el contenedor api_central...
docker-compose stop api_central --timeout 0
docker-compose up -d api_central
