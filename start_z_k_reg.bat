docker-compose down --timeout 0
docker-compose up zookeeper kafka -d
docker-compose up registry -d 