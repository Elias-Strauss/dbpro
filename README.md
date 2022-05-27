# dbpro

build image:
docker build . -t dbpro:1

run container:
run -it --rm dbpro:1 bash

list all containers:
docker ps -a

start container:
docker start CONTAINER

open bash shell
docker exec -it CONTAINER bash
