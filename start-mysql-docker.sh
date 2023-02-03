#!/bin/bash

#set -e

echo "Starting docker mysql container..."

CONTAINER_ID=$(docker run -e MYSQL_ROOT_PASSWORD=root -e MYSQL_PASSWORD=root -e MYSQL_DATABASE=db -e MYSQL_ROOT_HOST=% -d -p 3306:3306 mysql:latest)

echo "Id for container is $CONTAINER_ID..."

function rm_container_and_exit ()
{
    printf "Removing container $CONTAINER_ID...\n"
    docker container rm -f ${CONTAINER_ID}
    exit 0
}
trap "rm_container_and_exit" 2

echo "All setup!"

while true
do
    sleep 10000
done
