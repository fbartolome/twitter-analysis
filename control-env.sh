#!/bin/bash
function stop {
  echo "Stopping and removing containers"
  docker-compose --project-name wksp down
}

function cleanup {
  echo "Removing volume"
  docker volume rm wksp_postgres-data
}

function start {
  echo "Starting up"
  docker-compose --project-name wksp up -d
}

function token {
  echo 'Your TOKEN for Jupyter Notebook is:'
  SERVER=$(docker exec -it jupyter jupyter notebook list)
  echo "${SERVER}" | grep '/notebook' | sed -E 's/^.*=([a-z0-9]+).*$/\1/'
}

function psql {
  docker exec -it postgres psql -U workshop workshop
}

if [ $1 = "start" ]; then
    start
elif [ $1 = "stop" ]; then
    stop
elif [ $1 = "clean" ];then
    cleanup
elif [ $1 = "token" ];then
    token
elif [ $1 = "psql" ];then
    psql
fi