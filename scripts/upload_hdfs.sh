#!/bin/bash

docker exec namenode hdfs dfs -mkdir -p /data

docker cp ./data/abonnes.csv namenode:/tmp/abonnes.csv

docker exec namenode hdfs dfs -put -f /tmp/abonnes.csv /data/

docker exec namenode hdfs dfs -ls /data
