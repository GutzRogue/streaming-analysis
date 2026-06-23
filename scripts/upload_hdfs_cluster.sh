#!/bin/bash

FILE_LOCAL="./data/abonnes_cluster_demo.csv"
FILE_CONTAINER="/tmp/abonnes_cluster_demo.csv"
FILE_HDFS="/data/abonnes_cluster_demo.csv"

echo "Création du dossier HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data

echo "Suppression ancienne version HDFS si elle existe..."
docker exec namenode hdfs dfs -rm -f "$FILE_HDFS"

echo "Copie du gros CSV dans le NameNode..."
docker cp "$FILE_LOCAL" namenode:"$FILE_CONTAINER"

echo "Upload dans HDFS avec blocs de 1 MB..."
docker exec namenode hdfs dfs \
  -D dfs.blocksize=1048576 \
  -D dfs.replication=1 \
  -put -f "$FILE_CONTAINER" "$FILE_HDFS"

echo "Vérification..."
docker exec namenode hdfs dfs -ls /data

echo "Répartition des blocs..."
docker exec namenode hdfs fsck "$FILE_HDFS" -files -blocks -locations