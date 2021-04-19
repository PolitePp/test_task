#!/bin/bash
docker network create verticaspark
cd second_task/input_hdfs
unzip transactions.zip
cd ../../
docker-compose up -d
docker exec -it pg_db pg_restore -h localhost -U postgres -F c -d postgres /opt/input_data/dump.tar.gz 
docker exec -it namenode hdfs dfs -mkdir /raw
docker exec -it namenode hdfs dfs -put /opt/input_hdfs/transactions.json /raw