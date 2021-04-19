#!/bin/bash
docker network create verticaspark
cd second_task/input_hdfs
unzip transactions.zip
cd ../../
docker exec -it pg_db pg_dump -h localhost -U postgres -F c -f /opt/input_data/dump.tar.gz postgres
docker exec -it namenode hdfs dfs -mkdir /raw
docker exec -it namenode hdfs dfs -put /opt/input_hdfs/transactions.json /raw