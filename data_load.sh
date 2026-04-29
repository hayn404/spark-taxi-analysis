#!/bin/bash
# data_load.sh - Load NYC Taxi data into HDFS

echo "=== NYC Taxi Data Loading Script ==="
docker exec -it namenode hdfs dfs -mkdir -p /user/root/nyc_taxi
docker exec -it namenode hdfs dfs -put /hadoop-data/yellow_tripdata_2024-01.parquet /user/root/nyc_taxi/
docker exec -it namenode hdfs dfs -put /hadoop-data/taxi_zone_lookup.csv /user/root/nyc_taxi/
docker exec -it namenode hdfs dfs -ls /user/root/nyc_taxi/
echo "Data loaded!"
