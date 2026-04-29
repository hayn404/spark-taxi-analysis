#!/bin/bash
# run_all.sh - Run all Mini Project 2 scripts

echo "=========================================="
echo "Mini Project 2 - NYC Taxi Analysis"
echo "=========================================="

# Check if data exists in HDFS
echo "Checking HDFS data..."
docker exec -it namenode hdfs dfs -ls /user/root/nyc_taxi/

echo ""
echo "1. Running DataFrame API Analysis..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /hadoop-data/code/dataframe/taxi_analysis_dataframe.py \
    > /hadoop-data/output_dataframe.txt 2>&1

echo "DataFrame analysis complete. Output saved to output_dataframe.txt"

echo ""
echo "2. Running Spark SQL Analysis..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /hadoop-data/code/sql/taxi_analysis_sql.py \
    > /hadoop-data/output_sql.txt 2>&1

echo "SQL analysis complete. Output saved to output_sql.txt"

echo ""
echo "3. Running Performance Comparison..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /hadoop-data/code/comparison/performance_comparison.py \
    > /hadoop-data/output_comparison.txt 2>&1

echo "Comparison complete. Output saved to output_comparison.txt"

echo ""
echo "=========================================="
echo "All analyses complete!"
echo "Check output files in ~/bigdata-cluster/data/"
echo "=========================================="
