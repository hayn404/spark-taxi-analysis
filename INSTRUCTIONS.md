# Mini Project 2 - Setup & Run Instructions

## Step 1: Download Dataset

```bash
cd ~/bigdata-cluster/data

# Download NYC Taxi data (January 2024, ~300MB)
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# Download zone lookup table
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

## Step 2: Copy Project Code to Container

```bash
# Copy code folder to container (from your project directory)
cp -r ~/mini_project_2/code ~/bigdata-cluster/data/
cp -r ~/mini_project_2/data ~/bigdata-cluster/data/
```

## Step 3: Load Data into HDFS

```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/root/nyc_taxi
docker exec -it namenode hdfs dfs -put /hadoop-data/yellow_tripdata_2024-01.parquet /user/root/nyc_taxi/
docker exec -it namenode hdfs dfs -put /hadoop-data/taxi_zone_lookup.csv /user/root/nyc_taxi/
```

## Step 4: Convert Parquet to CSV (for RDD)

```bash
docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

```python
df = spark.read.parquet("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.parquet")
df.write.csv("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.csv", header=True, mode="overwrite")
exit()
```

## Step 5: Run Each API

### RDD API
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /hadoop-data/code/rdd/taxi_rdd.py
```

### DataFrame API
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /hadoop-data/code/dataframe/taxi_df.py
```

### Spark SQL
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /hadoop-data/code/sql/taxi_sql.py
```

### Performance Comparison
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /hadoop-data/code/comparison/compare.py
```

## Step 6: View Results

All outputs print to console. Take screenshots for your report.

## Web UIs
- HDFS: http://localhost:9870
- YARN: http://localhost:8088
- Spark: http://localhost:8081
