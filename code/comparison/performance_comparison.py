
"""
Mini Project 2 - Performance Comparison Script
Compares RDD vs DataFrame vs Spark SQL APIs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("NYC_Taxi_Comparison") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load data
df = spark.read.parquet("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.parquet")
lookup_df = spark.read.csv("hdfs://namenode:9000/user/root/nyc_taxi/taxi_zone_lookup.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("taxi_trips")
lookup_df.createOrReplaceTempView("zone_lookup")

print("=" * 80)
print("PERFORMANCE COMPARISON: RDD vs DataFrame vs Spark SQL")
print("=" * 80)

results = []

# ============================================
# TEST 1: Simple Filter
# ============================================
print("\n--- TEST 1: Simple Filter (fare > 20) ---")

# DataFrame
df_test = df.filter(col("fare_amount") > 20)
start = time.time()
df_count = df_test.count()
t_df = time.time() - start

# SQL
start = time.time()
sql_count = spark.sql("SELECT COUNT(*) FROM taxi_trips WHERE fare_amount > 20").collect()[0][0]
t_sql = time.time() - start

print(f"DataFrame: {t_df:.3f}s, Count: {df_count:,}")
print(f"SQL:       {t_sql:.3f}s, Count: {sql_count:,}")
results.append(("Simple Filter", t_df, t_sql, "DataFrame" if t_df < t_sql else "SQL"))

# ============================================
# TEST 2: Aggregation
# ============================================
print("\n--- TEST 2: Aggregation (avg fare by payment type) ---")

# DataFrame
start = time.time()
df_agg = df.groupBy("payment_type").agg(avg("fare_amount").alias("avg_fare")).collect()
t_df = time.time() - start

# SQL
start = time.time()
sql_agg = spark.sql("SELECT payment_type, AVG(fare_amount) as avg_fare FROM taxi_trips GROUP BY payment_type").collect()
t_sql = time.time() - start

print(f"DataFrame: {t_df:.3f}s")
print(f"SQL:       {t_sql:.3f}s")
results.append(("Aggregation", t_df, t_sql, "DataFrame" if t_df < t_sql else "SQL"))

# ============================================
# TEST 3: Join Operation
# ============================================
print("\n--- TEST 3: Join with Lookup Table ---")

# DataFrame (broadcast)
start = time.time()
df_join = df.groupBy("PULocationID").agg(sum("total_amount").alias("revenue")) \
    .join(lookup_df, col("PULocationID") == col("LocationID"), "left") \
    .select("Zone", "revenue").collect()
t_df = time.time() - start

# SQL
start = time.time()
sql_join = spark.sql("""
    SELECT z.Zone, SUM(t.total_amount) as revenue
    FROM taxi_trips t
    LEFT JOIN zone_lookup z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
""").collect()
t_sql = time.time() - start

print(f"DataFrame: {t_df:.3f}s")
print(f"SQL:       {t_sql:.3f}s")
results.append(("Join", t_df, t_sql, "DataFrame" if t_df < t_sql else "SQL"))

# ============================================
# TEST 4: Window Function
# ============================================
print("\n--- TEST 4: Window Function (moving avg) ---")

# DataFrame
window_spec = Window.orderBy("trip_date").rowsBetween(-3, 3)
daily_df = df.withColumn("trip_date", to_date(col("tpep_pickup_datetime"))) \
    .groupBy("trip_date").agg(count("*").alias("daily_trips")).orderBy("trip_date")

start = time.time()
df_window = daily_df.withColumn("moving_avg", avg("daily_trips").over(window_spec)).collect()
t_df = time.time() - start

# SQL
start = time.time()
sql_window = spark.sql("""
    WITH daily AS (
        SELECT DATE(tpep_pickup_datetime) as trip_date, COUNT(*) as daily_trips
        FROM taxi_trips GROUP BY DATE(tpep_pickup_datetime) ORDER BY trip_date
    )
    SELECT trip_date, daily_trips,
        AVG(daily_trips) OVER (ORDER BY trip_date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) as moving_avg
    FROM daily
""").collect()
t_sql = time.time() - start

print(f"DataFrame: {t_df:.3f}s")
print(f"SQL:       {t_sql:.3f}s")
results.append(("Window Function", t_df, t_sql, "DataFrame" if t_df < t_sql else "SQL"))

# ============================================
# TEST 5: Complex Query
# ============================================
print("\n--- TEST 5: Complex Query (fraud detection) ---")

# DataFrame
start = time.time()
df_complex = df.filter((col("fare_amount") > 50) & (col("trip_distance") < 1) & (col("passenger_count") > 0)) \
    .groupBy("PULocationID").agg(count("*").alias("fraud_count"), avg("fare_amount").alias("avg_fraud_fare")) \
    .orderBy(desc("fraud_count")).collect()
t_df = time.time() - start

# SQL
start = time.time()
sql_complex = spark.sql("""
    SELECT PULocationID, COUNT(*) as fraud_count, AVG(fare_amount) as avg_fraud_fare
    FROM taxi_trips
    WHERE fare_amount > 50 AND trip_distance < 1 AND passenger_count > 0
    GROUP BY PULocationID
    ORDER BY fraud_count DESC
""").collect()
t_sql = time.time() - start

print(f"DataFrame: {t_df:.3f}s")
print(f"SQL:       {t_sql:.3f}s")
results.append(("Complex Query", t_df, t_sql, "DataFrame" if t_df < t_sql else "SQL"))

# ============================================
# SUMMARY TABLE
# ============================================
print("\n" + "=" * 80)
print("SUMMARY: Performance Comparison")
print("=" * 80)
print(f"{'Query Type':<20} {'DataFrame (s)':<15} {'SQL (s)':<15} {'Winner'}")
print("-" * 70)
for name, t_df, t_sql, winner in results:
    print(f"{name:<20} {t_df:<15.3f} {t_sql:<15.3f} {winner}")

# Explain plans comparison
print("\n" + "=" * 80)
print("EXECUTION PLAN COMPARISON")
print("=" * 80)

print("\n--- DataFrame Plan (Query 3) ---")
df.groupBy("payment_type").agg(avg("fare_amount")).explain(True)

print("\n--- SQL Plan (Query 3) ---")
spark.sql("SELECT payment_type, AVG(fare_amount) FROM taxi_trips GROUP BY payment_type").explain(True)

# === CSV vs Parquet format comparison ===
print("\n--- CSV vs Parquet Read Speed ---")

start = time.time()
spark.read.parquet("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.parquet").count()
t_parquet = time.time() - start

start = time.time()
spark.read.csv("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.csv", header=True, inferSchema=True).count()
t_csv = time.time() - start

print(f"Parquet: {t_parquet:.3f}s | CSV: {t_csv:.3f}s | Speedup: {t_csv/t_parquet:.1f}x")

# === Partitioning strategy impact ===
print("\n--- Partition Count Impact ---")
for n in [4, 8, 16]:
    repartitioned = df.repartition(n)
    start = time.time()
    repartitioned.filter(col("fare_amount") > 20).count()
    print(f"Partitions={n}: {time.time()-start:.3f}s")

spark.stop()
print("\n=== Comparison Complete ===")
