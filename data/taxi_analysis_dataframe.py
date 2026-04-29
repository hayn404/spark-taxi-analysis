
"""
Mini Project 2 - DataFrame API Implementation
NYC Yellow Taxi Trip Analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

spark = SparkSession.builder \
    .appName("NYC_Taxi_DataFrame_Analysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load data
print("Loading data...")
df = spark.read.parquet("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.parquet")
lookup_df = spark.read.csv("hdfs://namenode:9000/user/root/nyc_taxi/taxi_zone_lookup.csv", header=True, inferSchema=True)

print(f"Total records: {df.count():,}")
print(f"Columns: {len(df.columns)}")
df.printSchema()

# Cache for performance
df.cache()

# ============================================
# QUERY 1: Complex Filtering (Potential Fraud)
# ============================================
print("\n=== QUERY 1: Complex Filtering ===")
start = time.time()
query1 = df.filter((col("fare_amount") > 50) & (col("trip_distance") < 1) & (col("passenger_count") > 0))
query1.show(10, truncate=False)
print(f"Potential fraud trips: {query1.count():,}")
print(f"Time: {time.time() - start:.2f}s")
query1.explain(True)

# ============================================
# QUERY 2: Aggregations (SUM, AVG, COUNT, MAX/MIN)
# ============================================
print("\n=== QUERY 2: Aggregations ===")
start = time.time()
query2 = df.filter(col("fare_amount") > 0).agg(
    sum("fare_amount").alias("total_fare"),
    avg("fare_amount").alias("avg_fare"),
    count("*").alias("trip_count"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare")
)
query2.show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")
query2.explain(True)

# ============================================
# QUERY 3: Grouping by Multiple Attributes
# ============================================
print("\n=== QUERY 3: Multi-Attribute Grouping ===")
start = time.time()
query3 = df.groupBy("PULocationID", "payment_type").agg(
    sum("total_amount").alias("revenue"),
    count("*").alias("trip_count"),
    avg("total_amount").alias("avg_fare")
).orderBy(desc("revenue"))
query3.show(10, truncate=False)
print(f"Time: {time.time() - start:.2f}s")
query3.explain(True)

# ============================================
# QUERY 4: Sorting and Ranking
# ============================================
print("\n=== QUERY 4: Sorting and Ranking ===")
start = time.time()
query4 = df.groupBy("PULocationID").agg(count("*").alias("trip_count")).orderBy(desc("trip_count"))
query4.show(10, truncate=False)
print(f"Time: {time.time() - start:.2f}s")
query4.explain(True)

# ============================================
# QUERY 5: Window Functions (Moving Average)
# ============================================
print("\n=== QUERY 5: Window Functions ===")
start = time.time()

# Extract date and compute daily trips
daily_df = df.withColumn("trip_date", to_date(col("tpep_pickup_datetime"))) \
    .groupBy("trip_date").agg(count("*").alias("daily_trips")).orderBy("trip_date")

# 7-day moving average window
window_spec = Window.orderBy("trip_date").rowsBetween(-3, 3)
query5 = daily_df.withColumn("moving_avg", avg("daily_trips").over(window_spec))
query5.show(10, truncate=False)
print(f"Time: {time.time() - start:.2f}s")
query5.explain(True)

# ============================================
# QUERY 6: Nested Query / Subquery
# ============================================
print("\n=== QUERY 6: Nested Query ===")
start = time.time()

# Compute average tip percentage
avg_tip = df.filter(col("fare_amount") > 0) \
    .agg(avg(col("tip_amount") / col("fare_amount") * 100)) \
    .collect()[0][0]
    
# Zones above average
zone_tips = df.filter(col("fare_amount") > 0).groupBy("PULocationID").agg(
    (avg(col("tip_amount") / col("fare_amount")) * 100).alias("tip_pct"),
    count("*").alias("trip_count")
).filter(col("tip_pct") > avg_tip).orderBy(desc("tip_pct"))

zone_tips.show(10, truncate=False)
print(f"Average tip %: {avg_tip:.2f}%")
print(f"Time: {time.time() - start:.2f}s")
zone_tips.explain(True)

# ============================================
# QUERY 7: Broadcast Join
# ============================================
print("\n=== QUERY 7: Broadcast Join ===")
start = time.time()

# Broadcast the small lookup table
from pyspark.sql.functions import broadcast

query7 = df.groupBy("PULocationID").agg(sum("total_amount").alias("revenue")) \
    .join(broadcast(lookup_df), col("PULocationID") == col("LocationID"), "left") \
    .select("Zone", "Borough", "revenue") \
    .orderBy(desc("revenue"))

query7.show(10, truncate=False)
print(f"Time: {time.time() - start:.2f}s")
query7.explain(True)

# ============================================
# QUERY 8: Sort-Merge Join (Self-join)
# ============================================
print("\n=== QUERY 8: Sort-Merge Join ===")
start = time.time()

pickups = df.groupBy("PULocationID").agg(count("*").alias("pickup_count"))
dropoffs = df.groupBy("DOLocationID").agg(count("*").alias("dropoff_count"))

# Force sort-merge join by hint
query8 = pickups.join(dropoffs.hint("merge"), col("PULocationID") == col("DOLocationID"), "inner") \
    .select(col("PULocationID"), col("pickup_count"), col("dropoff_count"),
            (col("pickup_count") + col("dropoff_count")).alias("total_activity")) \
    .orderBy(desc("total_activity"))

query8.show(10, truncate=False)
print(f"Time: {time.time() - start:.2f}s")
query8.explain(True)

# ============================================
# QUERY 9: Partition Pruning
# ============================================
print("\n=== QUERY 9: Partition Pruning ===")
start = time.time()

# Write partitioned data first
df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
    .write.partitionBy("pickup_date") \
    .parquet("hdfs://namenode:9000/user/root/nyc_taxi_partitioned", mode="overwrite")

# Read with partition pruning
partitioned_df = spark.read.parquet("hdfs://namenode:9000/user/root/nyc_taxi_partitioned")
jan_1st = partitioned_df.filter(col("pickup_date") == "2024-01-01")
jan_15th = partitioned_df.filter(col("pickup_date") == "2024-01-15")

print(f"January 1st trips: {jan_1st.count():,}")
print(f"January 15th trips: {jan_15th.count():,}")
jan_1st.explain(True)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 10: Cache vs No-Cache Performance
# ============================================
print("\n=== QUERY 10: Cache vs No-Cache ===")

# Without cache
no_cache = df.filter(col("fare_amount") > 20)
start = time.time()
no_cache.count()
no_cache.filter(col("trip_distance") > 5).count()
t1 = time.time() - start

# With cache
cached = df.filter(col("fare_amount") > 20).cache()
start = time.time()
cached.count()
cached.filter(col("trip_distance") > 5).count()
t2 = time.time() - start

print(f"Without cache: {t1:.2f}s")
print(f"With cache: {t2:.2f}s")
print(f"Speedup: {t1/t2:.2f}x")

spark.stop()
print("\n=== DataFrame Analysis Complete ===")
