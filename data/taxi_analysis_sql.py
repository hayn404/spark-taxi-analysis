
"""
Mini Project 2 - Spark SQL Implementation
NYC Yellow Taxi Trip Analysis
"""

from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("NYC_Taxi_SQL_Analysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load data
print("Loading data...")
df = spark.read.parquet("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.parquet")
lookup_df = spark.read.csv("hdfs://namenode:9000/user/root/nyc_taxi/taxi_zone_lookup.csv", header=True, inferSchema=True)

# Create temp views
df.createOrReplaceTempView("taxi_trips")
lookup_df.createOrReplaceTempView("zone_lookup")

print(f"Total records: {df.count():,}")

# ============================================
# QUERY 1: Complex Filtering (Potential Fraud)
# ============================================
print("\n=== QUERY 1: Complex Filtering ===")
start = time.time()
spark.sql("""
    SELECT VendorID, tpep_pickup_datetime, fare_amount, trip_distance, passenger_count
    FROM taxi_trips
    WHERE fare_amount > 50 
      AND trip_distance < 1 
      AND passenger_count > 0
    LIMIT 10
""").show(truncate=False)

fraud_count = spark.sql("""
    SELECT COUNT(*) as fraud_count
    FROM taxi_trips
    WHERE fare_amount > 50 AND trip_distance < 1 AND passenger_count > 0
""").collect()[0][0]
print(f"Potential fraud trips: {fraud_count:,}")
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 2: Aggregations
# ============================================
print("\n=== QUERY 2: Aggregations ===")
start = time.time()
spark.sql("""
    SELECT 
        SUM(fare_amount) as total_fare,
        AVG(fare_amount) as avg_fare,
        COUNT(*) as trip_count,
        MAX(fare_amount) as max_fare,
        MIN(fare_amount) as min_fare
    FROM taxi_trips
    WHERE fare_amount > 0
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 3: Grouping by Multiple Attributes
# ============================================
print("\n=== QUERY 3: Multi-Attribute Grouping ===")
start = time.time()
spark.sql("""
    SELECT 
        PULocationID,
        payment_type,
        SUM(total_amount) as revenue,
        COUNT(*) as trip_count,
        AVG(total_amount) as avg_fare
    FROM taxi_trips
    GROUP BY PULocationID, payment_type
    ORDER BY revenue DESC
    LIMIT 10
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 4: Sorting and Ranking
# ============================================
print("\n=== QUERY 4: Sorting and Ranking ===")
start = time.time()
spark.sql("""
    SELECT 
        PULocationID,
        COUNT(*) as trip_count
    FROM taxi_trips
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 10
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 5: Window Functions (Moving Average)
# ============================================
print("\n=== QUERY 5: Window Functions ===")
start = time.time()
spark.sql("""
    WITH daily_trips AS (
        SELECT 
            DATE(tpep_pickup_datetime) as trip_date,
            COUNT(*) as daily_trips
        FROM taxi_trips
        GROUP BY DATE(tpep_pickup_datetime)
        ORDER BY trip_date
    )
    SELECT 
        trip_date,
        daily_trips,
        AVG(daily_trips) OVER (
            ORDER BY trip_date 
            ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
        ) as moving_avg_7day
    FROM daily_trips
    LIMIT 10
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 6: Nested Query / Subquery
# ============================================
print("\n=== QUERY 6: Nested Query ===")
start = time.time()
spark.sql("""
    WITH zone_stats AS (
        SELECT 
            PULocationID,
            AVG(tip_amount / fare_amount * 100) as tip_pct,
            COUNT(*) as trip_count
        FROM taxi_trips
        WHERE fare_amount > 0
        GROUP BY PULocationID
    ),
    avg_tip AS (
        SELECT AVG(tip_pct) as overall_avg FROM zone_stats
    )
    SELECT 
        z.PULocationID,
        z.tip_pct,
        z.trip_count
    FROM zone_stats z, avg_tip a
    WHERE z.tip_pct > a.overall_avg
    ORDER BY z.tip_pct DESC
    LIMIT 10
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 7: Broadcast Join
# ============================================
print("\n=== QUERY 7: Broadcast Join ===")
start = time.time()
spark.sql("""
    SELECT /*+ BROADCAST(z) */
        z.Zone,
        z.Borough,
        SUM(t.total_amount) as revenue
    FROM taxi_trips t
    LEFT JOIN zone_lookup z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone, z.Borough
    ORDER BY revenue DESC
    LIMIT 10
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 8: Sort-Merge Join
# ============================================
print("\n=== QUERY 8: Sort-Merge Join ===")
start = time.time()
spark.sql("""
    WITH pickups AS (
        SELECT PULocationID as zone_id, COUNT(*) as pickup_count
        FROM taxi_trips
        GROUP BY PULocationID
    ),
    dropoffs AS (
        SELECT DOLocationID as zone_id, COUNT(*) as dropoff_count
        FROM taxi_trips
        GROUP BY DOLocationID
    )
    SELECT /*+ MERGE(p, d) */
        p.zone_id,
        p.pickup_count,
        d.dropoff_count,
        p.pickup_count + d.dropoff_count as total_activity
    FROM pickups p
    JOIN dropoffs d ON p.zone_id = d.zone_id
    ORDER BY total_activity DESC
    LIMIT 10
""").show(truncate=False)
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 9: Partition Pruning
# ============================================
print("\n=== QUERY 9: Partition Pruning ===")
start = time.time()

# Create partitioned table
spark.sql("""
    CREATE OR REPLACE TEMP VIEW taxi_partitioned AS
    SELECT *, DATE(tpep_pickup_datetime) as pickup_date
    FROM taxi_trips
""")

spark.sql("""
    SELECT COUNT(*) as jan_1st_trips
    FROM taxi_partitioned
    WHERE pickup_date = '2024-01-01'
""").show()

spark.sql("""
    SELECT COUNT(*) as jan_15th_trips
    FROM taxi_partitioned
    WHERE pickup_date = '2024-01-15'
""").show()
print(f"Time: {time.time() - start:.2f}s")

# ============================================
# QUERY 10: Cache vs No-Cache
# ============================================
print("\n=== QUERY 10: Cache vs No-Cache ===")

# Without cache
start = time.time()
spark.sql("SELECT COUNT(*) FROM taxi_trips WHERE fare_amount > 20").show()
spark.sql("SELECT COUNT(*) FROM taxi_trips WHERE fare_amount > 20 AND trip_distance > 5").show()
t1 = time.time() - start

# Create cached view
spark.sql("CACHE TABLE cached_trips AS SELECT * FROM taxi_trips WHERE fare_amount > 20")
start = time.time()
spark.sql("SELECT COUNT(*) FROM cached_trips").show()
spark.sql("SELECT COUNT(*) FROM cached_trips WHERE trip_distance > 5").show()
t2 = time.time() - start

print(f"Without cache: {t1:.2f}s")
print(f"With cache: {t2:.2f}s")
print(f"Speedup: {t1/t2:.2f}x")

spark.stop()
print("\n=== Spark SQL Analysis Complete ===")
