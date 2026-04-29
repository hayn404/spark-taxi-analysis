from pyspark import SparkContext
import time

sc = SparkContext("spark://spark-master:7077", "Taxi_RDD")

# Load CSV
rdd = sc.textFile("hdfs://namenode:9000/user/root/nyc_taxi/yellow_tripdata_2024-01.csv")
header = rdd.first()
data = rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

# Parse: fare=10, distance=4, passenger=3, pickup_zone=7, payment=9, tip=13, total=16
def parse(row):
    try:
        return {
            'fare': float(row[10]), 'distance': float(row[4]),
            'passenger': int(row[3] or 0), 'zone': int(row[7] or 0),
            'payment': int(row[9] or 0), 'tip': float(row[13]),
            'total': float(row[16]), 'date': row[1][:10]
        }
    except: return None

taxi = data.map(parse).filter(lambda x: x is not None).cache()

print("Total:", taxi.count())

# Q1: Fraud (fare > 50, distance < 1)
start = time.time()
print("\nQ1 Fraud:", taxi.filter(lambda x: x['fare'] > 50 and x['distance'] < 1).count())
print("Time:", time.time()-start)

# Q2: Aggregations
start = time.time()
fares = taxi.map(lambda x: x['fare']).filter(lambda x: x > 0)
print("\nQ2 Total:", fares.sum(), "Avg:", fares.mean(), "Max:", fares.max())
print("Time:", time.time()-start)

# Q3: Group by zone + payment
start = time.time()
print("\nQ3 Top zone+payment:")
for row in taxi.map(lambda x: ((x['zone'], x['payment']), (x['total'], 1))).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).map(lambda x: (x[0], x[1][0], x[1][1])).top(5, lambda x: x[1]):
    print(" ", row)
print("Time:", time.time()-start)

# Q4: Top zones
start = time.time()
print("\nQ4 Top zones:")
for row in taxi.map(lambda x: (x['zone'], 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], False).take(5):
    print(" ", row)
print("Time:", time.time()-start)

# Q5: Daily trips + moving avg
start = time.time()
daily = taxi.map(lambda x: (x['date'], 1)).reduceByKey(lambda a,b: a+b).sortByKey().collect()
print("\nQ5 Daily (first 5):")
for i, (d, c) in enumerate(daily[:5]):
    window = daily[max(0,i-3):min(len(daily), i+4)]
    avg = sum(x[1] for x in window) / len(window)
    print(f" {d}: {c}, avg7={avg:.1f}")
print("Time:", time.time()-start)

# Q6: Zones with above-average tip %
start = time.time()
zone_tips = taxi.filter(lambda x: x['fare'] > 0).map(lambda x: (x['zone'], (x['tip']/x['fare']*100, 1))).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).map(lambda x: (x[0], x[1][0]/x[1][1]))
avg = zone_tips.map(lambda x: x[1]).mean()
print(f"\nQ6 Avg tip: {avg:.2f}%, Above avg zones:")
for row in zone_tips.filter(lambda x: x[1] > avg).sortBy(lambda x: x[1], False).take(5):
    print(" ", row)
print("Time:", time.time()-start)

# Q7: Broadcast join with zone lookup
start = time.time()
lookup = sc.textFile("hdfs://namenode:9000/user/root/nyc_taxi/taxi_zone_lookup.csv").filter(lambda x: "LocationID" not in x).map(lambda x: x.split(",")).map(lambda x: (int(x[0]), x[2])).collectAsMap()
lookup_b = sc.broadcast(lookup)
print("\nQ7 Top revenue zones:")
for row in taxi.map(lambda x: (x['zone'], x['total'])).reduceByKey(lambda a,b: a+b).map(lambda x: (lookup_b.value.get(x[0], "Unknown"), x[1])).sortBy(lambda x: x[1], False).take(5):
    print(" ", row)
print("Time:", time.time()-start)

# Q8: Self join (pickups vs dropoffs)
start = time.time()
pu = taxi.map(lambda x: (x['zone'], 1)).reduceByKey(lambda a,b: a+b)
do = taxi.map(lambda x: (x['zone'], 1)).reduceByKey(lambda a,b: a+b)
print("\nQ8 Top activity zones:")
for row in pu.join(do).map(lambda x: (x[0], x[1][0]+x[1][1])).sortBy(lambda x: x[1], False).take(5):
    print(" ", row)
print("Time:", time.time()-start)

# Q9: Partition pruning (filter by date)
start = time.time()
print("\nQ9 Jan 1:", taxi.filter(lambda x: x['date'] == '2024-01-01').count())
print("Jan 15:", taxi.filter(lambda x: x['date'] == '2024-01-15').count())
print("Time:", time.time()-start)

# Q10: Cache vs no cache
start = time.time()
no_cache = taxi.filter(lambda x: x['fare'] > 20)
no_cache.count()
no_cache.filter(lambda x: x['distance'] > 5).count()
t1 = time.time()-start

cached = taxi.filter(lambda x: x['fare'] > 20).cache()
cached.count()
cached.filter(lambda x: x['distance'] > 5).count()
t2 = time.time()-start

print(f"\nQ10 No cache: {t1:.2f}s, Cache: {t2:.2f}s, Speedup: {t1/t2:.1f}x")

sc.stop()
print("\nDone!")
