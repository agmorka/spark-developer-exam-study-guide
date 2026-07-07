# Section 4: Troubleshooting and Tuning Apache Spark DataFrame API Applications

---

## Table of Contents

1. [Objective 1: Implement performance tuning strategies](#objective-1-implement-performance-tuning-strategies--optimize-cluster-utilization)
2. [Objective 2: Describe Adaptive Query Execution](#objective-2-describe-adaptive-query-execution-aqe-and-its-benefits)
3. [Objective 3: Perform logging and monitoring](#objective-3-perform-logging-and-monitoring-of-spark-applications)
4. [Summary](#summary-section-4-key-takeaways)

---

## Objective 1: Implement performance tuning strategies & optimize cluster utilization

### Core Concept
Spark jobs are slow when: data is unevenly distributed (skew), partitions don't match executors, shuffles happen too often, or cluster resources are wasted.

### Partitioning Strategies

#### Number of Partitions

**Rule of thumb:** Number of partitions ≈ (number of executors × cores per executor) × 2–4

```python
# Check parallelism
sc = spark.sparkContext
print(f"Default parallelism: {sc.defaultParallelism}")
# Default = num executors × cores per executor

# Set shuffle partitions (for groupBy, join, etc.)
spark.conf.set("spark.sql.shuffle.partitions", 200)
```

#### Repartition vs. Coalesce

| Operation | Effect | Cost | Use Case |
|-----------|--------|------|----------|
| **repartition(n)** | Shuffle data to exactly n partitions | Expensive (shuffle) | Fix skew; increase parallelism |
| **coalesce(n)** | Combine partitions without shuffle | Cheap (no shuffle) | Reduce partitions before write |

```python
# Problem: 1000 partitions, but only 10 executors (wasteful)
# Solution: coalesce to 20 partitions
df_optimized = df.coalesce(20)  # No shuffle; combines nearby partitions
df_optimized.write.parquet("/output/data")

# Problem: One partition has 1M rows, others have 100K (skewed)
# Solution: repartition to rebalance
df_balanced = df.repartition(100)  # Shuffles; evenly distributes rows
```

### Identify and Fix Data Skew

**Skew problem:** One partition is 10x larger than others → bottleneck.

```python
# Detect skew: check partition sizes
df.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
# Output: [100000, 95000, 500000, 98000]  ← 500K is skewed!

# Fix 1: Repartition on a high-cardinality column
df_repart = df.repartition(200, "user_id")  # Distributes by user_id hash

# Fix 2: Salting — add random prefix to skewed keys to spread them across partitions
from pyspark.sql.functions import rand, concat, lit

skewed_col = "category"
num_salt_buckets = 10

# Add random salt to the skewed key
df_salted = df.withColumn(
    "salted_key",
    concat(col(skewed_col), lit("_"), (rand() * num_salt_buckets).cast("int"))
)

# Now repartition by salted key (spreads "hot_category" across 10 partitions)
df_salted_repartitioned = df_salted.repartition(num_salt_buckets * 20, "salted_key")

# Use salted key in join, then remove salt if needed
result = df_salted_repartitioned.join(other_df_also_salted, "salted_key")
result = result.drop("salted_key")  # Remove salt column

# Fix 3: Sample skewed data and repartition separately
skewed_col = "category"
df_skewed = df.filter(df[skewed_col] == "hot_category")
df_other = df.filter(df[skewed_col] != "hot_category")

df_skewed_repart = df_skewed.repartition(100, "id")
df_combined = df_skewed_repart.union(df_other)
```

**When to use Salting:**
- Join has skewed key (one value appears in 80% of rows)
- Repartitioning alone doesn't solve skew
- You need fine-grained control over data distribution

### Reduce Shuffles

Shuffles move data across the network (slow). Minimize them.

```python
# Before: Multiple shuffles
result = df.groupBy("dept").count()  # Shuffle 1
result = result.filter(result["count"] > 10)  # OK, no shuffle
result = result.join(other_df, "dept")  # Shuffle 2

# After: Fewer shuffles
result = df.groupBy("dept").count()
result = result.filter(result["count"] > 10)
# Don't join unless needed; filter first
```

### On Databricks

- **Adaptive Query Execution (AQE)** automatically tunes partitions (see Objective 2)
- **Spark UI shows partition sizes** — Click "Stages" tab to see skew
- **Monitor cluster utilization** — If some executors idle, repartition

### Broadcast Join Configuration

Spark automatically broadcasts small DataFrames during joins. You can control the threshold:

```python
# Set broadcast threshold to 20 MB (values must include unit: b, kb, mb, gb)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20mb")

# Disable auto-broadcast (use sort-merge join always)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Force broadcast on a specific DataFrame
from pyspark.sql.functions import broadcast

small_df = spark.read.parquet("/path/to/small_data")
result = large_df.join(broadcast(small_df), "key", "inner")
```

**Note:** The value must include a unit suffix ("20mb" not just 20). Default is "10mb".

### Out-of-Memory (OOM) Prevention Strategies (Q55)

**OOM happens when:**
- Executors run out of memory during shuffle, join, or broadcast
- Caching a DataFrame larger than available storage memory
- Accumulating data on driver during `.collect()`

**Prevention strategies:**

| Strategy | Example | Effect |
|----------|---------|--------|
| **Reduce partition size** | Increase partitions from 50 to 200 | Each partition processes less data per executor |
| **Reduce broadcast size** | Increase `autoBroadcastJoinThreshold` to 500mb | Only small tables are broadcast; large ones use sort-merge join |
| **Increase parallelism** | Set `default.parallelism` to 16 | More tasks in parallel = smaller chunks per task |
| **Avoid string concatenation** | Use `concat()` or SQL string functions instead of Python loops | Python loops build full strings in memory before Spark processes |
| **Serialization compression** | Use `MEMORY_ONLY_SER` storage level | Compress cached data to fit more in memory |
| **Limit cores per executor** | Set `spark.executor.cores = 4` (vs 8) | Fewer concurrent tasks per executor = less peak memory |

**Code examples:**

```python
# ✗ OOM Risk: Concatenating in Python builds entire string in memory
result = ""
for row in df.collect():  # Pulls ALL data to driver!
    result += row["name"] + " "  # String grows in memory

# ✓ CORRECT: Use Spark's concat() function
from pyspark.sql.functions import concat, col, lit
df_concat = df.select(concat(col("first"), lit(" "), col("last")))

# ✗ OOM Risk: Caching 500GB on 100GB cluster
df.cache()  # If df is 500GB, won't fit; data evicted constantly

# ✓ CORRECT: Cache only what you use
df_filtered = df.filter(df.age > 30)  # Smaller dataset
df_filtered.cache()

# ✗ OOM Risk: collect() on huge DataFrame
large_result = df.collect()  # Pulls entire DataFrame to driver memory!

# ✓ CORRECT: Use limit() or write to storage
large_result = df.limit(1000).collect()  # Get first 1000 rows instead
# OR
df.write.parquet("/output/result")  # Write to distributed storage
```

### Common Mistakes

1. **Too many partitions** — Each partition = overhead; 10,000 small partitions waste resources
2. **Too few partitions** — Can't parallelize; one partition on 100 executors wastes 99
3. **Repartitioning before write** — Write to Parquet with 1000s partitions = 1000s files (slow to read)
4. **Not checking for skew** — Assuming data is even distributed
5. **Using `.collect()` on large DataFrames** — Pulls all data to driver; causes OOM
6. **Broadcasting huge DataFrames** — If table > autoBroadcastJoinThreshold, don't broadcast; use sort-merge join

### Code Example: Detect and Fix Skew

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SkewTuningExample").getOrCreate()

# Create skewed data
df = spark.createDataFrame(
    [(1, "hot", 100)] * 900 +  # 900 rows in "hot" category
    [(i, "cold", 1) for i in range(2, 101)],  # 99 rows in "cold"
    ["id", "category", "value"]
)

print("=== Original Partition Sizes ===")
sizes = df.rdd.glom().map(len).collect()
print(f"Partition sizes: {sizes}")
print(f"Skew detected: min={min(sizes)}, max={max(sizes)}")

# Repartition to distribute evenly
df_repart = df.repartition(50, "id")
print("=== After repartition(50, 'id') ===")
sizes_after = df_repart.rdd.glom().map(len).collect()
print(f"Partition sizes: {sizes_after}")
print(f"More balanced: min={min(sizes_after)}, max={max(sizes_after)}")

# Coalesce to reduce partitions before write
df_coalesced = df_repart.coalesce(10)
print("=== After coalesce(10) ===")
sizes_coalesced = df_coalesced.rdd.glom().map(len).collect()
print(f"Partition sizes: {sizes_coalesced}")
```

---

## Objective 2: Describe Adaptive Query Execution (AQE) and its benefits

### Core Concept
**AQE** is Spark's automatic optimizer. It watches your job run and adjusts the plan mid-execution to fix problems like skew and miscalculated partition sizes.

### What AQE Does

| Problem | AQE Solution |
|---------|------|
| Skewed partitions | Splits large partitions; combines small ones |
| Wrong partition count | Adjusts based on actual data size |
| Suboptimal join strategy | Switches to broadcast join if a table is small |

### AQE Features

#### 1. Coalesce Partitions

If a shuffle creates many small partitions, AQE combines them.

```
Before AQE:
1000 partitions × 100KB each = 100MB total
(Overhead: 1000 small files)

After AQE:
100 partitions × 1MB each = 100MB total
(Less overhead; faster read)
```

#### 2. Skew Join Optimization

When joining skewed data, AQE splits the large partition.

```
Before AQE:
Left partition A: 1M rows
Right partition A: 500K rows
(Unbalanced; one executor takes 10x longer)

After AQE:
Left partition A1: 500K rows
Left partition A2: 500K rows
Right partition A: 500K rows (duplicated)
(Both joins now run in parallel)
```

#### 3. Dynamic Switch to Broadcast Join

If AQE discovers one side of a join is tiny, it switches to broadcast.

```
Original plan: Shuffle join (both shuffled)
Runtime discovery: Right side is only 50MB
AQE switches to: Broadcast join (faster)
```

### Enabling AQE

```python
# AQE is ON by default in Databricks 7.3+
# Check if enabled
spark.conf.get("spark.sql.adaptive.enabled")  # Should be True

# Disable if needed (rare)
spark.conf.set("spark.sql.adaptive.enabled", False)

# Fine-tune AQE settings
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

### On Databricks

- **AQE is enabled by default** — No setup needed
- **Spark UI shows AQE optimizations** — Look for "Skew Join Optimization" in Stages tab
- **Best for complex queries** — Simple queries may not need AQE

### Common Mistakes

1. **Disabling AQE to "control" your job** — Usually hurts performance; let AQE optimize
2. **Not understanding AQE overhead** — Slight extra latency to monitor; usually worth it
3. **Assuming AQE fixes everything** — Manual tuning (partitioning, caching) still matters

### Code Example: See AQE in Action

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AQEExample").getOrCreate()

# Ensure AQE is enabled
spark.conf.set("spark.sql.adaptive.enabled", True)

# Create skewed data
left = spark.createDataFrame(
    [(i, f"val_{i}") for i in range(1000)],
    ["id", "left_val"]
)

right = spark.createDataFrame(
    [(i, f"right_{i}") for i in range(100)],  # Smaller
    ["id", "right_val"]
)

# Join (AQE may optimize)
result = left.join(right, "id", "inner")

# Check Spark UI for AQE optimizations
result.explain(mode="formatted")  # Shows execution plan

print("Check Spark UI Stages tab for 'Skew Join Optimization' or 'Coalesce Partitions'")
```

## Objective 3: Perform logging and monitoring of Spark applications

### Core Concept
When a job fails or runs slow, you read logs from the **Driver** (your code) and **Executors** (workers). These logs tell you what went wrong.

### Log Types

| Log Type | Source | Contains |
|----------|--------|----------|
| **Driver logs** | Driver node | Your code output, task scheduling decisions |
| **Executor logs** | Worker nodes | Task execution, serialization errors, out-of-memory |
| **Event logs** | Spark history | Job timing, stage progression, shuffle metrics |

### Common Errors and Diagnostics

#### Out-of-Memory (OOM) Error

**Symptom:** `java.lang.OutOfMemoryError: Java heap space` or `GC overhead limit exceeded`

**Root Causes:**
1. **Large partitions** — One partition > executor memory
2. **Too much caching** — Cached data fills memory, no room for execution
3. **Skewed data** — One executor gets 90% of rows; others get 10%
4. **Memory-intensive operations** — joins, groupBy, shuffle collect all data into memory

**Diagnosis:**

```python
# Calculate partition size
df = spark.read.parquet("/data/file.parquet")
total_size = df.rdd.map(lambda x: len(str(x))).sum()  # Rough estimate in bytes
num_partitions = df.rdd.getNumPartitions()
avg_partition_size = total_size / num_partitions if num_partitions > 0 else 0

print(f"Total size: {total_size / (1024**3):.2f} GB")
print(f"Partitions: {num_partitions}")
print(f"Avg partition size: {avg_partition_size / (1024**2):.2f} MB")

# Check executor memory
executor_memory = spark.conf.get("spark.executor.memory")
print(f"Executor memory: {executor_memory}")
```

**Table: OOM Scenarios and Fixes**

| Scenario | Cause | Fix |
|----------|-------|-----|
| Job fails on first groupBy | Too few partitions; large partitions | `.repartition(500)` or increase executor memory |
| Job runs slow, then crashes | Caching large intermediate results | Remove `.cache()` or use `.persist(StorageLevel.DISK_ONLY)` |
| One stage is slow, others fast | Data skew; some partitions huge | Repartition on high-cardinality column |
| Fails on `.collect()` | Trying to bring entire table to driver | Use `.take()` or filter first; don't use `.collect()` on huge data |

**Fixes (in order of preference):**

```python
from pyspark.sql import StorageLevel

# Fix 1: Repartition to increase parallelism
df_repartitioned = df.repartition(500)  # More partitions = smaller average size

# Fix 2: Increase executor memory (costs more)
spark.conf.set("spark.executor.memory", "16g")

# Fix 3: Use memory-efficient storage
df.persist(StorageLevel.DISK_ONLY)  # Cache to disk instead of memory

# Fix 4: Filter early to reduce data
from pyspark.sql.functions import col
df_filtered = df.filter(col("date") >= "2024-01-01")  # Smaller dataset

# Fix 5: Use approximate functions
from pyspark.sql.functions import approx_count_distinct
df.agg(approx_count_distinct("user_id"))  # Faster than count(distinct)
```

#### Cluster Underutilization

**Symptom:** Job runs slow; Spark UI shows executors idle.

**Diagnosis:**
1. Check partition count vs. executor count
2. Check for single-threaded bottlenecks (collect(), rdd.take(), etc.)

```
Example:
Partitions: 4
Executors: 100
Only 4 of 100 executors are doing work; 96 idle!
```

**Fix:**
- Repartition: `df.repartition(200)`
- Use more executors only if needed (costs money)

### Spark UI Deep Dive: Understanding Performance Metrics

**The Spark UI is your best debugging tool.** Here's what each tab shows:

#### Jobs Tab
- Shows each action (`.count()`, `.show()`, `.write()`)
- Click into a job to see stages

```
Job 1: collect() at notebook:45
├─ Status: SUCCEEDED
├─ Duration: 2.5s
├─ Stages: 2 (shows shuffle happened)
```

#### Stages Tab
- Shows each stage (separated by shuffles)
- **Key metrics:**
  - **Duration** — How long this stage took
  - **Tasks** — Number of parallel tasks
  - **Shuffle Bytes** — How much data was moved between partitions
  - **Max Task Duration** — Longest single task (skew if very high)
  - **Shuffle Read/Write** — Bytes shuffled

```
Stage 1: groupBy at notebook:40
├─ Duration: 0.5s
├─ Tasks: 100 (one per partition)
├─ Shuffle Write: 500 MB (data moving between partitions)
```

#### Tasks Tab (Click into a Stage)
- Shows individual task execution times
- **Red bars** = slow tasks (likely skew)
- **Look for:** Max task time >> Min task time (sign of skew)

```
Task 1: 10ms ✅ (Normal)
Task 2: 10ms ✅
...
Task 47: 5000ms 🔴 (Skewed! 500x slower)
```

**Action:** If one task is 10x slower than others, you have skew → repartition.

#### Executors Tab
- Shows memory/CPU usage per executor
- **Key metrics:**
  - **Used Memory** — How much heap is in use
  - **Max Memory** — Total executor memory
  - **GC Time** — Time spent garbage collecting (high = memory pressure)

```
Executor 1: 7.2 GB / 8 GB used (90% full!) → OOM risk
Executor 2: 2.1 GB / 8 GB used (OK)
Executor 3: 1.5 GB / 8 GB used (Idle?)
```

**Action:** If executors are 90%+ full, increase memory or repartition to spread load.

#### Storage Tab
- Shows cached DataFrames
- Size, partitions, memory used

```
df_users: 250 MB cached in memory
df_orders: 1.2 GB cached in memory (taking space!)
```

**Action:** Remove unnecessary caches; they take up memory needed for execution.

### Accessing Logs on Databricks

```python
# Method 1: Spark UI (built-in)
# - Click "Spark UI" link in notebook output
# - View "Stages" tab for duration, data sizes
# - Click stage to see task-level details

# Method 2: Driver logs (via notebook stderr)
# Already visible in notebook output

# Method 3: Executor logs (via Databricks UI)
# - Go to cluster page → "Driver logs" or "Executor logs"
# - Download and grep for errors

# Method 4: Inspect via code (advanced)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SparkUIExample").getOrCreate()

# Load data
df = spark.read.csv("/data/users.csv", header=True)

# Check partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Stage 1: Filter (no shuffle)
df_filtered = df.filter(col("age") > 30)

# Stage 2: GroupBy (shuffle!)
result = df_filtered.groupBy("department").count()

# Trigger execution
result.show()

# Now check Spark UI:
# - Jobs tab: 1 job
# - Stages tab: 2 stages (Stage 1 before shuffle, Stage 2 after)
# - Tasks tab: Many tasks in each stage
# - Click Stage 2 to see if some tasks are slow (skew indicator)
```
```

### Using Spark Listener (Advanced)

```python
from pyspark.sql import SparkSession

class LogListener:
    def onApplicationEnd(self, appEnd):
        print(f"App ended: {appEnd.appId}")

spark = SparkSession.builder.appName("LoggingExample").getOrCreate()
sc = spark.sparkContext

listener = LogListener()
sc.addSparkListener(listener)

# Run job
df = spark.read.csv("/data/file.csv", header=True)
result = df.groupBy("category").count()
result.show()
```

### Enabling Verbose Logging

```python
# Set log level for Spark
import logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log4jLogger.LogManager.getLogger("org.apache.spark").setLevel(log4jLogger.Level.DEBUG)
```

### On Databricks

- **Databricks provides a managed cluster** — Some logs are auto-collected
- **Check compute resources** — Spark UI clearly shows memory usage per executor
- **Use Jobs and Runs API** — Programmatically fetch run logs

### Common Mistakes

1. **Not checking partition count** — Assuming data is evenly parallelized
2. **Ignoring Spark UI Stages tab** — It shows everything: shuffle size, task duration, skew
3. **Blaming Spark when it's the code** — A slow Python UDF on 1M rows is 1M function calls
4. **Not monitoring cost** — Each executor hour costs money; optimize before scaling up

### Code Example: Monitor and Diagnose

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder.appName("MonitoringExample").getOrCreate()
sc = spark.sparkContext

# Create a job that shows typical issues
df = spark.read.parquet("/mnt/data/large_data.parquet")

print(f"=== Job Info ===")
print(f"Partitions: {df.rdd.getNumPartitions()}")
print(f"Executors (parallelism): {sc.defaultParallelism}")

# GroupBy (causes shuffle)
result = df.groupBy("category").agg(
    sum("amount").alias("total"),
    avg("amount").alias("avg"),
    col("*").count().alias("count")
)

# Trigger execution
result.show()

# Check Spark UI for:
# - Stage duration
# - Shuffle bytes read/written
# - Task skew (max time vs. min time)
print("\nCheck Spark UI for stage details and performance metrics")
```

---

## Summary: Section 4 Key Takeaways

1. **Partitioning**: Match partition count to executor count × cores; use `repartition()` for skew, `coalesce()` before write
2. **Skew detection**: Check partition sizes; large partitions bottleneck; fix by repartitioning
3. **Shuffle reduction**: GroupBy, joins, distinct cause shuffles; use sparingly
4. **AQE**: Automatic optimizer; improves skewed joins, coalesces small partitions, switches to broadcast
5. **Out-of-memory**: Caused by large partitions or caching too much; increase executor memory or partitions
6. **Cluster underutilization**: Too few partitions or sequential operations; increase partitions or use parallelism
7. **Logging**: Check Driver logs for your code; Executor logs for task errors; Spark UI for metrics
8. **Spark UI**: Most important tool; shows stages, tasks, shuffle metrics, skew

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Previous: Section 3 - DataFrame API](03_DataFrame_API.md) | [Next: Section 5 - Structured Streaming →](05_Structured_Streaming.md)
