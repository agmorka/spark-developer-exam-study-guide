# Section 1: Apache Spark Architecture and Components

---

## Table of Contents

1. [Objective 1: Identify the advantages and challenges of implementing Spark](#objective-1-identify-the-advantages-and-challenges-of-implementing-spark)
2. [Objective 2: Identify the role of core components of Apache Spark's Architecture](#objective-2-identify-the-role-of-core-components-of-apache-sparks-architecture)
3. [Objective 3: Describe the architecture of Apache Spark](#objective-3-describe-the-architecture-of-apache-spark-dataframe-dataset-sparksession-caching-storage-levels-garbage-collection)
4. [RDD vs. DataFrame: Understanding Spark Data Structures](#rdd-vs-dataframe-understanding-spark-data-structures)
5. [Objective 4: Explain the Apache Spark Architecture execution hierarchy](#objective-4-explain-the-apache-spark-architecture-execution-hierarchy)
6. [Objective 5: Configure Spark partitioning in distributed data processing](#objective-5-configure-spark-partitioning-in-distributed-data-processing-shuffles-and-partitions)
7. [Objective 6: Describe the execution patterns of Apache Spark](#objective-6-describe-the-execution-patterns-of-apache-spark-actions-transformations-lazy-evaluation)
8. [Objective 7: Identify the features of Apache Spark Modules](#objective-7-identify-the-features-of-apache-spark-modules-core-spark-sql-dataframes-pandas-api-on-spark-structured-streaming-mllib)
9. [Summary](#summary-section-1-key-takeaways)

---

## Objective 1: Identify the advantages and challenges of implementing Spark

### Core Concept
Apache Spark is a distributed data processing framework. It lets you process large datasets in parallel across multiple machines, but introduces complexity around cluster management, debugging, and resource coordination.

### Advantages on Databricks

| Advantage | What It Means |
|-----------|---------------|
| **Speed** | In-memory processing is faster than disk-based systems like Hadoop |
| **Ease of use** | Python, SQL, Scala, Java APIs let you write familiar code |
| **Unified processing** | Batch, streaming, ML, and SQL all in one platform |
| **Fault tolerance** | If a machine fails, Spark recovers lost work automatically |
| **Scalability** | Add more machines to handle bigger data without rewriting code |

### Challenges on Databricks

| Challenge | What It Means |
|-----------|---------------|
| **Memory management** | Improperly sized clusters can cause out-of-memory errors; garbage collection pauses can slow jobs |
| **Debugging** | Errors happen on remote executors; seeing what went wrong requires reading logs |
| **Network shuffles** | Moving data between machines is slow; inefficient joins/aggregations can cripple performance |
| **Cluster costs** | Running large clusters 24/7 is expensive; sizing the cluster correctly is hard |
| **Data skew** | If one machine gets most of the data, one slow executor delays the entire job |

### When to Use Spark vs. Alternatives

- **Use Spark** when: processing GB-TB of data, need SQL + Python together, want distributed fault tolerance
- **Use SQL warehouses instead** when: data is small (<10GB), you only need SQL queries, cost matters more than speed
- **Use Pandas** when: data fits in one machine's memory and you don't need distributed processing

### Common Mistakes

1. **Assuming Spark is always faster** — For small data (< 1GB), Spark overhead often makes it slower than Pandas
2. **Not understanding the cluster cost** — Running a 50-node cluster for an hour costs real money
3. **Ignoring memory limits** — Asking Spark to cache 500GB on a 100GB cluster causes failures
4. **Over-parallelizing small jobs** — A simple transformation on a tiny dataset doesn't need 100 partitions

### Code Example: Check Cluster & Data Size

```python
from pyspark.sql import SparkSession

# Start a session
spark = SparkSession.builder.appName("CheckCluster").getOrCreate()

# Read some data
df = spark.read.csv("/mnt/data/large_file.csv", header=True)

# Check how much data you have
print(f"Partitions: {df.rdd.getNumPartitions()}")
print(f"Estimated size: {df.rdd.map(lambda x: len(str(x))).sum()} bytes")

# Check cluster info via SparkContext
sc = spark.sparkContext
print(f"Executors: {sc.defaultParallelism}")
print(f"Master: {sc.master}")
```

### Recommended Resources
1. [Databricks: Getting Started with Apache Spark](https://www.databricks.com/spark/getting-started-with-apache-spark)
2. [Apache Spark: Cluster Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
3. [Databricks: Best practices for performance efficiency](https://docs.databricks.com/aws/en/lakehouse-architecture/performance-efficiency/best-practices)

---

## Objective 2: Identify the role of core components of Apache Spark's Architecture

### Core Concept
Spark runs as a cluster with a **driver** that orchestrates work and **executors** that do the work. The driver splits a job into tasks and sends them to executors. Each executor has memory and CPU cores.

### Architecture Diagram (Text)

```
┌─────────────────────────────────────────┐
│         Spark Driver Node               │
│  (Spark Context, Task Scheduler)        │
└─────────────────────────────────────────┘
              ↓ (distribute tasks)
    ┌─────────┬──────────┬──────────┐
    ↓         ↓          ↓          ↓
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│Executor│ │Executor│ │Executor│ │Executor│
│        │ │        │ │        │ │        │
│CPU     │ │CPU     │ │CPU     │ │CPU     │
│Memory  │ │Memory  │ │Memory  │ │Memory  │
└────────┘ └────────┘ └────────┘ └────────┘
 Worker 1   Worker 2   Worker 3   Worker 4
```

### Roles of Each Component

| Component | Responsibility |
|-----------|-----------------|
| **Driver** | Runs your Spark application code; breaks the job into tasks; talks to the cluster manager (Kubernetes, YARN, etc.) |
| **Executor** | Receives tasks from driver; runs them in parallel; stores data in memory/disk; sends results back to driver |
| **Worker Node** | A machine in the cluster; hosts one or more executors; provides CPU cores and memory |
| **CPU Core** | Runs one task at a time; a machine with 16 cores can run 16 tasks in parallel |
| **Memory (RAM)** | Stores cached data and intermediate results; if you run out, Spark spills to disk (slow) |

### On Databricks Specifically

- Databricks **manages the cluster** — you don't run Spark manually; Databricks handles the driver and executor provisioning
- The **driver runs in a special node** — this is where your code executes and where you see print output
- **Memory is shared** — if you cache a 50GB DataFrame and the executor only has 60GB RAM, other tasks have almost no room
- **Databricks monitors this** — you can see CPU/memory usage in the Cluster Metrics UI

### Common Mistakes

1. **Confusing driver and executor memory** — Driver memory controls how much data your code can hold in Python; executor memory controls how much Spark transforms can hold
2. **Assuming more executors = always faster** — If you add 100 executors but your data has only 10 partitions, 90 sit idle
3. **Not checking partition count** — If your DataFrame has 200 partitions but only 4 executors, you're not using parallelism efficiently

### Code Example: Inspect Cluster Components

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ClusterInfo").getOrCreate()
sc = spark.sparkContext

# Check number of partitions (tasks can run in parallel)
df = spark.read.csv("/mnt/data/file.csv", header=True)
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# Check how many executor cores the cluster has
print(f"Default parallelism (Executors × Cores): {sc.defaultParallelism}")

# Show executor details
executors = sc.statusTracker().executorInfos()
print(f"Number of executors: {len(executors)}")

# Check memory settings (from Spark config)
print(f"Executor memory: {spark.conf.get('spark.executor.memory')}")
print(f"Driver memory: {spark.conf.get('spark.driver.memory')}")
```

### Recommended Resources
1. [Databricks: Spark Architecture Concepts](https://docs.databricks.com/en/compute/clusters/index.html)
2. [Apache Spark Cluster Architecture](https://spark.apache.org/docs/latest/cluster-overview.html#components)
3. [DataStax: Understanding Spark Executors](https://www.datastax.com/blog/apache-spark-performance-tuning-101) (2024)

---

## Objective 3: Describe the architecture of Apache Spark (DataFrame, Dataset, SparkSession, caching, storage levels, garbage collection)

### Core Concept
Spark manages how your data is structured (**DataFrame/Dataset**), how sessions are created (**SparkSession**), how data stays in memory (**caching and storage levels**), and how memory is freed (**garbage collection**).

### DataFrame vs. Dataset

| Aspect | DataFrame | Dataset |
|--------|-----------|---------|
| **Type safety** | No — column types checked at runtime | Yes — compile-time type checking (Scala/Java) |
| **Language** | Python, SQL, Scala, Java | Scala, Java only |
| **Optimization** | Catalyst optimizer handles it | Catalyst optimizer + Encoders |
| **Performance** | Excellent | Excellent (when using typed operations) |
| **Use case** | Most common; when you don't need compile-time types | When you need strong types or work in Scala/Java |

**On Databricks:** You almost always use **DataFrames**. Datasets are rarely used in Python.

### SparkSession Lifecycle

```
1. Create session:     SparkSession.builder.appName("MyApp").getOrCreate()
                       (Creates driver, connects to cluster)

2. Read data:          df = spark.read.csv(...) or spark.sql("SELECT ...")
                       (Creates RDDs, organizes into partitions)

3. Transform:          df2 = df.filter(...).select(...).groupBy(...)
                       (Builds logical plan, NOT executed yet — lazy!)

4. Action:             df2.show() or df2.write.parquet(...)
                       (NOW Spark builds physical plan and executes)

5. Session ends:       spark.stop()
                       (Frees executors, closes connection)
```

**Key insight:** Steps 1–3 are "lazy." Spark only runs code when you call an **action** in step 4.

### Caching and Storage Levels

**Why cache?** If you use the same DataFrame multiple times, caching stores it in memory so Spark doesn't recompute it.

```python
# Without cache: Spark reads and transforms the CSV twice
df = spark.read.csv("/data/file.csv")
print(f"Count: {df.count()}")  # Reads file, counts rows
print(f"First row: {df.first()}")  # Reads file AGAIN, gets first row

# With cache: Spark reads and transforms once, reuses result
df = spark.read.csv("/data/file.csv")
df.cache()
print(f"Count: {df.count()}")  # Reads file, counts rows, stores in memory
print(f"First row: {df.first()}")  # Uses cached data, much faster
```

**Storage Levels** (from most to least memory):

| Level | Memory? | Disk? | Serialized? | Use case |
|-------|---------|-------|-------------|----------|
| `MEMORY_ONLY` | Yes | No | No | Small data, unlimited memory |
| `MEMORY_AND_DISK` | Yes | Yes | No | Default; if memory full, spill to disk |
| `MEMORY_ONLY_SER` | Yes | No | Yes | Save memory by compressing data |
| `DISK_ONLY` | No | Yes | No | Huge data that doesn't fit in memory |

**On Databricks:** Use `MEMORY_AND_DISK` (default) unless you have a specific reason.

### Garbage Collection (GC)

**What it is:** Java automatically frees unused memory. When Spark uses lots of memory, GC pauses can freeze your job.

**What you need to know:**
- More executor memory = longer, more frequent GC pauses
- Too little memory = constant GC, job runs slowly
- Databricks can show you GC time in logs (`GCTimeMillis`)

**Common mistake:** Assuming more memory is always better. More memory = more data to garbage collect = longer pauses.

### Code Example: Caching and Storage

```python
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.appName("CachingExample").getOrCreate()

# Read data
df = spark.read.csv("/mnt/data/large_file.csv", header=True)

# Cache with default level (MEMORY_AND_DISK)
df.cache()

# Or use a specific level
df.persist(StorageLevel.MEMORY_ONLY)

# First action: reads from disk, caches to memory
count1 = df.count()
print(f"First count (from disk): {count1}")

# Second action: reads from cache (fast!)
count2 = df.count()
print(f"Second count (from cache): {count2}")

# Remove from cache when done
df.unpersist()

# Check cache status
print(f"Is cached: {df.is_cached}")
```

---

## Objective 4: Explain the Apache Spark Architecture execution hierarchy

### Core Concept
When you call an action (like `.show()` or `.count()`), Spark breaks the work into a hierarchy:

```
JOB
  ↓
STAGEs (separated by shuffles)
  ↓
TASKs (one per partition)
```

Each level represents a smaller unit of work.

### The Hierarchy Explained

| Level | What It Is | Boundary |
|-------|-----------|----------|
| **Job** | One action (`.count()`, `.show()`, `.write()`) | Your line of code |
| **Stage** | A set of transforms that can run without moving data | Each shuffle (join, groupBy) ends a stage and starts a new one |
| **Task** | One partition's work, run by one executor core | One partition = one task |

### Example: Watch the Hierarchy Happen

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HierarchyExample").getOrCreate()

# Read a 100-partition file
df = spark.read.csv("/mnt/data/file.csv", header=True)

# Transform (lazy, no work yet)
df2 = df.filter(df.age > 30).select("name", "salary")

# Group by department (shuffle happens here!)
df3 = df2.groupBy("department").agg({"salary": "avg"})

# Action: triggers execution
result = df3.collect()
print(result)
```

**What Spark does internally:**

```
JOB: .collect()
  ├─ STAGE 1 (before shuffle):
  │    TASK 1: Read partition 1, filter, select
  │    TASK 2: Read partition 2, filter, select
  │    ... (100 tasks, one per partition)
  │
  └─ STAGE 2 (after shuffle):
       TASK 101: Read shuffled data for dept="Sales", compute avg
       TASK 102: Read shuffled data for dept="Eng", compute avg
       ... (fewer tasks; depends on how many unique departments)
```

### Why This Matters

- **Partitions = parallelism** — 100 partitions = 100 tasks can run in parallel (if you have 100 cores)
- **Shuffles are slow** — Stages separated by shuffles must wait for each stage to finish
- **Executors run tasks in parallel** — An executor with 4 cores can run 4 tasks simultaneously

### Code Example: Observe Execution Hierarchy

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HierarchyObserve").getOrCreate()
sc = spark.sparkContext

# Read data (100 partitions)
df = spark.read.option("header", "true").csv("/mnt/data/file.csv")

# Partition info before action
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Trigger an action
result = df.groupBy("category").count().collect()

# The Spark UI (in Databricks) will show:
# - 1 JOB
# - 2 STAGEs (one before shuffle, one after)
# - Many TASKs

print("Check the Spark UI in Databricks to see Jobs, Stages, and Tasks!")
```

**Where to see this in Databricks:**
- Click "Spark UI" in the cell output
- View "Jobs" tab (each action = 1 job)
- View "Stages" tab (each shuffle boundary = 1 stage)
- View "Tasks" tab (each partition's work = 1 task)

---

## RDD vs. DataFrame: Understanding Spark Data Structures

### Core Concept
**RDDs** (Resilient Distributed Datasets) are Spark's low-level API. **DataFrames** are a higher-level API built on top of RDDs. Exam focus is on DataFrames, but understanding RDDs is useful context.

### RDD vs. DataFrame Comparison

| Aspect | RDD | DataFrame |
|--------|-----|-----------|
| **Abstraction** | Low-level; collection of objects | High-level; rows and columns (like SQL tables) |
| **Schema** | No schema; just data | Has schema (column names, types) |
| **Optimization** | None; runs code as-is | Catalyst optimizer optimizes automatically |
| **Performance** | Slower; no optimization | Faster; optimized by Catalyst |
| **Ease of use** | Complex; requires manual partitioning | Simple; intuitive SQL-like API |
| **Language** | Python, Scala, Java | Python, SQL, Scala, Java |
| **When to use** | Complex custom operations | 99% of use cases (standard analytics) |

### RDD Example (Low-level)

```python
from pyspark import SparkContext

sc = SparkContext("local", "RDD Example")

# Create RDD (unstructured collection of objects)
rdd = sc.parallelize([1, 2, 3, 4, 5])

# RDD operations
squared = rdd.map(lambda x: x * x)  # Transform each element
result = squared.filter(lambda x: x > 5).collect()  # Filter and return
print(result)  # [16, 25]

# RDDs have no schema; just raw data
rdd_strings = sc.parallelize(["Alice,30", "Bob,25"])
# Need to parse manually:
rdd_parsed = rdd_strings.map(lambda line: line.split(","))
```

### DataFrame Example (High-level)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pow

spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()

# Create DataFrame (structured with rows/columns)
df = spark.createDataFrame(
    [(1, "Alice", 30), (2, "Bob", 25)],
    ["id", "name", "age"]
)

# DataFrame operations (more readable)
df_filtered = df.filter(col("age") > 25)
df_with_squares = df_filtered.withColumn("age_squared", pow(col("age"), 2))
df_with_squares.show()

# Schema is automatically tracked
df.printSchema()
# Output:
# root
#  |-- id: long
#  |-- name: string
#  |-- age: long
```

### Why DataFrames are Better

1. **Catalyst Optimizer** — Spark automatically optimizes DataFrame operations (push filters down, combine operations)
2. **SQL Integration** — Can query DataFrames with SQL
3. **Readable code** — DataFrame API is intuitive; RDD code is verbose
4. **Performance** — DataFrame operations are 10-100x faster than RDD operations

### When You Might See RDDs

- **Legacy code** — Old Spark codebases use RDDs
- **Exam question** — May ask "what is an RDD?" or "how does RDD differ from DataFrame?"
- **Advanced operations** — Sometimes `.rdd` property is used to access low-level API

```python
# Accessing RDD from DataFrame (rarely needed)
df = spark.read.csv("/data/file.csv")
rdd = df.rdd  # Convert to RDD

# Get partition count
num_partitions = rdd.getNumPartitions()
```

### On Databricks

- **Always use DataFrames** — RDDs are legacy; DataFrames are the modern standard
- **SQL is equivalent** — Can write same logic as SQL or DataFrame API; Spark optimizes both
- **No performance difference** — DataFrame API and SQL have same performance

### Common Mistakes

1. **Mixing RDD and DataFrame code unnecessarily** — Performance degrades if you convert between them
2. **Not using DataFrame operations** — Writing `.map()` on DataFrame.rdd when you should use `.withColumn()`
3. **Assuming RDDs are faster** — RDDs are slower; DataFrames are optimized

### Code Example: RDD vs. DataFrame Side-by-Side

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("RDDvsDataFrame").getOrCreate()

# Data: age values
data = [18, 25, 30, 35, 40, 45]

# RDD approach (complex, manual)
rdd = spark.sparkContext.parallelize(data)
rdd_filtered = rdd.filter(lambda x: x > 25)
rdd_squared = rdd_filtered.map(lambda x: x * x)
rdd_result = rdd_squared.collect()
print(f"RDD result: {rdd_result}")  # [900, 1225, 1600, 2025]

# DataFrame approach (clean, optimized)
df = spark.createDataFrame([(age,) for age in data], ["age"])
df_result = df.filter(col("age") > 25) \
    .withColumn("age_squared", col("age") * col("age")) \
    .select("age_squared").collect()
print(f"DataFrame result: {[row[0] for row in df_result]}")  # [900, 1225, 1600, 2025]

# Same result, but DataFrame is 10x faster and more readable
```

### Recommended Resources
1. [Databricks: Understanding Jobs, Stages, Tasks](https://docs.databricks.com/en/performance/monitoring.html)
2. [Apache Spark: Monitoring & Instrumentation](https://spark.apache.org/docs/latest/monitoring.html)
3. [Databricks: Spark UI Guide (2023)](https://docs.databricks.com/en/clusters/index.html)

---

## Objective 5: Configure Spark partitioning in distributed data processing (shuffles and partitions)

### Core Concept
**Partitions** split your data across the cluster. **Shuffles** move data between partitions when you do a join, groupBy, or orderBy. Getting this right is critical for performance.

### What Are Partitions?

A partition is a chunk of data on one executor. If you have 100 partitions, your data is split into 100 pieces, and Spark can process them in parallel (one per executor core).

```
DataFrame with 100 partitions:
┌────┬────┬────┬─────┬────┐
│ P1 │ P2 │ P3 │ ... │P100│
└────┴────┴────┴─────┴────┘
  |    |    |           |
Exec1 Exec2 Exec3      Exec4
```

### What Is a Shuffle?

A shuffle moves data between partitions. This happens when you:
- `groupBy()` — gather all rows with the same key onto one partition
- `join()` — align rows from two DataFrames by key
- `distinct()` — remove duplicates (requires moving data)
- `orderBy()` — sort globally (requires all data on one partition temporarily)

**Shuffles are expensive** — all this data movement goes over the network.

### Configuring Partitions

| Operation | What It Does |
|-----------|--------------|
| `.repartition(n)` | Shuffle data to create exactly `n` new partitions |
| `.coalesce(n)` | Combine partitions without shuffling (faster, but data is imbalanced) |

```python
# 100 partitions becomes 10 (shuffles data)
df_repartitioned = df.repartition(10)

# 100 partitions becomes 10 (no shuffle, but executor 1 might have 90 partitions)
df_coalesced = df.coalesce(10)
```

### The `spark.sql.shuffle.partitions` Config

From the exam sample questions:

> "What will be the impact of setting `spark.sql.shuffle.partitions` to 200?"

This **global config** says: "When you do a groupBy or join, create 200 new partitions after the shuffle."

**Default:** 200 partitions

**Tuning rule:**
- **Increase** if: shuffles create data skew (one partition much larger than others)
- **Decrease** if: you have many tiny partitions causing overhead
- **Set to:** (num executors × cores per executor) for optimal parallelism

```python
spark.conf.set("spark.sql.shuffle.partitions", 100)  # Default 200, reduce to 100
```

### Common Mistakes

1. **Too many partitions** — 1000 partitions with only 10 executors wastes overhead
2. **Too few partitions** — 4 partitions with 100 executors wastes parallelism
3. **Repartitioning too often** — Each repartition shuffles; avoid unless necessary
4. **Ignoring data skew** — One partition with 99% of rows while others have 1% each

### Code Example: Partition Management

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionExample").getOrCreate()

# Read data
df = spark.read.csv("/mnt/data/file.csv", header=True)
print(f"Initial partitions: {df.rdd.getNumPartitions()}")  # Probably 1

# Set shuffle partition config
spark.conf.set("spark.sql.shuffle.partitions", 50)

# GroupBy will create 50 partitions after shuffle
result = df.groupBy("category").count()
print(f"After groupBy, partitions: {result.rdd.getNumPartitions()}")  # 50

# Repartition to 20 (expensive — shuffles data)
df_repartitioned = df.repartition(20)
print(f"After repartition(20): {df_repartitioned.rdd.getNumPartitions()}")  # 20

# Coalesce to 10 (no shuffle)
df_coalesced = df.coalesce(10)
print(f"After coalesce(10): {df_coalesced.rdd.getNumPartitions()}")  # 10
```

### Recommended Resources
1. [Databricks: Partitioning Best Practices](https://docs.databricks.com/en/performance/partitioning.html)
2. [Apache Spark: RDD Partitions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds)
3. [Databricks: Shuffle Performance](https://docs.databricks.com/en/performance/shuffle.html)

---

## Objective 6: Describe the execution patterns of Apache Spark (actions, transformations, lazy evaluation)

### Core Concept
Spark code is **lazy** — transforms don't run immediately. Only **actions** force Spark to actually execute. This is called **lazy evaluation**.

### Transformations (Lazy)

Transformations create a new DataFrame **without running code yet**. They build a plan.

| Transformation | Example |
|---|---|
| `.select()` | Choose columns |
| `.filter()` | Keep rows where condition is true |
| `.groupBy()` | Group rows by a column |
| `.join()` | Combine two DataFrames |
| `.withColumn()` | Add or modify a column |
| `.distinct()` | Remove duplicate rows |
| `.orderBy()` | Sort rows |

```python
df = spark.read.csv("/data/file.csv")
df2 = df.filter(df.age > 30)          # Lazy — no execution
df3 = df2.select("name", "salary")    # Lazy — no execution
df4 = df3.groupBy("department").count()  # Lazy — no execution
```

**Spark has built a plan but not run it yet.**

### Actions (Eager)

Actions **force execution** and return results to the driver.

| Action | What It Does |
|---|---|
| `.show()` | Print first 20 rows |
| `.collect()` | Return all rows to driver as a Python list (dangerous if huge!) |
| `.count()` | Return number of rows |
| `.first()` | Return first row |
| `.take(n)` | Return first n rows |
| `.write.parquet()` | Save to disk |
| `.write.csv()` | Save as CSV |

```python
df = spark.read.csv("/data/file.csv")
df2 = df.filter(df.age > 30)
df3 = df2.select("name", "salary")

# The following lines trigger execution:
df3.show()               # Action 1 — runs the whole plan
count = df3.count()      # Action 2 — runs the whole plan again!
```

**IMPORTANT:** Each action runs the entire chain of transforms. If you call `show()` and then `count()`, Spark reads the CSV and filters twice.

**Fix with caching:**
```python
df3.cache()        # Store in memory
df3.show()         # Runs once, caches result
count = df3.count()  # Uses cache (fast)
```

### Lazy Evaluation Benefits

1. **Optimization** — Spark sees the whole plan before running it, so it can optimize (push filters down, combine operations)
2. **Efficiency** — Spark can skip unnecessary work if you don't ask for the result
3. **Pipelining** — Multiple operations can run in one pass through the data

### Common Mistakes

1. **Calling `.collect()` on huge DataFrames** — Tries to move all rows to your driver; crashes if data > driver memory
2. **Not caching reused DataFrames** — Recomputes the same transforms over and over
3. **Assuming prints show all data** — `.show()` only prints 20 rows, doesn't tell you if there's more
4. **Misunderstanding what `.collect()` does** — It's not "collect garbage"; it collects all data to the driver

### Code Example: Lazy vs. Eager

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LazyExample").getOrCreate()

df = spark.read.csv("/mnt/data/file.csv", header=True)

# Build a plan (LAZY — no execution)
print("Building plan...")
df_filtered = df.filter(df.age > 30)
df_selected = df_filtered.select("name", "salary")
df_grouped = df_selected.groupBy("department").count()
print("Plan built. No execution yet.")

# Trigger execution with an action
print("Now executing...")
result = df_grouped.show()  # EAGER — runs now

# Each action re-runs the plan (inefficient!)
print("Count action — will re-run the plan:")
total = df_grouped.count()  # EAGER — runs the plan again from the CSV
print(f"Total: {total}")

# Fix with cache:
df_grouped.cache()
print("Cached. Now fast:")
df_grouped.show()  # Uses cache
total = df_grouped.count()  # Uses cache (much faster)
```

### Recommended Resources
1. [Apache Spark: Lazy Evaluation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#basics)
2. [Apache Spark: RDD Transformations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)
3. [Databricks: DataFrame Operations](https://docs.databricks.com/en/develop/dataframes.html)

---

## Objective 7: Identify the features of Apache Spark Modules (Core, Spark SQL, DataFrames, Pandas API on Spark, Structured Streaming, MLlib)

### Core Concept
Spark is modular. Different modules handle different workloads:

| Module | Purpose | Use Case |
|--------|---------|----------|
| **Spark Core** | Distributed processing engine; all others build on this | Low-level RDD operations, custom parallelization |
| **Spark SQL** | SQL queries and optimization | Running SQL against data, joining tables, analytics |
| **DataFrames** | Tabular (row/column) interface to Spark Core | Most common; Python, SQL, Scala, Java |
| **Pandas API on Spark** | Write Pandas-like code that runs on Spark | Familiar Pandas syntax on distributed data |
| **Structured Streaming** | Real-time processing with exactly-once semantics | Kafka streams, IoT sensors, live data |
| **MLlib** | Machine learning algorithms | Classification, regression, clustering, NLP |

### Spark Core

**What it is:** The lowest-level API. Works with **RDDs** (Resilient Distributed Datasets).

**When to use:** Almost never. Use DataFrames instead (Spark SQL optimizes them).

```python
# RDD (Spark Core) — rarely used in modern code
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * 2).collect()  # [2, 4, 6, 8, 10]
```

### Spark SQL

**What it is:** Run SQL queries. Combines with DataFrames for optimization.

**When to use:** You know SQL and want query syntax.

```python
# Create a table
df = spark.read.csv("/data/file.csv", header=True)
df.createOrReplaceTempView("users")

# Query with SQL
result = spark.sql("SELECT name, salary FROM users WHERE age > 30")
result.show()
```

### DataFrames

**What it is:** Tabular interface (rows, columns). Most common API.

**When to use:** Always (unless you have a specific reason not to).

```python
df = spark.read.csv("/data/file.csv", header=True)
df.filter(df.age > 30).select("name", "salary").show()
```

### Pandas API on Spark

**What it is:** Write code that looks like Pandas but runs on Spark.

**When to use:** You know Pandas and want familiar syntax on distributed data.

```python
import pyspark.pandas as ps

# Looks like Pandas, runs on Spark
pdf = ps.read_csv("/data/file.csv")
result = pdf[pdf.age > 30][["name", "salary"]].groupby("department").mean()
print(result)
```

**On Databricks:** This is supported; helps Pandas users transition to Spark.

### Structured Streaming

**What it is:** Real-time processing with the same DataFrame API.

**When to use:** Processing continuously arriving data (Kafka, cloud storage, sensors).

```python
# Read from a Kafka stream
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "topic") \
    .load()

# Transform (same as batch DataFrame API)
result = df_stream.select("value")

# Write to output (write, not action)
query = result.writeStream \
    .format("parquet") \
    .option("path", "/output") \
    .start()
```

**Key difference from batch:** `.readStream` and `.writeStream` instead of `.read` and `.write`.

### MLlib

**What it is:** Spark's machine learning library.

**When to use:** Building ML pipelines (feature engineering, training, prediction).

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

# Prepare features
assembler = VectorAssembler(inputCols=["age", "salary"], outputCol="features")
features_df = assembler.transform(df)

# Train logistic regression
lr = LogisticRegression(labelCol="hired")
model = lr.fit(features_df)

# Predict
predictions = model.transform(features_df)
```

### Code Example: Using Different Modules

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.appName("ModulesExample").getOrCreate()

# --- Spark SQL + DataFrames ---
df = spark.read.csv("/data/employees.csv", header=True)
df.createOrReplaceTempView("employees")

# SQL query
result = spark.sql("SELECT department, AVG(salary) as avg_sal FROM employees GROUP BY department")
result.show()

# --- Structured Streaming (conceptual) ---
# df_stream = spark.readStream.format("kafka").option(...).load()

# --- MLlib ---
# (Requires numeric features and labels — simplified example)
# assembler = VectorAssembler(inputCols=["age", "salary"], outputCol="features")
# lr = LogisticRegression()
# model = lr.fit(...)
```

### Recommended Resources
1. [Apache Spark: Spark SQL Overview](https://spark.apache.org/docs/latest/sql-index.html)
2. [Apache Spark: Structured APIs](https://spark.apache.org/docs/latest/rdd-programming-guide.html#overview)
3. [Apache Spark: MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)

---

## Summary: Section 1 Key Takeaways

1. **Spark is powerful but complex** — Know the tradeoffs (speed vs. cost, parallelism vs. overhead)
2. **Architecture is hierarchical** — Driver → Executors → Tasks → Partitions
3. **Lazy evaluation is key** — Transforms build plans; actions execute them
4. **Partitions and shuffles matter** — Right partitioning = fast jobs; wrong = slow
5. **Modules serve different needs** — DataFrames for ETL, SQL for queries, Streaming for real-time, MLlib for ML
6. **Caching prevents recomputation** — Use it when you reference a DataFrame multiple times
7. **On Databricks** — The platform abstracts cluster management; focus on writing good Spark code

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Next: Section 2 - Spark SQL →](02_Spark_SQL.md)
