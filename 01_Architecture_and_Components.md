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

### Spark Memory Management

Each executor's memory is divided into three categories:

| Category | Purpose | Size | Details |
|----------|---------|------|---------|
| **Execution Memory** | Sorting, shuffles, joins, aggregations | 60% (configurable) | Temporary memory for transformations |
| **Storage Memory** | Caching DataFrames, broadcast variables | 40% (configurable) | Used by `.cache()` and `.persist()` |
| **Reserved Memory** | Spark overhead | 300 MB (fixed) | System memory; not available to user code |

**Key Points:**
- If Execution memory is full, Spark spills to disk (slower)
- If Storage memory is full, cached data is evicted (recalculated on next use)
- You cannot cache more data than Storage memory allows

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

### Configuring Spark with spark.conf.set()

You can tune Spark's behavior by setting configuration options. **⚠️ IMPORTANT: All parameter values must be strings, including numbers.**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConfigExample").getOrCreate()

# ✓ CORRECT: Parameters are strings
spark.conf.set("spark.sql.shuffle.partitions", "20")      # String: "20"
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20mb")  # String: "20mb"
spark.conf.set("spark.default.parallelism", "16")          # String: "16"

# ✗ WRONG: Don't pass integers directly (will fail or be ignored)
spark.conf.set("spark.sql.shuffle.partitions", 20)         # Integer: Causes error!
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20)  # Integer: Missing "mb" unit!

# Reading config
shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Shuffle partitions: {shuffle_partitions}")
```

**Common Spark Configuration Options:**

| Option | Default | Example | Purpose |
|--------|---------|---------|---------|
| `spark.sql.shuffle.partitions` | 200 | "20" | Number of partitions after shuffle (groupBy, join) |
| `spark.sql.autoBroadcastJoinThreshold` | "10mb" | "20mb" | Max size to broadcast in joins; use "mb" suffix |
| `spark.default.parallelism` | Cores | "16" | Default parallelism for RDD operations |
| `spark.executor.memory` | "1g" | "4g" | Memory per executor (set at session creation, not conf.set) |
| `spark.driver.memory` | "1g" | "4g" | Driver memory (set at session creation) |
| `spark.sql.cbo.enabled` | false | "true" | Cost-based optimizer (for better query plans) |

**When to change these:**
- `spark.sql.shuffle.partitions` — Too high = overhead; too low = slow shuffles. 20-200 typically optimal.
- `spark.sql.autoBroadcastJoinThreshold` — Lower for small clusters (less memory), higher for broadcast-friendly workloads.

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

| Operation | Signature | What It Does |
|-----------|-----------|--------------|
| `.repartition(n)` | `repartition(numPartitions)` | Shuffle data to create exactly `n` new partitions |
| `.repartition(cols)` | `repartition(*cols)` | Repartition data by column values (e.g., by storeId, date) |
| `.coalesce(n)` | `coalesce(numPartitions)` | Combine partitions without shuffling (takes INT only, NOT columns) |

```python
# Repartition by number only
df_repartitioned = df.repartition(10)

# Repartition by column(s) — shuffles data across those columns
df_repartitioned_by_cols = df.repartition("storeId", "transactionDate")

# Coalesce by number only — NO column parameter
df_coalesced = df.coalesce(10)  # ✓ Correct: takes integer only

# ❌ WRONG: df.coalesce(14, ("storeId", "transactionDate"))
# → TypeError: coalesce() takes only integer, not tuple of columns
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

### Predicate Pushdown (Filter Optimization)

**What is it?** Spark automatically moves `.filter()` operations as early as possible, before reading/joining large datasets.

**Why it matters:** You read less data = faster queries.

```python
# Without pushdown (slow):
df = spark.read.parquet("/huge/dataset")  # Read all 1TB
df_filtered = df.filter(df["department"] == "Sales")  # Then filter
result = df_filtered.groupBy("department").count()

# Spark optimization (with pushdown, fast):
# Spark moves the filter BEFORE reading from Parquet
# Result: Only reads Sales department rows (~10 GB instead of 1TB)
df = spark.read.parquet("/huge/dataset").filter(df["department"] == "Sales")
result = df.groupBy("department").count()

# SQL (automatic pushdown):
spark.sql("""
    SELECT department, COUNT(*)
    FROM huge_dataset
    WHERE department = 'Sales'  -- Spark pushes this filter to the Parquet reader
    GROUP BY department
""").show()
```

**When Pushdown Happens:**
- Filters are pushed down to Parquet/Delta/CSV readers when possible
- Joins with filters are optimized to filter early
- Column pruning: Spark only reads columns you actually use

**When Pushdown DOESN'T Happen:**
- After a shuffle operation (like `groupBy` or `join`): can't push past shuffle
- After a UDF: can't optimize through custom code
- Complex nested filters: Spark can't always push these through

### Catalyst Optimizer

**What is it?** Spark's query optimization engine that converts your code into an optimized execution plan.

**How it works:**

1. **Logical Plan** — Your code as written (what to do)
   - Transforms stored in order: read → filter → select → groupBy
   - **Not optimized** — just a tree of operations

2. **Optimizer** — Catalyst improves the logical plan
   - Pushes filters down
   - Eliminates unnecessary columns
   - Combines operations
   - Reorders joins

3. **Physical Plan** — Optimized instructions (how to do it efficiently)
   - Decides which executor runs which task
   - Chooses join algorithm (broadcast, sort-merge, hash)
   - Specifies shuffle or no-shuffle operations

**Example:**

```python
# Your code (Logical Plan):
df = spark.read.parquet("/huge/data")  # Billions of rows
df = df.filter(df["dept"] == "Sales")  # Only 1% of rows
df = df.select("name", "salary")
result = df.groupBy("dept").agg({"salary": "avg"})

# Catalyst's optimized plan:
# 1. Push filter to Parquet reader (read only Sales rows, ~10GB)
# 2. Column pruning (only read dept, salary, name columns)
# 3. ReOrder groupBy to be more efficient
# 4. Result: 1000x faster than reading all data first
```

### Stages and Shuffles with Disk I/O

**What are stages?** A stage is a set of transformations that can run without shuffling data. Different stages CAN run in **parallel**.

```
Stage 1 (Read → Filter → Select): Can run in parallel on all partitions
  └─ No shuffle yet
Stage 2 (GroupBy → Agg): Different stage; requires shuffle from Stage 1
  └─ Shuffle: Write Stage 1 output to disk, read into Stage 2
Stage 3 (OrderBy): Another stage
  └─ Shuffle: Write Stage 2 output to disk, read into Stage 3
```

**Key points:**
- Stages within a job CAN execute in parallel if they don't depend on each other
- BUT: Stages separated by shuffles MUST wait (shuffle is a barrier)
- Each shuffle involves **disk I/O and network transfers** (very expensive)

**Example - Stages execute in parallel:**

```
Job: df.groupBy("dept").count().orderBy("count")
├─ Stage 1: Read → GroupBy partial aggregate (tasks 1-100 run in PARALLEL)
├─ SHUFFLE: Write to disk, network transfer
├─ Stage 2: GroupBy final aggregate (tasks 101-150 run in PARALLEL)
├─ SHUFFLE: Write to disk, network transfer
└─ Stage 3: OrderBy → Collect (tasks 151-160 run in PARALLEL)

Stages 1, 2, 3 run sequentially (because of shuffles)
But within each stage, tasks run in parallel
```

**Shuffle mechanics - Shuffles write to disk:**

```
Without Shuffle (Map):
Partition 1 on Executor A: Process locally, output to memory

With Shuffle (Map → Shuffle → Reduce):
Partition 1 on Executor A:
  - Process data
  - Write sorted output to LOCAL DISK (not memory!)
  
Executor B reads from Executor A's disk, brings data over network
```

### Cluster Manager and Driver/Executor Lifecycle

**Cluster Manager:** Allocates resources (CPU cores, memory) to Spark. Examples: YARN, Kubernetes, Standalone, Mesos.

**Driver Lifecycle:**
1. **Start** — User submits Spark job; driver process starts
2. **Create SparkContext** — Driver initializes SparkContext (or SparkSession)
3. **Contact Cluster Manager** — Driver asks for executors ("I need 10 executors with 4 cores each")
4. **Executor Launch** — Cluster Manager receives request from driver and allocates resources
5. **Execute Job** — Driver sends tasks to executors; monitors completion
6. **End** — When job finishes, driver shuts down (or goes idle in REPL)

**Executor Lifecycle:**
1. **Launch** — Cluster manager starts executor JVM on worker node (initiated by driver)
2. **Register** — Executor registers with driver
3. **Execute Tasks** — Receives and runs tasks from driver
4. **Stop** — Executor stops when application completes (or removed by cluster manager)

**Key relationships:**
- **Driver ← (sends tasks to) → Executors** — One-way: driver tells executors what to do
- **Cluster Manager ← (requests resources from) ← Driver** — Driver asks for resources
- **Multiple executors, ONE driver** — The driver is a single point of coordination

```python
# On Databricks, cluster manager is handled for you:
# - Driver automatically creates SparkSession
# - Cluster manager (Unity Catalog, Kubernetes) handles executor launch

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
# Internally:
# 1. Driver process started
# 2. SparkContext created
# 3. Driver contacts cluster manager
# 4. Executors launched
```

### Actions Return Data to Driver

**Actions like `.collect()` bring data from executors to driver memory.**

```python
# Transform (runs on executors, distributed)
df = spark.read.csv("/huge/file.csv")
df_filtered = df.filter(df["salary"] > 50000)

# Action: .collect() brings results to driver
results = df_filtered.collect()  # All results now in driver's memory!

# If results are huge (10GB), driver runs OOM
```

**⚠️ IMPORTANT:**
- `.collect()` is dangerous on large datasets
- `.show()` only shows first 20 rows (safe)
- `.count()` returns just one number (safe)
- `.take(n)` returns first n rows (safer than collect)

---
df = df.select("name", "salary")       # Only 2 columns
result = df.groupBy("name").sum("salary")

# Catalyst optimization:
# ✗ WRONG: Read → Filter → Select → GroupBy (reads everything)
# ✓ RIGHT: 
#   1. Push filter to Parquet reader (read only 1% of rows)
#   2. Prune to only 2 columns (don't load all columns)
#   3. Then groupBy (aggregates on small data)
# RESULT: 100x faster (minimal I/O)
```

**You can see the optimized plan:**

```python
# View logical and physical plans
df.explain()  # Prints both plans
df.explain(extended=True)  # Even more detail

# Output shows:
# == Logical Plan ==
# (Original operations in tree form)
# == Physical Plan ==
# (Optimized with code generation, broadcast hints, etc.)
```

**Catalyst Optimizations:**

| Technique | What it does | Example |
|-----------|-------------|---------|
| **Predicate Pushdown** | Moves filters earlier | Filter before join instead of after |
| **Column Pruning** | Read only columns you use | Select 2 columns → don't load others |
| **Constant Folding** | Evaluate constants at compile time | `1 + 2 < value` becomes `3 < value` |
| **Join Reordering** | Reorder joins for efficiency | Small join first, then large |
| **Null Propagation** | Eliminate unnecessary operations | `col > 5 AND col IS NULL` = always false |



**What is Lineage?** The complete record of all transformations applied to a DataFrame/RDD.

**Why it matters:** When an executor fails, Spark uses lineage to recalculate lost partitions instead of restarting from scratch.

```python
# Lineage example:
df1 = spark.read.parquet("/data/input")  # Lineage: [Read Parquet]
df2 = df1.filter(df1["age"] > 30)         # Lineage: [Read Parquet → Filter age > 30]
df3 = df2.select("name", "salary")        # Lineage: [Read Parquet → Filter age > 30 → Select]
df4 = df3.groupBy("name").sum("salary")   # Lineage: [... → Select → GroupBy Sum]
result = df4.collect()                    # Action: executes the entire lineage

# If an executor dies during Select operation:
# - Spark doesn't restart from Parquet read
# - It recalculates just the failed partition using the lineage
# - Other partitions continue normally
```

**Storage Levels and Fault Tolerance:**

```python
# MEMORY_ONLY: Fast but risky
df.persist(StorageLevel.MEMORY_ONLY)
# If executor dies, must recalculate entire DataFrame from lineage

# MEMORY_AND_DISK: Balanced (recommended)
df.persist(StorageLevel.MEMORY_AND_DISK)
# Spills to disk if memory full; survives executor failures

# If using RDD:
rdd.persist(StorageLevel.MEMORY_AND_DISK)  # Stores lineage for recovery
```

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
