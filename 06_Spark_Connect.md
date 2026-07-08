# Section 6: Using Spark Connect to Deploy Applications

---

## Table of Contents

1. [Objective 1: Describe the features of Spark Connect](#objective-1-describe-the-features-of-spark-connect)
2. [Objective 2: Describe different deployment mode types](#objective-2-describe-different-deployment-mode-types-client-cluster-local)
3. [Summary](#summary-section-6-key-takeaways)

---

## Objective 1: Describe the features of Spark Connect

### Core Concept
**Spark Connect** is a new (Spark 3.4+) way to run Spark applications. It separates the client (your Python/R code) from the Spark cluster using a gRPC protocol. This enables:
- Running Spark from your laptop and connecting to a remote cluster
- Multi-language support (Python, Scala, Java, R in future)
- Easier local development

### Running SparkSession Locally vs. Remote

#### Local SparkSession (Development & Testing)

Run Spark entirely on your machine without a cluster:

```python
from pyspark.sql import SparkSession

# Simplest: local mode with auto-detected cores
spark = SparkSession.builder \
    .appName("LocalApp") \
    .master("local") \
    .getOrCreate()

# With specific core count
spark = SparkSession.builder \
    .appName("LocalApp") \
    .master("local[4]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# Use all available cores
spark = SparkSession.builder \
    .appName("LocalApp") \
    .master("local[*]") \
    .getOrCreate()

# With all cores and 2 max retries
spark = SparkSession.builder \
    .appName("LocalApp") \
    .master("local[*,2]") \
    .getOrCreate()
```

**Use local mode when:**
- Developing locally on your laptop
- Testing code before sending to cluster
- Debugging logic (fast iteration)
- Data is small enough to fit in memory

#### Remote Cluster Connection (Spark Connect)

Connect from your machine to a remote cluster:

```python
from pyspark.sql import SparkSession

# Connect to Spark Connect server on remote cluster
spark = SparkSession.builder \
    .remote("sc://cluster-host.example.com:15002") \
    .getOrCreate()

# With authentication (if required)
spark = SparkSession.builder \
    .remote("sc://cluster-host.example.com:15002") \
    .config("spark.connect.auth", "token") \
    .config("spark.connect.token", "YOUR_TOKEN") \
    .getOrCreate()

# Databricks-specific (Spark Connect)
spark = SparkSession.builder \
    .remote("databricks") \
    .config("databricks_host", "https://your-instance.cloud.databricks.com") \
    .config("databricks_token", "dapi...") \
    .getOrCreate()
```

#### Mocking Cluster Behavior Locally

For testing/development, simulate cluster behavior on your machine:

```python
from pyspark.sql import SparkSession

# Simulate multi-partition processing
spark = SparkSession.builder \
    .appName("ClusterMock") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

# Create a small dataset and repartition to simulate multi-node
df = spark.createDataFrame([(i, f"row_{i}") for i in range(100)], ["id", "value"])

# Simulate 4 partitions (like 4 executor nodes)
df_repartitioned = df.repartition(4)

# Show how many partitions (same as number of "executor nodes" in mock)
print(f"Partitions (mock nodes): {df_repartitioned.rdd.getNumPartitions()}")

# Test shuffle behavior locally
result = df_repartitioned.groupBy("id").count()
result.show()
```

**Mocking setup useful for:**
- Testing shuffle behavior before cluster submission
- Validating partition distribution logic
- Catching skew problems early
- Unit testing data pipelines

### Traditional Spark (Before Connect)

```
Your code (PySpark)
    ↓
JVM embedded in your process
    ↓
Sends tasks to cluster
```

**Problem:** Your Python process runs in the JVM; hard to isolate; language-specific.

### Spark Connect (New)

```
Your Python/Scala code
    ↓
Spark Connect Client (thin library)
    ↓
gRPC Protocol (over network)
    ↓
Spark Connect Server on cluster
    ↓
Sends tasks to workers
```

**Benefits:**
- **Language agnostic** — Python, Scala, Java, R can all use same cluster
- **Lightweight client** — No JVM overhead on your machine
- **Remote development** — Run Spark jobs from your laptop against a cloud cluster
- **Isolation** — Cluster crash doesn't kill your client

### Spark Connect vs. Traditional Spark

| Aspect | Traditional | Spark Connect |
|--------|-------------|---|
| **Client location** | On cluster (driver node) | Your machine |
| **Connection** | None (embedded JVM) | gRPC over network |
| **Startup time** | Slow (JVM startup) | Fast (thin library) |
| **Language** | Python/Scala/Java in JVM | Extensible to many languages |
| **Best for** | Production cluster jobs | Development, data exploration |

### Spark Connect Architecture

```
Client (your laptop)
    ↓
Spark Connect Client
    (builds logical plans)
    ↓
gRPC (sends plan)
    ↓
Spark Connect Server (on cluster)
    (executes plan)
    ↓
Worker nodes (run tasks)
```

### On Databricks

- **Databricks uses Spark Connect** for notebooks and SDK
- **Enables Databricks SDKs** in Python, R, Scala
- **Unified API** — Same code works in notebooks and remote clients

### Local vs. Remote Configuration Comparison

| Aspect | Local | Remote (Spark Connect) |
|--------|-------|---|
| **Setup** | `master("local[4]")` | `remote("sc://host:15002")` |
| **Data size** | Small (fits in RAM) | Any size (distributed) |
| **Latency** | None (same machine) | Network overhead |
| **Parallelism** | Limited by cores | Unlimited (across cluster) |
| **For testing** | Single-machine logic | Multi-node behavior requires mock setup |
| **Memory constraint** | Your machine RAM | Cluster total memory |

### SparkSession Configuration Options

**Local Mode Options:**

```python
spark = SparkSession.builder \
    .master("local[4]")                              # 4 cores
    .config("spark.driver.memory", "8g")             # Driver memory
    .config("spark.executor.memory", "4g")           # Executor memory
    .config("spark.sql.shuffle.partitions", "4")     # Partitions for shuffle
    .config("spark.default.parallelism", "4")        # RDD parallelism
    .config("spark.driver.maxResultSize", "2g")      # Max result size
    .config("spark.sql.adaptive.enabled", "true")    # Adaptive Query Execution
    .appName("MyLocalApp") \
    .getOrCreate()
```

**Remote Connection Options:**

```python
spark = SparkSession.builder \
    .remote("sc://cluster-host:15002")               # Remote server
    .config("spark.connect.timeout", "300s")         # Connection timeout
    .config("spark.connect.retries", "3")            # Connection retries
    .config("spark.connect.userAgent", "MyApp/1.0")  # User agent
    .appName("MyRemoteApp") \
    .getOrCreate()
```

**Key Configuration Parameters:**

| Parameter | Default | Purpose | Local vs Remote |
|-----------|---------|---------|---|
| `spark.driver.memory` | 1g | Driver JVM memory | Local only |
| `spark.executor.memory` | 1g | Executor memory per node | Local: single, Remote: per executor |
| `spark.sql.shuffle.partitions` | 200 | Partitions after shuffle | Both (useful for mock testing) |
| `spark.default.parallelism` | CPU cores | RDD parallelism | Local: machine cores, Remote: across cluster |
| `spark.driver.maxResultSize` | 1g | Max result from collect() | Both (prevent driver OOM) |

### Common Mistakes

1. **Confusing with Spark Submit** — Spark Connect is a communication layer; Spark Submit is a launcher
2. **Thinking it replaces cluster mode** — Spark Connect still needs a cluster; you connect to it remotely
3. **Using for all workloads** — Traditional Spark still better for large batch jobs; Spark Connect for interactive use
4. **Running large jobs in local mode** — Local mode will OOM if data > machine RAM; use remote cluster instead
5. **Forgetting to set partitions in mock** — For realistic mock testing, manually set `spark.sql.shuffle.partitions` to simulate cluster partitions

### Code Example: Connect to Remote Cluster

```python
# Spark Connect (Spark 3.4+)
from pyspark.sql import SparkSession

# Connect to remote Spark cluster
spark = SparkSession.builder \
    .remote("sc://your-cluster-host:15002") \
    .getOrCreate()

# Same API as traditional Spark
df = spark.read.parquet("/data/file.parquet")
result = df.groupBy("category").count()
result.show()
```

**Without Spark Connect (traditional):**
```python
# Must run this on the cluster (or with spark-submit)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

df = spark.read.parquet("/data/file.parquet")
result = df.groupBy("category").count()
result.show()
```

## Objective 2: Describe different deployment mode types (Client, Cluster, Local)

### Core Concept
When you submit a Spark job with `spark-submit`, you specify a **deployment mode**. This determines where the **driver** (your main code) runs.

### Deployment Modes

| Mode | Driver Location | Cluster Manager | Use Case |
|------|-----------------|---|----------|
| **Local** | Your machine (single JVM) | None | Development, testing |
| **Client** | Submission node (edge node) | Kubernetes, YARN, Mesos | Interactive shells, notebooks |
| **Cluster** | Worker node in cluster | Kubernetes, YARN, Mesos | Production batch jobs |

### Local Mode

```
Your Machine
    ↓
JVM (driver + executor)
    ↓
No cluster needed
```

**Use:**
```bash
spark-submit --master local[4] my_job.py
```

- `local[4]` = use 4 cores on your machine
- No cluster; everything runs locally
- For development and small testing

### Client Mode

```
Submission Node (edge node)
    ↓
Driver runs here
    ↓
Cluster Manager (YARN, Kubernetes)
    ↓
Executors on cluster nodes
```

**Use:**
```bash
spark-submit --master yarn --deploy-mode client my_job.py
```

- Driver runs on the submission node (your machine, a gateway, or Databricks notebook)
- Executors run on cluster
- Output printed to your console
- Good for interactive work (notebooks, shells)

**Latency:** Driver is outside cluster; network overhead for task communication.

### Cluster Mode

```
Cluster
    ↓
Driver runs on a worker node
    ↓
Cluster Manager (YARN, Kubernetes)
    ↓
Executors on other cluster nodes
```

**Use:**
```bash
spark-submit --master yarn --deploy-mode cluster my_job.py
```

- Driver runs on cluster (on a worker node)
- Executors on other workers
- Output logged to driver logs; not printed to console
- Best for production batch jobs

**Advantages:**
- All computation on cluster; minimal latency
- Driver crash doesn't affect submission machine
- More efficient resource utilization

### Local Mode Variations

```python
spark-submit --master local           # Single thread (sequential)
spark-submit --master local[4]        # 4 local threads (parallelism)
spark-submit --master local[*]        # Use all cores available
spark-submit --master local[4,2]      # 4 cores, 2 max retries
```

### On Databricks

- **Databricks uses Cluster mode internally** — Jobs run on cluster driver node
- **Notebooks are Client mode** — Notebook driver on Databricks side; executors on cluster
- **Spark Connect** changes this model; driver is your machine; communication over network

### Common Mistakes

1. **Using Local mode for production** — Single machine can't handle big data
2. **Using Cluster mode for interactive work** — Can't see output in real-time
3. **Confusing Driver and Executor** — Driver orchestrates; Executors do work

### Code Example: Mode Selection

```python
# This code is the same; mode is selected at submission time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ModeExample").getOrCreate()
df = spark.read.csv("/data/file.csv", header=True)
result = df.groupBy("category").count()
result.show()
```

**Submit in different modes:**

```bash
# Local mode (development)
spark-submit --master local[4] my_job.py

# Client mode (interactive, notebook)
spark-submit --master yarn --deploy-mode client my_job.py

# Cluster mode (production batch)
spark-submit --master yarn --deploy-mode cluster my_job.py
```

### Mode Comparison Table

| Feature | Local | Client | Cluster |
|---------|-------|--------|---------|
| **Driver location** | Your machine | Submission node | Cluster worker |
| **Output location** | Console | Console | Logs |
| **Network latency** | None | Medium | Low |
| **Failure handling** | Lose everything | Submission node crash loses job | Driver restarts on another node |
| **Best for** | Development | Interactive | Production |
| **Cost** | Cheap (1 machine) | Medium (1 node + cluster) | Efficient (full cluster) |

### Code Example: Understanding Mode with Output

```python
# submission_node (client machine)
# spark-submit --master yarn --deploy-mode client my_job.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ModeDemoClient").getOrCreate()

# In CLIENT mode:
# - This Python process runs on submission_node
# - spark.read() creates logical plan on submission_node
# - Tasks sent to cluster executors
# - Results printed to submission_node console (where you ran spark-submit)

df = spark.read.csv("hdfs://cluster/data.csv", header=True)
result = df.groupBy("category").count()

# This shows on YOUR console (submission_node)
result.show()
```

```bash
# cluster_worker (in the cluster)
# spark-submit --master yarn --deploy-mode cluster my_job.py

# In CLUSTER mode:
# - This Python process runs on a cluster_worker
# - output is logged to /var/log/spark/driver.log (on the worker)
# - You won't see print() output on your machine
# - Results written to HDFS or database (not printed)
```

## Summary: Section 6 Key Takeaways

1. **Spark Connect**: Separates client from cluster; communicates via gRPC; enables remote development
2. **Local SparkSession**: Use `.master("local[n]")` for development/testing; limited by machine RAM
3. **Remote Connection**: Use `.remote("sc://host:15002")` to connect to cluster from your machine
4. **Mock Testing**: Set `spark.sql.shuffle.partitions` in local mode to simulate multi-node behavior
5. **Configuration**: Key options include driver memory, executor memory, parallelism, and timeouts
6. **When to use local**: Developing code, unit testing logic, small datasets
7. **When to use remote**: Large datasets, production workloads, testing distributed behavior
8. **Benefits of Spark Connect**: Lightweight, multi-language ready, isolated client/cluster
9. **Local mode**: Everything on your machine; no cluster; development only
10. **Client mode**: Driver on submission node; executors on cluster; interactive work (notebooks, shells)
11. **Cluster mode**: Driver and executors on cluster; best for production batch jobs
12. **Driver location matters**: Client mode = fast feedback; Cluster mode = efficient resources
13. **On Databricks**: Notebooks are Client mode; jobs are Cluster mode; Spark Connect is emerging option

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Previous: Section 5 - Structured Streaming](05_Structured_Streaming.md) | [Next: Section 7 - Pandas API →](07_Pandas_API.md)
