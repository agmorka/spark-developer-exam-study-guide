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

### Common Mistakes

1. **Confusing with Spark Submit** — Spark Connect is a communication layer; Spark Submit is a launcher
2. **Thinking it replaces cluster mode** — Spark Connect still needs a cluster; you connect to it remotely
3. **Using for all workloads** — Traditional Spark still better for large batch jobs; Spark Connect for interactive use

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

### Recommended Resources
1. [Databricks: Spark Connect](https://docs.databricks.com/en/dev-tools/spark-connect/index.html)
2. [Apache Spark: Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
3. [Databricks: Remote Development](https://docs.databricks.com/en/dev-tools/index.html)

---

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

### Recommended Resources
1. [Apache Spark: Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
2. [Databricks: Cluster Mode Configuration](https://docs.databricks.com/en/compute/clusters/configure.html)
3. [Databricks: Jobs and Automation](https://docs.databricks.com/en/jobs/index.html)

---

## Summary: Section 6 Key Takeaways

1. **Spark Connect**: Separates client from cluster; communicates via gRPC; enables remote development
2. **Benefits of Spark Connect**: Lightweight, multi-language ready, isolated client/cluster
3. **Local mode**: Everything on your machine; no cluster; development only
4. **Client mode**: Driver on submission node; executors on cluster; interactive work (notebooks, shells)
5. **Cluster mode**: Driver and executors on cluster; best for production batch jobs
6. **Driver location matters**: Client mode = fast feedback; Cluster mode = efficient resources
7. **On Databricks**: Notebooks are Client mode; jobs are Cluster mode; Spark Connect is emerging option

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Previous: Section 5 - Structured Streaming](05_Structured_Streaming.md) | [Next: Section 7 - Pandas API →](07_Pandas_API.md)
