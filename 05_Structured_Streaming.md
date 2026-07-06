# Section 5: Structured Streaming

---

## Table of Contents

1. [Objective 1: Explain the Structured Streaming engine](#objective-1-explain-the-structured-streaming-engine)
2. [Objective 2: Create and write Streaming DataFrames](#objective-2-create-and-write-streaming-dataframes-with-output-modes-and-sinks)
3. [Objective 3: Perform basic operations on Streaming DataFrames](#objective-3-perform-basic-operations-on-streaming-dataframes)
4. [Objective 4: Perform Streaming Deduplication](#objective-4-perform-streaming-deduplication-with-and-without-watermark)
5. [Summary](#summary-section-5-key-takeaways)

---

## Objective 1: Explain the Structured Streaming engine

### Core Concept
**Structured Streaming** processes continuous data the same way as batch DataFrames. Instead of reading a static file, you read from a stream (Kafka, files arriving continuously, etc.) and process data as it arrives.

### Batch vs. Stream Processing

| Aspect | Batch | Stream |
|--------|-------|--------|
| **Data source** | Static file | Continuous stream |
| **Processing** | All data at once | Micro-batches |
| **Latency** | Minutes/hours | Seconds |
| **State** | Stateless (each run independent) | Stateful (remembers previous events) |

### Micro-Batch Processing

Structured Streaming groups incoming data into **micro-batches** (e.g., every 1 second) and processes each batch like a DataFrame operation.

```
Timeline:
Time 0s:   batch 1: [event1, event2, event3] → processed
Time 1s:   batch 2: [event4, event5]          → processed
Time 2s:   batch 3: [event6, event7, event8]  → processed
```

### Exactly-Once Semantics

**Guarantee:** Each event is processed exactly once, even if the job crashes.

**How it works:**
1. Spark checkpoints state after each batch
2. If job crashes, it resumes from checkpoint (doesn't re-process)
3. Output is written atomically (all or nothing per batch)

```python
# Enable checkpointing
query = df_stream \
    .writeStream \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

### Fault Tolerance

If the cluster fails:
1. Spark reads the last checkpoint
2. Resumes from where it stopped
3. Doesn't re-process earlier batches

### On Databricks

- **Checkpoints stored in cloud storage** — S3, ADLS, GCS
- **State Store** — Persists aggregation state (for stateful operations like groupBy)
- **Watermarks** — Prevent infinite state growth by removing old data

### Common Mistakes

1. **Forgetting to set checkpointLocation** — Without checkpoints, job can't recover; data may be reprocessed
2. **Comparing to traditional streaming** — Structured Streaming is declarative (SQL-like); easier than Kafka Consumer code
3. **Assuming micro-batches are real-time** — Latency is ~1-10 seconds, not milliseconds

### Code Example: Basic Stream Processing

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("StreamingExample").getOrCreate()

# Read from a streaming source (Kafka example)
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Parse the event (Kafka sends bytes; decode them)
df_parsed = df_stream.select(
    col("value").cast("string").alias("event")
)

# Filter: keep events > 100
df_filtered = df_parsed.filter(col("value") > "100")

# Write to output
query = df_filtered.writeStream \
    .format("console") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Blocks until job ends (manually or crash)
query.awaitTermination()
```

### Recommended Resources
1. [Apache Spark: Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
2. [Databricks: Structured Streaming](https://docs.databricks.com/en/structured-streaming/index.html)
3. [Databricks: Streaming Fundamentals](https://docs.databricks.com/en/notebooks/streaming.html)

---

## Objective 2: Create and write Streaming DataFrames with output modes and sinks

### Core Concept
When you write a stream, you specify **where** (output sink) and **how often** (output mode).

### Output Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| **append** | Write only new rows | Events, logs (never update old rows) |
| **update** | Write new + updated rows | Aggregations, running totals |
| **complete** | Write all rows (recompute) | Small aggregations that fit in memory |

```python
# APPEND mode: write new rows only
query = df_stream.writeStream \
    .mode("append") \
    .format("parquet") \
    .option("path", "/output/events") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# UPDATE mode: write new and changed rows
query = df_stream.groupBy("user_id").agg(sum("amount")) \
    .writeStream \
    .mode("update") \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# COMPLETE mode: write all rows (warning: slow for large state)
query = df_stream.groupBy("category").count() \
    .writeStream \
    .mode("complete") \
    .format("console") \
    .start()
```

### Output Sinks

| Sink | Format | Use Case |
|------|--------|----------|
| **Parquet** | `format("parquet")` | Batch-like processing; data lake |
| **Delta** | `format("delta")` | ACID transactions; Databricks-native |
| **Kafka** | `format("kafka")` | Feed another stream processor |
| **Console** | `format("console")` | Debugging; prints to output |
| **Memory** | `format("memory")` | Testing; stores in temp table |

### Trigger Settings

Control how often batches are processed.

```python
# Default: process as fast as possible
query = df_stream.writeStream.start()

# Trigger every 10 seconds
query = df_stream.writeStream \
    .trigger(processingTime="10 seconds") \
    .start()

# Trigger once (batch-like)
query = df_stream.writeStream \
    .trigger(once=True) \
    .start()

# Trigger after 1000 events (micro-batch trigger)
query = df_stream.writeStream \
    .trigger(availableNow=True) \  # Spark 3.3+
    .start()
```

### On Databricks

- **Delta sink is recommended** — ACID transactions; supports exactly-once
- **Parquet is simpler** — But no ACID guarantees; use when durability not critical
- **Memory sink for testing** — Write to temp table; query with `spark.table("my_stream")`

### Common Mistakes

1. **Using COMPLETE mode on large aggregations** — Sends entire aggregation each batch (slow)
2. **Forgetting checkpointLocation** — Data won't be recoverable after crash
3. **Mixing output modes** — Can't use `append` and `update` simultaneously

### Code Example: Multiple Output Modes

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window, current_timestamp

spark = SparkSession.builder.appName("StreamingOutputExample").getOrCreate()

# Simulate a streaming source
df_stream = spark.readStream.format("rate").option("rowsPerSecond", 100).load()

# Add a value for aggregation
df_stream = df_stream.withColumn("amount", (col("value") % 100) + 1)

# 1. APPEND MODE: write events to Delta
query_append = df_stream.select("timestamp", "value", "amount") \
    .writeStream \
    .mode("append") \
    .format("delta") \
    .option("path", "/tmp/stream_events") \
    .option("checkpointLocation", "/tmp/checkpoint_append") \
    .start()

# 2. UPDATE MODE: write aggregation updates
query_update = df_stream.groupBy("value").agg(sum("amount")) \
    .writeStream \
    .mode("update") \
    .format("console") \
    .option("checkpointLocation", "/tmp/checkpoint_update") \
    .start()

# 3. COMPLETE MODE: write full aggregation (small data only)
query_complete = df_stream.groupBy(window("timestamp", "10 seconds")).count() \
    .writeStream \
    .mode("complete") \
    .format("console") \
    .option("checkpointLocation", "/tmp/checkpoint_complete") \
    .start()

print("Streaming jobs started. Check console output.")
```

### Recommended Resources
1. [Apache Spark: Output Modes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes)
2. [Databricks: Streaming Sinks](https://docs.databricks.com/en/structured-streaming/output-modes.html)
3. [Apache Spark: Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

---

## Objective 3: Perform basic operations on Streaming DataFrames

### Core Concept
Most DataFrame operations work on streams: select, filter, groupBy, window. Stateless operations are fast; stateful (aggregations) require state management.

### Stateless Operations

Operations that don't need to remember previous events.

```python
from pyspark.sql.functions import col, upper, length

# Select columns (stateless)
df_selected = df_stream.select("user_id", "event_type")

# Filter (stateless)
df_filtered = df_stream.filter(col("value") > 100)

# Add column (stateless)
df_with_upper = df_stream.withColumn("event_upper", upper("event_type"))

# These run fast because no state is maintained
```

### Stateful Operations: GroupBy

Aggregations require maintaining state (running totals).

```python
# GroupBy without time window (dangerous: state grows forever!)
result = df_stream.groupBy("user_id").agg(sum("amount"))

# Problem: state store grows infinitely; memory fills up
# Solution: use watermark (see Objective 4)
```

### Windowed Aggregations

Process data in **time windows** (e.g., every 10 seconds, or tumbling windows).

```python
from pyspark.sql.functions import window, sum as spark_sum

# Tumbling window: 10-second windows, no overlap
result = df_stream.groupBy(
    window("timestamp", "10 seconds")
).agg(
    spark_sum("amount").alias("total")
)

# Sliding window: 10-second windows, slide by 5 seconds (50% overlap)
result = df_stream.groupBy(
    window("timestamp", "10 seconds", "5 seconds")
).agg(
    spark_sum("amount").alias("total")
)
```

### Window Types

| Window | Duration | Slide | Example |
|--------|----------|-------|---------|
| **Tumbling** | 10s | 10s | [0-10], [10-20], [20-30] (no overlap) |
| **Sliding** | 10s | 5s | [0-10], [5-15], [10-20] (50% overlap) |

### On Databricks

- **Use windowed aggregations** — Prevents state explosion
- **Set watermarks** — Remove old data from state store

### Common Mistakes

1. **GroupBy without window** — State grows forever; job will run out of memory
2. **Window too short** — Many windows; more state
3. **Window too long** — Delayed results; high latency

### Code Example: Stateless and Stateful Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as spark_sum, count

spark = SparkSession.builder.appName("StreamingOpsExample").getOrCreate()

# Simulated stream
df_stream = spark.readStream.format("rate").option("rowsPerSecond", 100).load()
df_stream = df_stream.withColumn("amount", (col("value") % 100) + 1)

# 1. STATELESS: select and filter
df_simple = df_stream.select("timestamp", "value", "amount") \
    .filter(col("amount") > 50)

query1 = df_simple.writeStream.format("console").start()

# 2. STATEFUL: windowed aggregation
df_windowed = df_stream.groupBy(
    window("timestamp", "5 seconds")
).agg(
    spark_sum("amount").alias("total"),
    count("*").alias("event_count")
)

query2 = df_windowed.writeStream.format("console").start()

print("Streaming operations running. Check console.")
```

### Recommended Resources
1. [Apache Spark: Streaming DataFrame Operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#operations-on-streaming-dataframes)
2. [Databricks: Streaming Transformations](https://docs.databricks.com/en/structured-streaming/operations.html)
3. [Apache Spark: Windowing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time)

---

## Objective 4: Perform Streaming Deduplication with and without watermark

### Core Concept
Real-world streams have duplicates. Structured Streaming can remove them using **deduplication keys**. **Watermarks** tell Spark when to forget old data (prevent memory overflow).

### Deduplication without Watermark

```python
from pyspark.sql.functions import col

# Remove duplicates based on a key (e.g., event_id)
df_deduplicated = df_stream.dropDuplicates("event_id")

# Problem: state grows forever; remembers ALL past event_ids
# Memory eventually fills up
```

**When to use:** Small streams or only recent duplicates expected.

### Deduplication with Watermark

A **watermark** tells Spark: "Forget data older than X minutes." This prevents state explosion.

```python
from pyspark.sql.functions import col

# Set watermark: forget data older than 1 hour
df_with_watermark = df_stream.withWatermark("timestamp", "1 hour")

# Now deduplication only remembers last 1 hour of event_ids
df_deduplicated = df_with_watermark.dropDuplicates("event_id")

# Result: state stays bounded (only 1 hour of history)
```

### Watermark Semantics

**Watermark = allowed lateness**

```
Current time: 12:00
Watermark time = current - 1 hour = 11:00

Events before 11:00: Too late; dropped
Events after 11:00: Processed normally
```

**Example:**
```
Batch 1 (time 12:00): Events timestamped 11:30-12:00 → processed
Batch 2 (time 12:05): Events timestamped 11:45-12:05 → processed
Batch 3 (time 12:10): Event timestamped 10:50 → too late; dropped (before watermark 11:10)
```

### Watermark in Windowed Aggregations

Watermarks are critical for windowed operations to prevent memory leaks.

```python
from pyspark.sql.functions import window, col, sum as spark_sum

# Set watermark on timestamp
df_watermarked = df_stream.withWatermark("timestamp", "10 minutes")

# Windowed aggregation (1-hour windows)
result = df_watermarked.groupBy(
    window("timestamp", "1 hour")
).agg(
    spark_sum("amount").alias("total")
)

# Windows older than watermark are finalized (no more updates)
```

### On Databricks

- **Always use watermarks for stateful operations** — Prevents memory leaks
- **Balance between precision and latency** — Longer watermark = allow more lateness; more memory
- **Default watermark: 0** — Events arriving "late" are dropped immediately

### Common Mistakes

1. **Forgetting watermark on aggregations** — State grows forever; memory fills
2. **Watermark too short** — Legitimate late events are dropped
3. **Watermark too long** — High latency before results finalize; memory usage high

### Code Example: Deduplication with Watermark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum as spark_sum

spark = SparkSession.builder.appName("DeduplicationExample").getOrCreate()

# Simulated stream with duplicates
df_stream = spark.readStream.format("rate").option("rowsPerSecond", 100).load()
df_stream = df_stream.withColumn("event_id", col("value") % 50)  # Some duplicates
df_stream = df_stream.withColumn("amount", (col("value") % 100) + 1)

# 1. DEDUPLICATION WITHOUT WATERMARK
print("=== Without Watermark (state grows forever) ===")
df_dedup_no_wm = df_stream.dropDuplicates("event_id")
query1 = df_dedup_no_wm.select("timestamp", "event_id", "amount") \
    .writeStream.format("console").start()

# 2. DEDUPLICATION WITH WATERMARK
print("=== With Watermark (bounded state) ===")
df_watermarked = df_stream.withWatermark("timestamp", "10 minutes")
df_dedup_with_wm = df_watermarked.dropDuplicates("event_id")
query2 = df_dedup_with_wm.select("timestamp", "event_id") \
    .writeStream.format("console").start()

# 3. WINDOWED AGGREGATION WITH WATERMARK
print("=== Windowed Aggregation with Watermark ===")
result = df_watermarked.groupBy(
    window("timestamp", "5 seconds")
).agg(
    count("*").alias("total_events"),
    spark_sum("amount").alias("total_amount")
)
query3 = result.writeStream.format("console").start()

print("Streaming jobs running.")
```

### Recommended Resources
1. [Apache Spark: Streaming Deduplication](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#deduplication)
2. [Apache Spark: Event Time and Watermarks](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-event-time-and-late-data)
3. [Databricks: Streaming State Management](https://docs.databricks.com/en/structured-streaming/stateful-processing.html)

---

## Summary: Section 5 Key Takeaways

1. **Micro-batches**: Data grouped by time; processed like DataFrames
2. **Exactly-once**: Each event processed once; guaranteed by checkpoints
3. **Fault tolerance**: Checkpoints allow recovery without replay
4. **Output modes**: `append` (events), `update` (aggregation changes), `complete` (full state)
5. **Stateless operations**: select, filter, map (fast; no state)
6. **Stateful operations**: groupBy, aggregations (require state management)
7. **Windowed aggregations**: Prevent state explosion; use time windows
8. **Watermarks**: Set lateness tolerance; forget old data; bounded state
9. **Checkpoints**: Required for recovery; store in cloud storage
10. **Without watermarks**: State grows forever; memory fills; job crashes

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Previous: Section 4 - Troubleshooting](04_Troubleshooting_and_Tuning.md) | [Next: Section 6 - Spark Connect →](06_Spark_Connect.md)
