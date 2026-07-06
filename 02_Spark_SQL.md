# Section 2: Using Spark SQL

---

## Table of Contents

1. [Objective 1: Utilize common data sources](#objective-1-utilize-common-data-sources-jdbc-files-etc-to-efficiently-read-from-and-write-to-spark-dataframes)
2. [Objective 2: Execute SQL queries directly on files](#objective-2-execute-sql-queries-directly-on-files-including-orc-json-csv-text-files-and-delta-files)
3. [Schema Inference vs. Explicit Schema](#schema-inference-vs-explicit-schema)
4. [Objective 3: Save data to persistent tables](#objective-3-save-data-to-persistent-tables-while-applying-sorting-and-partitioning-to-optimize-data-retrieval)
5. [Delta Time Travel: Audit and Version Control](#delta-time-travel-audit-and-version-control)
6. [Objective 4: Register DataFrames as temporary views](#objective-4-register-dataframes-as-temporary-views-in-spark-sql-allowing-them-to-be-queried-with-sql-syntax)
7. [Summary](#summary-section-2-key-takeaways)

---

## Objective 1: Utilize common data sources (JDBC, files, etc.) to efficiently read from and write to Spark DataFrames

### Core Concept
Spark can read and write data in many formats: CSV, Parquet, JSON, ORC, Delta, and databases via JDBC. Each format has different tradeoffs (size, speed, schema support). You control how data is written (overwrite, append, etc.) and how it's partitioned across files.

### Common Data Formats

| Format | Compression | Schema | Speed | Use Case |
|--------|-------------|--------|-------|----------|
| **Parquet** | Yes (Snappy) | Embedded | Very Fast | Databricks default; columnar; great for analytics |
| **Delta** | Optional | Embedded | Very Fast | Databricks-native; ACID transactions; time travel |
| **CSV** | No | Inferred | Slow | Human-readable; import/export; small data |
| **JSON** | No | Inferred | Medium | APIs, log files, nested data |
| **ORC** | Yes | Embedded | Very Fast | Hive-compatible; Hadoop ecosystems |
| **JDBC** | N/A | Depends on DB | Medium | Relational databases (PostgreSQL, MySQL, SQL Server) |

### Reading Data

```python
# CSV
df = spark.read.option("header", "true").csv("/path/to/file.csv")

# Parquet (fast, recommended)
df = spark.read.parquet("/path/to/file.parquet")

# JSON
df = spark.read.json("/path/to/file.json")

# ORC
df = spark.read.orc("/path/to/file.orc")

# JDBC (database)
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "users") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Delta (Databricks format)
df = spark.read.format("delta").load("/path/to/table")
```

### Writing Data: Save Modes

When you write, you must specify what happens if the file already exists:

| Mode | Behavior |
|------|----------|
| `overwrite` | **Delete existing data, write new data** — use with caution |
| `append` | **Add new data to existing data** — rows are added, not replaced |
| `ignore` | If path exists, do nothing (silently skip) |
| `error` | If path exists, throw an error (default if you don't specify) |

```python
# Overwrite: delete old data, write new
df.write.mode("overwrite").parquet("/output/data.parquet")

# Append: add rows to existing file
df.write.mode("append").parquet("/output/data.parquet")

# Error if path exists (default)
df.write.parquet("/output/data.parquet")  # Throws error if /output/data.parquet exists
```

### Partitioning by Column

When you save large data, **partitioning** creates a folder structure that lets Spark skip reading irrelevant data.

```
Without partitioning:
/output/data.parquet  (one big file)

With partitioning by year:
/output/data.parquet/
  year=2023/
    part-00001.parquet
    part-00002.parquet
  year=2024/
    part-00003.parquet
    part-00004.parquet
```

**Spark skips entire year=2023 folder if your filter is `WHERE year > 2024`** (partition pruning).

```python
# Partition by country
df.write.mode("overwrite") \
    .partitionBy("country") \
    .parquet("/output/data")

# Partition by multiple columns
df.write.mode("overwrite") \
    .partitionBy("country", "year") \
    .parquet("/output/data")
```

### On Databricks

- **Default format:** Parquet (compressed, fast, schema embedded)
- **Best format for Databricks:** Delta (ACID, time travel, optimizations)
- **Paths:** Use `/mnt/` for cloud storage (S3, ADLS, GCS)
- **JDBC:** Secure credentials in Databricks secrets, not hardcoded

### Common Mistakes

1. **Forgetting `.option("header", "true")` for CSV** — Spark treats header as data; column names become Row 1
2. **Using `mode("error")` without checking if path exists** — Job fails silently; use `mode("overwrite")` if you're sure
3. **Over-partitioning** — Too many small partitions (e.g., partitionBy on ID) creates file overhead
4. **Under-partitioning** — Single file is too slow to read later; partition by time, region, etc.
5. **JDBC credentials hardcoded** — Use Databricks secrets instead

### Code Example: Read, Transform, Write

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadWriteExample").getOrCreate()

# Read CSV
users_df = spark.read.option("header", "true").csv("/mnt/data/users.csv")

# Read Parquet
sales_df = spark.read.parquet("/mnt/data/sales.parquet")

# Transform
result = users_df.join(sales_df, "user_id") \
    .filter("year > 2023") \
    .select("user_id", "sale_amount", "year")

# Write with partitioning and overwrite
result.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/mnt/output/user_sales")

print("Data written to /mnt/output/user_sales")
```

---

## Objective 2: Execute SQL queries directly on files, including ORC, JSON, CSV, Text Files, and Delta Files

### Core Concept
You don't always need to load a file into a DataFrame first. Spark SQL can **query files directly** using a temporary view or `spark.sql()` with file paths. This is useful for one-off queries without loading everything into memory.

### Query Files Directly

```python
# Method 1: spark.sql() with direct file path
result = spark.sql("""
    SELECT name, salary
    FROM parquet.`/mnt/data/employees.parquet`
    WHERE salary > 50000
""")
result.show()

# Method 2: Create a temporary view first
spark.sql("""
    CREATE TEMPORARY VIEW employees
    USING PARQUET
    LOCATION '/mnt/data/employees.parquet'
""")

result = spark.sql("SELECT * FROM employees WHERE salary > 50000")
result.show()
```

### File Formats and Syntax

| Format | SQL Syntax |
|--------|-----------|
| Parquet | ``FROM parquet.`/path/to/file.parquet` `` |
| Delta | ``FROM delta.`/path/to/table\` `` |
| CSV | ``FROM csv.`/path/to/file.csv\` `` |
| JSON | ``FROM json.`/path/to/file.json\` `` |
| ORC | ``FROM orc.`/path/to/file.orc\` `` |

```python
# CSV query
spark.sql("""
    SELECT * FROM csv.`/mnt/data/sales.csv`
    WHERE amount > 100
""")

# JSON query
spark.sql("""
    SELECT name, age FROM json.`/mnt/data/users.json`
    WHERE age > 18
""")

# ORC query
spark.sql("""
    SELECT * FROM orc.`/mnt/data/data.orc`
    LIMIT 10
""")
```

### Save Modes for Output

When you write with SQL, use `mode()` to control overwrite behavior:

```python
df = spark.sql("SELECT * FROM employees WHERE salary > 50000")

# Overwrite
df.write.mode("overwrite").parquet("/output/high_earners")

# Append
df.write.mode("append").parquet("/output/high_earners")
```

### On Databricks

- **Delta queries are fastest** — Delta tables are Databricks-native with optimizations
- **Backticks are required** — Paths with special characters need backticks: `` /path/to/file.parquet``
- **No manual inference needed** — Spark auto-detects schema in Parquet, Delta, ORC

### Common Mistakes

1. **Forgetting backticks around paths** — Will throw syntax error
2. **Assuming CSV has header** — Must add `.option("header", "true")` or query will treat first row as data
3. **Querying a huge file repeatedly** — Each query reads from disk; cache if you query multiple times

### Code Example: Query Multiple Files

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DirectQueryExample").getOrCreate()

# Query CSV directly
csv_result = spark.sql("""
    SELECT * FROM csv.`/mnt/data/employees.csv`
    WHERE department = 'Engineering'
    LIMIT 10
""")
csv_result.show()

# Query Parquet and aggregate
parquet_agg = spark.sql("""
    SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
    FROM parquet.`/mnt/data/employees.parquet`
    GROUP BY department
""")
parquet_agg.show()

# Write result to Delta (Databricks format)
parquet_agg.write.mode("overwrite").format("delta").save("/mnt/output/dept_summary")
```

---

## Objective 3: Save data to persistent tables while applying sorting and partitioning to optimize data retrieval

### Core Concept
A **persistent table** is stored on disk and can be queried later. Unlike temporary views (which die when the session ends), persistent tables survive and can be accessed by other users and jobs.

### Temporary View vs. Persistent Table

| Aspect | Temp View | Persistent Table |
|--------|-----------|-----------------|
| **Lifetime** | Dies when session ends | Survives session; permanent |
| **Storage** | Memory only | Disk (Databricks metastore) |
| **Query** | `SELECT * FROM view_name` | `SELECT * FROM table_name` |
| **Other users** | Can't see it | Can see and query it |

```python
# Temporary view (session only)
df.createOrReplaceTempView("temp_users")
# After session ends: temp_users is gone

# Persistent table (permanent)
df.write.mode("overwrite").format("delta").saveAsTable("users")
# After session ends: users table still exists
```

### Creating Persistent Tables

```python
# Method 1: saveAsTable() — most common
df.write.mode("overwrite").format("delta").saveAsTable("employees")

# Method 2: SQL CREATE TABLE AS
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees AS
    SELECT * FROM employees_staging
""")

# Method 3: External table (points to external path)
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees
    USING DELTA
    LOCATION '/mnt/data/employees'
""")
```

### Sorting and Partitioning for Performance

**Sorting:** Orders data within partitions. Speeds up range queries (`WHERE date > 2024-01-01`).

**Partitioning:** Splits data into folders. Allows Spark to skip entire partitions.

```python
# Partition by country, sort by date
df.write \
    .mode("overwrite") \
    .partitionBy("country") \
    .sortWithinPartitions("date") \
    .format("delta") \
    .saveAsTable("sales_by_country")
```

**Result structure:**
```
sales_by_country/
  country=USA/
    part-00001.parquet (sorted by date)
    part-00002.parquet (sorted by date)
  country=Canada/
    part-00003.parquet (sorted by date)
```

**Query optimization:**
- `SELECT * FROM sales_by_country WHERE country = 'USA'` — Spark skips Canada folders (partition pruning)
- `SELECT * FROM sales_by_country WHERE country = 'USA' AND date > '2024-01-01'` — Finds dates fast because partition is sorted

### On Databricks

- **Default format:** Delta (Databricks-native; includes sorting, partitioning optimization)
- **Metastore:** Databricks tracks all tables in the Unity Catalog or Hive Metastore
- **Best practice:** Use Delta format for persistent tables

### Common Mistakes

1. **Not partitioning** — Large table becomes one slow file
2. **Partitioning on high-cardinality columns** — E.g., `partitionBy("user_id")` creates thousands of folders (overhead)
3. **Forgetting `IF NOT EXISTS`** — Table creation fails if table already exists
4. **Not sorting** — Table is partitioned but still slow to query within partitions

### Code Example: Create Sorted, Partitioned Persistent Table

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PersistentTableExample").getOrCreate()

# Read data
df = spark.read.parquet("/mnt/data/raw_sales.parquet")

# Create persistent table with partitioning and sorting
df.write \
    .mode("overwrite") \
    .partitionBy("region") \
    .sortWithinPartitions("date", "product_id") \
    .format("delta") \
    .saveAsTable("sales_optimized")

# Query the persistent table
result = spark.sql("""
    SELECT region, product_id, SUM(amount) as total
    FROM sales_optimized
    WHERE region = 'North America' AND date > '2024-01-01'
    GROUP BY region, product_id
""")
result.show()
```

---

## Objective 4: Register DataFrames as temporary views in Spark SQL, allowing them to be queried with SQL syntax

### Core Concept
A **temporary view** lets you query a DataFrame using SQL syntax instead of the DataFrame API. It lives only in the current session.

### Creating Temporary Views

```python
# Method 1: createOrReplaceTempView() — replaces if exists
df.createOrReplaceTempView("users")

# Method 2: createTempView() — errors if exists
df.createTempView("users")  # Throws error if "users" already exists

# Method 3: createGlobalTempView() — visible across all sessions (rare)
df.createGlobalTempView("users")
# Access via: SELECT * FROM global_temp.users
```

### Using Temporary Views

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TempViewExample").getOrCreate()

# Create DataFrame
df = spark.read.csv("/mnt/data/employees.csv", header=True)

# Register as temp view
df.createOrReplaceTempView("employees")

# Query with SQL
result = spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    WHERE age > 30
    GROUP BY department
    ORDER BY avg_salary DESC
""")
result.show()
```

### Temp View vs. Persistent Table

| Feature | Temp View | Persistent Table |
|---------|-----------|-----------------|
| **Lifetime** | Current session only | Permanent (survives session) |
| **Storage** | RAM | Disk (Databricks metastore) |
| **Use case** | Quick queries, exploration | Production data, reports |

### Global Temp View (Rare)

```python
df.createGlobalTempView("users")

# Access in same session or other sessions
spark.sql("SELECT * FROM global_temp.users")
```

**Note:** Global temp views are rarely used; most teams use persistent tables instead.

### Common Mistakes

1. **Assuming temp view persists** — It dies when your notebook ends; create a persistent table instead
2. **Using `createTempView()` and running twice** — Second run errors; use `createOrReplaceTempView()` instead
3. **Naming conflicts** — If you have a temp view and a table with the same name, Spark prefers the temp view

### Code Example: Temp View for Multi-Step Analysis

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TempViewMultiStep").getOrCreate()

# Read multiple sources
users_df = spark.read.parquet("/mnt/data/users.parquet")
orders_df = spark.read.parquet("/mnt/data/orders.parquet")

# Create temp views
users_df.createOrReplaceTempView("users")
orders_df.createOrReplaceTempView("orders")

# Step 1: Get high-value customers
top_customers = spark.sql("""
    SELECT user_id, SUM(amount) as total_spent
    FROM orders
    GROUP BY user_id
    HAVING SUM(amount) > 10000
""")
top_customers.createOrReplaceTempView("top_customers")

# Step 2: Get details of top customers
result = spark.sql("""
    SELECT u.name, u.email, t.total_spent
    FROM users u
    JOIN top_customers t ON u.user_id = t.user_id
    ORDER BY total_spent DESC
""")
result.show()
```

---

## Summary: Section 2 Key Takeaways

1. **Read/Write**: Support multiple formats (CSV, Parquet, JSON, ORC, Delta); use Parquet/Delta for performance
2. **Save modes**: `overwrite` deletes old data; `append` adds rows; `error` is default
3. **Partitioning**: Create folder structure (by country, year, etc.) to skip irrelevant data
4. **Direct queries**: Query files without loading them first using backticks: `` FROM parquet.`/path` ``
5. **Persistent tables**: Survive session end; use Delta format on Databricks
6. **Sorting + Partitioning**: Sort within partitions for fast range queries
7. **Temp views**: Quick way to query DataFrames with SQL; die when session ends

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [← Previous: Section 1 - Architecture](01_Architecture_and_Components.md) | [Next: Section 3 - DataFrame API →](03_DataFrame_API.md)
