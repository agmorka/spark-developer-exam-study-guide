# Section 3: Developing Apache Spark DataFrame/DataSet API Applications

---

## Table of Contents

1. [Objective 1: Manipulate columns, rows, and table structures](#objective-1-manipulate-columns-rows-and-table-structures)
2. [Objective 2: Perform data deduplication and validation operations](#objective-2-perform-data-deduplication-and-validation-operations)
3. [Objective 3: Perform aggregate operations](#objective-3-perform-aggregate-operations-count-approximate-count-distinct-mean-summary)
4. [Window Functions: Advanced Aggregations Over Data Partitions](#window-functions-advanced-aggregations-over-data-partitions)
5. [Objective 4: Manipulate and utilize Date data type](#objective-4-manipulate-and-utilize-date-data-type)
6. [Objective 5: Combine DataFrames](#objective-5-combine-dataframes-joins-and-unions)
7. [Objective 6: Manage input and output operations](#objective-6-manage-input-and-output-operations-reading-writing-schemas)
8. [Objective 7: Perform operations on DataFrames](#objective-7-perform-operations-on-dataframes-sorting-iterating-printing-schema-conversion)
9. [Objective 8: Create and invoke user-defined functions](#objective-8-create-and-invoke-user-defined-functions-udfs)
10. [Objective 9: Describe different types of variables](#objective-9-describe-different-types-of-variables-broadcast-variables-and-accumulators)
11. [Objective 10: Describe the purpose and implementation of broadcast joins](#objective-10-describe-the-purpose-and-implementation-of-broadcast-joins)
12. [Summary](#summary-section-3-key-takeaways)

---

## Objective 1: Manipulate columns, rows, and table structures

### Core Concept
DataFrames are organized in rows and columns. You manipulate them by adding/dropping columns, renaming, filtering rows, splitting, and exploding arrays.

### Column Operations

#### Add a Column

```python
from pyspark.sql.functions import col, lit

df = spark.read.csv("/data/users.csv", header=True)

# Add a constant column
df_with_source = df.withColumn("data_source", lit("csv_import"))

# Add a computed column
df_with_age = df.withColumn("birth_year", lit(2024) - col("age"))

# Overwrite existing column
df_updated = df.withColumn("salary", col("salary") * 1.1)  # 10% raise
```

#### Drop Columns

```python
# Drop single column
df_reduced = df.drop("email")

# Drop multiple columns
df_reduced = df.drop("email", "phone", "address")
```

#### Rename Columns

```python
# Rename single column
df_renamed = df.withColumnRenamed("old_name", "new_name")

# Rename multiple (chain them)
df_renamed = df.withColumnRenamed("col1", "new_col1") \
               .withColumnRenamed("col2", "new_col2")

# Rename all at once (less common)
df_renamed = df.toDF("user_id", "name", "age")
```

### Row Operations

#### Filter Rows

```python
# Single condition
df_filtered = df.filter(col("age") > 30)

# Multiple conditions (AND)
df_filtered = df.filter((col("age") > 30) & (col("salary") > 50000))

# Multiple conditions (OR)
df_filtered = df.filter((col("department") == "Engineering") | (col("department") == "Sales"))

# Using SQL
df_filtered = df.filter("age > 30 AND salary > 50000")

# IN operator
df_filtered = df.filter(col("department").isin("Engineering", "Sales", "Marketing"))
```

#### Drop Rows with Missing Values

```python
# Drop any row with at least one NULL
df_clean = df.na.drop()

# Drop rows where specific columns are NULL
df_clean = df.na.drop(subset=["email"])

# Drop only if ALL values are NULL (rarely useful)
df_clean = df.na.drop(how="all")
```

### Array Operations

#### Explode Arrays

When a column contains arrays, `explode()` turns each array element into a separate row.

```
Before explode():
id  | hobbies
1   | [reading, gaming]
2   | [hiking, cooking]

After explode():
id  | hobby
1   | reading
1   | gaming
2   | hiking
2   | cooking
```

```python
from pyspark.sql.functions import explode

df = spark.createDataFrame([
    (1, ["reading", "gaming"]),
    (2, ["hiking", "cooking"])
], ["id", "hobbies"])

df_exploded = df.select("id", explode(col("hobbies")).alias("hobby"))
df_exploded.show()
# Output:
# +---+--------+
# | id|   hobby|
# +---+--------+
# |  1| reading|
# |  1|  gaming|
# |  2|  hiking|
# |  2| cooking|
```

### On Databricks

- **Column expressions**: Use `col()` to reference columns; SQL strings also work
- **Chaining operations**: Each method returns a new DataFrame; chain them efficiently
- **Performance**: Filter early to reduce data processed downstream

### Common Mistakes

1. **Forgetting to import functions** — `from pyspark.sql.functions import col, lit, etc.`
2. **Using string column names incorrectly** — `df.filter("age > 30")` works, but `col("age") > 30` is more flexible
3. **Assuming `withColumn()` modifies in place** — It doesn't; reassign: `df = df.withColumn(...)`
4. **Not using `alias()` after `explode()`** — Exploded column needs a name

### Code Example: Full Column/Row Manipulation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, explode

spark = SparkSession.builder.appName("ColumnRowExample").getOrCreate()

# Create sample data
df = spark.createDataFrame([
    (1, "Alice", 30, ["reading", "gaming"], None),
    (2, "Bob", 25, ["hiking", "cooking"], "Engineering"),
    (3, "Charlie", None, ["swimming"], "Sales")
], ["id", "name", "age", "hobbies", "department"])

# Add a column
df = df.withColumn("year_joined", lit(2024))

# Drop a column
df = df.drop("year_joined")

# Rename
df = df.withColumnRenamed("hobbies", "interests")

# Filter rows
df_clean = df.filter((col("age").isNotNull()) & (col("age") > 24))

# Explode interests
df_exploded = df_clean.select("id", "name", explode(col("interests")).alias("interest"))

df_exploded.show()
```

### Recommended Resources
1. [Apache Spark: DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)
2. [Apache Spark: Column Expressions](https://spark.apache.org/docs/latest/sql-programming-guide.html#untyped-dataframe-operations)
3. [Databricks: DataFrame Operations](https://docs.databricks.com/en/develop/dataframes.html)

---

## Objective 2: Perform data deduplication and validation operations

### Core Concept
Real data has duplicates and errors. Deduplication removes duplicate rows. Validation checks data quality.

### Remove Duplicates

#### drop_duplicates() / distinct()

```python
# Remove exact duplicates (all columns must match)
df_unique = df.drop_duplicates()

# Or use distinct()
df_unique = df.distinct()

# Both are identical; use whichever you prefer
```

#### Drop Duplicates on Specific Columns

```python
# Remove rows where these columns are duplicated; keep first
df_unique = df.drop_duplicates(subset=["email"])

# If user_id and email combo is duplicate, remove
df_unique = df.drop_duplicates(subset=["user_id", "email"])
```

### Validation Operations

#### Check for NULLs

```python
from pyspark.sql.functions import col, when, count

# Count NULL values in each column
df.select([count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]).show()

# Output:
# +-----+-----+-----+
# |email|phone|name|
# +-----+-----+-----+
# |   42|  100|    0|
# +-----+-----+-----+
# (42 NULLs in email, 100 in phone, 0 in name)
```

#### Validate Values

```python
# Check age is positive
df_valid = df.filter(col("age") > 0)

# Check email has '@'
df_valid = df.filter(col("email").like("%@%"))

# Check multiple conditions
df_valid = df.filter(
    (col("age") > 0) & 
    (col("age") < 150) & 
    (col("email").like("%@%"))
)

# Count invalid records
invalid_count = df.filter(col("age") <= 0).count()
print(f"Invalid records: {invalid_count}")
```

### On Databricks

- **Deduplication is expensive** — Requires shuffle; avoid if possible
- **Validation early** — Filter out bad data before expensive operations (joins, aggregations)

### Common Mistakes

1. **Deduplicating on wrong columns** — Specifying subset when you meant to drop exact duplicates
2. **Assuming `distinct()` sorts** — It doesn't; use `orderBy()` if you need sorted results
3. **Not checking for NULLs before validation** — NULL comparisons return NULL, not True/False

### Code Example: Deduplication and Validation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan

spark = SparkSession.builder.appName("DeduplicationExample").getOrCreate()

# Create data with duplicates and NULLs
df = spark.createDataFrame([
    (1, "Alice", "alice@example.com", 30),
    (1, "Alice", "alice@example.com", 30),  # Exact duplicate
    (2, "Bob", None, 25),  # NULL email
    (3, "Charlie", "charlie@example.com", None),  # NULL age
    (4, "Alice", "alice.new@example.com", 31),  # Same name, different email
], ["id", "name", "email", "age"])

print("=== Original ===")
df.show()

# Remove exact duplicates
df_unique = df.drop_duplicates()
print("=== After drop_duplicates() ===")
df_unique.show()

# Remove duplicates on email only
df_unique_email = df.drop_duplicates(subset=["email"])
print("=== After drop_duplicates(subset=['email']) ===")
df_unique_email.show()

# Validation: keep rows with valid email and age
df_valid = df.filter((col("email").isNotNull()) & (col("age").isNotNull()))
print("=== Valid records ===")
df_valid.show()

# Check NULL counts
print("=== NULL counts ===")
df.select([count(when(col(c).isNull(), 1)).alias(f"{c}_nulls") for c in df.columns]).show()
```

### Recommended Resources
1. [Apache Spark: dropDuplicates](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html)
2. [Databricks: Data Quality](https://docs.databricks.com/en/notebooks/data-quality.html)
3. [Apache Spark: NULL Handling](https://spark.apache.org/docs/latest/sql-programming-guide.html)

---

## Objective 3: Perform aggregate operations (count, approximate count distinct, mean, summary)

### Core Concept
Aggregations summarize data: count rows, find averages, get max/min, etc. Use `groupBy()` to aggregate by groups.

### Basic Aggregations

#### count(), sum(), avg(), min(), max()

```python
from pyspark.sql.functions import count, sum, avg, min, max, col

df = spark.read.csv("/data/sales.csv", header=True)

# Count total rows
total = df.count()

# Sum of column
total_revenue = df.agg(sum("amount")).collect()[0][0]

# Average (same as mean)
average_price = df.agg(avg("price")).collect()[0][0]

# Min and max
min_price = df.agg(min("price")).collect()[0][0]
max_price = df.agg(max("price")).collect()[0][0]
```

#### Group By and Aggregate

```python
# Total sales per department
df.groupBy("department").agg(sum("amount").alias("total_sales")).show()

# Multiple aggregations
df.groupBy("department").agg(
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount"),
    sum("amount").alias("total_amount")
).show()

# Result:
# +------------+---------+----------+----------+
# |   department|count(*) |avg_amount|total_amt |
# +------------+---------+----------+----------+
# |Engineering |      150| 45000    |6750000   |
# |Sales       |       80| 55000    |4400000   |
# +------------+---------+----------+----------+
```

### Approximate Count Distinct (approx_count_distinct)

**Why?** `count(distinct())` is slow because it requires a shuffle. `approx_count_distinct()` uses HyperLogLog algorithm for speed with ~2% error.

```python
from pyspark.sql.functions import approx_count_distinct

# Exact count (slow, requires shuffle)
exact = df.agg(count(distinct(col("user_id"))))

# Approximate count (fast, HyperLogLog)
approx = df.agg(approx_count_distinct("user_id"))

print(f"Exact: {exact}")
print(f"Approx: {approx}")  # Close to exact, much faster
```

**Use `approx_count_distinct()` when:**
- 2-3% error is acceptable
- Speed matters more than precision (dashboards, monitoring)

**Use `count(distinct())` when:**
- Exact count required
- Small dataset (performance not critical)

### Summary Statistics

```python
from pyspark.sql.functions import describe, mean, stddev, percentile_approx

# Describe all numeric columns
df.describe().show()

# Output (min, max, mean, stddev, count):
# +-------+-------+--------+
# |summary|  price| amount |
# +-------+-------+--------+
# |  count|  1000 |  1000  |
# |   mean| 45.32 | 234.67 |
# |  stddev| 12.45| 56.78  |
# |    min| 10.00 | 50.00  |
# |    max| 99.99 | 999.99 |
# +-------+-------+--------+

# Custom summary: percentiles
df.agg(
    percentile_approx("price", 0.25).alias("p25"),
    percentile_approx("price", 0.50).alias("p50"),
    percentile_approx("price", 0.75).alias("p75")
).show()
```

### On Databricks

- **GroupBy can be expensive** — Creates a shuffle; partition data wisely before groupBy
- **Use approximate functions** — When precision isn't critical, `approx_count_distinct()` and `percentile_approx()` are much faster

### Common Mistakes

1. **Calling `.collect()[0][0]` on aggregations** — Use `.show()` or access the result properly
2. **Forgetting `.alias()`** — Aggregation columns are named strange things like `sum(amount)`; rename them
3. **GroupBy on high-cardinality column** — E.g., `groupBy("user_id")` with millions of users creates huge shuffle
4. **Using exact `count(distinct())` on huge datasets** — Use `approx_count_distinct()` instead

### Code Example: Aggregations and GroupBy

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, approx_count_distinct, describe

spark = SparkSession.builder.appName("AggregationExample").getOrCreate()

# Create sample data
df = spark.createDataFrame([
    ("Engineering", 100000, "Alice"),
    ("Engineering", 95000, "Bob"),
    ("Sales", 50000, "Charlie"),
    ("Sales", 55000, "Diana"),
    ("Marketing", 70000, "Eve"),
], ["department", "salary", "name"])

# Describe all columns
print("=== Summary Statistics ===")
df.describe().show()

# GroupBy department, aggregate
print("=== GroupBy Department ===")
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_payroll")
).show()

# Approximate count distinct (on names)
print("=== Count Distinct Names ===")
df.agg(approx_count_distinct("name").alias("unique_names")).show()

# Filter high earners and aggregate
print("=== High Earners by Department ===")
df.filter(col("salary") > 60000).groupBy("department").agg(
    count("*").alias("high_earners")
).show()
```

### Recommended Resources
1. [Apache Spark: Aggregation Functions](https://spark.apache.org/docs/latest/sql-programming-guide.html#aggregations)
2. [Apache Spark: groupBy and agg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
3. [Databricks: Aggregation Performance](https://docs.databricks.com/en/performance/index.html)

---

## Window Functions: Advanced Aggregations Over Data Partitions

### Core Concept
**Window functions** let you compute aggregations (SUM, AVG, RANK) over a **window** of rows, without collapsing them. Unlike `groupBy()`, window functions keep all rows and add a new column with the computed value.

**When to use:**
- Rank employees by salary within each department
- Get running totals of sales over time
- Calculate differences between current and previous row
- Find top N items within each group

### Window vs. GroupBy

| Operation | Effect | Result |
|-----------|--------|--------|
| **GroupBy** | Collapses rows into groups | 1 row per group (fewer rows) |
| **Window** | Computes over groups but keeps all rows | Same number of rows + new aggregate column |

```
Data (5 rows):           GroupBy Result:       Window Result:
Dept | Salary           Dept | Avg Salary      Dept | Salary | Dept Avg
Eng  | 100K    -------> Eng  | 97.5K    -----> Eng  | 100K   | 97.5K
Eng  | 95K     (2 rows) Sales| 52.5K    (2)    Eng  | 95K    | 97.5K
Sales| 50K               (shrinks rows)      Sales| 50K    | 52.5K
Sales| 55K                                   Sales| 55K    | 52.5K
```

### Window Function Syntax

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, avg

# Define a window: partition by department, order by salary (descending)
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())

# Add a new column with window function result
df_ranked = df.withColumn(
    "salary_rank_in_dept",  # new column name
    rank().over(dept_window)  # compute rank within department
)
```

### Common Window Functions

| Function | What It Does |
|----------|-------------|
| **row_number()** | Assigns 1, 2, 3, ... to each row within window (ties get different numbers) |
| **rank()** | Assigns 1, 2, 2, 4, ... (ties get same rank; skips next rank) |
| **dense_rank()** | Assigns 1, 2, 2, 3, ... (ties get same rank; no skip) |
| **sum()** | Running total or total within window |
| **avg()** | Average within window |
| **lag()** | Get previous row's value |
| **lead()** | Get next row's value |

### Practical Examples

#### Rank Within Department

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# Window: partition by dept, order by salary (highest first)
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())

df_ranked = df.withColumn(
    "salary_rank_in_dept",
    rank().over(dept_window)
)

df_ranked.filter(col("salary_rank_in_dept") <= 3).show()  # Top 3 in each dept
```

#### Running Total

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

# Window: partition by customer, order by order_date, include all rows up to current
running_window = Window \
    .partitionBy("customer_id") \
    .orderBy(col("order_date")) \
    .rangeBetween(Window.unboundedPreceding, 0)  # All rows from start to current

df_running = df.withColumn(
    "running_total",
    sum("amount").over(running_window)
)
```

#### Compare to Previous Row

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

# Window: partition by product, order by date
time_window = Window.partitionBy("product").orderBy("date")

df_diff = df.withColumn(
    "previous_price",
    lag("price").over(time_window)  # Get previous row's price
).withColumn(
    "price_change",
    col("price") - col("previous_price")
)
```

### Window Specification

```python
# Basic window: partition but no ordering
window_basic = Window.partitionBy("department")

# Partition + order
window_ordered = Window.partitionBy("department").orderBy(col("salary").desc())

# Specify row range
window_range = Window \
    .partitionBy("customer_id") \
    .orderBy("date") \
    .rowsBetween(-3, 0)  # Current + 3 previous rows (moving window)

# Unbounded range (from start to current)
window_running = Window \
    .partitionBy("category") \
    .orderBy("timestamp") \
    .rangeBetween(Window.unboundedPreceding, 0)
```

### On Databricks

- **Window functions create shuffles** — Repartitions by `partitionBy()` column
- **Ordering is expensive** — `orderBy()` within window also creates shuffle
- **Use carefully** — More efficient than `groupBy()` for ranking but still costs network I/O

### Common Mistakes

1. **Forgetting `.over(window)`** — Window functions MUST be used with `.over()`
2. **Confusion with groupBy** — Window keeps all rows; groupBy collapses them
3. **Not partitioning correctly** — Window without `partitionBy()` treats entire dataset as one partition (very slow)
4. **Unbounded windows on huge data** — Entire dataset in memory per executor

### Code Example: Full Window Function Workflow

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, row_number, sum, col, lag

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

# Sample data: employee sales
df = spark.createDataFrame([
    ("Alice", "Engineering", 100000),
    ("Bob", "Engineering", 95000),
    ("Charlie", "Sales", 50000),
    ("Diana", "Sales", 55000),
    ("Eve", "Sales", 52000),
], ["name", "department", "salary"])

# Window: partition by department, order by salary (highest first)
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())

# Add ranking within each department
df_ranked = df.withColumn(
    "rank_in_dept",
    rank().over(dept_window)
).withColumn(
    "row_num",
    row_number().over(dept_window)
)

print("=== Ranked by Salary within Department ===")
df_ranked.show()

# Find top earner in each department
df_top = df_ranked.filter(col("rank_in_dept") == 1)
print("\n=== Top Earner per Department ===")
df_top.show()
```

### Recommended Resources
1. [Apache Spark: Window Functions](https://spark.apache.org/docs/latest/sql-programming-guide.html#window-functions)
2. [Databricks: Window Functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-window-functions.html)
3. [Apache Spark: Window Specification](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)

---

## Objective 4: Manipulate and utilize Date data type

### Core Concept
Dates are tricky: Unix timestamps (seconds since 1970-01-01), strings, and Date objects all exist. You need to convert between them and extract components (year, month, day).

### Date Formats

| Format | Example | Notes |
|--------|---------|-------|
| **Unix timestamp** | 1609459200 | Seconds since 1970-01-01 (integer) |
| **Date string** | "2021-01-01" | ISO format (YYYY-MM-DD) |
| **Date object** | date(2021, 1, 1) | Spark's date type |

### Convert Unix Timestamp to Date

```python
from pyspark.sql.functions import from_unixtime, to_date, col

df = spark.createDataFrame([
    (1, 1609459200),  # Unix timestamp for 2021-01-01
    (2, 1640995200),  # Unix timestamp for 2022-01-01
], ["id", "unix_ts"])

# Unix timestamp to date string
df_with_date = df.withColumn(
    "date_str",
    from_unixtime("unix_ts", "yyyy-MM-dd")
)

# Unix timestamp to date object
df_with_date = df.withColumn(
    "date_obj",
    to_date(from_unixtime("unix_ts", "yyyy-MM-dd"))
)

df_with_date.show()
```

### Extract Date Components

```python
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, col

df = spark.createDataFrame([
    (1, "2021-01-15"),
    (2, "2022-06-20"),
], ["id", "date_str"])

# Convert string to date first
df = df.withColumn("date", to_date(col("date_str")))

# Extract components
df_extracted = df.withColumn("year", year(col("date"))) \
                 .withColumn("month", month(col("date"))) \
                 .withColumn("day", dayofmonth(col("date"))) \
                 .withColumn("day_of_week", dayofweek(col("date")))

df_extracted.show()
# Output:
# +---+----------+----+-----+-----+----------+
# | id| date_str |year|month|day  |day_of_wk |
# +---+----------+----+-----+-----+----------+
# |  1|2021-01-15|2021|    1|  15 |        5 |
# |  2|2022-06-20|2022|    6|  20 |        1 |
```

### Date Arithmetic

```python
from pyspark.sql.functions import date_add, date_sub, datediff

# Add days to a date
df_plus_7 = df.withColumn("date_plus_7", date_add(col("date"), 7))

# Subtract days
df_minus_30 = df.withColumn("date_minus_30", date_sub(col("date"), 30))

# Days between two dates
df_diff = df.withColumn("days_since_epoch", datediff(col("date"), "1970-01-01"))
```

### On Databricks

- **Always convert to date type** — Timestamps and strings don't filter correctly in WHERE clauses
- **Use ISO format** — "YYYY-MM-DD" is universal and unambiguous

### Common Mistakes

1. **Forgetting to convert strings to date** — String dates don't support date math
2. **Mixing Unix timestamps and date strings** — Convert to one format first
3. **Day of week is 1-indexed** — Sunday = 1, Monday = 2, ..., Saturday = 7
4. **Not handling NULL dates** — Date functions return NULL if input is NULL

### Code Example: Date Manipulation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date, year, month, datediff, date_add

spark = SparkSession.builder.appName("DateExample").getOrCreate()

# Create data with various date formats
df = spark.createDataFrame([
    (1, 1609459200, "2021-01-15"),
    (2, 1640995200, "2022-06-20"),
], ["id", "unix_ts", "date_str"])

# Convert all to date objects
df = df.withColumn("unix_date", to_date(from_unixtime("unix_ts", "yyyy-MM-dd"))) \
       .withColumn("str_date", to_date(col("date_str")))

# Extract year and month
df = df.withColumn("year", year("str_date")) \
       .withColumn("month", month("str_date"))

# Calculate days since epoch (or any reference date)
df = df.withColumn("days_since_epoch", datediff("str_date", "1970-01-01"))

# Add 30 days
df = df.withColumn("future_date", date_add("str_date", 30))

df.show()
```

### Recommended Resources
1. [Apache Spark: Date and Timestamp Functions](https://spark.apache.org/docs/latest/sql-programming-guide.html#working-with-dates-and-timestamps)
2. [Databricks: Working with Dates](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html)
3. [Apache Spark: to_date and to_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html)

---

## Objective 5: Combine DataFrames (joins and unions)

### Core Concept
Join combines rows from two DataFrames based on a key. Union stacks DataFrames vertically.

### Types of Joins

| Join Type | Result | Example |
|-----------|--------|---------|
| **INNER** | Rows where both have key | A=1,B=1 → kept |
| **LEFT** | All from left + matching right | A=1,B=1 → kept; A=2 (no B match) → kept with NULL |
| **RIGHT** | Matching left + all from right | Mirror of LEFT |
| **OUTER** | All rows from both | All kept; NULLs where no match |
| **CROSS** | Cartesian product | Every row from left × every row from right |

```python
# Sample data
left = spark.createDataFrame([
    (1, "Alice"), (2, "Bob"), (3, "Charlie")
], ["id", "name"])

right = spark.createDataFrame([
    (1, "Engineering"), (2, "Sales")
], ["id", "department"])

# INNER JOIN: only id=1, id=2
inner = left.join(right, "id", "inner")
# Result: (1, Alice, Engineering), (2, Bob, Sales)

# LEFT JOIN: all from left, match from right
left_join = left.join(right, "id", "left")
# Result: (1, Alice, Engineering), (2, Bob, Sales), (3, Charlie, NULL)

# CROSS JOIN: every left × every right
cross = left.crossJoin(right)
# Result: 3 × 2 = 6 rows
```

### Join Syntax

```python
# Join on single column (both have same name)
df_result = left.join(right, "id", "inner")

# Join on different column names
df_result = left.join(right, left.id == right.user_id, "inner")

# Join on multiple columns
df_result = left.join(right, (left.id == right.id) & (left.year == right.year), "inner")
```

### Broadcast Join

A **broadcast join** is an optimization: send the smaller DataFrame to all executors, then join locally. Avoids expensive shuffle.

```python
from pyspark.sql.functions import broadcast

# Without broadcast: shuffle both dataframes (expensive)
result = large_df.join(small_df, "id", "inner")

# With broadcast: send small_df to all executors (fast)
result = large_df.join(broadcast(small_df), "id", "inner")
```

**Use broadcast when:**
- One DataFrame is small (< 100MB)
- Other DataFrame is huge
- Join is a bottleneck

### Union

Stack DataFrames vertically (must have same columns).

```python
# Union: keep all duplicates
df_combined = df1.union(df2)

# Union all (same as union)
df_combined = df1.unionByName(df2)  # Matches columns by name

# Union distinct: remove duplicate rows
df_combined = df1.unionByName(df2, allowMissingColumns=True)
```

### On Databricks

- **INNER join is fastest** — Filters rows before shuffle
- **Use broadcast for small tables** — Can be 10x faster than regular join
- **Avoid CROSS joins on large data** — Cartesian product explodes in size

### Common Mistakes

1. **Not joining on the right columns** — Causes unexpected results or Cartesian product
2. **Forgetting to specify join type** — Default is INNER; if you wanted LEFT, you get surprises
3. **Broadcast a huge DataFrame** — Causes out-of-memory errors; only broadcast small data
4. **Union with different column counts** — Both DataFrames must have same columns

### Code Example: Joins and Unions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Create data
users = spark.createDataFrame([
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35),
], ["user_id", "name", "age"])

orders = spark.createDataFrame([
    (1, 1, 100),  # user_id=1 orders item 1 for $100
    (2, 1, 50),   # user_id=2 orders item 1 for $50
    (3, 2, 200),  # user_id=3 orders item 2 for $200
], ["order_id", "user_id", "amount"])

# INNER JOIN
print("=== INNER JOIN ===")
inner = users.join(orders, "user_id", "inner")
inner.show()

# LEFT JOIN
print("=== LEFT JOIN ===")
left = users.join(orders, "user_id", "left")
left.show()

# BROADCAST JOIN (if small table exists)
print("=== BROADCAST JOIN ===")
broadcast_result = users.join(broadcast(orders), "user_id", "inner")
broadcast_result.show()

# Union two DataFrames (same schema)
users2 = spark.createDataFrame([
    (4, "Diana", 28),
], ["user_id", "name", "age"])
print("=== UNION ===")
combined = users.union(users2)
combined.show()
```

### Recommended Resources
1. [Apache Spark: SQL Joins](https://spark.apache.org/docs/latest/sql-programming-guide.html#joins)
2. [Apache Spark: DataFrame Join Methods](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
3. [Databricks: Join Performance Tuning](https://docs.databricks.com/en/performance/index.html)

---

## Objective 6: Manage input and output operations (reading, writing, schemas)

### Core Concept
When you write a DataFrame, you can specify (or let Spark infer) the schema. Reading data back requires schema compatibility.

### Writing with Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define a schema
schema = StructType([
    StructField("user_id", IntegerType(), False),  # False = not nullable
    StructField("name", StringType(), True),       # True = nullable
    StructField("age", IntegerType(), True),
])

# Write with this schema
df.write.mode("overwrite").schema(schema).parquet("/output/data")
```

### Reading with Schema

```python
# Read with explicit schema
df = spark.read.schema(schema).parquet("/output/data")

# Or let Spark infer schema (slow, but easier)
df = spark.read.parquet("/output/data")  # Spark infers schema from file
```

### Schema Evolution

**Problem:** You add a new column to your DataFrame. Old files don't have this column. How do you read them?

**Solution:** Use `mergeSchema` option.

```python
# Old data has: id, name
# New data has: id, name, email

# Read with mergeSchema=true to auto-fill missing columns
df = spark.read.option("mergeSchema", "true").parquet("/output/data")
```

### On Databricks

- **Delta handles schema evolution automatically** — No need for mergeSchema option
- **Parquet requires mergeSchema** — Explicitly set it if adding columns

### Common Mistakes

1. **Not specifying schema** — Spark has to read data twice (once for schema, once for data)
2. **Schema mismatch** — Reading a CSV with integers but column is actually strings
3. **Forgetting to handle nullable columns** — Set `True` for optional columns

### Code Example: Schema Management

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SchemaExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])

# Create and write DataFrame
df = spark.createDataFrame([
    (1, "Alice", 30),
    (2, "Bob", 25),
], schema=schema)

df.write.mode("overwrite").format("parquet").save("/tmp/data")

# Read back with explicit schema
df_read = spark.read.schema(schema).parquet("/tmp/data")
print("Schema:")
df_read.printSchema()
df_read.show()
```

### Recommended Resources
1. [Apache Spark: Loading Data Programmatically](https://spark.apache.org/docs/latest/sql-data-sources-load-save-sql.html)
2. [Databricks: Reading and Writing Data](https://docs.databricks.com/en/develop/dataframes.html#read-data)
3. [Apache Spark: Data Source Options](https://spark.apache.org/docs/latest/sql-data-sources.html)

---

## Objective 7: Perform operations on DataFrames (sorting, iterating, printing schema, conversion)

### Core Concept
Common DataFrame operations: sort rows, iterate through them, inspect schema, convert to/from Python lists.

### Sorting

```python
from pyspark.sql.functions import col, desc

df = spark.read.csv("/data/employees.csv", header=True)

# Sort ascending (default)
df_sorted = df.sort("name")

# Sort descending
df_sorted = df.sort(col("salary").desc())

# Sort by multiple columns
df_sorted = df.sort(col("department"), col("salary").desc())
```

### Print Schema

```python
# Print the DataFrame schema (structure)
df.printSchema()

# Output:
# root
#  |-- user_id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- age: long (nullable = true)
```

### Iterate Through Rows

```python
# Collect all rows (loads into memory; careful on huge data!)
rows = df.collect()
for row in rows:
    print(f"ID: {row['user_id']}, Name: {row['name']}")

# Or convert to list
row_list = df.collect()
print(row_list)
```

### Convert to/from Python Lists

```python
# DataFrame to list of Row objects
rows = df.collect()

# DataFrame to list of dicts
row_dicts = df.toPandas().to_dict("records")

# List of tuples
rows_tuples = [tuple(row) for row in rows]

# Create DataFrame from list
list_data = [(1, "Alice"), (2, "Bob")]
df_from_list = spark.createDataFrame(list_data, schema=["id", "name"])
```

### On Databricks

- **Use `.collect()` only on small DataFrames** — Loads all rows to driver; will crash on huge data
- **For iteration, use `.toLocalIterator()`** — Doesn't load all into memory at once

### Common Mistakes

1. **Calling `.collect()` on huge DataFrames** — Tries to fit all data in driver memory
2. **Not sorting before checking order** — Results may be in any order unless explicitly sorted
3. **Assuming `.show()` shows all rows** — `.show()` only prints first 20

### Code Example: Sorting, Iteration, Schema

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("OperationsExample").getOrCreate()

# Create data
df = spark.createDataFrame([
    (1, "Alice", 30, 50000),
    (2, "Bob", 25, 45000),
    (3, "Charlie", 35, 60000),
], ["id", "name", "age", "salary"])

# Print schema
print("=== Schema ===")
df.printSchema()

# Sort by salary descending
print("=== Sorted by Salary ===")
df_sorted = df.sort(col("salary").desc())
df_sorted.show()

# Iterate and print
print("=== Iterate Rows ===")
for row in df_sorted.collect():
    print(f"{row['name']}: ${row['salary']}")

# Convert to list of dicts
print("=== As List of Dicts ===")
rows_dicts = df.collect()
for row_dict in rows_dicts:
    print(row_dict.asDict())
```

### Recommended Resources
1. [Databricks: DataFrame Operations](https://docs.databricks.com/en/notebooks/notebooks-use-cases.html)
2. [Apache Spark: DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)
3. [Databricks: Data Inspection Techniques (2024)](https://www.databricks.com/blog/dataframe-inspection-patterns)

---

## Objective 8: Create and invoke user-defined functions (UDFs)

### Core Concept
Spark's built-in functions cover most use cases, but sometimes you need custom logic. UDFs let you write Python/SQL functions that Spark runs on each row.

### Python UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a Python function
def greet(name):
    return f"Hello, {name}!"

# Convert to UDF
greet_udf = udf(greet, StringType())

# Or use decorator
@udf(StringType())
def greet_decorated(name):
    return f"Hello, {name}!"

# Use in DataFrame
df = spark.createDataFrame([("Alice",), ("Bob",)], ["name"])
df_greeted = df.select("name", greet_udf(col("name")).alias("greeting"))
df_greeted.show()

# Output:
# +-----+---------------+
# | name|      greeting |
# +-----+---------------+
# |Alice| Hello, Alice! |
# |  Bob|   Hello, Bob! |
# +-----+---------------+
```

### Return Types

```python
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, ArrayType

# Returns integer
@udf(IntegerType())
def double(x):
    return x * 2

# Returns double (float)
@udf(DoubleType())
def half(x):
    return x / 2.0

# Returns boolean
@udf(BooleanType())
def is_adult(age):
    return age >= 18

# Returns array
@udf(ArrayType(StringType()))
def split_name(full_name):
    return full_name.split(" ")
```

### UDFs with Multiple Arguments

```python
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def add(x, y):
    return x + y

df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
df_sum = df.select("a", "b", add("a", "b").alias("sum"))
df_sum.show()
```

### SQL UDFs

```python
# Register UDF for SQL
spark.udf.register("greet_sql", greet, StringType())

# Use in SQL
result = spark.sql("SELECT greet_sql('Alice') as greeting")
result.show()
```

### On Databricks

- **Python UDFs are slow** — Each row is serialized Python → JVM, run, serialize back. Use Spark SQL functions when possible
- **Pandas UDFs are faster** — Use `@pandas_udf` decorator for better performance (covered in Section 7)

### Common Mistakes

1. **Not specifying return type** — UDF won't work; Spark doesn't know what it returns
2. **Using UDFs on every row** — 1M rows = 1M Python function calls; slow!
3. **UDF returning wrong type** — Runtime error if function returns string but UDF is StringType

### Code Example: Python and SQL UDFs

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, BooleanType

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

# Define Python UDF
@udf(StringType())
def uppercase_name(name):
    return name.upper()

@udf(BooleanType())
def is_senior(age):
    return age >= 65

# Create DataFrame
df = spark.createDataFrame([
    (1, "alice", 30),
    (2, "bob", 68),
    (3, "charlie", 45),
], ["id", "name", "age"])

# Use Python UDFs
df_transformed = df.select(
    "id",
    uppercase_name("name").alias("name_upper"),
    "age",
    is_senior("age").alias("is_senior")
)

print("=== Python UDFs ===")
df_transformed.show()

# Register SQL UDF
spark.udf.register("is_young", lambda age: age < 25, BooleanType())

# Use SQL UDF
result = spark.sql("""
    SELECT name, age, is_young(age) as is_young
    FROM (SELECT 'Alice' as name, 30 as age UNION SELECT 'Bob', 20)
""")

print("=== SQL UDF ===")
result.show()
```

### Recommended Resources
1. [Apache Spark: User Defined Functions](https://spark.apache.org/docs/latest/sql-programming-guide.html#creating-dataframes-with-udfs)
2. [Apache Spark: Python UDF API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
3. [Databricks: UDF Performance](https://docs.databricks.com/en/develop/python/custom-python-functions.html)

---

## Objective 9: Describe different types of variables (broadcast variables and accumulators)

### Core Concept
Normally, when you use a variable in a Spark function, each executor gets its own copy (serialized). **Broadcast variables** send the variable once and reuse it. **Accumulators** let you safely add values across executors.

### Broadcast Variables

**Problem:** You want a large lookup table (countries, zip codes, etc.) on every executor. Sending it to each task is wasteful.

**Solution:** Send it once, cache it.

```python
from pyspark.sql.functions import broadcast

# Large lookup table
lookup = {"1": "Engineering", "2": "Sales", "3": "Marketing"}

# Broadcast to all executors (sent once)
broadcast_lookup = sc.broadcast(lookup)

# Use in a function
def get_department(code):
    return broadcast_lookup.value.get(code, "Unknown")

# Or use in a join (more common)
lookup_df = spark.createDataFrame([(k, v) for k, v in lookup.items()], ["code", "dept"])
large_df = spark.read.parquet("/data/employees.parquet")

# Broadcast join
result = large_df.join(broadcast(lookup_df), "code", "left")
```

### Accumulators

**Problem:** You want to count errors across all executors. Each executor increments a counter, then you collect the total.

**Solution:** Use an accumulator.

```python
from pyspark.sql.functions import col, when

# Create accumulator for counting errors
error_count = sc.accumulator(0)

# Function that updates accumulator
def count_errors(row):
    if row.age < 0 or row.age > 150:
        error_count.add(1)
    return row

# Read data
df = spark.read.csv("/data/data.csv", header=True)

# Process each row (incrementing accumulator)
df.rdd.map(count_errors).collect()

# Get total error count
print(f"Total errors: {error_count.value}")
```

### On Databricks

- **Broadcast variables**: Use for lookup tables, ML models, configuration
- **Accumulators**: Use for counting errors, events, metrics
- **DataFrame API prefers native operations**: Use `.groupBy()` instead of accumulators when possible

### Common Mistakes

1. **Modifying broadcast variable** — It's immutable; you can't update it
2. **Accessing accumulator in wrong context** — Executors can add; only driver can read `.value`
3. **Using accumulator for filtering** — Use `.filter()` instead; accumulators are for monitoring

### Code Example: Broadcast and Accumulators

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BroadcastAccumExample").getOrCreate()
sc = spark.sparkContext

# Broadcast: large lookup table
country_codes = {"US": "United States", "CA": "Canada", "MX": "Mexico"}
broadcast_codes = sc.broadcast(country_codes)

# Create data
df = spark.createDataFrame([
    (1, "US"),
    (2, "CA"),
    (3, "XX"),  # Invalid code
], ["id", "country_code"])

# Use broadcast in UDF
def lookup_country(code):
    return broadcast_codes.value.get(code, "Unknown")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

lookup_udf = udf(lookup_country, StringType())

# Apply UDF
df_with_country = df.select("id", "country_code", lookup_udf("country_code").alias("country"))
df_with_country.show()

# Accumulator: count invalid codes
invalid_count = sc.accumulator(0)

def validate_code(code):
    if code not in broadcast_codes.value:
        invalid_count.add(1)
    return code

df.rdd.map(lambda row: validate_code(row.country_code)).collect()
print(f"Invalid codes: {invalid_count.value}")
```

### Recommended Resources
1. [Apache Spark: Broadcast Variables](https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables)
2. [Apache Spark: Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators-broadcast-variables)
3. [Databricks: Shared Variables](https://docs.databricks.com/en/develop/dataframes.html)

---

## Objective 10: Describe the purpose and implementation of broadcast joins

### Core Concept
A broadcast join sends the smaller DataFrame to all executors, avoiding a shuffle. If joining large_df (1GB) with small_df (10MB), broadcast small_df instead of shuffling both.

### When to Use Broadcast Join

| Scenario | Use Broadcast |
|----------|---|
| large_df (1GB) + small_df (10MB) | YES |
| large_df (100MB) + small_df (90MB) | NO — both are too big |
| large_df (100KB) + small_df (10GB) | NO — can't fit small_df in memory |

```python
from pyspark.sql.functions import broadcast

# Automatic optimization (Spark does this if small_df < threshold)
result = large_df.join(small_df, "id", "inner")

# Explicit broadcast (force it)
result = large_df.join(broadcast(small_df), "id", "inner")
```

### Broadcast Join Execution

**Without broadcast (regular join):**
```
Shuffle phase:
- Executor 1: sends rows to Executor 2
- Executor 2: sends rows to Executor 1
- Executor 3: sends rows to all
(Lots of network traffic)

Join phase:
- Each executor joins locally after shuffle
```

**With broadcast:**
```
Broadcast phase:
- Driver sends small_df to all executors (once)

Join phase:
- Each executor joins its partition of large_df with cached small_df (locally)
(Less network traffic; faster)
```

### Configuration

By default, Spark auto-broadcasts if small_df < 100MB (configurable).

```python
# Set broadcast threshold to 200MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 200 * 1024 * 1024)

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

### On Databricks

- **Spark auto-optimizes joins** — Usually broadcasts small tables automatically
- **Explicit broadcast is useful** — When Spark doesn't auto-optimize or you're sure it's beneficial

### Common Mistakes

1. **Broadcasting a huge DataFrame** — Out-of-memory error on executors
2. **Not checking DataFrame size** — Assume small_df is small; verify with `.count()` and `.explain()`
3. **Broadcast join is only for joins** — Can't use for groupBy or other operations

### Code Example: Broadcast Join Performance

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastJoinExample").getOrCreate()

# Create data
large_df = spark.createDataFrame(
    [(i, f"User{i}") for i in range(1000)],
    ["user_id", "name"]
)

small_df = spark.createDataFrame(
    [(1, "Engineering"), (2, "Sales"), (3, "Marketing")],
    ["dept_id", "department"]
).withColumn("user_id", (small_df.dept_id % 100) + 1)  # Simulate lookup table

# Regular join (shuffle)
print("=== Regular Join (Shuffle) ===")
result_regular = large_df.join(small_df, "user_id", "inner")
result_regular.explain()

# Broadcast join (no shuffle)
print("=== Broadcast Join ===")
result_broadcast = large_df.join(broadcast(small_df), "user_id", "inner")
result_broadcast.explain()

# Both produce same result, but broadcast is faster
result_broadcast.show()
```

### Recommended Resources
1. [Databricks: Broadcast Joins](https://docs.databricks.com/en/performance/broadcast-joins.html)
2. [Apache Spark: Broadcast Joins](https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hint)
3. [Apache Spark: broadcast Function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html)

---

## Summary: Section 3 Key Takeaways

1. **Column/row manipulation**: Use `withColumn()`, `drop()`, `filter()`, `select()`
2. **Array operations**: `explode()` turns arrays into rows
3. **Deduplication**: `drop_duplicates()` or `distinct()`
4. **Aggregations**: `groupBy()` + `agg()` for summaries; use `approx_count_distinct()` for speed
5. **Dates**: Convert Unix → date with `from_unixtime()`; extract components with `year()`, `month()`
6. **Joins**: INNER (fastest), LEFT (keep all left), BROADCAST (no shuffle for small tables)
7. **UDFs**: Use when built-in functions don't work; they're slower than native functions
8. **Broadcast**: Send small lookup tables once; join locally on each executor
9. **Accumulators**: Count errors/events across executors (read-only on executor side)
10. **Schema management**: Define schemas for performance; Delta handles evolution automatically

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Previous: Section 2 - Spark SQL](02_Spark_SQL.md) | [Next: Section 4 - Troubleshooting →](04_Troubleshooting_and_Tuning.md)
