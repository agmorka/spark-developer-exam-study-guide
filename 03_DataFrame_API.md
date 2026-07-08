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

#### Select with Column Negation (~)

Use the `~` operator to exclude columns in a select:

```python
from pyspark.sql.functions import col

# Select all columns EXCEPT these using negation (~)
df_selected = df.select(~col("predError"), ~col("productId"), ~col("value"))

# Equivalent to: df.drop("predError", "productId", "value")
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

##### Transforming Columns with withColumn()

When you want to add or replace a column, use `.withColumn()` (not direct assignment like `df["col"] = value`).

```python
# ✓ CORRECT: Using withColumn()
df = df.withColumn("new_column", value_expression)

# ✗ WRONG: Direct assignment (doesn't work in Spark)
df["new_column"] = value_expression  # This won't do what you expect!

# ✓ CORRECT: Drop old column, add new one
df = df.drop("old_col").withColumn("new_col", some_transformation)

# ✓ CORRECT: Replace existing column
df = df.withColumn("existing_col", new_value)  # Overwrites original
```

**Why?** DataFrames are immutable. `.withColumn()` returns a NEW DataFrame with the column added/replaced.

#### Unix Timestamp and Date Conversion

Convert between Unix timestamps (seconds since 1970-01-01) and dates/strings:

```python
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, to_timestamp, to_date

df = spark.createDataFrame([
    (1, "2021-01-15 10:30:00", 1609459200),
], ["id", "date_string", "unix_ts"])

# String → Unix timestamp
df.withColumn("ts", unix_timestamp(col("date_string"), "yyyy-MM-dd HH:mm:ss")).show()

# Unix timestamp → Date string (specify format)
df.withColumn("date_str", from_unixtime(col("unix_ts"), "yyyy-MM-dd")).show()

# String → Date object
df.withColumn("date_obj", to_date(col("date_string"))).show()

# String → Timestamp with time zone
df.withColumn("ts_obj", to_timestamp(col("date_string"))).show()
```

**⚠️ Common mistakes:**
```python
# WRONG: Direct assignment + wrong method
df["transactionTimestamp"] = unix_timestamp("transactionDate", "yyyy-MM-dd")

# WRONG: unix_timestamp on string without col() wrapper
transactionsDf = transactionsDf.drop("transactionDate") \
    .withColumn("transactionTimestamp", unix_timestamp("transactionDate"))  # Error: col already dropped!

# ✓ CORRECT: wrap in col(), drop AFTER adding new column
transactionsDf = transactionsDf \
    .withColumn("transactionTimestamp", unix_timestamp(col("transactionDate"), "yyyy-MM-dd HH:mm")) \
    .drop("transactionDate")
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

### String Functions

Common functions for manipulating string columns:

```python
from pyspark.sql.functions import col, length, lower, upper, substr, trim, regexp_replace, regexp_extract, concat, lit

df = spark.createDataFrame([
    (1, "  Alice Smith  ", "alice@example.com"),
    (2, "BOB JONES", "bob@example.com"),
], ["id", "name", "email"])

# String case transformation
df.select(lower(col("name")), upper(col("email"))).show()  # lowercase, UPPERCASE

# String length
df.select(length(col("name")).alias("name_length")).show()

# Trim whitespace
df.select(trim(col("name")).alias("name_trimmed")).show()

# Substring: substr(col, start, length)
df.select(substr(col("name"), 1, 5).alias("first_5_chars")).show()

# Replace text: regexp_replace(col, pattern, replacement)
df.select(regexp_replace(col("email"), "@.*", "@example.net").alias("new_email")).show()

# Extract pattern: regexp_extract(col, pattern, group_index)
df.select(regexp_extract(col("email"), "(.+)@", 1).alias("username")).show()  # Group 1 = before @

# Concatenate strings
df.select(concat(col("name"), lit(" - "), col("email")).alias("full_info")).show()

# Remove vowels (common exam trick)
df.select(
    length(regexp_replace(lower(col("name")), "[aeiou\\s]", "")).alias("consonant_count")
).show()

# Filter with string matching
df.filter(col("email").contains("example")).show()  # Find emails containing "example"
df.filter(col("name").like("A%")).show()            # Names starting with "A"

# Split string into array with limit (max 4 elements)
df.select(split(col("itemName"), "[\s\-]", 4).alias("words")).show()
# "Thick Coat for Walking" → ["Thick", "Coat", "for", "Walking in the Snow"]
```

| Function | Purpose | Example |
|----------|---------|---------|
| `lower(col)` | Convert to lowercase | `lower(col("Name"))` → "name" |
| `upper(col)` | Convert to uppercase | `upper(col("Name"))` → "NAME" |
| `length(col)` | String length | `length(col("Name"))` → 4 |
| `substr(col, start, len)` | Extract substring | `substr(col("Name"), 1, 2)` → "Na" |
| `trim(col)` | Remove leading/trailing spaces | `trim(col("  name  "))` → "name" |
| `split(col, pattern, limit)` | Split string into array with max limit | `split(col("text"), "[,\s]", 4)` → max 4 elements |
| `col.contains(pattern)` | Check if string contains substring (for filtering) | `col("Email").contains("@")` → true/false |
| `col.like(pattern)` | SQL LIKE matching (% = any chars, _ = single char) | `col("Name").like("A%")` → names starting with "A" |
| `regexp_replace(col, pattern, repl)` | Replace matching pattern | `regexp_replace(col("Name"), "a", "X")` → "NXme" |
| `regexp_extract(col, pattern, idx)` | Extract matched group | `regexp_extract(col("Email"), "(.+)@", 1)` → username |
### Math Functions

Transform numeric columns with mathematical operations:

```python
from pyspark.sql.functions import col, cos, sin, tan, sqrt, abs, round, degrees, radians, pow

df = spark.createDataFrame([
    (1, 45.0, 2.0),
    (2, 90.0, 3.0),
], ["id", "angle_rad", "num"])

# Trigonometric functions
df.select(cos(col("angle_rad")).alias("cos_val")).show()        # Cosine
df.select(sin(col("angle_rad")).alias("sin_val")).show()        # Sine
df.select(tan(col("angle_rad")).alias("tan_val")).show()        # Tangent

# Degree/radian conversion + trigonometry
df.select(
    degrees(col("angle_rad")).alias("degrees"),  # Convert radians to degrees
    cos(col("angle_rad")).alias("cos_rad")       # Cosine of radians
).show()

# Example: convert to degrees, then take cosine, round to 2 decimals
df.select(
    round(cos(degrees(col("angle_rad"))), 2).alias("cos_degrees")
).show()

# Other math functions
df.select(sqrt(col("num")).alias("sqrt")).show()       # Square root
df.select(abs(col("num")).alias("abs")).show()         # Absolute value
df.select(round(col("num"), 1).alias("rounded")).show() # Round to N decimals
df.select(pow(col("num"), 2).alias("squared")).show()  # Power (num^2)
```

| Function | Purpose | Example |
|----------|---------|---------|
| `cos(col)` | Cosine (radians) | `cos(0)` → 1.0 |
| `sin(col)` | Sine (radians) | `sin(PI/2)` → 1.0 |
| `tan(col)` | Tangent (radians) | `tan(PI/4)` → 1.0 |
| `degrees(col)` | Convert radians to degrees | `degrees(PI)` → 180.0 |
| `radians(col)` | Convert degrees to radians | `radians(180)` → PI |
| `sqrt(col)` | Square root | `sqrt(16)` → 4.0 |
| `abs(col)` | Absolute value | `abs(-5)` → 5 |
| `round(col, d)` | Round to d decimals | `round(3.14159, 2)` → 3.14 |
| `pow(col, n)` | Power (col^n) | `pow(2, 3)` → 8 |

### On Databricks

- **Column expressions**: Use `col()` to reference columns; SQL strings also work
- **Chaining operations**: Each method returns a new DataFrame; chain them efficiently
- **Performance**: Filter early to reduce data processed downstream

### Common Mistakes

1. **Forgetting to import functions** — `from pyspark.sql.functions import col, lit, etc.`
2. **Using string column names incorrectly** — `df.filter("age > 30")` works, but `col("age") > 30` is more flexible
3. **Assuming `withColumn()` modifies in place** — It doesn't; reassign: `df = df.withColumn(...)`
4. **Not using `alias()` after `explode()`** — Exploded column needs a name
5. **Chaining `.alias()` directly on `explode()` outside `select()`** — ❌ WRONG: `df.explode("attributes").alias("attribute")`. The method doesn't exist on DataFrame. ✓ CORRECT: `df.select(explode(col("attributes")).alias("attribute"))`

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

#### Using describe() vs summary()

```python
from pyspark.sql.functions import describe, mean, stddev, percentile_approx

# BOTH methods return statistics on numeric columns:

# Method 1: describe() — returns count, mean, stddev, min, max
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

# Method 2: summary() — specify which stats to include (more flexible)
df.summary("count", "mean", "stddev", "25%", "50%", "75%", "min", "max").show()

# Output (with percentiles):
# +-------+-------+--------+
# |summary|  price| amount |
# +-------+-------+--------+
# |  count|  1000 |  1000  |
# |   mean| 45.32 | 234.67 |
# |  stddev| 12.45| 56.78  |
# |   25%  | 25.12 | 145.34 |
# |   50%  | 45.32 | 234.67 |
# |   75%  | 65.43 | 323.90 |
# |    min| 10.00 | 50.00  |
# |    max| 99.99 | 999.99 |
# +-------+-------+--------+
```

**Difference:**
- `.describe()` — Always returns [count, mean, stddev, min, max]
- `.summary()` — You choose which statistics to include; more flexible for custom percentiles

#### Custom Aggregations

```python
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

### Union Operations

Stack DataFrames vertically (append rows). Different methods handle column mismatches differently.

| Method | Behavior | Use Case |
|--------|----------|----------|
| `.union(other)` | **By position** — ignores column names, combines 1st col with 1st col, 2nd with 2nd | Both have SAME column names in SAME order |
| `.unionByName(other)` | **By name** — matches columns by name, not position | Column names match but order may differ |
| `.unionByName(other, allowMissingColumns=True)` | **By name, allow missing** — columns in one DF but not other get filled with NULL | Different column sets; fill missing with NULL |

**Example:**

```python
# DF1: columns [A, B, C] with data [1, 2, 3]
# DF2: columns [C, B, A] with data [10, 20, 30]  (different order, same names)

# .union() — ignores names, uses position
result = df1.union(df2)
# Result: [1, 2, 3] and [10, 20, 30]  (A=10, B=20, C=30)

# .unionByName() — matches by name
result = df1.unionByName(df2)
# Result: [1, 2, 3] and [30, 20, 10]  (A=30, B=20, C=10, reordered)

# DF3: columns [A, B, D] (missing C from DF1)
# .unionByName(allowMissingColumns=True)
result = df1.unionByName(df3, allowMissingColumns=True)
# Result: [1, 2, 3, NULL] and [30, 20, NULL, 40]  (adds missing C column with NULLs)
```

### On Databricks

- **INNER join is fastest** — Filters rows before shuffle
- **Use broadcast for small tables** — Can be 10x faster than regular join
- **Avoid CROSS joins on large data** — Cartesian product explodes in size
- **Use `.union()` when column names differ but positions match** — Treats DataFrames by position
- **Use `.unionByName()` when column names match but order differs** — Matches by name

### Common Mistakes

1. **Not joining on the right columns** — Causes unexpected results or Cartesian product
2. **Forgetting to specify join type** — Default is INNER; if you wanted LEFT, you get surprises
3. **Broadcast a huge DataFrame** — Causes out-of-memory errors; only broadcast small data
4. **Using `.union()` when you mean `.unionByName()`** — Data is appended by position, not name; wrong columns combine
5. **Using `.unionByName()` without setting `allowMissingColumns=True`** — Fails if column sets don't match exactly

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

## Objective 6: Manage input and output operations (reading, writing, schemas)

### Core Concept
When you write a DataFrame, you can specify (or let Spark infer) the schema. Reading data back requires schema compatibility.

### Writing with Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Define a schema
schema = StructType([
    StructField("user_id", IntegerType(), False),  # False = not nullable
    StructField("name", StringType(), True),       # True = nullable
    StructField("age", IntegerType(), True),
    StructField("salary", FloatType(), True),      # Floating-point column
])

# Write with this schema
df.write.mode("overwrite").schema(schema).parquet("/output/data")
```

#### Creating DataFrames with explicit types

Use `createDataFrame()` with a schema that includes `FloatType` and other data types:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# Method 1: createDataFrame with data and schema
data = [
    (1, "Alice", 30, 75000.50),
    (2, "Bob", 25, 65000.25),
    (3, "Charlie", 35, 85000.75),
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", FloatType(), True),      # Float: 32-bit precision
])

df = spark.createDataFrame(data, schema=schema)
df.show()
# Output:
# +---+-------+---+----------+
# | id|   name|age|    salary|
# +---+-------+---+----------+
# |  1|  Alice| 30|75000.496...| (note: FloatType loses precision vs DoubleType)
# |  2|    Bob| 25|65000.246...|
# |  3|Charlie| 35|85000.746...|
# +---+-------+---+----------+

# Method 2: Print schema to verify types
df.printSchema()
# root
#  |-- id: integer (nullable = false)
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- salary: float (nullable = true)
```

**⚠️ Important:**
- **FloatType** = 32-bit IEEE 754 (less precise; use for memory efficiency)
- **DoubleType** = 64-bit IEEE 754 (more precise; default for decimal data)

```python
# If you need precision for money, use DoubleType instead:
from pyspark.sql.types import DoubleType

schema = StructType([
    StructField("salary", DoubleType(), True),  # Better for financial data
])
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

## Objective 7: Perform operations on DataFrames (sorting, iterating, printing schema, conversion)

### Core Concept
Common DataFrame operations: sort rows, iterate through them, inspect schema, convert to/from Python lists.

### Sorting

```python
from pyspark.sql.functions import col, desc, asc, desc_nulls_last, asc_nulls_first

df = spark.read.csv("/data/employees.csv", header=True)

# Sort ascending (default)
df_sorted = df.sort("name")

# Sort descending
df_sorted = df.sort(col("salary").desc())

# Sort by multiple columns
df_sorted = df.sort(col("department"), col("salary").desc())

# Sort with NULL handling
df_sorted = df.sort(desc_nulls_last("salary"))      # Largest first, NULLs last
df_sorted = df.sort(asc_nulls_first("salary"))      # Smallest first, NULLs first
```

**NULL ordering options:**

| Function | Behavior |
|----------|----------|
| `asc()` | Ascending; NULLs first (default) |
| `desc()` | Descending; NULLs last (default) |
| `asc_nulls_first()` | Ascending; NULLs first |
| `asc_nulls_last()` | Ascending; NULLs last |
| `desc_nulls_first()` | Descending; NULLs first |
| `desc_nulls_last()` | Descending; NULLs last |

**⚠️ Common mistake:**
```python
# WRONG - these conflict!
df.sort(desc("value"), asc_nulls_first("error"))  # Conflicting: desc defaults to NULLs last, but then you say NULLs first

# RIGHT - be explicit about NULL placement
df.sort(desc_nulls_last("value"), asc_nulls_first("error"))
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

### Sample, Limit, and Get Single Rows

**Action Methods (trigger execution):**

```python
# .first() — get first row (ACTION)
first_row = df.first()  # Returns single Row object
print(first_row)

# .take(n) — get first n rows (ACTION)
first_5 = df.take(5)  # Returns list of 5 Row objects

# .head(n) — same as .take(n)
first_3 = df.head(3)
```

**Transformation Methods (lazy):**

```python
# .limit(n) — limit rows (TRANSFORMATION)
df_limited = df.limit(100)  # Lazy; doesn't execute yet

# .sample(withReplacement, fraction, seed=None) — random sample (TRANSFORMATION)
# Returns approximately fraction * total rows
df_sampled = df.sample(False, 0.5)  # 50% of rows, no duplicates
df_sampled_dup = df.sample(True, 0.5)  # 50% of rows, may have duplicates

# With seed for reproducibility
df_sample_seed = df.sample(False, 0.1, seed=42)  # Always same 10%
```

| Method | Type | Returns | Use Case |
|--------|------|---------|----------|
| `.first()` | Action | Single Row | Get one row (e.g., check first value) |
| `.take(n)` | Action | List[Row] | Get first n rows for inspection |
| `.head(n)` | Action | List[Row] | Alias for .take(n) |
| `.limit(n)` | Transformation | DataFrame | Limit rows before write/collect |
| `.sample(with_repl, fraction)` | Transformation | DataFrame | Random sample (~50%, 10%, etc.) |

**⚠️ Key Differences:**

- `.first()` and `.take()` are **ACTIONS** — they execute the job immediately!
- `.limit()` is **LAZY** — it doesn't execute until you call an action
- `.sample(True, 0.5)` = **with replacement** (can have duplicates, faster)
- `.sample(False, 0.5)` = **without replacement** (all unique rows)

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
4. **Confusing `.limit()` (transformation) with `.take()` (action)** — `.limit()` is lazy, `.take()` executes immediately
5. **Using `.sample(False, fraction)` when duplicates are acceptable** — For speed, use `.sample(True, fraction)` to allow duplicates (30% faster typically)

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

**Why not iterrows() or pandas?** The pandas/iterrows approach won't work in Spark because:
- `iterrows()` is **NOT distributed** — it runs on the driver only, loads all data into memory
- `foreach()` with accumulators is the **distributed way** — each executor processes its partition and increments the counter

```python
from pyspark.sql.functions import col, when

# ✗ WRONG: iterrows() is slow and not distributed
counter = 0
for index, row in itemsDf.iterrows():  # Runs on driver only! Not parallel
    if 'Inc.' in row['supplier']:
        counter += 1

# ✓ CORRECT: foreach() + accumulator is distributed across executors
accum = sc.accumulator(0)

def check_if_inc_in_supplier(row):
    if 'Inc.' in row['supplier']:
        accum.add(1)

itemsDf.foreach(check_if_inc_in_supplier)  # Each executor processes its partition
print(accum.value)  # Get the total from driver
```

```python
# Create accumulator for counting errors
error_count = sc.accumulator(0)

# Function that updates accumulator
def count_errors(row):
    if row.age < 0 or row.age > 150:
        error_count.add(1)
    return row

# Read data
df = spark.read.csv("/data/data.csv", header=True)

# Process each row (incrementing accumulator) — DISTRIBUTED
df.rdd.map(count_errors).collect()

# Get total error count from driver
print(f"Total errors: {error_count.value}")
```

### On Databricks

- **Broadcast variables**: Use for lookup tables, ML models, configuration
- **Accumulators**: Use for counting errors, events, metrics
- **DataFrame API prefers native operations**: Use `.groupBy()` instead of accumulators when possible
- **Accumulator reliability**: Only **successful task attempts** contribute to accumulators. Failed attempts (retried tasks) don't increment the counter. This ensures accurate counts despite executor failures.

### Common Mistakes

1. **Modifying broadcast variable** — It's immutable; you can't update it
2. **Accessing accumulator in wrong context** — Executors can add; only driver can read `.value`
3. **Using accumulator for filtering** — Use `.filter()` instead; accumulators are for monitoring
4. **Expecting accumulators to count failed attempts** — Spark retries failed tasks; only the final successful attempt increments the accumulator. You won't see duplicate counts from retries.

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
