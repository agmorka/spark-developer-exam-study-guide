# Section 7: Using Pandas API on Spark


---

## Table of Contents

1. [Objective 1: Explain the advantages of using Pandas API on Spark](#objective-1-explain-the-advantages-of-using-pandas-api-on-spark)
2. [Objective 2: Create and invoke Pandas UDFs](#objective-2-create-and-invoke-pandas-udfs)
3. [Summary](#summary-section-7-key-takeaways)

---

## Objective 1: Explain the advantages of using Pandas API on Spark

### Core Concept
**Pandas API on Spark** lets you write code that looks exactly like Pandas, but it runs on Spark clusters. If you know Pandas, you can now use the same syntax on distributed data.

### Pandas vs. Pandas API on Spark

| Tool | Data Size | API Style | Performance | Ecosystem |
|------|-----------|-----------|-------------|-----------|
| **Pandas** | Single machine | DataFrame.groupby() | Fast (no serialization) | Data science libraries (scikit-learn, matplotlib) |
| **Pandas API on Spark** | Distributed cluster | Same as Pandas | Slower (serialization) | Partial (some libraries work) |
| **Spark SQL** | Distributed cluster | SQL / DataFrame API | Very fast (optimized) | Spark ecosystem |

### Code Example: Same Syntax, Different Backends

```python
# Traditional Pandas (in-memory, single machine)
import pandas as pd
pdf = pd.read_csv("file.csv")
result = pdf[pdf.age > 30].groupby("department")["salary"].mean()
print(result)

# Pandas API on Spark (distributed, cluster)
import pyspark.pandas as ps
psdf = ps.read_csv("file.csv")
result = psdf[psdf.age > 30].groupby("department")["salary"].mean()
print(result)
# Syntax is IDENTICAL; runs on Spark cluster
```

### Advantages of Pandas API on Spark

#### 1. Familiar Syntax for Pandas Users

```python
# Pandas users can use same commands on Spark
psdf = ps.read_csv("data.csv")

# Familiar operations
psdf["age"].mean()
psdf[psdf.age > 30]
psdf.groupby("department")["salary"].sum()
```

#### 2. Seamless Transition from Pandas to Spark

```python
# Start with Pandas on small data
import pandas as pd
pdf = pd.read_csv("small_file.csv")
result = pdf[pdf.age > 30].groupby("dept")["salary"].mean()

# Switch to Spark on large data (code is almost the same)
import pyspark.pandas as ps
psdf = ps.read_csv("large_file.csv")
result = psdf[psdf.age > 30].groupby("dept")["salary"].mean()
```

#### 3. Broader Compatibility

Pandas API on Spark supports more Pandas methods than native Spark SQL:
- `.apply()` for row-wise functions
- `.applymap()` for element-wise functions
- `.rolling()` for rolling windows
- Better handling of complex operations

```python
# Apply a custom function to each row
psdf["new_col"] = psdf.apply(lambda row: row["a"] + row["b"], axis=1)

# Rolling window
psdf["rolling_mean"] = psdf["value"].rolling(window=3).mean()
```

#### 4. Easier Onboarding

Data scientists who know Pandas don't need to learn Spark SQL.

### Disadvantages of Pandas API on Spark

1. **Slower than Spark SQL** — Pandas operations serialize data; Spark SQL is optimized
2. **Not all Pandas functions work** — Some are missing or behave differently
3. **Less efficient** — Each operation may trigger a shuffle

### When to Use Pandas API on Spark

| Scenario | Use Pandas API on Spark | Use Spark SQL | Use Pandas |
|----------|---|---|---|
| Pandas developer on large data | ✓ | | |
| Complex numerical operations | ✓ | | |
| Small data (< 1GB) | | | ✓ |
| Performance-critical ETL | | ✓ | |
| ML pipeline (scikit-learn compat) | ✓ | | |

### On Databricks

- **Pandas API on Spark is supported** — Pre-installed in Databricks
- **Good for interactive exploration** — Less friction for Pandas users
- **Recommend Spark SQL for production** — Better performance

### Common Mistakes

1. **Mixing Pandas and Pandas API on Spark** — Causes serialization overhead; keep data in one form
2. **Expecting identical performance** — Pandas API on Spark is slower due to serialization
3. **Using `.collect()` on large psdf** — Tries to load all data into driver memory

### Code Example: Pandas API on Spark

```python
import pyspark.pandas as ps
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PandasAPIExample").getOrCreate()

# Read as Pandas API on Spark DataFrame
psdf = ps.read_csv("/mnt/data/employees.csv")

# Familiar Pandas operations
print("Data shape:", psdf.shape)
print("Columns:", psdf.columns.tolist())

# Filtering
high_earners = psdf[psdf.salary > 100000]

# GroupBy aggregation
dept_avg = psdf.groupby("department")["salary"].mean()
print(dept_avg)

# Create new column
psdf["bonus"] = psdf["salary"] * 0.1

# Convert back to Spark DataFrame if needed
spark_df = psdf.to_spark()
spark_df.write.mode("overwrite").parquet("/output/bonuses")

# Convert to Pandas (only if data fits in memory)
pandas_df = psdf.to_pandas()
print(type(pandas_df))  # <class 'pandas.core.frame.DataFrame'>
```

## Objective 2: Create and invoke Pandas UDFs

### Core Concept
**Pandas UDFs** are user-defined functions that work on batches of data (Pandas Series/DataFrames) instead of single rows. They're much faster than row-at-a-time Python UDFs because they leverage Apache Arrow serialization.

### Python UDF vs. Pandas UDF

| Aspect | Python UDF | Pandas UDF |
|--------|-----------|-----------|
| **Input/Output** | Single row | Batch (Pandas Series) |
| **Speed** | Slow (serialize each row) | Fast (Arrow batches) |
| **Use case** | Complex logic only | Preferred when possible |

### Comparison: Speed

```python
# Python UDF: 1M rows = 1M function calls
@udf(IntegerType())
def python_udf(x):
    return x * 2
# Result: very slow

# Pandas UDF: 1M rows = few Arrow batches
@pandas_udf(IntegerType())
def pandas_udf(s):
    return s * 2
# Result: 10x+ faster
```

### Creating Pandas UDFs

#### Series → Series

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

# Takes a Series, returns a Series
@pandas_udf(IntegerType())
def double_values(s: pd.Series) -> pd.Series:
    return s * 2

# Use in DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
df_result = df.select(double_values("value").alias("doubled"))
df_result.show()
```

#### Iterator of Series → Iterator of Series

For very large batches, process one batch at a time:

```python
from typing import Iterator

@pandas_udf(IntegerType())
def process_batch(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for s in iterator:
        yield s * 2

# Same usage
df_result = df.select(process_batch("value").alias("doubled"))
```

#### Grouped Series (GroupedAgg)

Apply a function to each group:

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql import functions as F

@pandas_udf(DoubleType())
def mean_udf(s: pd.Series) -> float:
    return s.mean()

# Use in groupBy agg
result = df.groupby("category").agg(mean_udf("value").alias("avg_value"))
```

#### Multiple Arguments

```python
@pandas_udf(IntegerType())
def add_with_pandas(s1: pd.Series, s2: pd.Series) -> pd.Series:
    return s1 + s2

# Use
df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
df_result = df.select(add_with_pandas("a", "b").alias("sum"))
df_result.show()
```

### Return Types for Pandas UDFs

```python
from pyspark.sql.types import DoubleType, ArrayType, StringType

# Scalar function (Series → Series)
@pandas_udf(DoubleType())
def double(s: pd.Series) -> pd.Series:
    return s * 2

# Groupby aggregation (Series → scalar)
@pandas_udf(DoubleType())
def mean_value(s: pd.Series) -> float:
    return s.mean()

# Array output
@pandas_udf(ArrayType(StringType()))
def split_string(s: pd.Series) -> pd.Series:
    return s.str.split(",")
```

### On Databricks

- **Pandas UDFs are recommended** — Much faster than Python UDFs
- **Arrow serialization** — Handles Pandas Series efficiently
- **Better for ML inference** — Load a model, apply to batches

### Common Mistakes

1. **Confusing Pandas UDF with Python UDF** — Pandas UDF is decorator `@pandas_udf`; Python UDF is `@udf`
2. **Returning wrong type** — Pandas UDF must return Pandas Series, not single values (unless groupby agg)
3. **Processing row-by-row in Pandas UDF** — Defeats the purpose; use the vectorized operation

### Code Example: Pandas UDFs

```python
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType, StringType, IntegerType

spark = SparkSession.builder.appName("PandasUDFExample").getOrCreate()

# 1. SCALAR PANDAS UDF (Series → Series)
@pandas_udf(DoubleType())
def apply_discount(prices: pd.Series) -> pd.Series:
    return prices * 0.9  # 10% discount

# 2. GROUPBY AGGREGATION PANDAS UDF (Series → scalar)
@pandas_udf(DoubleType())
def mean_price(prices: pd.Series) -> float:
    return prices.mean()

# 3. MULTIPLE ARGUMENTS PANDAS UDF
@pandas_udf(DoubleType())
def add_tax(price: pd.Series, tax_rate: pd.Series) -> pd.Series:
    return price * (1 + tax_rate)

# Create DataFrame
df = spark.createDataFrame([
    (1, "Electronics", 100.0),
    (2, "Electronics", 200.0),
    (3, "Clothing", 50.0),
    (4, "Clothing", 75.0),
], ["id", "category", "price"])

print("=== Original Data ===")
df.show()

# 1. Apply discount
print("=== With Discount (Pandas UDF) ===")
df_discounted = df.select("id", "category", apply_discount("price").alias("discounted_price"))
df_discounted.show()

# 2. GroupBy with Pandas UDF aggregation
print("=== Mean Price by Category ===")
df_avg = df.groupby("category").agg(mean_price("price").alias("avg_price"))
df_avg.show()

# 3. Multiple arguments
print("=== Add Tax (10%) ===")
df_with_tax = df.select(
    "id",
    "category",
    "price",
    add_tax("price", col(lit(0.1))).alias("price_with_tax")  # Note: this example uses col() and lit()
)
# Actually, for constant tax, this is cleaner:
df_with_tax = df.select(
    "id",
    "category",
    "price",
    (col("price") * 1.1).alias("price_with_tax")
)
df_with_tax.show()
```

### Arrow and Serialization

Behind the scenes, Pandas UDFs use **Apache Arrow** for fast serialization:

```
Spark Series
    ↓
Arrow format (efficient, language-independent)
    ↓
Python Pandas Series
    ↓
Apply function
    ↓
Python Pandas Series
    ↓
Arrow format
    ↓
Spark Series
```

Arrow avoids Python pickle serialization, which is slow.

## Summary: Section 7 Key Takeaways

1. **Pandas API on Spark**: Write Pandas-like code; runs on Spark cluster
2. **Advantages**: Familiar syntax, easier for Pandas users, broad compatibility
3. **Disadvantages**: Slower than Spark SQL; not all Pandas functions supported
4. **Use when**: Pandas developer on large data; complex operations; exploratory work
5. **Pandas UDFs**: Process data in batches; 10x+ faster than row-at-a-time Python UDFs
6. **Arrow serialization**: Efficient batching; key performance advantage
7. **Pandas UDF types**: Scalar (Series → Series), GroupBy agg (Series → scalar), multi-arg
8. **Performance**: Always prefer Pandas UDF over Python UDF when possible

---

## Navigation

[← Back to Main Index](00_Spark_Developer_Study_Guide.md) | [Previous: Section 6 - Spark Connect](06_Spark_Connect.md)
- ✅ Be ready to identify problems from code snippets (common exam format)

Good luck! 🎉
