# Delta Lake — Schema Enforcement & Evolution: Key Learnings

A clean reference of concepts, patterns, and gotchas from completing the Delta Lake Schema Enforcement exercises in Databricks.

---

## Reading vs Writing: `spark.table()` vs `DeltaTable.forName()`

These two APIs serve fundamentally different purposes and are often confused.

| API | Returns | Use For |
|-----|---------|---------|
| `spark.table("catalog.schema.table")` | `DataFrame` | Read-only — transforms, filters, display |
| `DeltaTable.forName(spark, "catalog.schema.table")` | `DeltaTable` | Write operations — merge, update, delete, optimize, vacuum |

```python
# READ — use spark.table()
df = spark.table(f"{CATALOG}.{SCHEMA}.orders")

# WRITE — use DeltaTable.forName()
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.orders")
dt.update(...)
dt.delete(...)
dt.merge(...)
```

> **Rule of thumb**: If you are transforming or querying data → `spark.table()`.
> If you are mutating the Delta table → `DeltaTable.forName()`.

---

## Appending Data: `saveAsTable()` vs `insertInto()`

Both methods append data to a Delta table but behave differently when the table does not exist.

| Method | Table Exists | Table Missing | Type Matching |
|--------|-------------|---------------|---------------|
| `.write.mode("append").saveAsTable("name")` | Appends ✅ | Creates table ✅ | Lenient — some implicit casts |
| `.write.insertInto("name")` | Appends ✅ | **Fails** ❌ | Strict — enforces target schema |

```python
# saveAsTable — creates table if missing
sourceDF.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.orders")

# insertInto — semantically cleaner for inserting into an existing table
sourceDF.write.insertInto(f"{CATALOG}.{SCHEMA}.orders")
```

**Production preference**: Use `insertInto()` for inserting into known existing tables.
The stricter behaviour forces intentional type handling and makes pipelines auditable.

---

## Handling Column Type Mismatches

Delta Lake enforces the **target schema** at write time. When source and target column types differ,
Delta attempts a safe implicit cast. If the cast is unsafe (e.g., `Double → Integer`), the write fails.

```python
from pyspark.sql.functions import col

# Source: amount is StringType
# Target: amount is DoubleType
# Always apply explicit cast — never rely on implicit behaviour in production

castedDF = sourceDF.withColumn("amount", col("amount").cast("double"))
castedDF.write.insertInto(f"{CATALOG}.{SCHEMA}.schema_ex5_target")
```

### Safe vs Unsafe Implicit Casts

| Source → Target | Behaviour |
|-----------------|-----------|
| `String → Double` | ✅ Implicit cast succeeds |
| `String → Integer` | ✅ Implicit cast succeeds (if valid number) |
| `Double → Decimal(10,2)` | ✅ Implicit cast succeeds |
| `Long → Integer` | ⚠️ May truncate — data loss risk |
| `Double → Integer` | ❌ Fails — precision loss not allowed |

**Best practice**: Always apply explicit `.cast()` even when implicit casting works.
It makes intent clear and prevents silent null generation on bad data.

---

## Schema Evolution on Append: `mergeSchema`

By default, Delta Lake **rejects writes where the source has extra columns** not present in the target.
Use the `mergeSchema` option to allow the target to gain new columns during an append.

```python
# Without mergeSchema → AnalysisException if source has extra columns
# With mergeSchema → target gains new columns, existing rows get NULL for them

sourceDF.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.schema_ex3_target")
```

Existing rows that were not part of the append will have `NULL` for the newly added columns.

---

## Overwriting Schema Entirely: `overwriteSchema`

A normal `mode("overwrite")` preserves the existing target schema and rejects data with a different structure.
To replace the schema entirely, use the `overwriteSchema` option.

```python
# Replaces both data AND schema of the target table
sourceDF.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.schema_ex4_target")
```

> ⚠️ This is a destructive operation — the old schema and all existing data are gone.
> Use only when a breaking schema change is intentional.

---

## Schema Evolution Through MERGE

MERGE blocks schema changes by default. Two approaches exist to allow new columns from
the source to be added to the target during a merge operation.

### Approach 1 — SQL: `MERGE WITH SCHEMA EVOLUTION` ✅ Preferred

Scoped to a single statement — no session-level side effects.

```sql
MERGE WITH SCHEMA EVOLUTION INTO catalog.schema.target AS target
USING catalog.schema.source AS source
  ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
```

```python
# In a Databricks notebook
spark.sql(f"""
    MERGE WITH SCHEMA EVOLUTION INTO {CATALOG}.{SCHEMA}.schema_ex10_target AS target
    USING {CATALOG}.{SCHEMA}.schema_ex10_source AS source
      ON target.order_id = source.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Approach 2 — PySpark: `spark.conf.set()` ⚠️ Use with caution

Applies to the **entire Spark session** — all subsequent merges in the session will also allow
schema changes unless the config is explicitly reset.

```python
from delta.tables import DeltaTable

# Enable — affects all merges in this session
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

targetDT = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.schema_ex10_target")
sourceDF = spark.read.table(f"{CATALOG}.{SCHEMA}.schema_ex10_source")

targetDT.alias("target") \
    .merge(sourceDF.alias("source"), "target.order_id = source.order_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Always reset after — prevent config bleed to other operations
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
```

### Behaviour of Untouched Rows After Schema-Evolving MERGE

Rows in the target that are **not matched** by the MERGE are not deleted or modified.
They receive `NULL` for any newly added columns — consistent with Delta Lake's append behaviour.

---

## MERGE Clause Ordering

The order of `WHEN MATCHED` clauses is critical. Delta Lake evaluates them top-to-bottom
and applies the **first matching clause only**.

```
Always order WHEN MATCHED clauses:
MOST SPECIFIC (with condition) → LEAST SPECIFIC (no condition)

.whenMatchedDelete(condition="status = 'cancelled'")   ← specific condition first
.whenMatchedUpdate(condition="amount > 100")            ← specific condition second
.whenMatchedUpdateAll()                                 ← no condition — always last
```

```python
targetDT.alias("t") \
    .merge(sourceDF.alias("s"), "t.order_id = s.order_id") \
    .whenMatchedDelete(condition="s.status = 'cancelled'") \
    .whenMatchedUpdate(condition="s.amount > 100", set={"status": "'vip'"}) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

> If `whenMatchedUpdateAll()` is placed first, it catches all matches and the specific
> conditions below it are never evaluated.

---

## Delta Constraints

Delta Lake supports two types of constraints that enforce data quality at write time.

### NOT NULL Constraint

```sql
-- Add: prevents NULL values in the column
ALTER TABLE catalog.schema.orders ALTER COLUMN status SET NOT NULL;

-- Drop: restores nullable behaviour
ALTER TABLE catalog.schema.orders ALTER COLUMN status DROP NOT NULL;
```

### CHECK Constraint

```sql
-- Add: named constraint with a SQL expression
ALTER TABLE catalog.schema.orders ADD CONSTRAINT positive_amount CHECK (amount > 0);

-- Drop: by constraint name
ALTER TABLE catalog.schema.orders DROP CONSTRAINT positive_amount;
```

```python
# Same using spark.sql()
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex7_orders
    ADD CONSTRAINT valid_amount CHECK (amount > 0)
""")
```

### Discovering Existing Constraints

Delta stores CHECK constraints as table properties with the prefix `delta.constraints.`.

```python
props = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{SCHEMA}.schema_ex9_orders")
constraints = props.filter("key LIKE 'delta.constraints.%'") \
    .withColumnRenamed("key", "constraint_name") \
    .withColumnRenamed("value", "constraint_expression")
display(constraints)
```

### Constraint-Safe Writes — Pre-filter Before Writing

Delta rejects the **entire write** if any single row violates a constraint.
The correct production pattern is to filter out invalid rows before the write, not catch the error.

```python
# Filter out rows that would violate constraints BEFORE writing
validDF = sourceDF.filter("status IS NOT NULL AND amount > 0")
validDF.write.insertInto(f"{CATALOG}.{SCHEMA}.schema_ex11_orders")
```

For MERGE, apply the filter inside the `USING` clause:

```sql
MERGE INTO catalog.schema.target AS target
USING (
    SELECT * FROM catalog.schema.source
    WHERE status IS NOT NULL AND amount > 0   -- pre-filter here
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

## Join Syntax and Duplicate Column Behaviour

The join syntax used affects whether duplicate key columns appear in the result.

| Syntax | Duplicate join key columns? |
|--------|-----------------------------|
| `on="column_name"` (string) | No — PySpark deduplicates ✅ |
| `on=["col1", "col2"]` (list of strings) | No — PySpark deduplicates ✅ |
| `on=F.col("a.x") == F.col("b.x")` | Yes — both columns kept ⚠️ |
| `on=df1.x == df2.x` | Yes — both columns kept ⚠️ |

**Recommendation**: Use string or list-of-strings join syntax to avoid ambiguous column references
in downstream operations.

---

## `filter()` vs `where()`

Both `filter()` and `where()` are identical in PySpark — they are aliases for the same operation.

```python
# These are equivalent
df.filter("amount > 100")
df.where("amount > 100")

df.filter(col("amount") > 100)
df.where(col("amount") > 100)
```

Use whichever reads more naturally in context. `where()` often reads better in SQL-like chains;
`filter()` is more common in transformation-heavy PySpark code.

---

## Unity Catalog — Managed Table Format Restrictions

Unity Catalog enforces stricter rules on managed table formats compared to the legacy Hive Metastore.

| Table Type | Supported Formats |
|------------|-------------------|
| UC Managed Table | Delta Lake, Apache Iceberg **only** |
| UC External Table | Delta (recommended), Parquet, CSV, JSON, Avro, ORC, Text |
| Hive Metastore Managed | Delta + most formats (no governance) |

The restriction exists because Unity Catalog's governance model (ACID, time travel, schema enforcement,
audit logging) depends on capabilities that only Delta Lake and Iceberg provide.


## OPTIMIZE — Bin-Packing (File Compaction)
**Problem it solves:** The small file problem. Every time Auto Loader, streaming, or frequent small writes run, they create many tiny files (2KB, 5KB). Spark has to open each file individually — this overhead alone can dominate query time.

OPTIMIZE merges all those tiny files into fewer, optimally-sized files (~128MB each)

## OPTIMIZE with ZORDER — Data Co-location
Problem it solves: Even after bin-packing, if your query filters on city = 'Mumbai', Spark still has to read ALL files because Mumbai rows could be in any of them. Z-ORDER physically sorts and co-locates rows with the same column value together in the same files.

## Pain Points of Optimize and Z-Order

**PROBLEM 1 — Partitioning is rigid and unforgiving**
Table partitioned by order_date (daily)
-  3 years of data = 1,095 partition folders on ADLS
-  Query by customer_id? Partitioning helps ZERO — full scan
-  Want to change partition key? Rewrite the ENTIRE table ❌

**PROBLEM 2 — ZORDER is expensive and non-incremental**
OPTIMIZE ZORDER BY city
- Rewrites ALL files every time, even unchanged ones
- 1 TB table = 1 TB rewrite just to cluster 10 GB of new data
- Blocks queries during full rewrite ❌
- Changing the column? Rewrite the whole table again ❌

**PROBLEM 3 — Query patterns change, layouts cannot**
Last month: mostly filtered by city
This month: mostly filtered by customer_id
→ You are stuck with the old layout or face full rewrite ❌

## **LIQUID CLUSTERING**## 
Liquid Clustering was introduced to solve all three problems.

Technically: Liquid Clustering is an incremental, cursor-based clustering mechanism. Each OPTIMIZE run only clusters the data that needs it — new or unclustered files — not the entire table. Delta's transaction log remembers which files are already clustered. Future OPTIMIZE runs only process new/modified files.


## SQL (cleanest syntax, most common in interviews)## 
CREATE TABLE catalog.schema.orders (
    order_id    STRING,
    customer_id STRING,
    city        STRING,
    amount      DOUBLE,
    order_date  DATE
)
CLUSTER BY (city);               -- ← Liquid Clustering key

**Note** - Actual Optimization happens only when you run Optimize on the table.

- we can use Cluster BY (Auto) which will automatically clusters the data based on the queries ran on the workload.
- we can run the liquid clustering on an existing table as using `spark.sql(f"ALTER TABLE <TABLE-NAME> CLUSTER BY (COL-NAME)")`

### Example of Liquid clustering
#####  assume initially the orders table was clustered on city column.

 Step 1: Change the key — THIS IS METADATA ONLY
 Zero bytes read. Zero bytes written. Instant. ✅
spark.sql("ALTER TABLE orders CLUSTER BY (restaurant_id)")

 What happens internally:
 → Delta transaction log is updated with new clustering key
→ NO files are touched
 → Table stays fully queryable during this operation
 → Takes milliseconds, not hours ✅

 Step 2: Run OPTIMIZE — clusters ONLY new/unclustered files
spark.sql("OPTIMIZE orders")

 What happens internally:
 → Only files written AFTER the ALTER are clustered by restaurant_id
 → Old files still clustered by city (still valid — queries still work)
 → Over time, as new writes come in + periodic OPTIMIZE runs,
   the table gradually re-clusters itself by restaurant_id
 → No big-bang rewrite ✅

 Optional Step 3: Force full recluster if you need it NOW
spark.sql("OPTIMIZE orders FULL")
 → Reclusters ALL files by restaurant_id
 → Still more efficient than ZORDER because Delta tracks
   which files are already clustered and skips them in future runs