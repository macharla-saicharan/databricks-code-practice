# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # MERGE Operations
# MAGIC **Topic**: Delta Lake | **Exercises**: 9 | **Total Time**: ~90 min
# MAGIC
# MAGIC Practice Delta Lake MERGE INTO across multiple patterns: basic upserts, insert-only,
# MAGIC update-only, dedup, conditional updates, deletes, multi-condition, SCD Type 2,
# MAGIC and schema evolution.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/merge-operations-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC - `db_code.delta_lake.customers` - customer dimension
# MAGIC
# MAGIC **Schema** (`orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Primary key |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | amount | DOUBLE | Order total in USD (includes $0) |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | Order placement date |
# MAGIC | updated_at | TIMESTAMP | Last modification time |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/merge-operations-setup

# COMMAND ----------

# MAGIC %md
# MAGIC **Setup complete.** Exercise tables are in `{CATALOG}.{SCHEMA}` (merge_operations schema).
# MAGIC Base tables (orders, customers) are in `{CATALOG}.{BASE_SCHEMA}` (delta_lake schema).
# MAGIC Each exercise has its own `_target` + `_source` pair. You MERGE source into target.
# MAGIC - Ex 1-3 (easy): `merge_ex{1-3}_target` + `_source` - basic upsert, insert-only, update-only
# MAGIC - Ex 4 (medium): `merge_ex4_target` + `_source` - source has **duplicate** order_ids (must dedup)
# MAGIC - Ex 5 (medium): `merge_ex5_target` + `_source` - source has mixed timestamps (newer + older)
# MAGIC - Ex 6 (medium): `merge_ex6_target` + `_source` - source has a cancelled order (for DELETE)
# MAGIC - Ex 7 (hard): `merge_ex7_target` + `_source` - all four scenarios: delete + update + skip + insert
# MAGIC - Ex 8 (hard): `merge_ex8_target` + `_source` - SCD Type 2 **customer dimension** (different schema)
# MAGIC - Ex 9 (hard): `merge_ex9_target` + `_source` - source has extra `discount_pct` column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic Upsert
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC MERGE `merge_ex1_source` into `merge_ex1_target` to upsert orders.
# MAGIC Update existing orders with new values and insert orders that don't exist yet.
# MAGIC
# MAGIC **Source** (`merge_ex1_source`): 4 rows - ORD-001 (amount=109.50), ORD-002 (amount=175.00), ORD-101 (new), ORD-102 (new)
# MAGIC
# MAGIC **Target** (`merge_ex1_target`): 5 rows - ORD-001 through ORD-005 (from base orders)
# MAGIC
# MAGIC **Expected Output**: `merge_ex1_target` should have 7 rows. ORD-001 amount = 109.50. ORD-101 and ORD-102 exist.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target matching on `order_id`
# MAGIC 2. When matched: update all columns
# MAGIC 3. When not matched: insert all columns

# COMMAND ----------

# EXERCISE_KEY: merge_ex1
# TODO: Write your MERGE INTO statement

# Your code here
sourcedf = spark.read.table("db_code.merge_operations.merge_ex1_source")
display(sourcedf)

targetdf = spark.read.table("db_code.merge_operations.merge_ex1_target")
display(targetdf)


spark.sql("""
          merge into db_code.merge_operations.merge_ex1_target AS t 
          using db_code.merge_operations.merge_ex1_source AS s 
          on t.order_id = s.order_id
          when matched then update set *
          when not matched by target then insert *
          """)



# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex1_target")

assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
assert result.filter("order_id = 'ORD-102'").count() == 1, "ORD-102 should be inserted"
assert result.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 109.50, \
    "ORD-001 amount should be updated to 109.50"

print("Exercise 1 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Insert-Only Merge
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC MERGE `merge_ex2_source` into `merge_ex2_target`, but only insert new records.
# MAGIC Existing orders should NOT be updated, even if the source has different values.
# MAGIC
# MAGIC **Source** (`merge_ex2_source`): same 4 rows as Exercise 1 (2 existing + 2 new)
# MAGIC
# MAGIC **Target** (`merge_ex2_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 7 rows. ORD-001 amount unchanged. ORD-101 and ORD-102 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target matching on `order_id`
# MAGIC 2. Only insert rows that don't exist in target
# MAGIC 3. Do NOT update any existing records

# COMMAND ----------

# EXERCISE_KEY: merge_ex2
# TODO: Write your MERGE INTO statement

# Your code here
from delta.tables import DeltaTable
# we are using DeltaTable.forName() only for target table , the reason is we want to modify the taarget table only and .merge(), .update,delete, optimize,vaccum,restore work on a deltaTable object only. 
# if you also use the deltatable.forName() for source table also then it will also be a delta object but the .merge() function expects a dataframe not a delta table.

target = DeltaTable.forName(spark,"db_code.merge_operations.merge_ex2_target")

source = spark.table("db_code.merge_operations.merge_ex2_source")

target.alias("t")\
.merge(source.alias("s"), "t.order_id == s.order_id")\
.whenNotMatchedInsertAll()\
.execute() # with out this keyword the execution won't happen, it was a lazy transformation.

# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex2_target")

assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
# ORD-001 should NOT be updated (insert-only merge)
ord001_status = result.filter("order_id = 'ORD-001'").select("status").collect()[0][0]
assert ord001_status != "shipped", \
    f"ORD-001 should not be updated in insert-only merge, but status changed to '{ord001_status}'"

print("Exercise 2 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Update-Only Merge
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC MERGE `merge_ex3_source` into `merge_ex3_target`, but only update existing records.
# MAGIC New records in the source should be ignored entirely.
# MAGIC
# MAGIC **Source** (`merge_ex3_source`): 3 rows - ORD-001, ORD-002, ORD-003 with status='shipped' and amount+10
# MAGIC
# MAGIC **Target** (`merge_ex3_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: still 5 rows. ORD-001, ORD-002, ORD-003 have status='shipped'. ORD-004, ORD-005 unchanged.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target matching on `order_id`
# MAGIC 2. Only update rows that exist in both
# MAGIC 3. Do NOT insert any new records

# COMMAND ----------

# EXERCISE_KEY: merge_ex3
# TODO: Write your MERGE INTO statement

# Your code here
from delta.tables import DeltaTable

target = DeltaTable.forName(spark,"db_code.merge_operations.merge_ex3_target")

source = spark.read.table("db_code.merge_operations.merge_ex3_source")

(
    target.alias("t")\
        .merge(source.alias("s"),"t.order_id == s.order_id")\
            .whenMatchedUpdateAll()
            .execute()
)

# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex3_target")

assert result.count() == 5, f"Expected 5 rows (no inserts), got {result.count()}"
assert result.filter("status = 'shipped'").count() == 3, \
    "ORD-001, ORD-002, ORD-003 should all have status 'shipped'"
# ORD-004 and ORD-005 should be unchanged (not in source)
assert result.filter("order_id = 'ORD-004'").select("status").collect()[0][0] != "shipped", \
    "ORD-004 should not be modified (not in source)"

print("Exercise 3 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Deduplicate Before Merge
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC `merge_ex4_source` has duplicate `order_id` values. Deduplicate keeping the most
# MAGIC recent record per `order_id`, then MERGE into target.
# MAGIC
# MAGIC **Source** (`merge_ex4_source`): 3 rows - ORD-001 appears TWICE (99.50 at 08:00, 119.50 at 12:00), ORD-101 once
# MAGIC
# MAGIC **Target** (`merge_ex4_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 6 rows. ORD-001 amount=119.50 (the later record). ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Deduplicate source keeping latest `updated_at` per `order_id`
# MAGIC 2. MERGE deduped result into target
# MAGIC 3. Update matched, insert not matched
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Direct MERGE without dedup will fail (duplicate keys in source)

# COMMAND ----------

# EXERCISE_KEY: merge_ex4
# TODO: Dedup the source, then MERGE

# Your code here
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql import functions as f
target = DeltaTable.forName(spark,"db_code.merge_operations.merge_ex4_target")

source = spark.read.table("db_code.merge_operations.merge_ex4_source")

windowSpec = Window.partitionBy("order_id").orderBy(f.desc("updated_at"))

sourceDeDup = source.withColumn("rn",f.row_number().over(windowSpec)).filter(f.col("rn") == 1).drop("rn")
# display(sourceDeDup)

(
    target.alias("t")
    .merge(sourceDeDup.alias("s"),"t.order_id == s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex4_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 119.50, \
    "ORD-001 should have amount 119.50 (the later duplicate)"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 4 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Conditional Merge - Only Update If Newer
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC MERGE `merge_ex5_source` into `merge_ex5_target`, but only update when the
# MAGIC source record is newer. Stale source records should be ignored. New records always inserted.
# MAGIC
# MAGIC **Source** (`merge_ex5_source`): 3 rows - ORD-001 (timestamp 2026-03-01, newer), ORD-002 (timestamp 2025-01-01, older), ORD-101 (new)
# MAGIC
# MAGIC **Target** (`merge_ex5_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 6 rows. ORD-001 updated (amount=109.50). ORD-002 unchanged. ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target on `order_id`
# MAGIC 2. Only update matched rows where source `updated_at` is newer than target
# MAGIC 3. Insert rows not in target
# MAGIC 4. Matched rows with older source: do nothing

# COMMAND ----------

# EXERCISE_KEY: merge_ex5
# TODO: Write your conditional MERGE

# Your code here

from delta.tables import DeltaTable

target = DeltaTable.forName(spark,"db_code.merge_operations.merge_ex5_target")
source = spark.read.table("db_code.merge_operations.merge_ex5_source")
# print(CATALOG)

# display(source)


# display(target)

(
    target.alias("t")\
    .merge(source.alias("s"),"t.order_id==s.order_id")
    .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")
    .whenNotMatchedInsertAll()
    .execute()
)



# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex5_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
# ORD-001 should be updated (source is newer)
assert result.filter("order_id = 'ORD-001'").select("amount").collect()[0][0] == 109.50, \
    "ORD-001 should be updated to 109.50 (newer source)"
# ORD-002 should NOT be updated (source is older)
ord002_status = result.filter("order_id = 'ORD-002'").select("status").collect()[0][0]
assert ord002_status != "returned", \
    f"ORD-002 should not be updated (source is older), but status changed to '{ord002_status}'"
# ORD-101 should be inserted
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 5 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: MERGE with DELETE Clause
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC MERGE `merge_ex6_source` into `merge_ex6_target`. Handle three cases:
# MAGIC delete cancelled orders, update other matched orders, insert new orders.
# MAGIC
# MAGIC **Source** (`merge_ex6_source`): 3 rows - ORD-001 (status='cancelled'), ORD-002 (status='shipped'), ORD-101 (new)
# MAGIC
# MAGIC **Target** (`merge_ex6_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 5 rows. ORD-001 deleted. ORD-002 status='shipped'. ORD-101 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target on `order_id`
# MAGIC 2. Delete cancelled orders from target
# MAGIC 3. Update other matched orders
# MAGIC 4. Insert new orders

# COMMAND ----------

# EXERCISE_KEY: merge_ex6
# TODO: Write your MERGE with a DELETE clause

# Your code here

from delta.tables import DeltaTable

target = DeltaTable.forName(spark,"db_code.merge_operations.merge_ex6_target")

targetDF = spark.read.table("db_code.merge_operations.merge_ex6_target")
source = spark.read.table("db_code.merge_operations.merge_ex6_source")


# display(source)

# display(targetDF)


(
    target.alias("t")
    .merge(source.alias("s"),"t.order_id == s.order_id")
    .whenMatchedDelete(condition="s.status == 'cancelled' ")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex6_target")

assert result.count() == 5, f"Expected 5 rows (5 - 1 deleted + 1 inserted), got {result.count()}"
assert result.filter("order_id = 'ORD-001'").count() == 0, "ORD-001 should be deleted (cancelled)"
assert result.filter("order_id = 'ORD-002'").select("status").collect()[0][0] == "shipped", \
    "ORD-002 should be updated to 'shipped'"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"

print("Exercise 6 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Multi-Condition MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE `merge_ex7_source` into `merge_ex7_target` handling all four scenarios
# MAGIC in a single statement: delete, conditional update, skip, and insert.
# MAGIC
# MAGIC **Source** (`merge_ex7_source`): 5 rows - ORD-001 (cancelled), ORD-002 (newer, amount=175), ORD-003 (older, stale), ORD-101 (new), ORD-102 (new)
# MAGIC
# MAGIC **Target** (`merge_ex7_target`): 5 rows - ORD-001 through ORD-005
# MAGIC
# MAGIC **Expected Output**: 6 rows. ORD-001 deleted. ORD-002 updated (amount=175). ORD-003 unchanged. ORD-101, ORD-102 inserted.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. MERGE source into target on `order_id`
# MAGIC 2. Cancelled orders: delete from target
# MAGIC 3. Newer source records: update target
# MAGIC 4. Older source records: skip (do nothing)
# MAGIC 5. New orders: insert

# COMMAND ----------

# EXERCISE_KEY: merge_ex7
# TODO: Write your solution here
from delta.tables import DeltaTable
# Your code here
source = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex7_source")
# display(sourceDF)

target = DeltaTable.forName(spark,f"{CATALOG}.{SCHEMA}.merge_ex7_target")

(
  target.alias("t")\
    .merge(source.alias("s"),"s.order_id = t.order_id")\
      .whenMatchedDelete(condition="s.status = 'cancelled' ")\
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")\
          .whenNotMatchedInsertAll()\
            .execute()
)

# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex7_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("order_id = 'ORD-001'").count() == 0, "ORD-001 should be deleted (cancelled)"
assert result.filter("order_id = 'ORD-002'").select("amount").collect()[0][0] == 175.00, \
    "ORD-002 should be updated to amount 175.00"
# ORD-003 should not be updated (source timestamp is older)
# Source has amount=50.00 and status='pending' for ORD-003 - verify these values were NOT applied
ord003 = result.filter("order_id = 'ORD-003'").collect()[0]
assert not (ord003.amount == 50.00 and ord003.status == "pending"), \
    "ORD-003 should not be updated (source timestamp is older than target)"
assert result.filter("order_id = 'ORD-101'").count() == 1, "ORD-101 should be inserted"
assert result.filter("order_id = 'ORD-102'").count() == 1, "ORD-102 should be inserted"

print("Exercise 7 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: SCD Type 2 with MERGE
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Implement Slowly Changing Dimension Type 2 on a customer dimension table.
# MAGIC When customer attributes change, expire the old record and insert a new current version.
# MAGIC
# MAGIC **Source** (`merge_ex8_source`): 3 rows
# MAGIC | customer_id | name | email | region | tier |
# MAGIC |-------------|------|-------|--------|------|
# MAGIC | CUST-001 | Alice Smith | alice.new@example.com | US-East | platinum |
# MAGIC | CUST-003 | Carol Lee | carol@example.com | US-West | silver |
# MAGIC | CUST-010 | New Customer | new@example.com | EU-West | bronze |
# MAGIC
# MAGIC **Target** (`merge_ex8_target`): 3 current customer records
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | customer_id | STRING | Business key |
# MAGIC | name, email, region, tier | STRING | Attributes (may change) |
# MAGIC | is_current | BOOLEAN | true for active version |
# MAGIC | effective_start_date | DATE | When this version became active |
# MAGIC | effective_end_date | DATE | 9999-12-31 for current records |
# MAGIC
# MAGIC **Expected Output**: 6 rows.
# MAGIC CUST-001: 2 rows (old expired + new current with tier='platinum').
# MAGIC CUST-002: 1 row (unchanged). CUST-003: 2 rows (old expired + new current).
# MAGIC CUST-010: 1 row (new).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Expire old records ONLY for customers whose attributes actually changed (is_current=false, effective_end_date=today)
# MAGIC 2. Insert new current version for changed customers
# MAGIC 3. Insert brand new customers with is_current=true
# MAGIC 4. Leave customers not in source unchanged
# MAGIC 5. The pipeline must be idempotent: running twice with the same source does nothing on the second run
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Canonical approach is a single MERGE with a "staging union" subquery (one source row emits one expire action and one insert action). It is atomic and idempotent.
# MAGIC - A two-statement approach (MERGE to expire + INSERT new versions) is also valid but not atomic - a reader between the two statements sees a customer with zero current rows.
# MAGIC - Use null-safe comparison (`<=>` or `IS DISTINCT FROM`) when detecting changes; plain `<>` silently misses NULL -> value transitions.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col,to_date,lit,current_date


sourceDF = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex8_source")
target = DeltaTable.forName(spark,f"{CATALOG}.{SCHEMA}.merge_ex8_target")
# convert the delta table to a DF
targetDF = target.toDF()

# display(sourceDF)
# display(targetDF)


sourceColList = sourceDF.columns

print(sourceColList)

change_condition = None

# eqNullSafe helps us to return either true/false when compared values of type NULL
# without this when we compare values with Null data it will return Null eg Null == 'sai' -> Null, but with eqNullSafe it returns false.

for colName in sourceColList:
    col_changed = ~(col(f"s.{colName}").eqNullSafe(col(f"t.{colName}")))
    change_condition = col_changed if change_condition is None else (
        change_condition | col_changed
    )

current_TargetDF = targetdf.filter(col("is_current"))

expiredRowsDF = sourceDF.alias("s")\
                .join(current_TargetDF.alias("t"),on="customer_id",how="inner")\
                .filter(change_condition)\
                .select(
                    col("customer_id"),
                    col("t.email"),
                    col("t.name"),
                    col("t.region"),
                    col("t.tier"),
                    lit(False).alias("is_current"),
                    col("t.effective_start_date").alias("effective_start_date"),
                    lit(current_date()).alias("effective_end_date"),
                    lit("expire").alias("merge_action")
                )


updatedCustIDs = expiredRowsDF.select("customer_id")
newCustIDs = sourceDF.join(current_TargetDF,on="customer_id",how="left_anti").select("customer_id")


# display(newCustIDs)

unionDF = updatedCustIDs.union(newCustIDs)

# display(upsertDF)



upsertDF = sourceDF\
            .join(unionDF,on="customer_id",how="inner")\
            .select(
                col("customer_id"),
                col("name"),
                col("email"),
                col("region"),
                col("tier"),
                lit(True).alias("is_current"),
                lit(current_date()).alias("effective_start_date"),
                to_date(lit("9999-12-31")).alias("effective_end_date"),
                lit("upsert").alias("merge_action")
            )

stagedDF = upsertDF.union(expiredRowsDF)

display(stagedDF)


target.alias("t")\
    .merge(stagedDF.alias("s"),condition="""
           s.customer_id=t.customer_id and t.is_current = 'true' and s.merge_action='expire'
           """)\
    .whenMatchedUpdate(set={
        "is_current":lit(False),
        "effective_end_date":current_date()
    })\
    .whenNotMatchedInsert(values={
        "customer_id":col("s.customer_id"),
        "name":col("s.name"),
        "email":col("s.email"),
        "region":col("s.region"),
        "tier":col("s.tier"),
        "is_current":col("s.is_current"),
        "effective_start_date":col("s.effective_start_date"),
        "effective_end_date":col("s.effective_end_date")
    }).execute()


# COMMAND ----------

# MAGIC %md
# MAGIC Attempt - 2

# COMMAND ----------

print(sourceDF.columns)
print(targetDF.columns)

commonCols = list(set(sourceDF.columns) & set(targetDF.columns))

print(commonCols)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col,to_date,current_date,lit




# read the source and target , target as delta table to use merge operation
sourceDF = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex8_source")
target = DeltaTable.forName(spark,f"{CATALOG}.{SCHEMA}.merge_ex8_target")
targetDF = target.toDF()

# build the dynamic change condition which can be used on any df with null safe check.
change_condition = None

columnsList = sourceDF.columns

for colName in columnsList:
    col_condition = ~(col(f"s.{colName}").eqNullSafe(col(f"t.{colName}")))
    change_condition = col_condition if change_condition is None else (
        change_condition | col_condition
    )


# take only current records from target df to compare with sourcedf .
currentTargetDF = targetDF.filter(col("is_current"))

# build the expired rows df.
expiredRowsDF = sourceDF.alias("s")\
                .join(current_TargetDF.alias("t"),on="customer_id",how="inner")\
                .filter(change_condition)\
                .select("customer_id",
                        col("t.email"),
                        col("t.name"),
                        col("t.region"),
                        col("t.tier"),
                        lit(False).alias("is_current"),
                        col("t.effective_start_date"),
                        lit(current_date()).alias("effective_end_date"),
                        lit("expire").alias("merge_action")
                )
# fetch the common IDs
commonIDs = expiredRowsDF.select("customer_id")
# fetch the new IDs
newIDs = sourceDF.join(current_TargetDF,on="customer_id",how="left_anti").select("customer_id")
# union both common and new IDs
unionDF = commonIDs.union(newIDs)
# build a upsertDF
upsertDF = sourceDF.alias("s")\
            .join(unionDF,on="customer_id",how="inner")\
            .select("customer_id",
                    col("s.email"),
                    col("s.name"),
                    col("s.region"),
                    col("t.tier"),
                    lit(True).alias("is_current"),
                    current_date().alias("effective_start_date"),
                    to_date(lit("9999-12-31")).alias("effective_end_date"),
                    lit("upsert").alias("merge_action"))
            

# build a stageDF which has both expired rows and Union rows

stagedDF = expiredRowsDF.union(upsertDF)



# write the merge operation

target.alias("t")\
    .merge(stagedDF.alias("s"),condition="""
           s.customer_id = t.customer_id
           AND t.is_current = true
           AND s.merge_action = 'expire'
           """)\
    .whenMatchedUpdate(set={
        "is_current":lit(True),
        "effective_end_date":current_date()
    })\
    .whenNotMatchedInsert(values={
        "customer_id":col("s.customer_id"),
        "name":col("s.name"),
        "email":col("s.email"),
        "region":col("s.region"),
        "tier":col("s.tier"),
        "is_current":col("s.is_current"),
        "effective_start_date":col("s.effective_start_date"),
        "effective_end_date":col("s.effective_end_date")
    }).execute()

# COMMAND ----------


source = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex8_source")
# display(source)

targetdf = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex8_target")

display(source)

display(targetdf)

# COMMAND ----------

targetDF = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex8_target")
display(targetDF)

# COMMAND ----------

# EXERCISE_KEY: merge_ex8
# TODO: Write your solution here

from delta.tables import DeltaTable
from pyspark.sql.functions import col,lit,current_date,to_date


source = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex8_source")
print("***************** SOURCE DF ******************")
display(source)
target = DeltaTable.forName(spark,f"{CATALOG}.{SCHEMA}.merge_ex8_target")
targetdf = target.toDF()
print("************** TARGET DF *********************")
display(targetdf)

compareCols = source.columns

# compareCols = ["name", "email", "region", "tier"]

# creating a condition logic instead of hard-coding using the col names, in this way this code can be used to multiple dfs.
# in line -25,26 it's a way of concatenating the col conditions.
change_condition = None
for col_name in compareCols:
    col_changed = ~col(f"s.{col_name}").eqNullSafe(col(f"t.{col_name}"))
    change_condition = col_changed if change_condition is None else (
        change_condition | col_changed
    )

# retrieving only active records from target.
current_targetDF = targetdf.filter(col("is_current"))

# expiring the rows which are changed between source and target.
expired_rows = source.alias("s")\
                .join(current_targetDF.alias("t"),on="customer_id",how="inner")\
                .filter(change_condition)\
                .select(
                    col("customer_id"),
                    col("t.name"),
                    col("t.email"),
                    col("t.region"),
                    col("t.tier"),
                    lit(False).alias("is_current"), # converting the true to False.
                    col("t.effective_start_date"),
                    current_date().alias("effective_end_date"),
                    lit("expire").alias("merge_action")
                )

print("************** Expired Rows DF *********************")
display(expired_rows)

# upsert_records

# changed customer IDs
changed_customer_ids = expired_rows.select("customer_id").distinct()

# # new customer IDs
new_customer_ids = source.alias("s")\
                    .join(current_targetDF.alias("t"),on="customer_id",how="left_anti")\
                    .select("customer_id")

upsert_customer_ids = changed_customer_ids.unionByName(new_customer_ids).distinct()

# retrieving the data from source for the upsert related IDs
upsert_df = source.alias("s")\
            .join(upsert_customer_ids.alias("u"),on="customer_id",how="inner")\
            .select("customer_id","name","email","region","tier",
                    lit(True).alias("is_current"),
                    current_date().alias("effective_start_date"),
                    to_date(lit("9999-12-31")).alias("effective_end_date"),
                    lit("upsert").alias("merge_action")
                    )
print("***************** UPSERT DF ******************")
display(upsert_df)

# this DF helps in matched and notMatched records.
stagedDF = expired_rows.unionByName(upsert_df)

print("**************** STAGED DF ********************")
display(stagedDF)


(
    target.alias("t")\
    .merge(stagedDF.alias("s"),
           condition="""
           t.customer_id = s.customer_id
           AND t.is_current = true
           AND s.merge_action= 'expire'
           """)\
        .whenMatchedUpdate(set={
                "is_current":lit(False),
                "effective_end_date":current_date()
        })\
        .whenNotMatchedInsert(values={
            "customer_id":"s.customer_id",
            "name":"s.name",
            "email":"s.email",
            "region":"s.region",
            "tier":"s.tier",
            "is_current":"s.is_current",
            "effective_start_date":"s.effective_start_date",
            "effective_end_date":"s.effective_end_date",
        }).execute()
)

# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex8_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert result.filter("is_current = true").count() == 4, \
    "Should have 4 current records (CUST-001 new, CUST-002, CUST-003 new, CUST-010)"
assert result.filter("is_current = false").count() == 2, \
    "Should have 2 expired records (CUST-001 old, CUST-003 old)"
assert result.filter("customer_id = 'CUST-001' AND is_current = true") \
    .select("tier").collect()[0][0] == "platinum", \
    "CUST-001 current version should have tier='platinum'"
assert result.filter("customer_id = 'CUST-010'").count() == 1, \
    "CUST-010 should be inserted as new customer"
assert result.filter("customer_id = 'CUST-002' AND is_current = true").count() == 1, \
    "CUST-002 should be unchanged"

print("Exercise 8 passed!")

# --- Idempotency check (manual) ---
# Re-run your TODO cell above ONCE MORE without re-running the setup notebook,
# then re-run this validate cell. All assertions must still pass: 6 total rows,
# 4 current, 2 expired. If they fail on the second run, your solution is
# non-idempotent - it is expiring unchanged records and inserting spurious
# "current" versions on every run. A correct SCD Type 2 only writes when a
# source attribute actually changed.
print("Idempotency: re-run your TODO cell then this cell. Assertions must still pass.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: MERGE with Schema Evolution
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC MERGE `merge_ex9_source` into `merge_ex9_target`. The source has an extra column
# MAGIC (`discount_pct`) that doesn't exist in the target. Make the target schema evolve automatically.
# MAGIC
# MAGIC **Source** (`merge_ex9_source`): 2 rows - same schema as orders + `discount_pct DOUBLE`
# MAGIC - ORD-001 (existing, discount_pct=10.0), ORD-101 (new, discount_pct=5.0)
# MAGIC
# MAGIC **Target** (`merge_ex9_target`): 5 rows - standard orders schema (no discount_pct column)
# MAGIC
# MAGIC **Expected Output**: 6 rows. Column `discount_pct` exists.
# MAGIC ORD-001 discount_pct=10.0. ORD-101 discount_pct=5.0. Other rows discount_pct=null.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Enable automatic schema evolution
# MAGIC 2. MERGE source into target on `order_id`
# MAGIC 3. Update matched, insert not matched
# MAGIC 4. Target schema should gain the `discount_pct` column

# COMMAND ----------

# EXERCISE_KEY: merge_ex9
# TODO: Write your solution here

# Your code here
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
from delta.tables import DeltaTable
from pyspark.sql.functions import col

source = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex9_source")
# display(source)

targetdf = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex9_target")
# display(targetdf)

target = DeltaTable.forName(spark,f"{CATALOG}.{SCHEMA}.merge_ex9_target")

(
  target.alias("t").merge(
    source.alias("s"), condition="t.order_id=s.order_id"
  )\
  .whenMatchedUpdateAll()\
  .whenNotMatchedInsertAll()\
  .execute()
)


# COMMAND ----------

# spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
# or
spark.version

# COMMAND ----------

from pyspark.sql.functions import col
source = spark.read.table(f"{CATALOG}.{SCHEMA}.merge_ex9_source")
source_casted = source.withColumn("amount", col("amount").cast("double")) \
                      .withColumn("discount_pct", col("discount_pct").cast("double"))

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.merge_ex9_target
    SET TBLPROPERTIES ('delta.schemaAutoMerge.enabled' = 'true')
""")

# COMMAND ----------

from delta.tables import DeltaTable

# Load source and target
source = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex9_source")
target = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.merge_ex9_target")

# Standard upsert MERGE
# Spark 4.1 / DBR 16 handles schema evolution automatically
# On older runtimes, set spark.databricks.delta.schema.autoMerge.enabled = true before this
(
    target.alias("t")
    .merge(
        source_casted.alias("s"),
        condition="t.order_id = s.order_id"   # correct business key for orders table
    )
    .whenMatchedUpdateAll()     # update all columns including discount_pct
    .whenNotMatchedInsertAll()  # insert full row including discount_pct
    .execute()
)

source_casted.printSchema()
target.toDF().printSchema()

# COMMAND ----------

result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex9_target")
result.printSchema()
result.show()

# COMMAND ----------

spark.sql(f"""
          MERGE with schema evolution into {CATALOG}.{SCHEMA}.merge_ex9_target AS t
          USING {CATALOG}.{SCHEMA}.merge_ex9_source AS s 
          ON s.order_id = t.order_id
          when matched then update set *
          when not matched then insert *
          """)

# COMMAND ----------

# Validate Exercise 9
result = spark.table(f"{CATALOG}.{SCHEMA}.merge_ex9_target")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert "discount_pct" in result.columns, \
    "Target should have 'discount_pct' column after schema evolution"
assert result.filter("order_id = 'ORD-001'").select("discount_pct").collect()[0][0] == 10.0, \
    "ORD-001 should have discount_pct = 10.0"
assert result.filter("order_id = 'ORD-101'").select("discount_pct").collect()[0][0] == 5.0, \
    "ORD-101 should have discount_pct = 5.0"
assert result.filter("order_id = 'ORD-002'").select("discount_pct").collect()[0][0] is None, \
    "ORD-002 should have null discount_pct (not in source)"

print("Exercise 9 passed!")

# COMMAND ----------

# Check if Delta table property needs to be set
spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.merge_ex9_target").select("properties").show(truncate=False)
