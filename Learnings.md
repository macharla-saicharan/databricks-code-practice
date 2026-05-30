# Learnings of delta lake concepts

- DeltaTable.forName()  →  gives you a DeltaTable object
                         has write methods: .merge(), .update(),
                         .delete(), .optimize(), .vacuum()
                         USE THIS when you need to WRITE

- spark.table()         →  gives you a DataFrame object
                         has read/transform methods only
                         USE THIS when you only need to READ

- order of the match functions are very important

>     Always order WHEN MATCHED clauses from:
>     MOST SPECIFIC (with conditions) → LEAST SPECIFIC (no condition)
> 
>     Most specific  →  .whenMatchedDelete(condition="...")
>                       .whenMatchedUpdate(condition="...")
>     Least specific →  .whenMatchedUpdateAll()   ← always last, no condition
>                       .whenMatchedDeleteAll()   ← if used, always last

- Join Syntax                          │ Duplicate join key columns?
─────────────────────────────────────┼────────────────────────────
on="column_name"  (string)           │ NO  — PySpark deduplicates ✅
on=["col1","col2"] (list of strings) │ NO  — PySpark deduplicates ✅
on=F.col("a.x") == F.col("b.x")     │ YES — both columns kept ❌
on=df1.x == df2.x                    │ YES — both columns kept ❌

- filter and where are used for data filtrations and both are identiical.

- spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") = this command is used to enable schema evolution

- Two Ways to append the Data into delta table.
1. SaveAsTable() = It looks for the table, if unavailable creates one.
2. insertInto() = it inserts the data into the existing table, if there is no table then it will fail.

- To overwrite the schema completely in the target table and match with source use .mode("overwrite).option("overwriteSchema","true")


- ADDING CONSTRAINTS USING SPARK SQL

spark.sql(f"""
          alter table {CATALOG}.{SCHEMA}.schema_ex7_orders
          add constraint valid_amount check (amount > 0)
          """)