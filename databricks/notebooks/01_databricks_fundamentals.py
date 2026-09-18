# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Stage 5 — Databricks Fundamentals
# MAGIC
# MAGIC Goal: confirm, hands-on, what's different about running Spark
# MAGIC on Databricks versus the local `local[*]` session from Stage 4.
# MAGIC
# MAGIC Run this attached to **Serverless** compute (top-right "Connect"
# MAGIC menu in the notebook toolbar).
# MAGIC
# MAGIC **WARNING — do not `%pip install pyspark`, `%pip install delta-spark`,**
# MAGIC **or `%pip install -r requirements.txt` in this or any Databricks**
# MAGIC **notebook.** Those two packages are pinned by Databricks to match**
# MAGIC **this Runtime's Spark Connect protobuf generation exactly —**
# MAGIC **repinning either one breaks the kernel at startup, not just an**
# MAGIC **import. `requirements.txt` is for the LOCAL venv only (Stage 0).**
# MAGIC **Also check the notebook's Environment panel (right sidebar) has**
# MAGIC **no pinned dependencies for pyspark/delta-spark/requests before**
# MAGIC **you run this — a stale pinned Environment spec reintroduces the**
# MAGIC **same crash on every reattach even with no `%pip install` cell.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `spark` already exists
# MAGIC Unlike Stage 4's local script, we never call `SparkSession.builder`
# MAGIC here — the Databricks notebook environment provides a ready-made
# MAGIC `spark` object as soon as compute is attached.

# COMMAND ----------

print(f"Spark version: {spark.version}")
print(f"Session type: {type(spark)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Notebook parameters (widgets)
# MAGIC Widgets let a notebook accept parameters at run time instead of
# MAGIC hardcoding values — this is exactly how Stage 13's Databricks
# MAGIC Workflows will pass values INTO notebooks as scheduled job tasks.
# MAGIC Set these three to match what you actually created in Stage 3.

# COMMAND ----------

dbutils.widgets.text("catalog", "jobpulse")
dbutils.widgets.text("schema", "raw_landing")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
volume_root = f"/Volumes/{catalog}/{schema}/{volume}"

print(f"Reading from: {volume_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `dbutils` — a Databricks-only helper
# MAGIC `dbutils` does not exist in plain PySpark or in our Stage 4 local
# MAGIC script — it's injected by the Databricks notebook runtime. Here
# MAGIC we use `dbutils.fs.ls` to list what Stage 3's ingestion landed,
# MAGIC directly against the Volume path — no download step required,
# MAGIC unlike our local Python code which had to call the Files API
# MAGIC explicitly to read anything back.

# COMMAND ----------

arbeitnow_root = f"{volume_root}/raw/arbeitnow"
display(dbutils.fs.ls(arbeitnow_root))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read a landed file straight from the Volume
# MAGIC Same explicit-schema discipline as Stage 4 — and the same reason:
# MAGIC we don't want an inference scan, and we want control over types.
# MAGIC Update `date_partition` and `filename` below to match an actual
# MAGIC file from the `display()` output above.

# COMMAND ----------

from pyspark.sql.types import (
    ArrayType, BooleanType, StringType, StructField, StructType,
)

JOB_SCHEMA = StructType([
    StructField("slug", StringType()),
    StructField("company_name", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("remote", BooleanType()),
    StructField("url", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("job_types", ArrayType(StringType())),
    StructField("location", StringType()),
])

PAYLOAD_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("fetched_at", StringType()),
    StructField("record_count", StringType()),
    StructField("records", ArrayType(JOB_SCHEMA)),
])

# EDIT this path to point at one real landed file from cell above:
date_partition = "2026-09-16"          
filename = "arbeitnow_20260916T183930Z.json"

file_path = f"{arbeitnow_root}/{date_partition}/{filename}"

raw_df = spark.read.schema(PAYLOAD_SCHEMA).json(file_path)
raw_df.printSchema()
display(raw_df.select("source", "fetched_at", "record_count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta Lake is already installed — no pip install
# MAGIC In Stage 0/4 locally, `delta-spark` was a dependency WE added.
# MAGIC On Databricks Runtime, it's simply part of the platform. Do NOT
# MAGIC `%pip install` it here — see the warning at the top of this notebook.

# COMMAND ----------

import delta
print(f"delta-spark version (pre-installed by the Runtime): {delta.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Where the Spark UI lives here
# MAGIC There's no `localhost:4040` on Databricks. Instead: click the
# MAGIC compute name at the top of this notebook -> it opens the compute
# MAGIC details page, which has its own **Spark UI** tab, scoped to this
# MAGIC session's jobs/stages/tasks. We'll use this for real starting
# MAGIC Stage 11 (performance) — for now, just confirm you can find it.