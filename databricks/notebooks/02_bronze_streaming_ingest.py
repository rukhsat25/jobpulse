# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 6, Part A — Bronze Layer via Auto Loader
# MAGIC
# MAGIC Parameterized by `source` — run once per source (remotive /
# MAGIC arbeitnow / remoteok), changing the widget each time. Stage 13
# MAGIC (Workflows) will run all three as parallel tasks calling this
# MAGIC same notebook with different parameters — that's exactly why
# MAGIC this is one parameterized notebook, not three near-duplicates.
# MAGIC
# MAGIC Run attached to **Serverless** compute.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, explode
from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, StringType, StructField, StructType,
)

dbutils.widgets.text("catalog", "jobpulse")
dbutils.widgets.text("raw_schema", "raw_landing")
dbutils.widgets.text("raw_volume", "landing")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.dropdown("source", "remotive", ["remotive", "arbeitnow", "remoteok"])

catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
raw_volume = dbutils.widgets.get("raw_volume")
bronze_schema = dbutils.widgets.get("bronze_schema")
source = dbutils.widgets.get("source")

volume_root = f"/Volumes/{catalog}/{raw_schema}/{raw_volume}"
source_path = f"{volume_root}/raw/{source}"
checkpoint_path = f"{volume_root}/_checkpoints/{source}"
target_table = f"{catalog}.{bronze_schema}.{source}_jobs"

print(f"Source:    {source}")
print(f"Reading:   {source_path}")
print(f"Checkpoint:{checkpoint_path}")
print(f"Writing to:{target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-source job schemas
# MAGIC Confirmed against real API output (Stages 1-2), not guessed from
# MAGIC docs. Every ambiguous/numeric-looking field is StringType — see
# MAGIC Stage 6 notes on why Bronze avoids early type-casting. `remote`
# MAGIC on Arbeitnow stays BooleanType — already verified working against
# MAGIC real data in Stage 4/5.

# COMMAND ----------

REMOTIVE_JOB_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("url", StringType()),
    StructField("title", StringType()),
    StructField("company_name", StringType()),
    StructField("company_logo", StringType()),
    StructField("category", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("job_type", StringType()),
    StructField("publication_date", StringType()),
    StructField("candidate_required_location", StringType()),
    StructField("salary", StringType()),
    StructField("description", StringType()),
    StructField("company_logo_url", StringType()),
])

ARBEITNOW_JOB_SCHEMA = StructType([
    StructField("slug", StringType()),
    StructField("company_name", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("remote", BooleanType()),
    StructField("url", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("job_types", ArrayType(StringType())),
    StructField("location", StringType()),
    StructField("created_at", StringType()),
])

REMOTEOK_JOB_SCHEMA = StructType([
    StructField("slug", StringType()),
    StructField("id", StringType()),
    StructField("epoch", StringType()),
    StructField("date", StringType()),
    StructField("company", StringType()),
    StructField("company_logo", StringType()),
    StructField("position", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("description", StringType()),
    StructField("location", StringType()),
    StructField("apply_url", StringType()),
    StructField("salary_min", StringType()),
    StructField("salary_max", StringType()),
    StructField("logo", StringType()),
    StructField("url", StringType()),
])

JOB_SCHEMAS = {
    "remotive": REMOTIVE_JOB_SCHEMA,
    "arbeitnow": ARBEITNOW_JOB_SCHEMA,
    "remoteok": REMOTEOK_JOB_SCHEMA,
}

if source not in JOB_SCHEMAS:
    raise ValueError(f"Unknown source '{source}', expected one of {list(JOB_SCHEMAS)}")

job_schema = JOB_SCHEMAS[source]

payload_schema = StructType([
    StructField("source", StringType()),
    StructField("fetched_at", StringType()),
    StructField("record_count", IntegerType()),
    StructField("records", ArrayType(job_schema)),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader read
# MAGIC No `cloudFiles.schemaLocation` — that's specifically for when Auto
# MAGIC Loader infers/evolves a schema itself. We supply our own, so it's
# MAGIC not needed. `checkpointLocation` (below, on the write side) is
# MAGIC what actually tracks which files have been processed — that one
# MAGIC is never optional.

# COMMAND ----------

raw_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .schema(payload_schema)
    .load(source_path)
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_file_modified_at", col("_metadata.file_modification_time"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unwrap the ingestion envelope
# MAGIC One row per landed FILE (with a `records` array) becomes one row
# MAGIC per JOB POSTING. This is structural unnesting, not cleaning — every
# MAGIC field still passes through untouched. File lineage columns are
# MAGIC selected explicitly so they survive the explode.

# COMMAND ----------

bronze_df = (
    raw_stream_df
    .select(
        "source",
        "fetched_at",
        "_source_file",
        "_file_modified_at",
        explode(col("records")).alias("record"),
    )
    .select("source", "fetched_at", "_source_file", "_file_modified_at", "record.*")
    .withColumn("_bronze_ingested_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write: streaming machinery, batch cadence
# MAGIC `trigger(availableNow=True)` processes everything currently
# MAGIC available, then stops — this call returns once the batch is done,
# MAGIC it does not keep running. `toTable` creates the Delta table on
# MAGIC first run if it doesn't exist yet, using this DataFrame's schema.

# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()

print(f"Done. Row count now in {target_table}:")
display(spark.table(target_table).count())