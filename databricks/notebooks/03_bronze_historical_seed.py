# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 6, Part B — Bronze Historical Seed
# MAGIC
# MAGIC One-time batch load of the LinkedIn Job Postings (2023-2024)
# MAGIC dataset (Kaggle: arshkon/linkedin-job-postings), uploaded to the
# MAGIC Volume in Stage 6's setup. This is genuinely a one-time load — no
# MAGIC Auto Loader, no checkpoint, no streaming: there's no "new files
# MAGIC arriving" pattern here, so that machinery would be the wrong tool.
# MAGIC
# MAGIC **Deliberate exception to "always use explicit schema":** for a
# MAGIC single one-time load of an already-downloaded, well-documented
# MAGIC static file, schema inference's one-time cost is a non-issue —
# MAGIC the whole argument for explicit schemas (avoiding repeated
# MAGIC inference cost on recurring runs, guarding against a *live* API
# MAGIC drifting under us) doesn't apply to a file that isn't recurring
# MAGIC and isn't going to change after we've downloaded it. This is a
# MAGIC cost/benefit call, not a rule broken by accident.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

dbutils.widgets.text("catalog", "jobpulse")
dbutils.widgets.text("raw_schema", "raw_landing")
dbutils.widgets.text("raw_volume", "landing")
dbutils.widgets.text("bronze_schema", "bronze")

catalog = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
raw_volume = dbutils.widgets.get("raw_volume")
bronze_schema = dbutils.widgets.get("bronze_schema")

volume_root = f"/Volumes/{catalog}/{raw_schema}/{raw_volume}"
seed_path = f"{volume_root}/historical_seed/postings.csv"
target_table = f"{catalog}.{bronze_schema}.historical_seed_postings"

print(f"Reading:   {seed_path}")
print(f"Writing to:{target_table}")

# COMMAND ----------

seed_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")   # acceptable here — see note above
    .option("multiLine", "true")     # job descriptions may contain embedded newlines
    .option("escape", '"')
    .csv(seed_path)
)

print(f"Row count: {seed_df.count()}")
print(f"Columns:   {seed_df.columns}")

# Sanity check against the dataset's documented structure — flags
# immediately if the download/upload didn't produce what we expected,
# rather than silently proceeding on the wrong file.
EXPECTED_COLUMNS = {
    "job_id", "company_id", "title", "description", "max_salary", "med_salary",
    "min_salary", "pay_period", "formatted_work_type", "location", "applies",
    "original_listed_time", "remote_allowed", "views", "job_posting_url",
    "application_url", "application_type", "expiry", "closed_time",
    "formatted_experience_level", "skills_desc", "listed_time", "posting_domain",
    "sponsored", "work_type", "currency", "compensation_type",
}
missing = EXPECTED_COLUMNS - set(seed_df.columns)
if missing:
    print(f"WARNING: expected columns not found in file: {missing}")
else:
    print("Column check passed: all expected columns present.")

# COMMAND ----------

bronze_seed_df = (
    seed_df
    .withColumn("_bronze_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(seed_path))
    .withColumn("_seed_source", lit("kaggle_linkedin_job_postings_2023_2024"))
)

(
    bronze_seed_df.write
    .format("delta")
    .mode("append")   # append, not overwrite — safe to re-run without duplicating
    .saveAsTable(target_table)
)

print(f"Done. Row count now in {target_table}:")
display(spark.table(target_table).count())