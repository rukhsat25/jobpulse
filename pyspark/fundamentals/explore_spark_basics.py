"""
Stage 4 — Spark & PySpark fundamentals, demonstrated against real
JobPulse data: the Arbeitnow sample file saved back in Stage 1.

Run this and keep http://localhost:4040 open in a browser while it
runs — the script pauses at a few points specifically so you can go
look at the Spark UI in real time.

This is a learning/exploration script, not part of the production
pipeline (Bronze/Silver/Gold logic starts in Stage 6). It runs Spark
LOCALLY, not on Databricks, on purpose: build a solid mental model of
Spark itself before Stage 5 adds Databricks into the mix.

Requires data/samples/arbeitnow_sample.json — regenerate it by running
src/ingestion/explore.py if it's missing (e.g. after a fresh clone).
"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

SAMPLE_FILE = Path(__file__).resolve().parents[2] / "data" / "samples" / "arbeitnow_sample.json"

# Defined once, reused everywhere below — the same discipline we'll
# want for Bronze in Stage 6: one canonical schema, not one redefined
# per function.
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

# Arbeitnow's raw response envelope is {"data": [...jobs...], "links": {...}, "meta": {...}}.
# We only declare "data" below — Spark simply won't materialize columns
# for "links"/"meta" since they're not in the schema. That's
# schema-on-read in action: we choose what to keep.
ENVELOPE_SCHEMA = StructType([
    StructField("data", ArrayType(JOB_SCHEMA)),
])


def build_spark_session() -> SparkSession:
    """
    `local[*]`: the driver and "executors" are all threads inside this
    one Python process, using as many threads as your machine has CPU
    cores. Stage 5 replaces this with a real Databricks cluster.
    """
    return (
        SparkSession.builder.appName("JobPulse-Stage4-Fundamentals")
        .master("local[*]")
        .getOrCreate()
    )


def part_a_inferred_vs_explicit_schema(spark: SparkSession) -> None:
    print("\n=== PART A: inferred schema vs explicit schema ===")
    print("Open http://localhost:4040/jobs/ now, in your browser.\n")

    print("-- Reading WITHOUT a schema (Spark infers it) --")
    inferred_df = spark.read.option("multiLine", True).json(str(SAMPLE_FILE))
    # By the time this line finishes, Spark has ALREADY scanned the file
    # to work out types — a DataFrame can't exist without a known schema,
    # so unlike a transformation, that scan can't be deferred.
    inferred_df.printSchema()
    input("\nCheck the Spark UI 'Jobs' tab — note the job that already ran. Press Enter...")

    print("\n-- Reading WITH an explicit schema --")
    explicit_df = spark.read.option("multiLine", True).schema(ENVELOPE_SCHEMA).json(str(SAMPLE_FILE))
    # No scan needed this time — we told Spark the shape, so this line
    # triggers no job at all.
    explicit_df.printSchema()
    input("\nCheck the Spark UI again — no new job for this read. Press Enter...")

    print("Why this matters: schema inference isn't free — it's a real pass")
    print("over the data. On a 5 GB file, that's a slow surprise hiding in")
    print("a line of code that looks completely harmless. Production")
    print("pipelines almost always pass an explicit schema.")


def part_b_transformations_vs_actions(spark: SparkSession) -> None:
    print("\n=== PART B: transformations (lazy) vs actions (execute) ===")

    raw_df = spark.read.option("multiLine", True).schema(ENVELOPE_SCHEMA).json(str(SAMPLE_FILE))

    # Everything below is a TRANSFORMATION: each line defines a new plan
    # but runs nothing yet. explode() turns the single row's "data"
    # array into one row per job.
    jobs_df = raw_df.select(explode(col("data")).alias("job")).select("job.*")
    remote_jobs = jobs_df.filter(col("remote"))
    titled = remote_jobs.select("title", "company_name", "location")

    print("Nothing has executed yet — 'titled' is a plan (a DAG), not data.")
    print("\nPhysical + logical plan:")
    titled.explain(True)

    print("\nNow calling .count() — THIS is what triggers an actual job:")
    n = titled.count()  # ACTION
    print(f"Remote job count in this sample: {n}")

    print(f"\nPartitions backing this DataFrame: {jobs_df.rdd.getNumPartitions()}")
    print("One small local file -> Spark isn't splitting this into many")
    print("partitions. Partition count becomes a real tuning decision once")
    print("we're processing much larger files from Stage 6 onward — full")
    print("depth on that is Stage 11.")

    titled.show(5, truncate=False)  # ACTION


def part_c_same_question_three_ways(spark: SparkSession) -> None:
    print("\n=== PART C: DataFrame API vs Spark SQL — same question ===")
    print("Watch the Spark UI Jobs tab for THIS part especially — the")
    print("groupBy below causes a SHUFFLE, and you'll see a job with")
    print("2 stages instead of 1. We go deep on this in Stage 11, but")
    print("it's worth seeing for real right now.\n")

    raw_df = spark.read.option("multiLine", True).schema(ENVELOPE_SCHEMA).json(str(SAMPLE_FILE))
    jobs_df = raw_df.select(explode(col("data")).alias("job")).select("job.*")

    print("-- DataFrame API --")
    df_result = (
        jobs_df.filter(col("remote"))
        .groupBy("company_name")
        .agg(count("*").alias("postings"))
        .orderBy(col("postings").desc())
    )
    df_result.show(5)

    print("-- Spark SQL (same question, SQL syntax) --")
    jobs_df.createOrReplaceTempView("jobs")
    sql_result = spark.sql("""
        SELECT company_name, COUNT(*) AS postings
        FROM jobs
        WHERE remote = true
        GROUP BY company_name
        ORDER BY postings DESC
        LIMIT 5
    """)
    sql_result.show()

    print("Compare df_result.explain() and sql_result.explain() yourself —")
    print("both compile to the same Catalyst physical plan. The DataFrame")
    print("API and Spark SQL are two syntaxes over one execution engine;")
    print("neither is 'faster' than the other on its own.")


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        part_a_inferred_vs_explicit_schema(spark)
        part_b_transformations_vs_actions(spark)
        part_c_same_question_three_ways(spark)
    finally:
        input("\nAll parts done — check the Spark UI once more if you like. Press Enter to stop Spark...")
        spark.stop()
