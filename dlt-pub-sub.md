# DLT Pipeline: Streaming from Pub/Sub via GCS (Unity Catalog)

This pipeline uses Delta Live Tables (DLT) to ingest messages streamed from Google Pub/Sub, written to GCS, and processes them using the medallion architecture.

## 🔧 Configuration

- **Pipeline Mode**: Continuous
- **Target Catalog**: `main`
- **Schemas Used**: `bronze`, `silver`, `gold`
- **Input Path**: `/mnt/pubsub_data/` (GCS mount)
- **Format**: Newline-delimited JSON
- **Streaming**: Enabled (Auto Loader)

## 🧾 Code

```python
from pyspark.sql.functions import col, from_json, to_timestamp, window, count
from pyspark.sql.types import StructType, StringType
import dlt

# Schema for incoming Pub/Sub message
message_schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("payload", StringType()) \
    .add("event_time", StringType())

@dlt.table(
    name="bronze.pubsub_raw",
    comment="Raw messages from Pub/Sub via GCS",
    table_properties={"quality": "bronze"}
)
def bronze_pubsub():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.useNotifications", "true")
            .option("cloudFiles.schemaLocation", "/mnt/schema/pubsub/")
            .load("/mnt/pubsub_data/")
    )

@dlt.table(
    name="silver.cleaned_pubsub",
    comment="Validated and typed messages",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_event_id", "event_id IS NOT NULL")
@dlt.expect("valid_event_time", "event_time IS NOT NULL")
def silver_pubsub():
    df = dlt.read("bronze.pubsub_raw")
    return df.select(from_json(col("value").cast("string"), message_schema).alias("data")).select("data.*") \
             .withColumn("event_time", to_timestamp("event_time"))

@dlt.table(
    name="gold.hourly_event_counts",
    comment="Hourly event count per event_type",
    table_properties={"quality": "gold"}
)
def gold_summary():
    df = dlt.read("silver.cleaned_pubsub")
    return (
        df.withWatermark("event_time", "30 minutes")
          .groupBy(window(col("event_time"), "1 hour"), col("event_type"))
          .agg(count("*").alias("events_per_hour"))
          .selectExpr("window.start as hour", "event_type", "events_per_hour")
    )
