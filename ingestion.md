# 🔄 Databricks Auto Loader: Advanced readStream Example for Pub/Sub via GCS

This section provides a production-ready `spark.readStream` configuration using Databricks Auto Loader to consume high-throughput data (millions of messages/hour) from a GCS bucket that receives messages from Google Pub/Sub.

## ✅ Objective

- Enable fast, scalable ingestion using GCS notifications
- Use schema evolution and control batch size
- Tune for performance with trigger size options
- Integrate with Delta Live Tables (DLT)

---

## ⚙️ Auto Loader Streaming Configuration with Options

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")                              # Input file format
    .option("cloudFiles.useNotifications", "true")                    # Enable GCS Pub/Sub notifications
    .option("cloudFiles.schemaLocation", "/mnt/schema/pubsub/")      # Required path for schema tracking
    .option("cloudFiles.includeExistingFiles", "true")                # Process historical files
    .option("cloudFiles.maxFilesPerTrigger", "1000")                  # Tune micro-batch size (file-based)
    .option("cloudFiles.backfillInterval", "1 hour")                  # Optional: for recovering missed files
    .option("recursiveFileLookup", "true")                            # Enable nested folder scanning
    .option("cloudFiles.inferColumnTypes", "true")                    # Infer schema automatically
    .option("cloudFiles.allowOverwrites", "false")                    # Prevent reprocessing of overwritten files
    .option("cloudFiles.minBytesPerTrigger", "134217728")            # 128 MB; batch size based on file size
    .load("/mnt/pubsub_data/")
```
--
```python
from pyspark.sql.types import StructType, StringType

schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("payload", StringType()) \
    .add("event_time", StringType())

spark.readStream.format("cloudFiles") \
    .schema(schema) \
    .options({
        "cloudFiles.format": "json",
        "cloudFiles.useNotifications": "true",
        "cloudFiles.schemaLocation": "/mnt/schema/pubsub/",
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.maxFilesPerTrigger": "1000",
        "cloudFiles.allowOverwrites": "false"
    }) \
    .load("/mnt/pubsub_data/")
```
