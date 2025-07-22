
---

### 📦 2. `dlt_pipeline_files_batch.md`

```markdown
# DLT Pipeline: Batch Processing from File Drop (Unity Catalog)

This Delta Live Tables pipeline processes files dropped into a GCS bucket (mounted to DBFS), using batch (non-streaming) mode and medallion architecture.

## 🔧 Configuration

- **Pipeline Mode**: Triggered
- **Target Catalog**: `main`
- **Schemas Used**: `bronze`, `silver`, `gold`
- **Input Path**: `/mnt/batch_data/`
- **Format**: CSV or JSON

## 🧾 Code

```python
from pyspark.sql.functions import col, to_date, sum
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import dlt

sales_schema = StructType() \
    .add("sale_id", StringType()) \
    .add("product_id", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("sale_date", StringType())

@dlt.table(
    name="bronze.raw_sales",
    comment="Raw static sales files from GCS",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    return (
        spark.read.format("json")
             .schema(sales_schema)
             .load("/mnt/batch_data/")
    )

@dlt.table(
    name="silver.cleaned_sales",
    comment="Cleaned static sales data",
    table_properties={"quality": "silver"}
)
@dlt.expect("positive_price", "price > 0")
@dlt.expect("non_null_product", "product_id IS NOT NULL")
def silver_sales():
    df = dlt.read("bronze.raw_sales")
    return (
        df.withColumn("sale_date", to_date("sale_date", "yyyy-MM-dd"))
    )

@dlt.table(
    name="gold.sales_summary",
    comment="Total revenue per product",
    table_properties={"quality": "gold"}
)
def gold_sales_summary():
    df = dlt.read("silver.cleaned_sales")
    return (
        df.groupBy("product_id", "sale_date")
          .agg(
              sum(col("price") * col("quantity")).alias("total_revenue"),
              sum("quantity").alias("units_sold")
          )
    )
