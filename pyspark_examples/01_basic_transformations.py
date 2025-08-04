
# PySpark Cheatsheet: Basic Transformations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum, avg, count, when, lit

# Initialize Spark Session (in Databricks, 'spark' is pre-configured)
spark = SparkSession.builder.appName("pyspark-cheatsheet-transformations").getOrCreate()

# Load the sample data
df = spark.read.csv("/Users/rubenoreamunoarias/Documents/Github/sparkCheatSheet/data/sample_data.csv", header=True, inferSchema=True)

# -----------------------------------------------------------------------------
# 1. Basic DataFrame Operations
# -----------------------------------------------------------------------------

# Show the first 5 rows
print("--- Showing first 5 rows ---")
df.show(5)

# Print the schema to understand data types
print("--- DataFrame Schema ---")
df.printSchema()

# Get basic statistics for numeric columns
print("--- Summary Statistics ---")
df.describe().show()

# -----------------------------------------------------------------------------
# 2. Selecting and Renaming Columns
# -----------------------------------------------------------------------------

# Select specific columns
print("--- Selecting 'product_name' and 'price' ---")
df.select("product_name", "price").show(5)

# Rename a column
print("--- Renaming 'price' to 'unit_price' ---")
df.withColumnRenamed("price", "unit_price").show(5)

# -----------------------------------------------------------------------------
# 3. Filtering Data
# -----------------------------------------------------------------------------

# Filter for orders with a quantity greater than 1
print("--- Orders with quantity > 1 ---")
df.filter(col("quantity") > 1).show()

# Filter using a SQL-like expression
print("--- Laptops sold ---")
df.filter("product_name = 'Laptop'").show()

# -----------------------------------------------------------------------------
# 4. Adding and Modifying Columns
# -----------------------------------------------------------------------------

# Add a new column for total price
print("--- Adding 'total_price' column ---")
df_with_total = df.withColumn("total_price", col("quantity") * col("price"))
df_with_total.show(5)

# Add a new column with a conditional expression
print("--- Adding 'order_type' column ---")
df_with_order_type = df_with_total.withColumn(
    "order_type",
    when(col("total_price") > 100, "Large").otherwise("Small")
)
df_with_order_type.show(5)

# Add a column with a literal value
print("--- Adding 'source' column ---")
df_with_source = df.withColumn("source", lit("Online"))
df_with_source.show(5)

# -----------------------------------------------------------------------------
# 5. Type Casting
# -----------------------------------------------------------------------------

# Convert the 'order_date' string to a date type
print("--- Casting 'order_date' to date type ---")
df_with_date = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
df_with_date.printSchema()
df_with_date.show(5)

# -----------------------------------------------------------------------------
# 6. Dropping Columns
# -----------------------------------------------------------------------------

# Drop the 'source' column
print("--- Dropping 'source' column ---")
df_with_source.drop("source").show(5)

