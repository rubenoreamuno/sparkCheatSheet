

# PySpark Cheatsheet: Joins and Aggregations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("pyspark-cheatsheet-joins-aggs").getOrCreate()

# Load the sample data
df_orders = spark.read.csv("/Users/rubenoreamunoarias/Documents/Github/sparkCheatSheet/data/sample_data.csv", header=True, inferSchema=True)

# Create a second DataFrame for customer information
data_customers = [ (101, "Alice"), (102, "Bob"), (103, "Charlie"), (104, "David") ]
columns_customers = ["customer_id", "customer_name"]
df_customers = spark.createDataFrame(data_customers, columns_customers)

# -----------------------------------------------------------------------------
# 1. Joins
# -----------------------------------------------------------------------------

# Inner join: only matching records from both DataFrames are included
print("--- Inner Join ---")
df_inner_join = df_orders.join(df_customers, on="customer_id", how="inner")
df_inner_join.show()

# Left join: all records from the left DataFrame and matching records from the right
print("--- Left Join ---")
df_left_join = df_orders.join(df_customers, on="customer_id", how="left")
df_left_join.show()

# Right join: all records from the right DataFrame and matching records from the left
print("--- Right Join ---")
df_right_join = df_orders.join(df_customers, on="customer_id", how="right")
df_right_join.show()

# -----------------------------------------------------------------------------
# 2. Aggregations
# -----------------------------------------------------------------------------

# Group by a single column and calculate aggregations
print("--- Aggregations by Product ---")
df_product_agg = df_orders.groupBy("product_name").agg(
    sum("quantity").alias("total_quantity_sold"),
    avg("price").alias("average_price"),
    count("order_id").alias("number_of_orders")
)
df_product_agg.show()

# Group by multiple columns
print("--- Aggregations by Customer and Product ---")
df_customer_product_agg = df_orders.groupBy("customer_id", "product_name").agg(
    sum("quantity").alias("total_quantity")
)
df_customer_product_agg.sort(col("customer_id")).show()

# -----------------------------------------------------------------------------
# 3. Combining Joins and Aggregations
# -----------------------------------------------------------------------------

# Calculate the total amount spent by each customer
print("--- Total Spent by Customer ---")
df_total_spent = df_orders.withColumn("total_price", col("quantity") * col("price")) \
    .join(df_customers, on="customer_id", how="inner") \
    .groupBy("customer_name") \
    .agg(sum("total_price").alias("total_spent")) \
    .sort(desc("total_spent"))

df_total_spent.show()

