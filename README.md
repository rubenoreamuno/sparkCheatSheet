# Spark Cheat Sheet for Data Engineers

This repository is a collection of PySpark code examples, best practices, and cheat sheets for data engineers working with Apache Spark, with a focus on Databricks.

## Table of Contents

- [PySpark Examples](./pyspark_examples)
  - [01 - Basic Transformations](./pyspark_examples/01_basic_transformations.py)
  - [02 - Joins and Aggregations](./pyspark_examples/02_joins_and_aggregations.py)
  - [03 - Window Functions](./pyspark_examples/03_window_functions.py)
- [Best Practices](./best_practices)
  - [PySpark Best Practices](./best_practices/pyspark_best_practices.md)
- [Delta Live Tables (DLT)](./dlt-batch-files.md)
- [Databricks Auto Loader](./ingestion.md)
- [PySpark + Databricks Cheatsheet](./spark_databricks_cheatsheet.md)

## How to Use

1. **Clone the repository:**
   ```bash
   git clone https://github.com/rubenoreamuno/sparkCheatSheet.git
   ```
2. **Explore the examples:** The `pyspark_examples` directory contains several Python scripts with detailed examples of common PySpark operations. You can run these scripts in your Databricks environment or a local Spark installation.
3. **Review the best practices:** The `best_practices` directory contains a guide with tips for writing efficient and maintainable PySpark code.
4. **Use the cheat sheets:** The root directory contains several Markdown files with quick references for DLT, Auto Loader, and general PySpark commands.

## Sample Data

The `data` directory contains a sample CSV file (`sample_data.csv`) that is used in the PySpark examples.