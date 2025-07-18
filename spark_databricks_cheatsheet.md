# 🚀 PySpark + Databricks Cheatsheet

## 📦 1. Inicialización y Contexto

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

> En Databricks, ya tienes `spark` inicializado por defecto.

---

## 📁 2. Lectura y escritura de datos

### Leer archivos
```python
df = spark.read.format("csv").option("header", True).load("/mnt/data/myfile.csv")
df_parquet = spark.read.parquet("/mnt/data/myfile.parquet")
```

### Escribir archivos
```python
df.write.format("parquet").mode("overwrite").save("/mnt/output/")
df.write.csv("/mnt/output/", header=True, mode="overwrite")
```

---

## 🔍 3. Exploración de datos

```python
df.show()
df.printSchema()
df.describe().show()
df.columns
df.count()
```

---

## 🧪 4. Transformaciones comunes

```python
from pyspark.sql.functions import col, when, lit

df.select("nombre", "edad")
df.filter(col("edad") > 18)
df.withColumn("mayor_de_edad", col("edad") > 18)
df.withColumn("estado", when(col("score") > 80, "aprobado").otherwise("reprobado"))

df.drop("columna1")
df.distinct()
df.orderBy("edad", ascending=False)
```

---

## 🔗 5. Joins

```python
df1.join(df2, on="id", how="inner")
df1.join(df2, df1.id == df2.user_id, "left")
```

---

## 🧱 6. GroupBy y agregaciones

```python
from pyspark.sql.functions import sum, avg, count

df.groupBy("categoria").agg(
    count("*").alias("total"),
    avg("precio").alias("precio_prom")
)
```

---

## 🧠 7. Funciones comunes

```python
from pyspark.sql.functions import *

current_date()
year(col("fecha"))
concat_ws("-", col("a"), col("b"))
regexp_replace("col", "\\s+", "")
```

---

## 🪪 8. Manejo de nulls

```python
df.dropna()
df.fillna({"edad": 0, "nombre": "N/A"})
df.na.replace("None", "Desconocido")
```

---

## 🔥 9. Escritura en Delta Lake

```python
df.write.format("delta").mode("overwrite").save("/mnt/output/delta/")
df_delta = spark.read.format("delta").load("/mnt/output/delta/")
```

---

## 🧼 10. Optimización

```python
df.cache()
df.persist(StorageLevel.DISK_ONLY)
df.repartition(4)
df.coalesce(1)
```

---

## 📊 11. SQL en Spark

```python
df.createOrReplaceTempView("ventas")

spark.sql("""
  SELECT categoria, SUM(monto) 
  FROM ventas 
  GROUP BY categoria
""").show()
```

---

## 📅 12. Fechas y tiempos

```python
from pyspark.sql.functions import to_date, to_timestamp

df.withColumn("fecha", to_date("string_fecha", "yyyy-MM-dd"))
df.withColumn("hora", to_timestamp("string_hora", "yyyy-MM-dd HH:mm:ss"))
```

---

## 🧱 13. Esquemas definidos

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True)
])

df = spark.read.schema(schema).csv("/mnt/data/file.csv")
```

---

## 🧪 14. Escribir tabla a metastore (Unity Catalog)

```python
df.write.mode("overwrite").saveAsTable("catalog.schema.nombre_tabla")
```
