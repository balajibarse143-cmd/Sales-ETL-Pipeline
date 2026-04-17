from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize Spark
spark = SparkSession.builder.appName("SalesETL").getOrCreate()

# Extract
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# Transform
df = df.dropna()
df = df.withColumn("total_sales", col("sales") * col("quantity"))

# Aggregate
result = df.groupBy("category").agg(sum("total_sales").alias("total_sales"))

# Load
result.write.mode("overwrite").csv("output/")

# Show result
result.show()
