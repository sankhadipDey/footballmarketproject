from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("GCS to HDFS Transfer") \
    .getOrCreate()

# Set the GCS and HDFS paths
gcs_path = "gs://transfermarket/sourcedata/appearances.csv"
hdfs_path = "/user/hadoop/trasansfermarket/"

# Read the table from GCS
df = spark.read.format("csv").option("header", "true").load(gcs_path)

# Write the table to HDFS
df.write.option("compression","snappy").mode("overwrite").format("parquet").saveAsTable('transfermarket_analysis.appearances',path=hdfs_path)

# Stop the SparkSession
spark.stop()

