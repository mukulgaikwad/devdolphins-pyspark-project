from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def process_chunk(spark, chunk_path, customer_importance_path):
    # Read chunk
    chunk_df = spark.read.csv(chunk_path, header=True, inferSchema=True)
    # Read customer importance
    customer_importance_df = spark.read.csv(customer_importance_path, header=True, inferSchema=True)
    # Add timestamps
    chunk_df = chunk_df.withColumn("YStartTime", current_timestamp()).withColumn("detectionTime", current_timestamp())
    return chunk_df, customer_importance_df