from pyspark.sql import SparkSession

def write_detections(detections_df, batch_id):
    output_path = f"s3://raw-bank-transactions/detections/detection_{batch_id}.csv"
    detections_df.limit(100).write.csv(output_path, header=True, mode="overwrite")
    return output_path