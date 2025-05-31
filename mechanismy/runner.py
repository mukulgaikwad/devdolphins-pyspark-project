from pyspark.sql import SparkSession
from chunk_processor import process_chunk
from pattern_detector import detect_patterns
from s3_writer import write_detections
import time

spark = SparkSession.builder.appName("MechanismY").getOrCreate()
CHUNK_PATH = "s3://raw-bank-transactions/chunks/"
CUSTOMER_IMPORTANCE_PATH = "s3://raw-bank-transactions/input/CustomerImportance.csv"
POSTGRES_TABLE = "all_transactions"

def run():
    batch_id = 0
    while True:
        try:
            # Check for new chunk files (use Spark Streaming or file listing)
            chunk_files = spark.read.csv(CHUNK_PATH, header=True, inferSchema=True)
            if chunk_files.count() == 0:
                print("No new chunks to process.")
                time.sleep(7)
                continue

            # Process chunk
            chunk_df, customer_importance_df = process_chunk(spark, CHUNK_PATH, CUSTOMER_IMPORTANCE_PATH)
            detections_df = detect_patterns(spark, chunk_df, customer_importance_df, POSTGRES_TABLE)
            
            # Write detections
            if detections_df.count() > 0:
                output_path = write_detections(detections_df, batch_id)
                print(f"Written detections to {output_path}")
                batch_id += 1
            else:
                print("No detections found.")
                
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(7)

if __name__ == "__main__":
    run()