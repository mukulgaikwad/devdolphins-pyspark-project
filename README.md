# PySpark Data Engineer project 1 

This repository contains the implementation of a data processing pipeline for the PySpark Data Engineer assignment. The project consists of two mechanisms, **Mechanism X** and **Mechanism Y**, built using PySpark on Databricks, AWS S3 for storage, and PostgreSQL for temporary data storage. The pipeline processes transaction data, detects specific patterns, and generates outputs as per the assignment requirements.

## Project Overview

The assignment involves processing transaction data (`transactions.csv`) and customer importance data (`CustomerImportance.csv`) to:
1. **Mechanism X**: Read `transactions.csv` from S3, create chunks of 1000 rows every 7 seconds, and upload them to `s3://raw-bank-transactions/chunks/`.
2. **Mechanism Y**: Monitor the chunks, detect three specified patterns (PatId1, PatId2, PatId3), store intermediate data in PostgreSQL, and write detections to S3 in batches of 100 rows.

The solution is implemented in Databricks using PySpark, with PostgreSQL for temporary storage and AWS S3 for input/output storage.

## Folder Structure

```
.
├── mechanism_x/
│   ├── config/
│   │   ├── aws_config.json  # AWS credentials (placeholder)
│   │   └── db_config.json   # PostgreSQL credentials (placeholder)
│   ├── runner.py # Main script for Mechanism X
|   ├── db_handler.py
|   ├── gdrive_reader
|   ├── s3_uploader
|
├── mechanism_y/
│   ├── config/
│   │   ├── aws_config.json  # Reused from mechanism_x
│   │   └── db_config.json   # Reused from mechanism_x
│   ├── chunk_processor.py   # Reads chunks and customer importance data
│   ├── pattern_detector.py  # Detects patterns (PatId1, PatId2, PatId3)
│   ├── s3_writer.py        # Writes detections to S3
│   └── runner.py           # Main script for Mechanism Y
├── README.md               # This file
└── architecture.png        # Architecture diagram
```

## Prerequisites

- **Databricks**: A Databricks workspace with PySpark support.
- **AWS Account**: An AWS account with S3 buckets (`raw-bank-transactions`) and IAM roles for S3 access.
- **PostgreSQL**: A PostgreSQL database (`transaction_pipeline`) accessible from Databricks.
- **Input Files**: `transactions.csv` and `CustomerImportance.csv` uploaded to `s3://raw-bank-transactions/input/`.
- **Dependencies**:
  - PySpark (included in Databricks)
  - PostgreSQL JDBC driver (e.g., `org.postgresql:postgresql:42.2.23`)
  - Python libraries: `psycopg2`, `boto3` (for local testing, optional in Databricks)

## Setup Instructions

1. **AWS Setup**:
   - Create an S3 bucket named `raw-bank-transactions`.
   - Create folders: `input/`, `chunks/`, and `detections/`.
   - Upload `transactions.csv` and `CustomerImportance.csv` to `s3://raw-bank-transactions/input/`.
   - Configure an IAM role with read/write access to the bucket and attach it to Databricks or set up AWS credentials in Databricks secrets:
     ```python
     dbutils.secrets.put("aws", "access_key_id", "<your-access-key>")
     dbutils.secrets.put("aws", "secret_access_key", "<your-secret-key>")
     ```

2. **PostgreSQL Setup**:
   - Set up a PostgreSQL database (`transaction_pipeline`) on a server accessible from Databricks.
   - Create the following tables:
     ```sql
     CREATE TABLE transaction_pointer (
         id SERIAL PRIMARY KEY,
         last_index INT DEFAULT 0,
         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     );
     CREATE TABLE all_transactions (
         CustomerId VARCHAR,
         MerchantId VARCHAR,
         transaction_value DOUBLE PRECISION,
         gender VARCHAR,
         transaction_type VARCHAR,
         YStartTime TIMESTAMP,
         detectionTime TIMESTAMP
     );
     ```
   - Store credentials in `/dbfs/FileStore/config/db_config.json`:
     ```json
     {
         "host": "<your-host>",
         "port": 5432,
         "database": "transaction_pipeline",
         "user": "<your-user>",
         "password": "<your-password>"
     }
     ```

3. **Databricks Setup**:
   - Create a Databricks workspace and cluster (e.g., with Databricks Runtime 10.4 LTS or later).
   - Install the PostgreSQL JDBC driver on the cluster:
     - Go to `Libraries` > `Install New` > `Maven` > Search for `org.postgresql:postgresql:42.2.23`.
   - Upload `mechanism_x/` and `mechanism_y/` folders to Databricks FileStore or a notebook.
   - Configure AWS credentials in the cluster:
     ```python
     spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", dbutils.secrets.get("aws", "access_key_id"))
     spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", dbutils.secrets.get("aws", "secret_access_key"))
     ```

4. **Input Files**:
   - Download `transactions.csv` and `CustomerImportance.csv` from the provided Google Drive link.
   - Upload to `s3://raw-bank-transactions/input/` using AWS CLI or the AWS Console:
     ```bash
     aws s3 cp transactions.csv s3://raw-bank-transactions/input/
     aws s3 cp CustomerImportance.csv s3://raw-bank-transactions/input/
     ```

## Implementation Steps

### Mechanism X: Chunking Transactions
**Objective**: Read `transactions.csv` from S3, create chunks of 1000 rows every 7 seconds, and upload to `s3://raw-bank-transactions/chunks/`.

1. **Read Input**:
   - The script `mechanism_x_runner.py` reads `transactions.csv` from `s3://raw-bank-transactions/input/transactions.csv` using PySpark.
   - A row number is added using a `Window` function ordered by `CustomerId` (or another column if needed).

2. **Chunking Logic**:
   - The `read_next_chunk` function filters rows between `last_index + 1` and `last_index + 1000` to create a chunk of 1000 rows.
   - The `last_index` is retrieved from the `transaction_pointer` table in PostgreSQL.

3. **Upload to S3**:
   - Each chunk is written as a CSV file (e.g., `txn_batch_0_20250531...csv`) to `s3://raw-bank-transactions/chunks/`.
   - The filename includes the `last_index` and a timestamp for uniqueness.

4. **Track Progress**:
   - The `last_index` is updated in the `transaction_pointer` table after each chunk is processed.
   - The script runs every 7 seconds using `time.sleep(7)` (for prototyping; Databricks Workflows is recommended for production).

5. **Error Handling**:
   - Handles empty chunks, S3 access errors, and PostgreSQL connection issues with try-except blocks.

**Files**:
- `mechanism_x_runner.py`: Main script orchestrating chunking, S3 uploads, and PostgreSQL updates.
- `config/db_config.json`: PostgreSQL connection details.

### Mechanism Y: Pattern Detection
**Objective**: Monitor the `chunks/` folder, process chunks, detect patterns, and write detections to `s3://raw-bank-transactions/detections/` in batches of 100.

1. **Monitor Chunks**:
   - The `runner.py` script monitors `s3://raw-bank-transactions/chunks/` for new chunk files using PySpark (or Spark Streaming for production).
   - New chunks are read as they become available.

2. **Process Chunks**:
   - The `chunk_processor.py` script reads chunks and `CustomerImportance.csv` from S3.
   - Adds `YStartTime` and `detectionTime` (in IST) to each chunk.

3. **Store Intermediate Data**:
   - Chunks are appended to the `all_transactions` table in PostgreSQL for incremental pattern detection.

4. **Detect Patterns**:
   - The `pattern_detector.py` script implements three patterns:
     - **PatId1**: Identifies customers in the top 1% by transaction count and bottom 20% by weightage for merchants with >50K transactions. ActionType: `UPGRADE`.
     - **PatId2**: Identifies customers with ≥80 transactions and average transaction value < Rs 20. ActionType: `CHILD`.
     - **PatId3**: Identifies merchants with ≥1000 female customers and female customers < 10% of male customers. ActionType: `DEI-NEEDED`.
   - Uses PySpark aggregations (e.g., `groupBy`, `percentile_approx`, `pivot`) to compute patterns.

5. **Write Detections**:
   - The `s3_writer.py` script combines detections from all patterns.
   - Writes batches of 100 rows to unique CSV files (e.g., `detection_0.csv`) in `s3://raw-bank-transactions/detections/`.
   - Output format: `YStartTime`, `detectionTime`, `patternId`, `ActionType`, `customerName` (CustomerId), `MerchantId`, `Transaction Type` (optional).

**Files**:
- `chunk_processor.py`: Reads and prepares chunk data.
- `pattern_detector.py`: Implements pattern detection logic.
- `s3_writer.py`: Writes detections to S3.
- `runner.py`: Orchestrates Mechanism Y.
- `config/db_config.json`: PostgreSQL connection details (reused).

## How It Works

1. **Data Flow**:
   - **Input**: `transactions.csv` and `CustomerImportance.csv` are uploaded to `s3://raw-bank-transactions/input/`.
   - **Mechanism X**:
     - Reads `transactions.csv` in chunks of 1000 rows.
     - Writes chunks to `s3://raw-bank-transactions/chunks/` every 7 seconds.
     - Updates the `last_index` in the `transaction_pointer` table.
   - **Mechanism Y**:
     - Monitors `chunks/` for new files.
     - Processes chunks, stores them in `all_transactions` table.
     - Detects patterns using data from `all_transactions` and `CustomerImportance.csv`.
     - Writes detections to `s3://raw-bank-transactions/detections/` in batches of 100.

2. **Architecture**:
   - **Databricks**: Runs PySpark scripts for both mechanisms.
   - **S3**: Stores input files, chunks, and detections.
   - **PostgreSQL**: Stores temporary data (`transaction_pointer` for Mechanism X, `all_transactions` for Mechanism Y).
   - See `architecture.png` for a visual representation.

3. **Execution**:
   - Run `mechanism_x/mechanism_x_runner.py` in a Databricks notebook to start chunking.
   - Run `mechanism_y/runner.py` in another notebook to start pattern detection.
   - Both mechanisms run concurrently, with Mechanism Y processing chunks as they appear.

## Assumptions

- **Column Names**:
  - `transactions.csv`: `CustomerId`, `MerchantId`, `transaction_value`, `gender`, `transaction_type`.
  - `CustomerImportance.csv`: `CustomerId`, `transaction_type`, `weightage`.
- **Data Handling**:
  - Missing or null values are dropped.
  - Transactions are ordered by `CustomerId` for chunking.
- **Timezone**: `YStartTime` and `detectionTime` are in IST.
- **Scheduling**: `time.sleep(7)` is used for prototyping; Databricks Workflows is recommended for production.
- **Streaming**: Mechanism Y uses file listing for simplicity; Spark Streaming is suggested for production.

## Running the Code

1. **Upload Files**:
   - Upload `mechanism_x/` and `mechanism_y/` to Databricks FileStore or a notebook.
   - Upload `config/db_config.json` to `/dbfs/FileStore/config/`.

2. **Run Mechanism X**:
   - Open `mechanism_x/mechanism_x_runner.py` in a Databricks notebook.
   - Execute the script to start generating chunks.

3. **Run Mechanism Y**:
   - Open `mechanism_y/runner.py` in another notebook.
   - Execute the script to start processing chunks and generating detections.

4. **Monitor Outputs**:
   - Check `s3://raw-bank-transactions/chunks/` for chunk files.
   - Check `s3://raw-bank-transactions/detections/` for detection files.
   - Query PostgreSQL tables (`transaction_pointer`, `all_transactions`) to verify data.

## Testing

- **Mechanism X**:
  - Tested with a sample `transactions.csv` (5000 rows).
  - Verified chunk files are created every 7 seconds with 1000 rows each.
  - Confirmed `last_index` updates in `transaction_pointer` table.
- **Mechanism Y**:
  - Tested with sample chunks and `CustomerImportance.csv`.
  - Verified correct detection of PatId1, PatId2, and PatId3.
  - Confirmed detection files contain 100 rows with correct columns and IST timestamps.
- **End-to-End**:
  - Ran both mechanisms concurrently to ensure chunks are processed and detections are generated.



## Important 

- **Security**: AWS and PostgreSQL credentials are stored in Databricks secrets for production use.
- **Scalability**: The solution uses PySpark for distributed processing and can handle large datasets.
- **Improvements**:
  - Use Databricks Delta Live Tables for streaming and scheduling.
  - Optimize PostgreSQL writes with partitioning for large datasets.
- **Deadline**: Submitted by June 2, 2025, 3:48 PM IST.

For any questions, contact `<YourFullName>` at `<your-email>`.
