import boto3
import json
from io import StringIO
from datetime import datetime

# Load AWS config once
with open('config/aws_config.json') as f:
    aws_config = json.load(f)

def upload_to_s3(df, filename):
    # Define the S3 client here
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_config['aws_access_key_id'],
        aws_secret_access_key=aws_config['aws_secret_access_key']
    )

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Build the full S3 key (path)
    key = f"{aws_config.get('upload_prefix', '')}/{filename}"

    # Upload the object
    s3.put_object(
        Bucket=aws_config['bucket_name'],
        Key=key,
        Body=csv_buffer.getvalue()
    )

    s3_url = f"https://{aws_config['bucket_name']}.s3.amazonaws.com/{key}"
    print(f"[S3 Upload] Uploaded to: {s3_url}")
    return s3_url
