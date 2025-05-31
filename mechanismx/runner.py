import time
from datetime import datetime
from gdrive_reader import read_next_chunk
from s3_uploader import upload_to_s3
from db_handler import get_last_index, update_last_index

# Replace with your actual public file link from Google Drive
GDRIVE_URL = "https://drive.google.com/file/d/1AGXVlDhbMbhoGXDJG0IThnqz86Qy3hqb/view?usp=drive_link"


def run():
    while True:
        try:
            last_index = int(get_last_index())  # ✅ FIXED HERE
            df = read_next_chunk(GDRIVE_URL, last_index)
            if df.empty:
                print("No more data to read.")
                break

            filename = f"txn_batch_{last_index}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            upload_to_s3(df, filename)
            update_last_index(last_index + len(df))

            print(f"Uploaded {filename} to S3 with {len(df)} rows.")
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(7)


if __name__ == "__main__":
    run()