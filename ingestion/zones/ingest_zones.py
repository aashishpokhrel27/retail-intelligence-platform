import boto3
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET = "retail-intel-bronze"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
FILENAME = "taxi_zone_lookup.csv"
S3_KEY = f"zones/{FILENAME}"
TEMP_DIR = Path("ingestion/zones/temp")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

def download_zones():
    filepath = TEMP_DIR / FILENAME
    print(f"Downloading {FILENAME}...")
    response = requests.get(ZONE_URL)
    response.raise_for_status()
    with open(filepath, "wb") as f:
        f.write(response.content)
    print(f"Downloaded {filepath}")
    return filepath

def upload_to_s3(filepath):
    s3 = boto3.client("s3")
    print(f"Uploading to s3://{BUCKET}/{S3_KEY}...")
    s3.upload_file(str(filepath), BUCKET, S3_KEY)
    print(f"Uploaded successfully")
    filepath.unlink()
    print(f"Cleaned up {FILENAME}")

def main():
    filepath = download_zones()
    upload_to_s3(filepath)
    print("Zone lookup ingestion completed successfully.")

if __name__ == "__main__":
    main()