import boto3
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET = "retail-intel-bronze"
MONTHS = [
    (2024, 1), (2024, 2), (2024, 3),
    (2024, 4), (2024, 5), (2024, 6)
]
TEMP_DIR = Path("ingestion/taxi/temp")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

def download_taxi(year, month):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    filepath = TEMP_DIR / filename
    print(f"Downloading {filename}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {filepath}")
    return filepath

def upload_to_s3(filepath, year, month):
    # boto3 auto-detects credentials from ~/.aws/credentials (set via aws configure)
    # To use explicit keys instead: boto3.client("s3",
    #   aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    #   aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    #   region_name=os.getenv("AWS_REGION"))
    s3 = boto3.client("s3")
    key = f"taxi/year={year}/month={month:02d}/{filepath.name}"
    print(f"Uploading to s3://{BUCKET}/{key}...")
    s3.upload_file(str(filepath), BUCKET, key)
    print(f"Uploaded successfully")
    filepath.unlink()  # deletes the file at filepath, equivalent to os.remove(filepath)
    print(f"Cleaned up local file")
    
def main():
    for year, month in MONTHS:
        filename = download_taxi(year, month)
        upload_to_s3(filename, year, month)
    print("All taxi data ingested successfully.")
    
if __name__ == "__main__":
    main()