import boto3
import requests
import json
import gzip
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET = "retail-intel-bronze"
LATITUDE = 40.7128
LONGITUDE = -74.0060
START_DATE = "2024-01-01"
END_DATE = "2024-06-30"
RAW_S3_KEY = f"weather/raw/weather_{START_DATE}_to_{END_DATE}.json.gz"
TEMP_DIR = Path("ingestion/weather/temp")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

s3 = boto3.client("s3")

def raw_exists_in_s3():
    try:
        s3.head_object(Bucket=BUCKET, Key=RAW_S3_KEY)
        return True
    except Exception:
        return False

def fetch_from_api():
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
            "weathercode"
        ],
        "timezone": "America/New_York"
    }
    print("Fetching weather data from Open-Meteo API...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    print("Weather data fetched successfully.")
    return response.json()

def upload_raw(data, filepath):
    print(f"Saving raw gzipped response to {filepath}...")
    with gzip.open(filepath, "wb") as f:
        f.write(json.dumps(data).encode("utf-8"))
    print(f"Uploading raw to s3://{BUCKET}/{RAW_S3_KEY}...")
    s3.upload_file(str(filepath), BUCKET, RAW_S3_KEY)
    print("Raw file uploaded successfully")
    
def download_raw(filepath):
    print(f"Raw file found in S3 -- downloading to {filepath}...")
    s3.download_file(BUCKET, RAW_S3_KEY, str(filepath))
    print("Raw file downloaded successfully")
    
def load_raw(filepath):
    with gzip.open(filepath, "rb") as f:
        return json.loads(f.read().decode("utf-8"))
    
def daily_exists_in_s3(year, month, day, filename):
    key = f"weather/organized/year={year}/month={month:02d}/day={day:02d}/{filename}"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False
    
def split_and_upload_daily(data):
    daily = data["daily"]
    dates = daily["time"]
    ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"Splitting data into {len(dates)} daily files...")
    
    skipped = 0
    uploaded = 0
    
    for i, date in enumerate(dates):
        dt = datetime.strptime(date, "%Y-%m-%d")
        year = dt.year
        month = dt.month
        day = dt.day
        filename = f"weather_{date}.json"
        
        # skip if already uploaded -- partial upload recovery
        if daily_exists_in_s3(year, month, day, filename):
            print(f"Skipping {filename} -- already in S3")
            skipped += 1
            continue
        
        daily_record = {
            "date": date,
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "timezone": "America/New_York",
            "temperature_max_c": daily["temperature_2m_max"][i],
            "temperature_min_c": daily["temperature_2m_min"][i],
            "precipitation_mm": daily["precipitation_sum"][i],
            "windspeed_max_kmh": daily["windspeed_10m_max"][i],
            "weathercode": daily["weathercode"][i],
            "ingestion_timestamp_utc": ingestion_ts
        }
        
        filepath = TEMP_DIR / filename
        with open(filepath, "w") as f:
            json.dump(daily_record, f, indent=2)
            
        key = f"weather/organized/year={year}/month={month:02d}/day={day:02d}/{filename}"
        s3.upload_file(str(filepath), BUCKET, key)
        filepath.unlink()
        print(f"Uploaded {filename}")
        uploaded += 1
        
    print(f"Done -- {uploaded} uploaded, {skipped} skipped")
    
def main():
    raw_filepath = TEMP_DIR / f"weather_{START_DATE}_to_{END_DATE}.json.gz"
    
    if raw_exists_in_s3():
        print("Raw file already exists in S3 -- skipping API call")
        download_raw(raw_filepath)
    else:
        print("Raw file not found in S3 -- fetching from API")
        data = fetch_from_api()
        upload_raw(data, raw_filepath)
        
    data = load_raw(raw_filepath)
    raw_filepath.unlink()
    print("Cleaned up local raw file")
    
    split_and_upload_daily(data)
    print("Weather data ingestion completed successfully.")
    
if __name__ == "__main__":
    main()