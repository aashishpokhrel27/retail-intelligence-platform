import boto3
import requests
import json
import gzip
import time
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET = "retail-intel-bronze"
BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
START_DATE = "2024-01-01"
END_DATE = "2024-01-31"
PAGE_SIZE = 1000
MAX_RETRIES = 3
RETRY_BACKOFF = 2 # seconds -- doubles on each retry
TEMP_DIR = Path("ingestion/complaints/temp")
TEMP_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_FILE = TEMP_DIR / "checkpoint.json"

s3 = boto3.client("s3")


def load_checkpoint():
    """Load checkpoint file tracking completed pages and uploaded dates."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"completed_pages": [], "uploaded_dates": []}


def save_checkpoint(checkpoint):
    """Persist checkpoint to local file after each successful page."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)
        

def raw_page_exists_in_s3(page_num):
    """Check if a raw page file already exists in S3."""
    key = f"complaints/raw/complaints_page_{page_num:04d}.json.gz"
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False
    

def daily_organized_exists_in_s3(date_str):
    """Check if organized daily file already exists in S3."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    key = (
        f"complaints/organized/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"complaints_{date_str}.json.gz"
    )
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False
    
    
def fetch_page_with_retry(offset):
    """Fetch one page from Socrata API with exponential backoff on failure."""
    params =  {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$where": (
            f"created_date >= '{START_DATE}T00:00:00.000' "
            f"AND created_date <= '{END_DATE}T23:59:59.999'"
        ),
        "$select": (
            "unique_key,created_date,complaint_type,"
            "descriptor,incident_zip,borough"
        ),
        "$order": "created_date ASC"
    }
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f" Fetching offset {offset} (attempt {attempt})...")
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Failed to fetch offset {offset} after {MAX_RETRIES} attempts: {e}"
                )
            wait = RETRY_BACKOFF ** attempt
            print(f" Attempt {attempt} failed -- retrying in {wait}s...")
            time.sleep(wait)
            
            
def upload_raw_page_with_retry(data, page_num):
    """Gzip and upload one raw page to S3 with retry on failure."""
    key = f"complaints/raw/complaints_page_{page_num:04d}.json.gz"
    filepath = TEMP_DIR / f"complaints_page_{page_num:04d}.json.gz"
    
    with gzip.open(filepath, "wb") as f:
        f.write(json.dumps(data).encode("utf-8"))
        
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3.upload_file(str(filepath), BUCKET, key)
            filepath.unlink()
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                filepath.unlink()
                raise RuntimeError(
                    f"Failed to upload page {page_num} after {MAX_RETRIES} attempts: {e}"
                )
            wait = RETRY_BACKOFF ** attempt
            print(f" Upload attempt {attempt} failed -- retrying in {wait}s...")
            time.sleep(wait)
            
            
def download_raw_page(page_num):
    """Download existing raw page from s3 to temp directory."""
    key = f"complaints/raw/complaints_page_{page_num:04d}.json.gz"
    filepath = TEMP_DIR / f"complaints_page_{page_num:04d}.json.gz"
    s3.download_file(BUCKET, key, str(filepath))
    with gzip.open(filepath, "rb") as f:
        data = json.loads(f.read().decode("utf-8"))
    filepath.unlink()
    return data


def group_records_by_data(records):
    """Group complaint records by their created_date (YYYY-MM-DD)."""
    grouped = {}
    for record in records:
        date_str = record.get("created_date", "")[:10]
        if not date_str:
            continue
        if date_str not in grouped:
            grouped[date_str] = []
        grouped[date_str].append(record)
    return grouped


def upload_daily_organized_with_retry(date_str, records, ingestion_ts):
    """Gzip and upload organized daily complaints file to s3 with retry."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    key = (
        f"complaints/organized/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"complaints_{date_str}.json.gz"
    )
    filepath = TEMP_DIR / f"complaints_{date_str}.json.gz"
    
    enriched = [
        {**record, "ingestion_timestamp_utc": ingestion_ts}
        for record in records
    ]
    
    with gzip.open(filepath, "wb") as f:
        f.write(json.dumps(enriched).encode("utf-8"))
        
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3.upload_file(str(filepath), BUCKET, key)
            filepath.unlink()
            print(f" Uploaded organized file for {date_str} ({len(records)})")
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                filepath.unlink()
                raise RuntimeError(
                    f"Failed to upload organized {date_str} after {MAX_RETRIES} attempts: {e}"
                )
            wait = RETRY_BACKOFF ** attempt
            print(f" Upload attempt {attempt} failed -- retrying in {wait}s...")
            time.sleep(wait)
            
            
def ingest_pages(checkpoint):
    """Paginate through Socrata API, fetch all complaint records, upload raw pages."""
    offset = 0
    page_num = 1
    all_records = []
    
    while True:
        print(f"\nPage {page_num} (offset {offset}):")
        
        if page_num in checkpoint["completed_pages"]:
            print(f" Checkpoint found -- loading page {page_num} from S3")
            records = download_raw_page(page_num)
        elif raw_page_exists_in_s3(page_num):
            print(f" Page {page_num} exists in S3 -- downloading")
            records = download_raw_page(page_num)
            checkpoint["completed_pages"].append(page_num)
            save_checkpoint(checkpoint)
        else:
            records = fetch_page_with_retry(offset)
            if not records:
                print(f" Empty response -- all pages fetched")
                break
            upload_raw_page_with_retry(records, page_num)
            checkpoint["completed_pages"].append(page_num)
            save_checkpoint(checkpoint)
            print(f" Fetched and uploaded {len(records)} records")
            
        if not records:
            break
        
        all_records.extend(records)
        offset += PAGE_SIZE
        page_num += 1
        
    return all_records


def organize_and_upload(all_records, checkpoint):
    """Group records by date and upload organized daily files to S3."""
    ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    grouped = group_records_by_data(all_records)
    
    print(f"\nOrganizing {len(all_records)} records across {len(grouped)} dates...")
    
    skipped = 0
    uploaded = 0
    
    for date_str in sorted(grouped.keys()):
        if date_str in checkpoint["uploaded_dates"]:
            print(f" Skipping {date_str} -- already uploaded (checkpoint)")
            skipped += 1
            continue
        
        if daily_organized_exists_in_s3(date_str):
            print(f" Skipping {date_str} -- already in S3")
            checkpoint["uploaded_dates"].append(date_str)
            save_checkpoint(checkpoint)
            skipped += 1
            continue
        
        upload_daily_organized_with_retry(
            date_str, grouped[date_str], ingestion_ts
        )
        checkpoint["uploaded_dates"].append(date_str)
        save_checkpoint(checkpoint)
        uploaded += 1
        
    print(f"\nDone -- {uploaded} uploaded, {skipped} skipped")
    
    
def main():
    """Main entry point -- orchestrates full 311 complaints ingestion pipeline."""
    print("Starting NYC 311 Complaints ingestion...")
    checkpoint = load_checkpoint()
    print(f"Checkpoint loaded -- {len(checkpoint['completed_pages'])} pages previously completed")
    
    all_records = ingest_pages(checkpoint)
    print(f"\nTotal records fetched: {len(all_records)}")
    
    organize_and_upload(all_records, checkpoint)
    
    print("\n311 Complaints ingestion completed successfully")
    
    

if __name__ == "__main__":
    main()