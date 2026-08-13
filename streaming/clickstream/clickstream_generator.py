import json
import gzip
import uuid
import random
import time
import boto3
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ────────────────────────────────────────────────────────────────────────────────────
LOCAL_MODE = True # True = save locally; False = upload to S3 (EC2)
BUCKET = "retail-intel-bronze"
BATCH_INTERVAL_SECONDS = 30
EVENTS_PER_BATCH = 50
LOCAL_OUTPUT_DIR = Path("streaming/clickstream/local_output")

# ── Static Data ────────────────────────────────────────────────────────────────────────────────────
CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Beauty", "Food"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "remove_from_cart"]
EVENT_WEIGHTS = [0.60, 0.20, 0.15, 0.05]

NYC_STORES = [
    {"store_id": "NYC-001", "borough": "MANHATTAN"},
    {"store_id": "NYC-002", "borough": "BROOKLYN"},
    {"store_id": "NYC-003", "borough": "QUEENS"},
    {"store_id": "NYC-004", "borough": "BRONX"},
    {"store_id": "NYC-005", "borough": "STATEN ISLAND"},
]

# ── Event Generation ────────────────────────────────────────────────────────────────────────────────────
def generate_event():
    """Generate a single synthetic retail clickstream event."""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    store = random.choice(NYC_STORES)
    price = round(random.uniform(5.0, 500.0), 2)
    quantity = random.randint(1, 5) if event_type == "purchase" else 1
    revenue = round(price * quantity, 2) if event_type == "purchase" else 0.0
    now = datetime.now(timezone.utc)
    
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "product_id": f"PROD-{random.randint(1000, 9999)}",
        "category": random.choice(CATEGORIES),
        "price": price,
        "quantity": quantity,
        "revenue": revenue,
        "store_id": store["store_id"],
        "borough": store["borough"],
        "event_timestamp_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "business_date": now.strftime("%Y-%m-%d")
    }
    

def generate_batch():
    """Generate a batch of synthetic events."""
    return [generate_event() for _ in range(EVENTS_PER_BATCH)]


# ── Storage ────────────────────────────────────────────────────────────────────────────────────
def build_s3_key(now):
    """Build partitioned S3 key from current timestamp."""
    return (
        f"clickstream/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day}/hour={now.hour:02d}/"
        f"events_{now.strftime('%Y%m%d_%H%M%S')}.json.gz"
    )
    

def build_local_path(now):
    """Build partitioned local path mirroring S3 structure."""
    folder = (
        LOCAL_OUTPUT_DIR /
        f"year={now.year}" /
        f"month={now.month:02d}" /
        f"day={now.day:02d}" /
        f"hour={now.hour:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"events_{now.strftime('%Y%m%d_%H%M%S')}.json.gz"

def compress_batch(events):
    """Gzip compress a list of events to bytes."""
    return gzip.compress(json.dumps(events).encode("utf-8"))


def save_locally(events, now):
    """Save compressed batch to local output directory."""
    filepath = build_local_path(now)
    with open(filepath, "wb") as f:
        f.write(compress_batch(events))
    print(f" Saved locally → {filepath}")
    
    
def upload_to_s3_with_retry(events, now, max_retries=3, backoff=2):
    """Upload compressed batch to S3 with exponential backoff retry."""
    s3 = boto3.client("s3")
    key = build_s3_key(now)
    compressed = compress_batch(events)
    
    for attempt in range(1, max_retries + 1):
        try:
            s3.put_object(Bucket=BUCKET, Key=key, Body=compressed)
            print(f" Upload to s3://{BUCKET}/{key}")
            return
        except Exception as e:
            if attempt == max_retries:
                raise RuntimeError(
                    f"S3 upload failed after {max_retries} attempts: {e}"
                )
            wait = backoff ** attempt
            print(f" Upload attempt {attempt} failed -- retrying in {wait}s...")
            time.sleep(wait)
            
            
def store_batch(events, now):
    """Route batch to local storage or S3 based on LOCAL_MODE flag."""
    if LOCAL_MODE:
        save_locally(events, now)
    else:
        upload_to_s3_with_retry(events, now)
        
        
# ── Main Loop ────────────────────────────────────────────────────────────────────────────────────
def main():
    """Run continuous event generation loop until manually stopped."""
    mode = "LOCAL" if LOCAL_MODE else "S3"
    print(f"Starting clicksteam generator -- mode: {mode}")
    print(f"Batch size: {EVENTS_PER_BATCH} events every {BATCH_INTERVAL_SECONDS}s")
    print("Press Ctrl+C to stop\n")
    
    batch_count = 0
    
    while True:
        try:
            now = datetime.now(timezone.utc)
            events = generate_batch()
            batch_count += 1
            
            print(f"Batch {batch_count} -- {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f" Generated {len(events)} events")
            
            store_batch(events, now)
            
            purchase_count = sum(1 for e in events if e["event_type"] == "purchase")
            revenue = sum(e["revenue"] for e in events)
            print(f" Purchase: {purchase_count} | Revenue: ${revenue:.2f}")
            print(f" Sleeping {BATCH_INTERVAL_SECONDS}s...\n")
            
            time.sleep(BATCH_INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            print(f"\nGenerator stopped -- {batch_count} batches written")
            break
        except Exception as e:
            print(f"Error in batch {batch_count}: {e} -- continuing...")
            time.sleep(BATCH_INTERVAL_SECONDS)
            
            
if __name__ == "__main__":
    main()