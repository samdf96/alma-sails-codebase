# ============================================
# STEP 1: Create a work pool (run in terminal)
# ============================================
# prefect work-pool create "test-pool" --type process

# ============================================
# STEP 2: Start a worker (run in separate terminal)
# ============================================
# prefect worker start --pool "test-pool"

# ============================================
# STEP 3: Test Flow Script (save as test_flow.py)
# ============================================

import argparse
import random
import time

from prefect import flow, task


@task(name="Fetch Data", log_prints=True)
def fetch_data(source: str):
    """Simulate fetching data from a source"""
    print(f"🔍 Fetching data from {source}...")
    time.sleep(2)
    data_size = random.randint(100, 1000)
    print(f"✅ Fetched {data_size} records from {source}")
    return data_size

@task(name="Process Data", log_prints=True)
def process_data(data_size: int):
    """Simulate processing the data"""
    print(f"⚙️  Processing {data_size} records...")
    time.sleep(3)
    processed = data_size * 0.95  # simulate some filtering
    print(f"✅ Processed {int(processed)} records (filtered {data_size - int(processed)})")
    return int(processed)

@task(name="Save Results", log_prints=True)
def save_results(processed_count: int, destination: str):
    """Simulate saving results"""
    print(f"💾 Saving {processed_count} records to {destination}...")
    time.sleep(2)
    print(f"✅ Successfully saved to {destination}")
    return f"Saved {processed_count} records"

@flow(name="Data Pipeline Demo", log_prints=True)
def data_pipeline(source: str = "API", destination: str = "Database"):
    """
    A demo data pipeline that shows tasks flowing through Prefect.
    Watch the UI to see tasks execute in real-time!
    """
    print(f"🚀 Starting data pipeline: {source} -> {destination}")
    
    # Execute tasks in sequence
    data_size = fetch_data(source)
    processed = process_data(data_size)
    result = save_results(processed, destination)
    
    print(f"🎉 Pipeline complete! {result}")
    return result

# ============================================
# STEP 4: Run the flow directly (for testing)
# ============================================
if __name__ == "__main__":
    
    data_pipeline()
