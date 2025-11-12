#!/usr/bin/env python3
"""
Script to monitor training progress
Usage: python scripts/monitor_training.py <task_id>
"""

import sys
import time
import requests
import json
from datetime import datetime


def monitor_training(task_id: str, api_url: str = "http://localhost:8000"):
    """Monitor training progress"""
    
    print(f"Monitoring training task: {task_id}")
    print("=" * 60)
    
    start_time = datetime.now()
    last_progress = 0
    
    while True:
    try:
    response = requests.get(f"{api_url}/train/status/{task_id}")
    
    if response.status_code == 404:
    print(f" Task {task_id} not found")
    return False
    
    if response.status_code != 200:
    print(f" Error: {response.status_code}")
    time.sleep(5)
    continue
    
    data = response.json()
    status = data['status']
    progress = data.get('progress', 0)
    message = data['message']
    
    # Calculate elapsed time
    elapsed = (datetime.now() - start_time).total_seconds()
    elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
    
    # Only print if progress changed
    if progress != last_progress:
    # Create progress bar
    bar_length = 40
    filled = int(bar_length * progress / 100)
    bar = "" * filled + "" * (bar_length - filled)
    
    print(f"\r[{bar}] {progress:.1f}% | {elapsed_str} | {message}", end="", flush=True)
    last_progress = progress
    
    # Check if completed or failed
    if status == 'completed':
    print("\n")
    print("=" * 60)
    print(" Training Completed!")
    print("=" * 60)
    
    result = data.get('result', {})
    print(f"\nModel Path: {result.get('model_path')}")
    print(f"Training Time: {result.get('training_time', 0):.2f} seconds")
    print(f"Dataset Rows: {result.get('dataset_rows', 0):,}")
    
    return True
    
    elif status == 'failed':
    print("\n")
    print("=" * 60)
    print(" Training Failed!")
    print("=" * 60)
    print(f"\nError: {data.get('error')}")
    
    return False
    
    # Wait before next poll
    time.sleep(2)
    
    except KeyboardInterrupt:
    print("\n\n⏸ Monitoring stopped (training continues in background)")
    print(f" Resume with: python scripts/monitor_training.py {task_id}")
    return None
    
    except Exception as e:
    print(f"\n Error: {e}")
    time.sleep(5)


def main():
    if len(sys.argv) < 2:
    print("Usage: python scripts/monitor_training.py <task_id>")
    print("\nExample:")
    print(" python scripts/monitor_training.py 550e8400-e29b-41d4-a716-446655440000")
    sys.exit(1)
    
    task_id = sys.argv[1]
    api_url = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8000"
    
    result = monitor_training(task_id, api_url)
    
    if result is True:
    sys.exit(0)
    elif result is False:
    sys.exit(1)
    else:
    sys.exit(2)


if __name__ == "__main__":
    main()
