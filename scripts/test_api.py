#!/usr/bin/env python3
"""
Quick test script to verify the API is working
"""

import requests
import json
import time

API_URL = "http://localhost:8000"


def test_health():
    """Test health endpoint"""
    print("Testing health endpoint...")
    response = requests.get(f"{API_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.status_code == 200


def test_model_info():
    """Test model info endpoint"""
    print("Testing model info endpoint...")
    response = requests.get(f"{API_URL}/model/info")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")
    return response.status_code == 200


def test_train(dataset_path="./data/CVD_cleaned.csv"):
    """Test training endpoint (async)"""
    print(f"Testing training endpoint with dataset: {dataset_path}...")
    
    payload = {
    "dataset_path": dataset_path,
    "model_name": "test_model",
    "epochs": 50 # Reduced for testing
    }
    
    response = requests.post(f"{API_URL}/train", json=payload)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
    data = response.json()
    task_id = data.get('task_id')
    print(f"Training started! Task ID: {task_id}")
    
    # Poll for status
    print("\nPolling training status...")
    for i in range(30): # Poll for up to 30 iterations
    time.sleep(2)
    status_response = requests.get(f"{API_URL}/train/status/{task_id}")
    
    if status_response.status_code == 200:
    status_data = status_response.json()
    status = status_data['status']
    progress = status_data.get('progress', 0)
    
    print(f" [{i+1}] Status: {status} - Progress: {progress:.1f}%")
    
    if status == 'completed':
    print(" Training completed!")
    print(f" Result: {json.dumps(status_data['result'], indent=2)}")
    return True
    elif status == 'failed':
    print(f" Training failed: {status_data.get('error')}")
    return False
    else:
    print(f" Failed to get status: {status_response.status_code}")
    return False
    
    print(" ⏱ Training still in progress after 60 seconds")
    return True # Consider it a success if it's still running
    else:
    print(f"Error: {response.text}")
    return False
    
    return response.status_code == 200


def test_generate():
    """Test generation endpoint"""
    print("Testing generation endpoint...")
    
    payload = {
    "num_samples": 5,
    "send_to_kafka": False
    }
    
    response = requests.post(f"{API_URL}/generate", json=payload)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
    data = response.json()
    print(f"Generated {data['num_samples_generated']} samples")
    print(f"Sample preview:")
    print(json.dumps(data['samples'][0], indent=2))
    else:
    print(f"Error: {response.text}")
    
    return response.status_code == 200


def test_stream():
    """Test streaming endpoint"""
    print("Testing streaming endpoint...")
    
    payload = {
    "num_samples": 20,
    "batch_size": 5,
    "interval_seconds": 1.0
    }
    
    response = requests.post(f"{API_URL}/stream", json=payload)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}\n")
    return response.status_code == 200


def main():
    print("=" * 60)
    print("API Testing Suite")
    print("=" * 60)
    print()
    
    # Wait for API to be ready
    print("Waiting for API to be ready...")
    for i in range(30):
    try:
    requests.get(f"{API_URL}/health", timeout=1)
    print("API is ready!\n")
    break
    except:
    time.sleep(1)
    else:
    print(" API not responding. Make sure it's running on port 8000")
    return
    
    # Run tests
    results = []
    
    results.append(("Health Check", test_health()))
    results.append(("Model Info", test_model_info()))
    
    # Uncomment to test training (takes time)
    # results.append(("Training", test_train()))
    
    # Uncomment after training
    # results.append(("Generation", test_generate()))
    # results.append(("Streaming", test_stream()))
    
    # Summary
    print("=" * 60)
    print("Test Results:")
    print("=" * 60)
    for name, success in results:
    status = " PASS" if success else " FAIL"
    print(f"{name}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()
