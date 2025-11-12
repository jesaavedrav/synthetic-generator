"""
Test script for the factory pattern implementation
"""

import sys
import requests
from loguru import logger

BASE_URL = "http://localhost:8000"


def test_health():
    """Test health endpoint"""
    logger.info("Testing health endpoint...")
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200, f"Health check failed: {response.status_code}"
    data = response.json()
    logger.success(f"✓ Health check passed: {data['status']}")


def test_available_methods():
    """Test available methods endpoint"""
    logger.info("Testing /train/methods endpoint...")
    response = requests.get(f"{BASE_URL}/train/methods")
    assert response.status_code == 200, f"Methods endpoint failed: {response.status_code}"
    data = response.json()
    logger.info(f"Available methods: {data}")
    assert "methods" in data, "Response missing 'methods' field"
    assert "default_method" in data, "Response missing 'default_method' field"
    assert data["default_method"] == "ctgan", f"Unexpected default method: {data['default_method']}"
    logger.success(f"✓ Found {len(data['methods'])} available methods")
    for method in data["methods"]:
        logger.info(f"  - {method['method']}: {method['name']} - {method['description']}")


def test_list_datasets():
    """Test datasets listing endpoint"""
    logger.info("Testing /datasets endpoint...")
    response = requests.get(f"{BASE_URL}/datasets")
    assert response.status_code == 200, f"Datasets endpoint failed: {response.status_code}"
    data = response.json()
    logger.info(f"Found {data['total_count']} datasets")
    logger.success(f"✓ Datasets endpoint working")
    for ds in data["datasets"]:
        logger.info(f"  - {ds['name']} ({ds['size_bytes']} bytes)")


def test_train_with_method():
    """Test training with explicit method"""
    logger.info("Testing training with explicit method...")

    # Test with invalid method
    logger.info("Testing invalid method rejection...")
    response = requests.post(
        f"{BASE_URL}/train",
        json={"method": "invalid_method", "model_name": "test_model", "epochs": 10},
    )
    assert response.status_code == 400, "Should reject invalid method"
    logger.success("✓ Invalid method correctly rejected")

    # Test with valid method (CTGAN)
    logger.info("Testing valid CTGAN method...")
    response = requests.post(
        f"{BASE_URL}/train",
        json={
            "dataset_path": "./data/CVD_cleaned.csv",
            "model_name": "test_ctgan_model",
            "method": "ctgan",
            "epochs": 10,
            "overwrite_existing": True,
        },
    )

    if response.status_code == 200:
        data = response.json()
        logger.success(f"✓ Training started: {data['message']}")
        logger.info(f"Task ID: {data['task_id']}")
        return data["task_id"]
    else:
        logger.warning(f"Training request failed: {response.status_code} - {response.text}")
        return None


def test_model_exists_check():
    """Test that model existence is properly checked"""
    logger.info("Testing model existence check...")

    # This should fail if model already exists
    response = requests.post(
        f"{BASE_URL}/train",
        json={
            "dataset_path": "./data/CVD_cleaned.csv",
            "model_name": "cardiovascular_model",
            "method": "ctgan",
            "epochs": 10,
            "overwrite_existing": False,  # Don't overwrite
        },
    )

    if response.status_code == 409:
        logger.success("✓ Correctly detected existing model and rejected overwrite")
    elif response.status_code == 200:
        logger.info("No existing model found, training initiated")
    else:
        logger.warning(f"Unexpected status: {response.status_code}")


def main():
    """Run all tests"""
    logger.info("Starting factory pattern tests...")
    logger.info(f"Testing API at {BASE_URL}")
    print("-" * 60)

    try:
        test_health()
        print()

        test_available_methods()
        print()

        test_list_datasets()
        print()

        test_model_exists_check()
        print()

        task_id = test_train_with_method()
        print()

        logger.success("All tests completed!")

        if task_id:
            logger.info(f"\nTo monitor training progress, run:")
            logger.info(f"  python scripts/monitor_training.py {task_id}")

    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        logger.error(f"Could not connect to {BASE_URL}. Is the API running?")
        logger.info("Start the API with: make run-api")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
