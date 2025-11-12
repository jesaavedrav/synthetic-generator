#!/usr/bin/env python3
"""
Script to download the Cardiovascular Disease Risk Prediction dataset from Kaggle
Supports both environment variables and ~/.kaggle/kaggle.json for authentication
"""

import os
import sys
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

# You can authenticate using either:
# 1. Environment variables: KAGGLE_USERNAME and KAGGLE_KEY in .env file
# 2. Kaggle config file: ~/.kaggle/kaggle.json


def setup_kaggle_credentials():
    """
    Setup Kaggle credentials from environment variables if available
    This takes precedence over ~/.kaggle/kaggle.json
    """
    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY")

    if username and key:
        print("Using Kaggle credentials from environment variables")
        os.environ["KAGGLE_USERNAME"] = username
        os.environ["KAGGLE_KEY"] = key
        return True

    # Check if kaggle.json exists
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if kaggle_json.exists():
        print(f"Using Kaggle credentials from {kaggle_json}")
        return True

    print("Kaggle credentials not found!")
    print("\nPlease set up authentication using one of these methods:")
    print("\n1. Environment variables (recommended):")
    print("   Add to your .env file:")
    print("   KAGGLE_USERNAME=your_username")
    print("   KAGGLE_KEY=your_api_key")
    print("\n2. Kaggle config file:")
    print("   Download kaggle.json from https://www.kaggle.com/settings")
    print("   Place it at ~/.kaggle/kaggle.json")
    print("   chmod 600 ~/.kaggle/kaggle.json")
    return False


def download_dataset():
    """Download the cardiovascular dataset from Kaggle"""

    # Setup credentials first
    if not setup_kaggle_credentials():
        return False

    dataset_name = "alphiree/cardiovascular-diseases-risk-prediction-dataset"
    data_dir = Path("./data")

    # Create data directory
    data_dir.mkdir(exist_ok=True)

    print(f"Downloading dataset: {dataset_name}")
    print(f"Destination: {data_dir.absolute()}")

    # Download using Kaggle CLI
    import kaggle

    try:
        kaggle.api.dataset_download_files(dataset_name, path=str(data_dir), unzip=True)
        print("Dataset downloaded successfully!")

        # List downloaded files
        print("\nDownloaded files:")
        for file in data_dir.iterdir():
            if file.is_file():
                print(f"  - {file.name} ({file.stat().st_size / 1024 / 1024:.2f} MB)")

        return True

    except Exception as e:
        print(f"Error downloading dataset: {e}")
        print("\nMake sure you have:")
        print("1. Installed kaggle: pip install kaggle")
        print("2. Configured API credentials: ~/.kaggle/kaggle.json")
        print("3. Accepted the dataset terms on Kaggle website")
        return False


def verify_dataset():
    """Verify the downloaded dataset"""
    import pandas as pd

    data_file = Path("./data/CVD_cleaned.csv")

    if not data_file.exists():
        print(f"Dataset file not found: {data_file}")
        return False

    try:
        df = pd.read_csv(data_file)
        print(f"\nDataset verified!")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nFirst few rows:")
        print(df.head())

        return True

    except Exception as e:
        print(f"Error reading dataset: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Cardiovascular Disease Dataset Download Script")
    print("=" * 60)

    if download_dataset():
        verify_dataset()

    print("\n" + "=" * 60)
    print("Next steps:")
    print("1. Copy .env.example to .env")
    print("2. Configure Snowflake credentials in .env")
    print("3. Start services: docker-compose up -d")
    print("4. Train model: POST http://localhost:8000/train")
    print("=" * 60)
