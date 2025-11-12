#!/bin/bash

# Setup script for Cardiovascular Synthetic Data Generator

set -e

echo "=========================================="
echo "Setting up Cardiovascular Data Generator"
echo "=========================================="

# Create necessary directories
echo "Creating directories..."
mkdir -p data models logs

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
 echo "Creating .env file from template..."
 cp .env.example .env
 echo " Please edit .env file with your Snowflake credentials"
else
 echo ".env file already exists"
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "=========================================="
echo "Setup completed!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Edit .env file with your Snowflake credentials"
echo "2. Download dataset: python scripts/download_dataset.py"
echo "3. Start infrastructure: docker-compose up -d"
echo "4. Access API docs: http://localhost:8000/docs"
echo "5. Access Kafka UI: http://localhost:8080"
echo ""
echo "=========================================="
