# Makefile for Cardiovascular Synthetic Data Generator

.PHONY: help setup install start stop logs clean test test-factory train generate run-api run-consumer run-infra

help:
	@echo "Cardiovascular Synthetic Data Generator - Commands"
	@echo "=================================================="
	@echo "setup        - Initial setup (create dirs, .env)"
	@echo "install      - Install Python dependencies"
	@echo "start        - Start all Docker services"
	@echo "stop         - Stop all Docker services"
	@echo "restart      - Restart all services"
	@echo "logs         - Show service logs"
	@echo "clean        - Clean up containers and volumes"
	@echo "test         - Run API tests"
	@echo "test-factory - Test factory pattern implementation"
	@echo "download     - Download dataset from Kaggle"
	@echo "train        - Start training (async)"
	@echo "train-status - Check training status (requires TASK_ID)"
	@echo "train-tasks  - List all training tasks"
	@echo "generate     - Generate synthetic data"
	@echo ""
	@echo "Local Development (without Docker):"
	@echo "run-infra    - Start only Kafka/Zookeeper (Docker)"
	@echo "run-api      - Run API locally (Python)"
	@echo "run-consumer - Run consumer locally (Python)"
	@echo "=================================================="

setup:
	@echo "Setting up project..."
	chmod +x scripts/setup.sh
	./scripts/setup.sh

install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt

start:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Services started!"
	@echo "API Docs: http://localhost:8000/docs"
	@echo "Kafka UI: http://localhost:8080"

stop:
	@echo "Stopping services..."
	docker-compose down

restart: stop start

logs:
	docker-compose logs -f

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf logs/*.log

test:
	@echo "Running tests..."
	conda run -n synthetic_generator python scripts/test_api.py

test-factory:
	@echo "Testing factory pattern implementation..."
	@echo "Make sure API is running (make run-api or make start)"
	@echo ""
	conda run -n synthetic_generator python scripts/test_factory.py

download:
	@echo "Downloading dataset..."
	python scripts/download_dataset.py

train:
	@echo "Starting async training (this will return immediately)..."
	@curl -X POST http://localhost:8000/train \
		-H "Content-Type: application/json" \
		-d '{"dataset_path": "./data/CVD_cleaned.csv", "epochs": 300}' \
		| python -m json.tool
	@echo "\nUse 'make train-status TASK_ID=<your-task-id>' to check progress"

train-status:
	@echo "Checking training status..."
	@curl -s http://localhost:8000/train/status/$(TASK_ID) | python -m json.tool

train-tasks:
	@echo "Listing all training tasks..."
	@curl -s http://localhost:8000/train/tasks | python -m json.tool

generate:
	@echo "Generating 100 synthetic samples..."
	curl -X POST http://localhost:8000/generate \
		-H "Content-Type: application/json" \
		-d '{"num_samples": 100, "send_to_kafka": true}'

# Local Development Commands
run-infra:
	@echo "Starting Kafka infrastructure (Docker)..."
	docker-compose up -d kafka zookeeper kafka-ui
	@echo ""
	@echo "Kafka infrastructure started!"
	@echo "Kafka: localhost:9092"
	@echo "Kafka UI: http://localhost:8080"

run-api:
	@echo "Starting API locally..."
	@echo "Make sure Kafka is running (make run-infra)"
	@echo ""
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | xargs) && \
		eval "$$(conda shell.zsh hook)" && \
		conda activate synthetic_generator && \
		python -m uvicorn app.main:app --host 0.0.0.0 --port 8000; \
	else \
		echo "Error: .env file not found!"; \
		exit 1; \
	fi

run-consumer:
	@echo "Starting consumer locally..."
	@echo "Make sure Kafka and API are running"
	@echo ""
	@if [ -f .env ]; then \
		export $$(cat .env | grep -v '^#' | xargs) && \
		eval "$$(conda shell.zsh hook)" && \
		conda activate synthetic_generator && \
		python -m consumer.kafka_consumer; \
	else \
		echo "Error: .env file not found!"; \
		exit 1; \
	fi
