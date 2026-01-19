# AI Agent Instructions - Cardiovascular Synthetic Data Generator

## Project Overview

This is a **production-ready ML pipeline** for generating synthetic cardiovascular health data using CTGAN (Conditional Tabular GAN). The system streams synthetic data through Kafka to Snowflake for analytics.

**Architecture**: FastAPI → CTGAN Model → Kafka → Snowflake
**Dataset**: 308K+ cardiovascular disease records from Kaggle
**Key Constraint**: Training takes 5-30 minutes, requires async handling

## Critical Architectural Decisions

### 1. Async Training Pattern (MOST IMPORTANT)
Training is **non-blocking** with progress tracking. Never create synchronous training endpoints.

- Training happens in `BackgroundTasks` (FastAPI background jobs)
- Progress tracked via `app/utils/task_tracker.py` - in-memory task store
- Progress callbacks from CTGAN training loop update tracker
- Endpoints: `POST /train` (returns task_id), `GET /train/status/{task_id}`, `GET /train/tasks`

**Example flow:**
```python
# main.py creates task, adds to background
task_id = str(uuid.uuid4())
task = task_tracker.create_task(task_id, request.dict())
background_tasks.add_task(train_background, task_id, request)
# Returns immediately with task_id

# train_background() function calls generator.train() with progress_callback
def progress_callback(pct):
    task_tracker.update_task_progress(task_id, pct)
```

### 2. CTGAN Training Mechanics
- Uses SDV library's `CTGANSynthesizer` (not raw CTGAN)
- Progress estimation: 5% data load → 10% metadata → 15% init → 20-90% training → 95% save
- **No native progress from SDV** - we estimate based on time/epochs
- Model saved as pickle to `./models/{model_name}.pkl`
- Metadata detection automatic from pandas DataFrame

### 3. Data Flow Pipeline
```
CSV → SyntheticDataGenerator.train() → Model (.pkl)
     → SyntheticDataGenerator.generate() → JSON records
     → KafkaProducerService.send_message() → Topic: cardiovascular-data
     → CardiovascularConsumer (batched reads) → SnowflakeWriter.write_batch()
```

**Batching is critical**: Consumer reads 100 records, writes to Snowflake in batch for performance.

### 4. Configuration Pattern
- **Pydantic Settings** (`config/settings.py`) loads from `.env`
- Cached singleton via `@lru_cache()` on `get_settings()`
- Snowflake credentials **required** (no defaults)
- Kafka defaults to `localhost:9092` for local dev
- Docker Compose overrides with `kafka:29092` (internal network)

## Development Workflows

### Starting the System
```bash
# Method 1: Docker Compose (recommended)
docker-compose up -d                    # All services
docker-compose logs -f api-producer    # Watch API logs

# Method 2: Local development
docker-compose up kafka zookeeper kafka-ui  # Infrastructure only
python -m uvicorn app.main:app --reload    # API (terminal 1)
python -m consumer.kafka_consumer           # Consumer (terminal 2)
```

### Training a Model
```bash
# Via Make (preferred)
make train                              # Starts async training
make train-status TASK_ID=<uuid>       # Check progress
make train-tasks                        # List all tasks

# Via curl
curl -X POST http://localhost:8000/train \
  -H "Content-Type: application/json" \
  -d '{"dataset_path": "./data/CVD_cleaned.csv", "epochs": 300}'

# Via Python script (with progress monitoring)
python scripts/monitor_training.py <task_id>
```

### Testing
```bash
make test                               # Runs scripts/test_api.py
python scripts/test_snowflake.py       # Tests Snowflake connection
curl http://localhost:8000/health      # Quick health check
```

## Code Conventions

### Import Organization
Standard order in all files:
1. Standard library (json, os, sys)
2. Third-party (fastapi, pandas, kafka)
3. Local imports (`from config import`, `from app.models import`)

### Error Handling
- Use `loguru.logger` for all logging (configured in `app/utils/logger.py`)
- API raises `HTTPException` with proper status codes
- Kafka/Snowflake errors logged but consumer continues (graceful degradation)
- Training failures captured in task tracker with `error` field

### Naming Patterns
- Services: `{Domain}Service` or `{Domain}Generator` (e.g., `SyntheticDataGenerator`)
- Consumers: `{Domain}Consumer` (e.g., `CardiovascularConsumer`)
- Pydantic models: `{Action}{Type}` (e.g., `TrainingRequest`, `GenerateResponse`)
- Task IDs: UUID4 strings

### File Structure Rules
```
app/
  main.py              # FastAPI app, all endpoints
  models/schemas.py    # ALL Pydantic models (Request/Response)
  services/            # Business logic (generator, kafka producer)
  utils/               # Helpers (logger, task_tracker)
consumer/              # Kafka consumer + Snowflake writer (separate process)
config/settings.py     # Pydantic Settings (singleton pattern)
scripts/               # Operational scripts (download, test, monitor)
```

**DO NOT** create `app/routers/` - all endpoints in `main.py` (small API).

## Critical Integration Points

### Kafka Producer (app/services/kafka_producer.py)
- Uses `kafka-python` library
- Serializes to JSON with UTF-8 encoding
- Key: record ID or None
- Error handling: logs failure, raises exception (caller decides retry)

### Kafka Consumer (consumer/kafka_consumer.py)
- **Manual commit** after Snowflake write succeeds (data safety)
- Batches messages (default 100) before writing
- Graceful shutdown on SIGINT/SIGTERM (drains current batch)
- Auto-reconnect on Kafka errors

### Snowflake Writer (consumer/snowflake_writer.py)
- Auto-creates table if missing (`CREATE TABLE IF NOT EXISTS`)
- Schema maps CSV columns + `ingested_at` timestamp
- Uses `snowflake.connector` (not SQLAlchemy for batch writes)
- Connection pooling via context manager

### Dataset Schema (CVD_cleaned.csv)
Columns: `General_Health`, `Checkup`, `Exercise`, `Heart_Disease`, `Skin_Cancer`, `Other_Cancer`, `Depression`, `Diabetes`, `Arthritis`, `Sex`, `Age_Category`, `Height_(cm)`, `Weight_(kg)`, `BMI`, `Smoking_History`, `Alcohol_Consumption`, `Fruit_Consumption`, `Green_Vegetables_Consumption`, `FriedPotato_Consumption`

**Column names have special characters** (parentheses, underscores) - handle carefully in SQL.

## Docker Compose Networking
- Internal network: `cardiovascular-network` (bridge)
- Kafka accessible as `kafka:29092` inside Docker, `localhost:9092` from host
- API exposes port 8000 (maps to container 8000)
- Kafka UI on port 8080 (useful for debugging topics)

## Environment Variables (Required)
```bash
# Snowflake (NO DEFAULTS - will fail if missing)
SNOWFLAKE_ACCOUNT=xyz12345.us-east-1
SNOWFLAKE_USER=admin
SNOWFLAKE_PASSWORD=secret
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CARDIOVASCULAR_DB
SNOWFLAKE_SCHEMA=SYNTHETIC_DATA
SNOWFLAKE_TABLE=PATIENT_DATA

# Kaggle (optional - for dataset download)
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key

# Kafka (has defaults)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092  # Override in docker-compose.yml
```

## Performance Considerations
- **Training**: 300 epochs ≈ 10-15 minutes on modern CPU (dataset dependent)
- **Generation**: 1000 samples ≈ 2-5 seconds (after model loaded)
- **Kafka throughput**: ~10k messages/sec (not bottleneck)
- **Snowflake writes**: Batch of 100 ≈ 1-2 seconds (network latency)

## Common Pitfalls
1. **Never block on training** - always use BackgroundTasks
2. **Commit Kafka offsets AFTER Snowflake write** - prevents data loss
3. **Check model exists before generate** - load model in `__init__` or fail gracefully
4. **Docker network names** - use `kafka:29092` in containers, `localhost:9092` on host
5. **CTGAN memory** - 300+ epochs on large datasets can use 4-8GB RAM
6. **Snowflake session timeout** - consumer reconnects automatically

## Professional Standards
- No emojis in code/logs (academic/professional context)
- 4-space indentation (PEP 8)
- Type hints on functions (not enforced by mypy but encouraged)
- Docstrings on public methods (Google style)
- Black formatter with 100 char line length (`pyproject.toml` configured)

## When Adding Features
- **New endpoints**: Add to `app/main.py`, create Pydantic models in `app/models/schemas.py`
- **New data sources**: Create service in `app/services/`, inject via dependency
- **New background tasks**: Use task_tracker pattern (see async training)
- **New Kafka topics**: Update `config/settings.py`, add consumer logic
- **New Snowflake tables**: Modify `consumer/snowflake_writer.py`, update schema

## Quick Reference Commands
```bash
make help              # Show all available commands
make start             # Start all services (Docker)
make logs              # Follow service logs
make train             # Async training (returns task_id)
make generate          # Generate 100 samples → Kafka
docker-compose ps      # Check service health
docker-compose down -v # Clean reset (removes volumes)
```

## Debugging Tips
- API logs: `docker-compose logs -f api-producer`
- Consumer logs: `docker-compose logs -f consumer`
- Kafka messages: Open http://localhost:8080 (Kafka UI)
- Snowflake queries: Connect via SnowSQL or web UI
- Training stuck: Check task status endpoint, review API logs for memory errors
- Indentation issues: Use `black app/ consumer/ config/` (formatter configured)

---

**Remember**: This is a streaming ML pipeline, not a REST API CRUD app. Think: train → generate → stream → store → analyze.
