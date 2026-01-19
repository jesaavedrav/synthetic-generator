from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from loguru import logger
import uuid
import os
from pathlib import Path

from config import get_settings
from app.models import (
    HealthResponse,
    TrainingRequest,
    TrainingResponse,
    GenerateRequest,
    GenerateResponse,
    StreamingRequest,
    StreamingResponse,
    ModelInfoResponse,
    TrainingStatusResponse,
    AvailableMethodsResponse,
    DatasetsResponse,
    DatasetInfo,
    TrainedModelsResponse,
    TrainedModelInfo,
)
from app.services.generator_factory import GeneratorFactory, check_model_exists
from app.services.kafka_producer import KafkaProducerService
from app.utils import setup_logging
from app.services.postgres_task_service import PostgresTaskService
from app.models.schemas import GenerateWithAnomalyRequest, GenerateWithAnomalyResponse, AnomalyMetadata
from app.services.anomaly_injector import AnomalyInjectorFactory

# Import all generators to trigger factory registration
from app.services import (
    CTGANGenerator,
    TVAEGenerator,
    GaussianCopulaGenerator,
    SMOTEGenerator,
)


settings = get_settings()
setup_logging()

# Initialize PostgresTaskService singleton
postgres_task_service = PostgresTaskService()

# Initialize Kafka producers
kafka_producer = KafkaProducerService()

# Helper for audit logging
class AuditLogger:
    @staticmethod
    def log_event(event_type: str, message: str, data: dict = None, success: bool = True):
        try:
            audit_event = {
                "id": str(uuid.uuid4()),
                "event_type": event_type,
                "message": message,
                "data": data,
                "success": success,
                "created_at": datetime.utcnow().isoformat(),
            }
            # Create a separate producer for audit logs with the audit-log topic
            audit_producer = KafkaProducerService()
            audit_producer.topic = settings.kafka_audit_log_topic
            audit_producer.send_single(audit_event, key=audit_event["id"])
            logger.info(f"Audit log sent: {event_type} - {message}")
        except Exception as e:
            logger.error(f"Failed to send audit log: {e}")

audit_logger = AuditLogger()

# Initialize FastAPI
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="API for generating synthetic cardiovascular disease data and streaming to Kafka",
)


# --- GENERATE WITH ANOMALY ENDPOINT ---
@app.post("/generate/anomaly", response_model=GenerateWithAnomalyResponse)
async def generate_data_with_anomaly(request: GenerateWithAnomalyRequest):
    """
    Generate synthetic data and inject anomalies using the specified method/model.

    - **num_samples**: Number of samples to generate
    - **send_to_kafka**: Whether to send generated data to Kafka
    - **model_name**: Name of the trained model to use
    - **method**: Generation method to use
    - **anomaly**: Specification of anomaly injection (type, columns, params)
    """
    try:
        method_lower = request.method.lower()
        if not GeneratorFactory.is_method_available(method_lower):
            available = [m["method"] for m in GeneratorFactory.get_available_methods()]
            raise HTTPException(
                status_code=400,
                detail=f"Invalid method '{request.method}'. Available: {available}",
            )

        if not check_model_exists(request.model_name, method_lower):
            raise HTTPException(
                status_code=404,
                detail=f"Model '{request.model_name}' with method '{method_lower}' not found. Train it first using /train endpoint.",
            )

        logger.info(
            f"Generating {request.num_samples} synthetic samples using {request.model_name} ({method_lower}) with anomaly {request.anomaly.type} on {request.anomaly.columns}"
        )

        # Generate synthetic data
        generator = GeneratorFactory.create(method_lower, model_name=request.model_name)
        synthetic_data = generator.generate(num_samples=request.num_samples)

        # Inject anomaly
        injector = AnomalyInjectorFactory.get_injector(
            anomaly_type=request.anomaly.type,
            columns=request.anomaly.columns,
            params=request.anomaly.params,
        )
        synthetic_data_anom = injector.inject(synthetic_data)

        # Prepare anomaly metadata
        anomaly_metadata = AnomalyMetadata(
            applied=True,
            type=request.anomaly.type,
            columns=request.anomaly.columns,
            params=request.anomaly.params or {},
        )

        sent_to_kafka = False
        # Prepare records with message_id and metadata
        records = synthetic_data_anom.to_dict(orient="records")
        for r in records:
            r["message_id"] = str(uuid.uuid4())
            r["generation_metadata"] = {
                "method": method_lower,
                "model_name": request.model_name,
                "anomaly": anomaly_metadata.dict(),
                "num_samples": request.num_samples,
            }

        if request.send_to_kafka:
            kafka_producer.send_batch(records)
            sent_to_kafka = True
            logger.info(f"Sent {len(records)} samples with anomaly to Kafka")

        # Return preview (first 10)
        samples = records

        return GenerateWithAnomalyResponse(
            status="success",
            message=f"Generated {len(samples)} synthetic samples with anomaly {request.anomaly.type} using {request.model_name} ({method_lower})",
            num_samples_generated=len(samples),
            sent_to_kafka=sent_to_kafka,
            anomaly_metadata=anomaly_metadata,
            samples=samples[:10] if len(samples) > 10 else samples,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Generation with anomaly failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
# CORS - Configuración permisiva para desarrollo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas las origins (desarrollo)
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permite todos los headers
    expose_headers=["*"],  # Expone todos los headers en la respuesta
)

# --- AUDIT LOG ENDPOINT ---
import snowflake.connector
from app.models.schemas import AuditLogPage, AuditLogEntry

@app.get("/audit-log", response_model=AuditLogPage)
async def get_audit_log(skip: int = 0, limit: int = 50, event_type: str = None, q: str = None):
    """Paginated and filtered audit log endpoint."""
    from config import get_settings
    settings = get_settings()
    conn = snowflake.connector.connect(
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        account=settings.snowflake_account,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )
    try:
        import uuid
        sent_to_kafka = False
        # Prepare records with message_id and metadata
        records = synthetic_data_anom.to_dict(orient="records")
        for r in records:
            r["message_id"] = str(uuid.uuid4())
            r["generation_metadata"] = {
                "method": method_lower,
                "model_name": request.model_name,
                "anomaly": anomaly_metadata.dict(),
                "num_samples": request.num_samples,
            }

        if request.send_to_kafka:
            kafka_producer.send_batch(records)
            sent_to_kafka = True
            logger.info(f"Sent {len(records)} samples with anomaly to Kafka")

        # Return preview (first 10)
        samples = records

        return GenerateWithAnomalyResponse(
            status="success",
            message=f"Generated {len(samples)} synthetic samples with anomaly {request.anomaly.type} using {request.model_name} ({method_lower})",
            num_samples_generated=len(samples),
            sent_to_kafka=sent_to_kafka,
            anomaly_metadata=anomaly_metadata,
            samples=samples[:10] if len(samples) > 10 else samples,
        )
        with conn.cursor() as cur:
            cur.execute(count_sql, tuple(params[:-2]))
            (total,) = cur.fetchone()
        log_entries = [
            AuditLogEntry(
                id=row["ID"],
                event_type=row["EVENT_TYPE"],
                message=row["MESSAGE"],
                data=row["DATA"],
                success=row["SUCCESS"],
                created_at=row["CREATED_AT"].isoformat() if row["CREATED_AT"] else None,
            )
            for row in rows
        ]
        return AuditLogPage(total=total, skip=skip, limit=limit, logs=log_entries)
        # Crea la tabla AUDIT_LOG si no existe
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS AUDIT_LOG (
                    ID STRING PRIMARY KEY,
                    EVENT_TYPE STRING,
                    MESSAGE STRING,
                    DATA STRING,
                    SUCCESS BOOLEAN,
                    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
    finally:
        conn.close()

# Services
kafka_producer = KafkaProducerService()


def get_default_generator(model_name: str = "cardiovascular_model", method: str = "ctgan"):
    """
    Get a generator instance for the specified model and method
    Defaults to CTGAN method if not specified
    """
    return GeneratorFactory.create(method.lower(), model_name=model_name)



@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("Starting Cardiovascular Synthetic Data Generator API")
    # Usa la variable global 'settings' para el log
    global settings
    logger.info(f"API Version: {settings.api_version}")

    # Ensure TRAINING_TASKS table exists in Snowflake
    import snowflake.connector
    from config import get_settings
    local_settings = get_settings()
    conn = snowflake.connector.connect(
        user=local_settings.snowflake_user,
        password=local_settings.snowflake_password,
        account=local_settings.snowflake_account,
        warehouse=local_settings.snowflake_warehouse,
        database=local_settings.snowflake_database,
        schema=local_settings.snowflake_schema,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS TRAINING_TASKS (
                    TASK_ID STRING PRIMARY KEY,
                    TYPE STRING,
                    STATUS STRING,
                    REQUEST_DATA STRING,
                    PROGRESS INTEGER,
                    ERROR STRING,
                    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_LTZ
                )
            """)
    finally:
        conn.close()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down API")
    kafka_producer.close()


@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint - health check"""
    return HealthResponse(
        status="healthy", timestamp=datetime.utcnow(), version=settings.api_version
    )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy", timestamp=datetime.utcnow(), version=settings.api_version
    )


@app.get("/train/methods", response_model=AvailableMethodsResponse)
async def get_available_methods():
    """
    Get available synthetic data generation methods

    Returns a list of supported generation methods:
    - ctgan: Conditional Tabular GAN
    - tvae: Tabular Variational AutoEncoder
    - gaussian_copula: Gaussian Copula
    """
    try:
        methods = GeneratorFactory.get_available_methods()
        return AvailableMethodsResponse(methods=methods, default_method="ctgan")
    except Exception as e:
        logger.error(f"Error retrieving methods: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve methods: {str(e)}")


@app.get("/datasets", response_model=DatasetsResponse)
async def list_datasets():
    """
    List available datasets in the data directory

    Returns information about CSV files found in ./data directory
    """
    try:
        data_dir = Path("./data")
        if not data_dir.exists():
            return DatasetsResponse(datasets=[], total_count=0)

        datasets = []
        for file_path in data_dir.glob("*.csv"):
            stat = file_path.stat()
            datasets.append(
                DatasetInfo(
                    name=file_path.name,
                    path=str(file_path),
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(stat.st_mtime),
                )
            )

        datasets.sort(key=lambda x: x.modified_at, reverse=True)
        return DatasetsResponse(datasets=datasets, total_count=len(datasets))

    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list datasets: {str(e)}")


@app.get("/models", response_model=TrainedModelsResponse)
async def list_trained_models():
    """
    List all trained models available in the models directory

    Returns information about each trained model including:
    - Model name
    - Method (ctgan, tvae, etc.)
    - File information (size, modification date)
    - Training metadata (epochs, batch_size, etc.)
    """
    try:
        import pickle

        models_dir = Path("./models")
        if not models_dir.exists():
            return TrainedModelsResponse(models=[], total_count=0)

        trained_models = []
        for file_path in models_dir.glob("*.pkl"):
            # Parse filename: {model_name}_{method}.pkl
            file_name = file_path.stem  # Without .pkl
            parts = file_name.rsplit("_", 1)  # Split from the right, max 1 split

            if len(parts) == 2:
                model_name, method = parts
            else:
                # Old format without method suffix
                model_name = file_name
                method = "unknown"

            # Try to load training metadata from the model file
            training_metadata = None
            try:
                with open(file_path, "rb") as f:
                    model_data = pickle.load(f)
                    training_metadata = model_data.get("training_metadata", None)
            except Exception as e:
                logger.warning(f"Could not load metadata from {file_path.name}: {e}")

            stat = file_path.stat()
            trained_models.append(
                TrainedModelInfo(
                    model_name=model_name,
                    method=method,
                    file_name=file_path.name,
                    file_path=str(file_path),
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(stat.st_mtime),
                    training_metadata=training_metadata,
                )
            )

        trained_models.sort(key=lambda x: x.modified_at, reverse=True)
        return TrainedModelsResponse(models=trained_models, total_count=len(trained_models))

    except Exception as e:
        logger.error(f"Error listing trained models: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list trained models: {str(e)}")


@app.post("/train", response_model=TrainingResponse)
async def train_model(request: TrainingRequest, background_tasks: BackgroundTasks):
    """
    Train the synthetic data generation model (Async)

    This endpoint starts training in the background and returns immediately.
    Use the task_id to check progress with GET /train/status/{task_id}

    - **dataset_path**: Path to the cardiovascular dataset CSV
    - **model_name**: Name for the trained model
    - **method**: Generation method (ctgan, tvae, gaussian_copula)
    - **epochs**: Number of training epochs (default: 300)
    - **overwrite_existing**: Overwrite existing model if it exists
    """
    try:
        # Validate method
        method_lower = request.method.lower()
        if not GeneratorFactory.is_method_available(method_lower):
            available = [m["method"] for m in GeneratorFactory.get_available_methods()]
            raise HTTPException(
                status_code=400,
                detail=f"Invalid method '{request.method}'. Available: {available}",
            )

        # Check if model exists
        if check_model_exists(request.model_name, method_lower):
            if not request.overwrite_existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"Model '{request.model_name}' with method '{method_lower}' already exists. Set overwrite_existing=true to retrain.",
                )
            logger.info(f"Overwriting existing model: {request.model_name} ({method_lower})")

        # Generate task ID
        task_id = str(uuid.uuid4())

        logger.info(
            f"Creating training task {task_id} with {request.epochs} epochs using {method_lower}"
        )

        # Crea el task en Postgres
        postgres_task_service.create_task(
            task_id=task_id,
            type_="training",
            status="pending",
            request_data=request.dict(),
            progress=0,
            error=None,
        )

        # Send audit log
        audit_logger.log_event(
            event_type="training_started",
            message=f"Training task {task_id} initiated with method {method_lower}",
            data={
                "task_id": task_id,
                "method": method_lower,
                "model_name": request.model_name,
                "epochs": request.epochs,
                "dataset_path": request.dataset_path,
            },
            success=True,
        )

        # Start training in background
        background_tasks.add_task(train_model_task, task_id=task_id, request=request)

        return TrainingResponse(
            status="started",
            message=f"Training task initiated using {method_lower}. Use task_id to check progress.",
            task_id=task_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start training: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/train/status/{task_id}", response_model=TrainingStatusResponse)
async def get_training_status(task_id: str):
    """
    Get the status of a training task

    - **task_id**: The task ID returned from POST /train
    """
    task = postgres_task_service.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return TrainingStatusResponse(
        task_id=task["TASK_ID"],
        status=task["STATUS"],
        message="Training task status",
        progress=task.get("PROGRESS"),
        started_at=task.get("CREATED_AT").isoformat() if task.get("CREATED_AT") else None,
        completed_at=task.get("UPDATED_AT").isoformat() if task.get("UPDATED_AT") else None,
        error=task.get("ERROR"),
        result=None,
    )


@app.get("/train/tasks")
async def list_training_tasks():
    """
    List all training tasks
    """
    total = postgres_task_service.count_tasks()
    tasks = postgres_task_service.list_tasks()
    return {
        "total": total,
        "tasks": [
            {
                "task_id": t.task_id,
                "status": t.status,
                "progress": t.progress,
                "error": t.error,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "request_data": t.request_data,
            }
            for t in tasks
        ],
    }


async def train_model_task(task_id: str, request: TrainingRequest):
    """Background task to train the model"""
    import asyncio

    task = postgres_task_service.get_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found in tracker")
        return

    try:
        # Mark as started
        postgres_task_service.update_task(task_id, status="running")
        logger.info(f"Task {task_id}: Starting training with method {request.method}")

        dataset_path = request.dataset_path or settings.dataset_path

        # Create generator using factory
        generator = GeneratorFactory.create(request.method.lower(), model_name=request.model_name)

        # Training happens in executor to not block event loop
        def progress_callback(pct):
            postgres_task_service.update_task(task_id, progress=int(pct))

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: generator.train(
                dataset_path=dataset_path,
                model_name=request.model_name,
                epochs=request.epochs,
                batch_size=request.batch_size,
                progress_callback=progress_callback,
            ),
        )

        # Mark as completed
        postgres_task_service.update_task(task_id, status="completed", progress=100)
        logger.info(f"Task {task_id}: Training completed successfully")
        
        # Send audit log
        audit_logger.log_event(
            event_type="training_completed",
            message=f"Training task {task_id} completed successfully",
            data={
                "task_id": task_id,
                "method": request.method.lower(),
                "model_name": request.model_name,
                "epochs": request.epochs,
            },
            success=True,
        )

    except FileNotFoundError as e:
        error_msg = f"Dataset not found: {e}"
        logger.error(f"Task {task_id}: {error_msg}")
        postgres_task_service.update_task(task_id, status="failed", error=error_msg)
        
        # Send audit log
        audit_logger.log_event(
            event_type="training_failed",
            message=f"Training task {task_id} failed: {error_msg}",
            data={"task_id": task_id, "error": error_msg},
            success=False,
        )

    except Exception as e:
        error_msg = f"Training failed: {e}"
        logger.error(f"Task {task_id}: {error_msg}")
        postgres_task_service.update_task(task_id, status="failed", error=error_msg)
        
        # Send audit log
        audit_logger.log_event(
            event_type="training_failed",
            message=f"Training task {task_id} failed: {error_msg}",
            data={"task_id": task_id, "error": error_msg},
            success=False,
        )


@app.post("/generate", response_model=GenerateResponse)
async def generate_data(request: GenerateRequest):
    """
    Generate synthetic cardiovascular data using a trained model

    - **num_samples**: Number of samples to generate (1-10000)
    - **send_to_kafka**: Whether to send generated data to Kafka topic
    - **model_name**: Name of the trained model to use (default: cardiovascular_model)
    - **method**: Generation method to use (default: ctgan)
    """
    try:
        # Validate method
        method_lower = request.method.lower()
        if not GeneratorFactory.is_method_available(method_lower):
            available = [m["method"] for m in GeneratorFactory.get_available_methods()]
            raise HTTPException(
                status_code=400,
                detail=f"Invalid method '{request.method}'. Available: {available}",
            )

        # Check if model exists
        if not check_model_exists(request.model_name, method_lower):
            raise HTTPException(
                status_code=404,
                detail=f"Model '{request.model_name}' with method '{method_lower}' not found. Train it first using /train endpoint.",
            )

        logger.info(
            f"Generating {request.num_samples} synthetic samples using {request.model_name} ({method_lower})"
        )

        # Create generator for the specified model and method
        generator = GeneratorFactory.create(method_lower, model_name=request.model_name)

        synthetic_data = generator.generate(num_samples=request.num_samples)
        import pandas as pd
        import uuid
        # Defensive: ensure DataFrame
        if isinstance(synthetic_data, list):
            synthetic_data = pd.DataFrame(synthetic_data)
        if not isinstance(synthetic_data, pd.DataFrame):
            raise HTTPException(status_code=500, detail="Generator did not return a DataFrame.")
        # Prepare records with message_id and metadata
        records = synthetic_data.to_dict(orient="records")
        for r in records:
            r["message_id"] = str(uuid.uuid4())
            r["generation_metadata"] = {
                "method": method_lower,
                "model_name": request.model_name,
                "num_samples": request.num_samples,
            }

        sent_to_kafka = False
        if request.send_to_kafka:
            kafka_producer.send_batch(records)
            sent_to_kafka = True
            logger.info(f"Sent {len(records)} samples to Kafka")
            # Send audit log
            audit_logger.log_event(
                event_type="data_generated",
                message=f"Generated and sent {len(records)} samples to Kafka",
                data={
                    "num_samples": len(records),
                    "model_name": request.model_name,
                    "method": method_lower,
                },
                success=True,
            )

        samples = records

        return GenerateResponse(
            status="success",
            message=f"Generated {len(samples)} synthetic samples using {request.model_name} ({method_lower})",
            num_samples_generated=len(samples),
            sent_to_kafka=sent_to_kafka,
            samples=samples[:10] if len(samples) > 10 else samples,  # Return first 10 as preview
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stream", response_model=StreamingResponse)
async def stream_data(request: StreamingRequest, background_tasks: BackgroundTasks):
    """
    Stream synthetic data to Kafka in batches

    - **num_samples**: Total number of samples to generate
    - **batch_size**: Samples per batch
    - **interval_seconds**: Delay between batches
    - **model_name**: Name of the trained model to use
    - **method**: Generation method to use
    """
    try:
        # Validate method
        method_lower = request.method.lower()
        if not GeneratorFactory.is_method_available(method_lower):
            available = [m["method"] for m in GeneratorFactory.get_available_methods()]
            raise HTTPException(
                status_code=400,
                detail=f"Invalid method '{request.method}'. Available: {available}",
            )

        # Check if model exists
        if not check_model_exists(request.model_name, method_lower):
            raise HTTPException(
                status_code=404,
                detail=f"Model '{request.model_name}' with method '{method_lower}' not found. Train it first using /train endpoint.",
            )

        task_id = str(uuid.uuid4())

        logger.info(
            f"Starting streaming task {task_id}: {request.num_samples} samples using {request.model_name} ({method_lower})"
        )

        background_tasks.add_task(
            stream_to_kafka_task,
            task_id=task_id,
            num_samples=request.num_samples,
            batch_size=request.batch_size,
            interval_seconds=request.interval_seconds,
            model_name=request.model_name,
            method=method_lower,
        )

        return StreamingResponse(
            status="started",
            message=f"Streaming task initiated using {request.model_name} ({method_lower})",
            task_id=task_id,
            total_samples=request.num_samples,
            batch_size=request.batch_size,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Streaming failed to start: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/model/info", response_model=ModelInfoResponse)
async def get_model_info():
    """Get information about the default trained model (CTGAN cardiovascular_model)"""
    try:
        generator = get_default_generator()
        info = generator.get_model_info()
        return ModelInfoResponse(**info)
    except Exception as e:
        logger.error(f"Failed to get model info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def stream_to_kafka_task(
    task_id: str,
    num_samples: int,
    batch_size: int,
    interval_seconds: float,
    model_name: str,
    method: str,
):
    """Background task to stream data to Kafka"""
    import asyncio

    logger.info(f"Task {task_id}: Starting streaming using {model_name} ({method})")

    try:
        # Create generator for the specified model and method
        generator = GeneratorFactory.create(method, model_name=model_name)

        total_sent = 0
        while total_sent < num_samples:
            current_batch = min(batch_size, num_samples - total_sent)

            # Generate synthetic data
            synthetic_data = generator.generate(num_samples=current_batch)

            # Send to Kafka
            kafka_producer.send_batch(synthetic_data)

            total_sent += current_batch
            logger.info(f"Task {task_id}: Sent {total_sent}/{num_samples} samples")

            if total_sent < num_samples:
                await asyncio.sleep(interval_seconds)

        logger.info(f"Task {task_id}: Completed streaming {total_sent} samples")

    except Exception as e:
        logger.error(f"Task {task_id}: Streaming failed - {e}")

# --- ADMIN: TRUNCATE TABLE ---
from fastapi import Query

VALID_TABLES = ["TRAINING_TASKS", "AUDIT_LOG"]

@app.post("/admin/truncate-table")
async def truncate_table(table: str = Query(..., description=f"Nombre de la tabla a truncar. Opciones: {', '.join(VALID_TABLES)}")):
    """
    Trunca una tabla específica en Snowflake.
    Nombres válidos: TRAINING_TASKS, AUDIT_LOG
    """
    table_upper = table.upper()
    if table_upper not in VALID_TABLES:
        return {"status": "error", "message": f"Nombre de tabla inválido. Usa uno de: {', '.join(VALID_TABLES)}"}
    import snowflake.connector
    from config import get_settings
    settings = get_settings()
    conn = snowflake.connector.connect(
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        account=settings.snowflake_account,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE IF EXISTS {table_upper}")
        return {"status": "success", "message": f"Tabla {table_upper} truncada"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

# --- Admin Endpoints ---
@app.post("/admin/truncate-postgres-table", tags=["Admin"])
async def truncate_postgres_table(table_name: str):
    """
    Truncate a table in the PostgreSQL database.
    USE WITH CAUTION. This will delete all data in the table.
    """
    try:
        logger.warning(f"Attempting to truncate PostgreSQL table: {table_name}")
        postgres_task_service.truncate_table(table_name)
        message = f"Successfully truncated table '{table_name}' in PostgreSQL."
        logger.info(message)
        return {"message": message}
    except ValueError as e:
        logger.error(f"Error truncating table: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

@app.get("/admin/postgres-tables", tags=["Admin"], response_model=list[str])
async def get_postgres_tables():
    """
    Get a list of all table names in the PostgreSQL database that can be truncated.
    """
    try:
        return postgres_task_service.get_table_names()
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching postgres tables: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.api_host, port=settings.api_port, reload=True)
