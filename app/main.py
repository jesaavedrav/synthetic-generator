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
from app.utils import setup_logging, task_tracker

# Import all generators to trigger factory registration
from app.services import (
    CTGANGenerator,
    TVAEGenerator,
    GaussianCopulaGenerator,
    SMOTEGenerator,
)

# Setup
settings = get_settings()
setup_logging()

# Initialize FastAPI
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="API for generating synthetic cardiovascular disease data and streaming to Kafka",
)

# CORS - Configuración permisiva para desarrollo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas las origins (desarrollo)
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permite todos los headers
    expose_headers=["*"],  # Expone todos los headers en la respuesta
)

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
    logger.info(f"API Version: {settings.api_version}")


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

        # Create task in tracker
        task = task_tracker.create_task(task_id=task_id, request_data=request.dict())

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
    task = task_tracker.get_task(task_id)

    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    return TrainingStatusResponse(**task.to_dict())


@app.get("/train/tasks")
async def list_training_tasks():
    """
    List all training tasks
    """
    tasks = task_tracker.list_tasks()
    return {"total": len(tasks), "tasks": [task.to_dict() for task in tasks.values()]}


async def train_model_task(task_id: str, request: TrainingRequest):
    """Background task to train the model"""
    import asyncio

    task = task_tracker.get_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found in tracker")
        return

    try:
        # Mark as started
        task.start()
        logger.info(f"Task {task_id}: Starting training with method {request.method}")

        dataset_path = request.dataset_path or settings.dataset_path

        # Create generator using factory
        generator = GeneratorFactory.create(request.method.lower(), model_name=request.model_name)

        # Training happens in executor to not block event loop
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: generator.train(
                dataset_path=dataset_path,
                model_name=request.model_name,
                epochs=request.epochs,
                batch_size=request.batch_size,
                progress_callback=lambda p: task.update_progress(p),
            ),
        )

        # Mark as completed
        task.complete(result)
        logger.info(f"Task {task_id}: Training completed successfully")

    except FileNotFoundError as e:
        error_msg = f"Dataset not found: {e}"
        logger.error(f"Task {task_id}: {error_msg}")
        task.fail(error_msg)

    except Exception as e:
        error_msg = f"Training failed: {e}"
        logger.error(f"Task {task_id}: {error_msg}")
        task.fail(error_msg)


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

        sent_to_kafka = False
        if request.send_to_kafka:
            kafka_producer.send_batch(synthetic_data)
            sent_to_kafka = True
            logger.info(f"Sent {len(synthetic_data)} samples to Kafka")

        # Convert to list of dicts for response
        samples = synthetic_data.to_dict(orient="records")

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=settings.api_host, port=settings.api_port, reload=True)
