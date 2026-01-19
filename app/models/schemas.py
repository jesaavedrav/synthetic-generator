from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

# --- Audit Log Schemas ---
class AuditLogEntry(BaseModel):
    id: int
    event_type: str
    message: str
    data: Optional[Dict[str, Any]] = None
    success: bool
    created_at: str

class AuditLogPage(BaseModel):
    total: int
    skip: int
    limit: int
    logs: List[AuditLogEntry]
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
class AnomalySpec(BaseModel):
    """Specification for anomaly injection"""
    type: str = Field(..., description="Type of anomaly: outlier, missing, category_noise, etc.")
    columns: Optional[List[str]] = Field(None, description="Target columns for anomaly injection")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Parameters for the anomaly injector")


class GenerateWithAnomalyRequest(BaseModel):
    """Request model for generating synthetic data with anomaly injection"""
    num_samples: int = Field(default=10, ge=1, le=10000)
    send_to_kafka: bool = Field(default=False)
    model_name: str = Field(default="cardiovascular_model", description="Name of the trained model to use")
    method: str = Field(default="ctgan", description="Generation method: ctgan, tvae, or gaussian_copula")
    anomaly: AnomalySpec = Field(..., description="Anomaly injection specification")

    class Config:
        json_schema_extra = {
            "example": {
                "num_samples": 100,
                "send_to_kafka": True,
                "model_name": "cardiovascular_model",
                "method": "ctgan",
                "anomaly": {
                    "type": "outlier",
                    "columns": ["BMI"],
                    "params": {"factor": 4, "proportion": 0.02}
                }
            }
        }


class AnomalyMetadata(BaseModel):
    """Metadata about the anomaly injected in the sample"""
    applied: bool = Field(..., description="Whether anomaly was injected")
    type: Optional[str] = Field(None, description="Type of anomaly")
    columns: Optional[List[str]] = Field(None, description="Columns affected")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Parameters used for anomaly injection")


class GenerateWithAnomalyResponse(BaseModel):
    """Response model for data generation with anomaly injection"""
    status: str
    message: str
    num_samples_generated: int
    sent_to_kafka: bool
    anomaly_metadata: AnomalyMetadata
    samples: Optional[List[Dict[str, Any]]] = None


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    timestamp: datetime
    version: str


class TrainingRequest(BaseModel):
    """Request model for training synthetic data generator"""

    dataset_path: Optional[str] = None
    model_name: str = "cardiovascular_model"
    method: str = Field(
        default="ctgan", description="Generation method: ctgan, tvae, or gaussian_copula"
    )
    epochs: int = Field(default=50, ge=1, le=1000)
    batch_size: int = Field(
        default=500,
        ge=100,
        le=2000,
        description="Batch size for training (higher = faster but more memory)",
    )
    overwrite_existing: bool = Field(
        default=False, description="Overwrite existing model if it exists"
    )

    class Config:
        protected_namespaces = ()
        json_schema_extra = {
            "example": {
                "dataset_path": "./data/cardiovascular_data.csv",
                "model_name": "cardiovascular_model",
                "method": "ctgan",
                "epochs": 300,
                "batch_size": 500,
                "overwrite_existing": False,
            }
        }


class TrainingResponse(BaseModel):
    """Response model for training completion"""

    status: str
    message: str
    task_id: str
    model_path: Optional[str] = None
    training_time_seconds: Optional[float] = None
    dataset_rows: Optional[int] = None

    class Config:
        protected_namespaces = ()


class GenerateRequest(BaseModel):
    """Request model for generating synthetic data"""

    num_samples: int = Field(default=10, ge=1, le=10000)
    send_to_kafka: bool = Field(default=False)
    model_name: str = Field(
        default="cardiovascular_model", description="Name of the trained model to use"
    )
    method: str = Field(
        default="ctgan", description="Generation method: ctgan, tvae, or gaussian_copula"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "num_samples": 100,
                "send_to_kafka": True,
                "model_name": "cardiovascular_model",
                "method": "ctgan",
            }
        }


class GenerateResponse(BaseModel):
    """Response model for data generation"""

    status: str
    message: str
    num_samples_generated: int
    sent_to_kafka: bool
    samples: Optional[List[Dict[str, Any]]] = None


class StreamingRequest(BaseModel):
    """Request model for streaming data to Kafka"""

    num_samples: int = Field(default=100, ge=1, le=100000)
    batch_size: int = Field(default=10, ge=1, le=1000)
    interval_seconds: float = Field(default=1.0, ge=0.1, le=60.0)
    model_name: str = Field(
        default="cardiovascular_model", description="Name of the trained model to use"
    )
    method: str = Field(
        default="ctgan", description="Generation method: ctgan, tvae, or gaussian_copula"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "num_samples": 1000,
                "batch_size": 50,
                "interval_seconds": 2.0,
                "model_name": "cardiovascular_model",
                "method": "ctgan",
            }
        }


class StreamingResponse(BaseModel):
    """Response model for streaming initiation"""

    status: str
    message: str
    task_id: str
    total_samples: int
    batch_size: int


class ModelInfoResponse(BaseModel):
    """Response model for model information"""

    model_exists: bool
    model_path: Optional[str] = None
    model_type: Optional[str] = None
    trained_on_rows: Optional[int] = None
    columns: Optional[List[str]] = None

    class Config:
        protected_namespaces = ()


class TrainingStatusResponse(BaseModel):
    """Response model for training status check"""

    task_id: str
    status: str  # 'running', 'completed', 'failed', 'not_found'
    message: str
    progress: Optional[float] = None  # 0-100
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class MethodInfo(BaseModel):
    """Information about a generation method"""

    method: str
    name: str
    description: str
    supports_epochs: bool = Field(
        default=True, description="Whether this method supports configurable epochs"
    )
    supports_batch_size: bool = Field(
        default=True, description="Whether this method supports configurable batch size"
    )
    default_epochs: Optional[int] = Field(
        default=None, description="Default number of epochs if supported"
    )
    default_batch_size: Optional[int] = Field(
        default=None, description="Default batch size if supported"
    )


class AvailableMethodsResponse(BaseModel):
    """Response model for available generation methods"""

    methods: List[MethodInfo]
    default_method: str


class DatasetInfo(BaseModel):
    """Information about a dataset file"""

    name: str
    path: str
    size_bytes: int
    modified_at: Optional[datetime] = None


class DatasetsResponse(BaseModel):
    """Response model for available datasets"""

    datasets: List[DatasetInfo]
    total_count: int


class TrainedModelInfo(BaseModel):
    """Information about a trained model"""

    model_name: str
    method: str
    file_name: str
    file_path: str
    size_bytes: int
    modified_at: Optional[datetime] = None
    training_metadata: Optional[Dict[str, Any]] = None  # Training parameters


class TrainedModelsResponse(BaseModel):
    """Response model for available trained models"""

    models: List[TrainedModelInfo]
    total_count: int
