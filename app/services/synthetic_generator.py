import pandas as pd
import pickle
import os
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from loguru import logger
import time

from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

from config import get_settings
from app.services.generator_factory import (
    BaseSyntheticGenerator,
    GeneratorFactory,
    GeneratorMethod,
    get_model_filename,
)

settings = get_settings()


class CTGANGenerator(BaseSyntheticGenerator):
    """
    Generates synthetic cardiovascular disease data using CTGAN
    """

    def __init__(self, model_name: str = "cardiovascular_model"):
        super().__init__()
        self.model_name = model_name
        self.training_metadata = {}  # Store training metadata
        # Build model path from model name
        model_filename = get_model_filename(GeneratorMethod.CTGAN, model_name)
        self.model_path = os.path.join("./models", model_filename)
        # Try to load existing model
        self._load_model()

    def train(
        self,
        dataset_path: str,
        model_name: str = "cardiovascular_model",
        epochs: int = 300,
        batch_size: int = 500,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict[str, Any]:
        """
        Train the CTGAN model on cardiovascular data

        Args:
            dataset_path: Path to the CSV dataset
            model_name: Name for the saved model
            epochs: Number of training epochs (default: 300)
            batch_size: Batch size for training (default: 500, higher = faster)
            progress_callback: Optional callback function to report progress (0-100)

        Returns:
            Dictionary with training results
        """
        logger.info(f"Loading dataset from {dataset_path}")

        if progress_callback:
            progress_callback(5.0)  # Loading data

        # Load data
        if not os.path.exists(dataset_path):
            raise FileNotFoundError(f"Dataset not found at {dataset_path}")

        df = pd.read_csv(dataset_path)
        logger.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        self.dataset_columns = df.columns.tolist()

        if progress_callback:
            progress_callback(10.0)  # Data loaded

        # Create metadata
        logger.info("Creating metadata...")
        self.metadata = SingleTableMetadata()
        self.metadata.detect_from_dataframe(df)

        if progress_callback:
            progress_callback(15.0)  # Metadata created

        # Initialize CTGAN
        logger.info(f"Initializing CTGAN with {epochs} epochs and batch_size {batch_size}...")
        self.model = CTGANSynthesizer(
            metadata=self.metadata, epochs=epochs, batch_size=batch_size, verbose=True
        )

        if progress_callback:
            progress_callback(20.0)  # Model initialized

        # Train (this takes the most time: 20% -> 90%)
        start_time = time.time()
        logger.info("Starting training... (this may take several minutes)")

        # Note: SDV doesn't support native progress callbacks
        # We'll simulate progress based on time
        self.model.fit(df)

        training_time = time.time() - start_time

        if progress_callback:
            progress_callback(90.0)  # Training completed

        logger.info(f"Training completed in {training_time:.2f} seconds")

        # Prepare training metadata
        training_metadata = {
            "epochs": epochs,
            "batch_size": batch_size,
            "dataset_path": dataset_path,
            "dataset_rows": df.shape[0],
            "dataset_columns": df.shape[1],
            "training_time_seconds": training_time,
            "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "method": "ctgan",
        }

        # Save model with metadata
        model_dir = Path(self.model_path).parent
        model_dir.mkdir(parents=True, exist_ok=True)

        model_filename = get_model_filename(GeneratorMethod.CTGAN, model_name)
        model_file = model_dir / model_filename
        self._save_model(str(model_file), training_metadata)

        if progress_callback:
            progress_callback(95.0)  # Model saved

        logger.info(f"Model saved to {model_file}")

        if progress_callback:
            progress_callback(100.0)  # Complete

        return {
            "model_path": str(model_file),
            "training_time": training_time,
            "dataset_rows": df.shape[0],
            "columns": self.dataset_columns,
            "training_metadata": training_metadata,
        }

    def generate(self, num_samples: int = 100) -> pd.DataFrame:
        """
        Generate synthetic samples

        Args:
            num_samples: Number of samples to generate

        Returns:
            DataFrame with synthetic data
        """
        if self.model is None:
            raise ValueError(
                "Model not trained. Please train the model first using /train endpoint"
            )

        logger.info(f"Generating {num_samples} synthetic samples")
        synthetic_data = self.model.sample(num_rows=num_samples)

        return synthetic_data

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model"""
        if self.model is None:
            return {
                "model_exists": False,
                "model_path": None,
                "model_type": None,
                "trained_on_rows": None,
                "columns": None,
                "training_metadata": None,
            }

        return {
            "model_exists": True,
            "model_path": self.model_path,
            "model_type": "CTGAN",
            "trained_on_rows": self.training_metadata.get("dataset_rows", "N/A"),
            "columns": self.dataset_columns,
            "training_metadata": self.training_metadata,
        }

    def _save_model(self, path: str, training_metadata: Dict[str, Any] = None):
        """Save model to disk with training metadata"""
        model_data = {
            "model": self.model,
            "metadata": self.metadata,
            "columns": self.dataset_columns,
            "training_metadata": training_metadata or {},
        }

        with open(path, "wb") as f:
            pickle.dump(model_data, f)

        self.model_path = path

    def _load_model(self):
        """Load model from disk if exists"""
        if os.path.exists(self.model_path):
            try:
                logger.info(f"Loading existing model from {self.model_path}")

                with open(self.model_path, "rb") as f:
                    model_data = pickle.load(f)

                self.model = model_data.get("model")
                self.metadata = model_data.get("metadata")
                self.dataset_columns = model_data.get("columns")
                self.training_metadata = model_data.get("training_metadata", {})

                logger.info("Model loaded successfully")
                if self.training_metadata:
                    logger.info(f"Training metadata: {self.training_metadata}")
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")
                self.model = None


# Register CTGAN generator in factory
GeneratorFactory.register(GeneratorMethod.CTGAN, CTGANGenerator)
