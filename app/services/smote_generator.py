import pandas as pd
import pickle
import os
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from loguru import logger
import time

from imblearn.over_sampling import SMOTE
from sklearn.preprocessing import LabelEncoder

from config import get_settings
from app.services.generator_factory import (
    BaseSyntheticGenerator,
    GeneratorFactory,
    GeneratorMethod,
    get_model_filename,
)

settings = get_settings()


class SMOTEGenerator(BaseSyntheticGenerator):
    """
    Generates synthetic cardiovascular disease data using SMOTE (Synthetic Minority Over-sampling Technique)
    Note: SMOTE is an oversampling technique, not a generative model. It requires the original dataset.
    """

    def __init__(self, model_name: str = "cardiovascular_model"):
        super().__init__()
        self.model_name = model_name
        self.training_metadata = {}
        self.original_data = None
        self.label_encoders = {}
        self.categorical_columns = []
        self.target_column = "Heart_Disease"
        model_filename = get_model_filename(GeneratorMethod.SMOTE, model_name)
        self.model_path = os.path.join("./models", model_filename)
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
        'Train' the SMOTE model (actually just stores the dataset and prepares encoders)

        Args:
            dataset_path: Path to the CSV dataset
            model_name: Name for the saved model
            epochs: Ignored for SMOTE - included for interface compatibility
            batch_size: Ignored for SMOTE - included for interface compatibility
            progress_callback: Optional callback function to report progress (0-100)

        Returns:
            Dictionary with training results
        """
        logger.info(f"Loading dataset from {dataset_path}")
        if progress_callback:
            progress_callback(5.0)

        try:
            df = pd.read_csv(dataset_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Dataset not found at {dataset_path}")

        logger.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        self.dataset_columns = df.columns.tolist()

        if progress_callback:
            progress_callback(20.0)

        # Store original data
        self.original_data = df.copy()

        if progress_callback:
            progress_callback(40.0)

        # Prepare label encoders for categorical columns
        logger.info("Preparing label encoders for categorical columns...")
        start_time = time.time()

        for col in df.columns:
            if df[col].dtype == "object" or df[col].dtype.name == "category":
                self.categorical_columns.append(col)
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))
                self.label_encoders[col] = le

        if progress_callback:
            progress_callback(60.0)

        # Initialize SMOTE
        logger.info("Initializing SMOTE...")
        if self.target_column not in df.columns:
            raise ValueError(
                f"Target column '{self.target_column}' not found in dataset. "
                "SMOTE requires a target variable for oversampling."
            )

        self.model = SMOTE(random_state=42, k_neighbors=5)

        training_time = time.time() - start_time

        if progress_callback:
            progress_callback(80.0)

        logger.info(f"SMOTE preparation completed in {training_time:.2f} seconds")

        # Prepare training metadata
        training_metadata = {
            "epochs": "N/A (oversampling method)",
            "batch_size": "N/A (oversampling method)",
            "dataset_path": dataset_path,
            "dataset_rows": df.shape[0],
            "dataset_columns": df.shape[1],
            "training_time_seconds": training_time,
            "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "method": "smote",
            "target_column": self.target_column,
            "categorical_columns": self.categorical_columns,
        }

        # Save model with metadata
        model_dir = Path(self.model_path).parent
        model_dir.mkdir(parents=True, exist_ok=True)

        model_filename = get_model_filename(GeneratorMethod.SMOTE, model_name)
        model_file = model_dir / model_filename
        self._save_model(str(model_file), training_metadata)

        if progress_callback:
            progress_callback(95.0)

        logger.info(f"Model saved to {model_file}")

        if progress_callback:
            progress_callback(100.0)

        return {
            "model_path": str(model_file),
            "training_time": training_time,
            "dataset_rows": df.shape[0],
            "columns": self.dataset_columns,
            "training_metadata": training_metadata,
        }

    def generate(self, num_samples: int = 100) -> pd.DataFrame:
        """
        Generate synthetic samples using SMOTE
        Note: SMOTE generates samples by oversampling the minority class
        """
        if self.model is None or self.original_data is None:
            raise ValueError(
                "Model not trained. Please train the model first using /train endpoint"
            )

        logger.info(f"Generating {num_samples} synthetic samples using SMOTE")

        # Prepare data for SMOTE - use original data
        df = self.original_data.copy()

        # Encode categorical columns using the stored encoders
        for col in self.categorical_columns:
            # Handle unknown labels by using fit_transform instead of transform
            if col in df.columns:
                # For categorical columns, ensure we handle any new values
                df[col] = df[col].astype(str)
                # Get unique values
                unique_vals = df[col].unique()
                # Check if encoder knows all values
                known_vals = set(self.label_encoders[col].classes_)
                unknown_vals = set(unique_vals) - known_vals
                
                if unknown_vals:
                    logger.warning(
                        f"Column '{col}' has unknown values: {unknown_vals}. "
                        f"Re-fitting encoder."
                    )
                    # Re-fit the encoder with all values
                    self.label_encoders[col].fit(df[col])
                
                df[col] = self.label_encoders[col].transform(df[col])

        # Separate features and target
        if self.target_column not in df.columns:
            raise ValueError(f"Target column '{self.target_column}' not found in data")
        
        X = df.drop(columns=[self.target_column])
        y = df[self.target_column]

        # Apply SMOTE
        try:
            X_resampled, y_resampled = self.model.fit_resample(X, y)
        except Exception as e:
            logger.error(f"SMOTE fit_resample failed: {e}")
            raise ValueError(f"SMOTE generation failed: {e}")

        # Combine features and target
        df_resampled = X_resampled.copy()
        df_resampled[self.target_column] = y_resampled

        # Decode categorical columns back to original values
        for col in self.categorical_columns:
            if col in df_resampled.columns:
                # Ensure values are integers before inverse transform
                df_resampled[col] = df_resampled[col].round().astype(int)
                # Clip values to valid range for the encoder
                min_val = 0
                max_val = len(self.label_encoders[col].classes_) - 1
                df_resampled[col] = df_resampled[col].clip(min_val, max_val)
                # Inverse transform
                df_resampled[col] = self.label_encoders[col].inverse_transform(
                    df_resampled[col]
                )

        # Sample the requested number of rows
        if len(df_resampled) >= num_samples:
            synthetic_data = df_resampled.sample(n=num_samples, random_state=42)
        else:
            logger.warning(
                f"SMOTE generated {len(df_resampled)} samples, "
                f"which is less than requested {num_samples}. Returning all samples."
            )
            synthetic_data = df_resampled

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
            "model_type": "SMOTE",
            "trained_on_rows": self.training_metadata.get("dataset_rows", "N/A"),
            "columns": self.dataset_columns,
            "training_metadata": self.training_metadata,
        }

    def _save_model(self, path: str, training_metadata: Dict[str, Any] = None):
        """Save model to disk with training metadata"""
        model_data = {
            "model": self.model,
            "original_data": self.original_data,
            "label_encoders": self.label_encoders,
            "categorical_columns": self.categorical_columns,
            "target_column": self.target_column,
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
                self.original_data = model_data.get("original_data")
                self.label_encoders = model_data.get("label_encoders", {})
                self.categorical_columns = model_data.get("categorical_columns", [])
                self.target_column = model_data.get("target_column", "Heart_Disease")
                self.dataset_columns = model_data.get("columns")
                self.training_metadata = model_data.get("training_metadata", {})

                logger.info("Model loaded successfully")
                if self.training_metadata:
                    logger.info(f"Training metadata: {self.training_metadata}")
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")
                self.model = None


# Register SMOTE generator in factory
GeneratorFactory.register(GeneratorMethod.SMOTE, SMOTEGenerator)
