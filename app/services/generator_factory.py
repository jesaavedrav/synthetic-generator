"""
Factory for creating different types of synthetic data generators
"""

from typing import Dict, Type, List, Any
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from loguru import logger


class BaseSyntheticGenerator(ABC):
    """Base class for all synthetic data generators"""

    def __init__(self):
        self.model = None
        self.metadata = None
        self.dataset_columns = None

    @abstractmethod
    def train(self, dataset_path: str, model_name: str, epochs: int, **kwargs) -> Dict:
        """Train the generator model"""
        pass

    @abstractmethod
    def generate(self, num_samples: int) -> pd.DataFrame:
        """Generate synthetic samples"""
        pass

    @abstractmethod
    def get_model_info(self) -> Dict:
        """Get information about the current model"""
        pass

    @abstractmethod
    def _save_model(self, path: str):
        """Save model to disk"""
        pass

    @abstractmethod
    def _load_model(self):
        """Load model from disk"""
        pass


class GeneratorMethod:
    """Available generator methods"""

    CTGAN = "ctgan"
    TVAE = "tvae"
    GAUSSIAN_COPULA = "gaussian_copula"
    SMOTE = "smote"


class GeneratorFactory:
    """Factory for creating synthetic data generators"""

    _generators: Dict[str, Type[BaseSyntheticGenerator]] = {}

    @classmethod
    def register(cls, method: str, generator_class: Type[BaseSyntheticGenerator]):
        """Register a new generator method"""
        cls._generators[method] = generator_class
        logger.info(f"Registered generator method: {method}")

    @classmethod
    def create(
        cls, method: str, model_name: str = "cardiovascular_model"
    ) -> BaseSyntheticGenerator:
        """Create a generator instance"""
        if method not in cls._generators:
            raise ValueError(
                f"Unknown generator method: {method}. Available: {list(cls._generators.keys())}"
            )

        generator_class = cls._generators[method]
        return generator_class(model_name=model_name)

    @classmethod
    def get_available_methods(cls) -> List[Dict[str, Any]]:
        """Get list of available generator methods with parameter support info"""
        # Define parameter support for each method
        method_configs = {
            GeneratorMethod.CTGAN: {
                "supports_epochs": True,
                "supports_batch_size": True,
                "default_epochs": 50,
                "default_batch_size": 500,
            },
            GeneratorMethod.TVAE: {
                "supports_epochs": True,
                "supports_batch_size": True,
                "default_epochs": 50,
                "default_batch_size": 500,
            },
            GeneratorMethod.GAUSSIAN_COPULA: {
                "supports_epochs": False,
                "supports_batch_size": False,
                "default_epochs": None,
                "default_batch_size": None,
            },
            GeneratorMethod.SMOTE: {
                "supports_epochs": False,
                "supports_batch_size": False,
                "default_epochs": None,
                "default_batch_size": None,
            },
        }

        methods = []
        for method, generator_class in cls._generators.items():
            config = method_configs.get(method, {})
            methods.append(
                {
                    "method": method,
                    "name": generator_class.__name__,
                    "description": (
                        generator_class.__doc__.strip()
                        if generator_class.__doc__
                        else "No description available"
                    ),
                    "supports_epochs": config.get("supports_epochs", True),
                    "supports_batch_size": config.get("supports_batch_size", True),
                    "default_epochs": config.get("default_epochs"),
                    "default_batch_size": config.get("default_batch_size"),
                }
            )
        return methods

    @classmethod
    def is_method_available(cls, method: str) -> bool:
        """Check if a method is available"""
        return method in cls._generators


def get_model_filename(method: str, model_name: str) -> str:
    """Generate model filename based on method and name"""
    return f"{model_name}_{method}.pkl"


def check_model_exists(model_name: str, method: str, models_dir: str = "./models") -> bool:
    """Check if a model already exists"""
    model_path = Path(models_dir) / get_model_filename(method, model_name)
    return model_path.exists()
