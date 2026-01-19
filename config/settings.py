from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Postgres Configuration
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_db: str = "synthetic_generator"

    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_title: str = "Cardiovascular Synthetic Data Generator"
    api_version: str = "1.0.0"

    # Kafka Configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "cardiovascular-data"
    kafka_group_id: str = "cardiovascular-consumer-group"
    kafka_auto_offset_reset: str = "earliest"

    # Audit log topic
    kafka_audit_log_topic: str = "audit-log"

    # Snowflake Configuration
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str
    snowflake_database: str = "CARDIOVASCULAR_DB"
    snowflake_schema: str = "SYNTHETIC_DATA"
    snowflake_table: str = "PATIENT_DATA"

    # Data Generation
    dataset_path: str = "./data/cardiovascular_data.csv"
    model_path: str = "./models/cardiovascular_model.pkl"
    batch_size: int = 10
    generation_interval: int = 5

    # Logging
    log_level: str = "INFO"

    # Kaggle API (optional)
    kaggle_username: str = ""
    kaggle_key: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = False
        protected_namespaces = ()


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
