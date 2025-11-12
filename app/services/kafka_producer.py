import json
from typing import List, Dict, Any
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from config import get_settings

settings = get_settings()


class KafkaProducerService:
    """
    Kafka producer for sending synthetic cardiovascular data
    """

    def __init__(self):
        self.producer = None
        self.topic = settings.kafka_topic
        self._connect()

    def _connect(self):
        """Initialize Kafka producer connection"""
        try:
            logger.info(f"Connecting to Kafka at {settings.kafka_bootstrap_servers}")

            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
            )

            logger.info("Kafka producer connected successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            logger.warning("Kafka producer will retry connection on next send")

    def send_single(self, data: Dict[str, Any], key: str = None) -> bool:
        """
        Send a single record to Kafka

        Args:
            data: Dictionary representing one record
            key: Optional message key

        Returns:
            True if successful, False otherwise
        """
        if self.producer is None:
            self._connect()

        if self.producer is None:
            logger.error("Cannot send message: Kafka producer not available")
            return False

        try:
            future = self.producer.send(self.topic, value=data, key=key)

            # Wait for confirmation
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )

            return True

        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False

    def send_batch(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Send a batch of records to Kafka

        Args:
            df: DataFrame with synthetic data

        Returns:
            Dictionary with send statistics
        """
        if self.producer is None:
            self._connect()

        if self.producer is None:
            logger.error("Cannot send batch: Kafka producer not available")
            return {"success": False, "sent": 0, "failed": 0, "total": len(df)}

        sent = 0
        failed = 0

        records = df.to_dict(orient="records")

        for idx, record in enumerate(records):
            # Use row index as key for partitioning
            key = str(idx)

            # Convert numpy types to native Python types for JSON serialization
            clean_record = self._clean_record(record)

            if self.send_single(clean_record, key=key):
                sent += 1
            else:
                failed += 1

        # Flush to ensure all messages are sent
        self.producer.flush()

        logger.info(f"Batch send completed: {sent} sent, {failed} failed out of {len(records)}")

        return {"success": failed == 0, "sent": sent, "failed": failed, "total": len(records)}

    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert numpy/pandas types to native Python types"""
        import numpy as np

        clean = {}
        for key, value in record.items():
            if pd.isna(value):
                clean[key] = None
            elif isinstance(value, (np.integer, np.int64, np.int32)):
                clean[key] = int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                clean[key] = float(value)
            elif isinstance(value, np.bool_):
                clean[key] = bool(value)
            elif isinstance(value, np.ndarray):
                clean[key] = value.tolist()
            else:
                clean[key] = value

        return clean

    def close(self):
        """Close Kafka producer connection"""
        if self.producer:
            logger.info("Closing Kafka producer")
            self.producer.close()
            self.producer = None

    def health_check(self) -> bool:
        """Check if Kafka connection is healthy"""
        if self.producer is None:
            return False

        try:
            # Try to get cluster metadata
            self.producer.bootstrap_connected()
            return True
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return False
