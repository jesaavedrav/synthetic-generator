import json
import signal
import sys
from typing import Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from loguru import logger

from config import get_settings
from consumer.snowflake_writer import SnowflakeWriter

settings = get_settings()


class CardiovascularConsumer:
    """
    Kafka consumer that reads synthetic cardiovascular data
    and writes to Snowflake
    """

    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.snowflake_writer = SnowflakeWriter()
        self.running = True
        self.shutting_down = False

        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _connect(self):
        """Initialize Kafka consumer"""
        try:
            logger.info(f"Connecting to Kafka at {settings.kafka_bootstrap_servers}")
            topics = [settings.kafka_topic, settings.kafka_audit_log_topic]
            logger.info(f"Subscribing to topics: {topics}")

            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
                group_id=settings.kafka_group_id,
                auto_offset_reset=settings.kafka_auto_offset_reset,
                enable_auto_commit=False,  # Manual commit for safety
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                max_poll_records=100,  # Process in batches
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )

            logger.info("Kafka consumer connected successfully")

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def start(self):
        """Start consuming messages"""
        logger.info("Starting Cardiovascular Data Consumer")

        self._connect()

        try:
            cardiovascular_batch = []
            audit_batch = []
            batch_size = settings.batch_size

            logger.info(f"Listening for messages (batch size: {batch_size})...")

            for message in self.consumer:
                if not self.running:
                    break

                try:
                    # Extract data
                    data = message.value

                    logger.debug(
                        f"Received message - Topic: {message.topic}, "
                        f"Partition: {message.partition}, Offset: {message.offset}"
                    )

                    # Route to appropriate batch based on topic
                    if message.topic == settings.kafka_topic:
                        cardiovascular_batch.append(data)
                        # Process batch when full
                        if len(cardiovascular_batch) >= batch_size:
                            self._process_batch(cardiovascular_batch, topic_type="cardiovascular")
                            cardiovascular_batch = []
                    elif message.topic == settings.kafka_audit_log_topic:
                        audit_batch.append(data)
                        # Process batch when full
                        if len(audit_batch) >= batch_size:
                            self._process_batch(audit_batch, topic_type="audit")
                            audit_batch = []

                    # Commit offset after successful processing
                    self.consumer.commit()
                    logger.debug(f"Committed offset: {message.offset}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    logger.error(f"Message content: {message.value}")
                    # Continue processing other messages

            # Process remaining messages
            if cardiovascular_batch:
                self._process_batch(cardiovascular_batch, topic_type="cardiovascular")
            if audit_batch:
                self._process_batch(audit_batch, topic_type="audit")
                self.consumer.commit()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")

        finally:
            self._cleanup()

    def _process_batch(self, batch: list, topic_type="cardiovascular"):
        """Process a batch of messages and write to Snowflake"""
        logger.info(f"Processing batch of {len(batch)} {topic_type} messages")

        try:
            # Write to Snowflake based on topic type
            if topic_type == "cardiovascular":
                success = self.snowflake_writer.write_batch(batch)
            elif topic_type == "audit":
                success = self.snowflake_writer.write_audit_batch(batch)
            else:
                logger.error(f"Unknown topic type: {topic_type}")
                return

            if success:
                logger.info(f"Successfully wrote {len(batch)} {topic_type} records to Snowflake")
            else:
                logger.error(f"Failed to write {topic_type} batch to Snowflake")

        except Exception as e:
            logger.error(f"Error writing batch to Snowflake: {e}")
            # In production, you might want to write to a dead letter queue

    def _shutdown(self, signum, frame):
        """Handle shutdown signals"""
        if self.shutting_down:
            logger.warning("Already shutting down, please wait...")
            return

        self.shutting_down = True
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

        # Force exit if stuck (after second signal)
        if signum == signal.SIGINT:
            signal.signal(signal.SIGINT, signal.SIG_DFL)

    def _cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")

        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

        self.snowflake_writer.close()

        logger.info("Consumer shutdown complete")


def main():
    """Main entry point for the consumer"""
    # Setup logging
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=settings.log_level,
    )
    logger.add(
        "logs/consumer_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        level=settings.log_level,
    )

    # Start consumer
    consumer = CardiovascularConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
