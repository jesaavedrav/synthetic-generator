import json
from config.settings import get_settings
from kafka import KafkaProducer

class AuditLogKafkaProducer:
    def __init__(self):
        settings = get_settings()
        self.topic = getattr(settings, "kafka_audit_log_topic", "audit-log")
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def send_log(self, event_type: str, message: str, data=None, success=True):
        log_event = {
            "event_type": event_type,
            "message": message,
            "data": data,
            "success": success,
        }
        self.producer.send(self.topic, value=log_event)
        self.producer.flush()
