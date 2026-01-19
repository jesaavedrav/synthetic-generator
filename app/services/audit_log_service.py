
from typing import Optional, Dict, Any
from app.services.audit_log_producer import AuditLogKafkaProducer

class AuditLogService:
    _producer = AuditLogKafkaProducer()

    @staticmethod
    def log_event(event_type: str, message: str, data: Optional[Dict[str, Any]] = None, success: bool = True):
        AuditLogService._producer.send_log(event_type, message, data, success)
