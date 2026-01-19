import json
from config.settings import get_settings
from kafka import KafkaConsumer
import snowflake.connector

def write_audit_log_to_snowflake(event):
    settings = get_settings()
    conn = snowflake.connector.connect(
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        account=settings.snowflake_account,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO AUDIT_LOG (EVENT_TYPE, MESSAGE, DATA, SUCCESS)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    event.get("event_type"),
                    event.get("message"),
                    json.dumps(event.get("data")) if event.get("data") else None,
                    event.get("success", True),
                ),
            )
    finally:
        conn.close()

def main():
    settings = get_settings()
    topic = getattr(settings, "kafka_audit_log_topic", "audit-log")
    # Crear tabla solo una vez al inicio
    conn = snowflake.connector.connect(
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        account=settings.snowflake_account,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS AUDIT_LOG (
                    ID INTEGER AUTOINCREMENT PRIMARY KEY,
                    EVENT_TYPE STRING,
                    MESSAGE STRING,
                    DATA STRING,
                    SUCCESS BOOLEAN,
                    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
    finally:
        conn.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="audit-log-consumer-group",
    )
    print(f"[audit-log-consumer] Listening to topic: {topic}")
    for msg in consumer:
        event = msg.value
        write_audit_log_to_snowflake(event)
        print(f"[audit-log-consumer] Wrote event: {event}")

if __name__ == "__main__":
    main()
