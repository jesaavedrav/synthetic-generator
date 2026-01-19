"""
DEPRECATED: This service used to persist tasks in Snowflake. All task persistence is now handled by PostgresTaskService (see postgres_task_service.py).
This file is retained for reference only and should not be used in production.
"""
import json
from typing import Optional, Dict, Any, List
from config.settings import get_settings
import snowflake.connector

class TaskService:
    @staticmethod
    def create_task(task_id: str, type_: str, status: str, request_data: Dict[str, Any], progress: int = 0, error: Optional[str] = None):
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
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS TRAINING_TASKS (
                        TASK_ID STRING PRIMARY KEY,
                        TYPE STRING,
                        STATUS STRING,
                        REQUEST_DATA STRING,
                        PROGRESS INTEGER,
                        ERROR STRING,
                        CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                        UPDATED_AT TIMESTAMP_LTZ
                    )
                """)
                cur.execute(
                    """
                    INSERT INTO TRAINING_TASKS (TASK_ID, TYPE, STATUS, REQUEST_DATA, PROGRESS, ERROR, UPDATED_AT)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                    """,
                    (
                        task_id,
                        type_,
                        status,
                        json.dumps(request_data) if request_data else None,
                        progress,
                        error,
                    ),
                )
        finally:
            conn.close()

    @staticmethod
    def update_task(task_id: str, status: Optional[str] = None, progress: Optional[int] = None, error: Optional[str] = None):
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
                updates = []
                params = []
                if status is not None:
                    updates.append("STATUS = %s")
                    params.append(status)
                if progress is not None:
                    updates.append("PROGRESS = %s")
                    params.append(progress)
                if error is not None:
                    updates.append("ERROR = %s")
                    params.append(error)
                if not updates:
                    return
                updates.append("UPDATED_AT = CURRENT_TIMESTAMP()")
                sql = f"UPDATE TRAINING_TASKS SET {', '.join(updates)} WHERE TASK_ID = %s"
                params.append(task_id)
                cur.execute(sql, tuple(params))
        finally:
            conn.close()

    @staticmethod
    def get_task(task_id: str) -> Optional[dict]:
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
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute("SELECT * FROM TRAINING_TASKS WHERE TASK_ID = %s", (task_id,))
                row = cur.fetchone()
                if row and row["REQUEST_DATA"]:
                    try:
                        row["REQUEST_DATA"] = json.loads(row["REQUEST_DATA"])
                    except Exception:
                        row["REQUEST_DATA"] = None
                return row
        finally:
            conn.close()

    @staticmethod
    def list_tasks(skip: int = 0, limit: int = 100) -> List[dict]:
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
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute("""
                    SELECT * FROM TRAINING_TASKS
                    ORDER BY CREATED_AT DESC
                    LIMIT %s OFFSET %s
                """, (limit, skip))
                rows = cur.fetchall()
                for row in rows:
                    if row["REQUEST_DATA"]:
                        try:
                            row["REQUEST_DATA"] = json.loads(row["REQUEST_DATA"])
                        except Exception:
                            row["REQUEST_DATA"] = None
                return rows
        finally:
            conn.close()

    @staticmethod
    def count_tasks() -> int:
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
                cur.execute("SELECT COUNT(*) FROM TRAINING_TASKS")
                (count,) = cur.fetchone()
                return count
        finally:
            conn.close()
