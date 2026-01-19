import snowflake.connector
from snowflake.connector import DictCursor
from typing import List, Dict, Any, Optional
from loguru import logger
import pandas as pd
import json

from config import get_settings

settings = get_settings()


class SnowflakeWriter:
    """
    Writes synthetic cardiovascular data to Snowflake
    """

    def __init__(self):
        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None
        self.cursor: Optional[DictCursor] = None
        self._connect()

    def _connect(self):
        """Establish connection to Snowflake"""
        try:
            logger.info("Connecting to Snowflake...")

            self.connection = snowflake.connector.connect(
                account=settings.snowflake_account,
                user=settings.snowflake_user,
                password=settings.snowflake_password,
                warehouse=settings.snowflake_warehouse,
                database=settings.snowflake_database,
                schema=settings.snowflake_schema,
                session_parameters={
                    "QUERY_TAG": "cardiovascular_synthetic_data_ingestion",
                },
            )

            self.cursor = self.connection.cursor(DictCursor)

            logger.info(
                f"Connected to Snowflake: {settings.snowflake_database}.{settings.snowflake_schema}"
            )

            # Ensure table exists
            self._create_table_if_not_exists()

            # Ensure audit log table exists
            self._create_audit_log_table_if_not_exists()

        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the cardiovascular data table if it doesn't exist"""

        # This schema should match the Kaggle cardiovascular dataset
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {settings.snowflake_table} (
            -- Demographics
            general_health VARCHAR(50),
            checkup VARCHAR(50),
            exercise VARCHAR(10),
            heart_disease VARCHAR(10),
            skin_cancer VARCHAR(10),
            other_cancer VARCHAR(10),
            depression VARCHAR(10),
            diabetes VARCHAR(50),
            arthritis VARCHAR(10),
            sex VARCHAR(10),
            age_category VARCHAR(50),
            height_cm FLOAT,
            weight_kg FLOAT,
            bmi FLOAT,
            smoking_history VARCHAR(10),
            alcohol_consumption FLOAT,
            fruit_consumption FLOAT,
            green_vegetables_consumption FLOAT,
            fried_potato_consumption FLOAT,

            -- Metadata
            message_id VARCHAR(100),
            source VARCHAR(50) DEFAULT 'synthetic_generator',
            generation_metadata VARIANT,
            ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """

        try:
            self.cursor.execute(create_table_sql)
            logger.info(f"Table {settings.snowflake_table} ensured to exist")

            self._ensure_required_columns()

        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _ensure_required_columns(self):
        """Ensure legacy tables contain required metadata columns."""
        required_columns = {
            "message_id": "VARCHAR(100)",
            "source": "VARCHAR(50) DEFAULT 'synthetic_generator'",
            "generation_metadata": "VARIANT",
            "ingested_at": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()",
        }

        try:
            self.cursor.execute(f"DESC TABLE {settings.snowflake_table}")
            existing = {row["name"].lower() for row in self.cursor.fetchall()}
        except Exception as e:
            logger.warning(f"Unable to describe table {settings.snowflake_table}: {e}")
            return

        for column, definition in required_columns.items():
            if column not in existing:
                statement = (
                    f"ALTER TABLE {settings.snowflake_table} "
                    f"ADD COLUMN {column} {definition}"
                )
                try:
                    self.cursor.execute(statement)
                    logger.info(f"Column {column} added to {settings.snowflake_table}")
                except Exception as alter_error:
                    logger.error(
                        f"Failed to add column {column} to {settings.snowflake_table}: {alter_error}"
                    )

    def _create_audit_log_table_if_not_exists(self):
        """Create the audit log table if it doesn't exist"""
        sql = """
        CREATE TABLE IF NOT EXISTS AUDIT_LOG (
            ID STRING,
            EVENT_TYPE STRING,
            MESSAGE STRING,
            DATA VARIANT,
            SUCCESS BOOLEAN,
            CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        try:
            self.cursor.execute(sql)
            logger.info("Table AUDIT_LOG ensured to exist")
        except Exception as e:
            logger.error(f"Failed to create AUDIT_LOG table: {e}")
            raise

    def write_batch(self, records: List[Dict[str, Any]]) -> bool:
        """
        Write a batch of records to Snowflake

        Args:
            records: List of dictionaries with cardiovascular data

        Returns:
            True if successful, False otherwise
        """
        if not records:
            logger.warning("No records to write")
            return True

        try:
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(records)

            # Normalize column names to match Snowflake schema
            # Explicit mapping to avoid any transformation issues
            COLUMN_MAPPING = {
                'General_Health': 'general_health',
                'Checkup': 'checkup',
                'Exercise': 'exercise',
                'Heart_Disease': 'heart_disease',
                'Skin_Cancer': 'skin_cancer',
                'Other_Cancer': 'other_cancer',
                'Depression': 'depression',
                'Diabetes': 'diabetes',
                'Arthritis': 'arthritis',
                'Sex': 'sex',
                'Age_Category': 'age_category',
                'Height_(cm)': 'height_cm',
                'Weight_(kg)': 'weight_kg',
                'BMI': 'bmi',
                'Smoking_History': 'smoking_history',
                'Alcohol_Consumption': 'alcohol_consumption',
                'Fruit_Consumption': 'fruit_consumption',
                'Green_Vegetables_Consumption': 'green_vegetables_consumption',
                'FriedPotato_Consumption': 'fried_potato_consumption',
                'message_id': 'message_id',
                'source': 'source',
                'generation_metadata': 'generation_metadata',
            }
            
            column_mapping = {}
            for col in df.columns:
                if col in COLUMN_MAPPING:
                    column_mapping[col] = COLUMN_MAPPING[col]
                else:
                    # Fallback: convert to lowercase with underscores
                    import re
                    normalized_col = re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower()
                    normalized_col = re.sub(r'_+', '_', normalized_col).strip('_')
                    column_mapping[col] = normalized_col
                    logger.warning(f"Unknown column '{col}', using fallback mapping: '{normalized_col}'")



            # Rename columns
            df = df.rename(columns=column_mapping)
            # Serializa metadata de generación para almacenarla como VARIANT
            if 'generation_metadata' in df.columns:
                df['generation_metadata'] = df['generation_metadata'].apply(
                    lambda val: json.dumps(val) if isinstance(val, (dict, list)) else val
                )

            # Ensure message_id column exists and fill if missing
            if 'message_id' not in df.columns:
                import uuid
                df['message_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
            df['message_id'] = df['message_id'].astype(str)

            # Ensure source column exists and fill if missing
            if 'source' not in df.columns:
                df['source'] = 'synthetic_generator'
            df['source'] = df['source'].fillna('synthetic_generator')

            # Ensure all columns in table schema are present (order matters for insert)
            table_columns = [
                'general_health', 'checkup', 'exercise', 'heart_disease', 'skin_cancer',
                'other_cancer', 'depression', 'diabetes', 'arthritis', 'sex', 'age_category',
                'height_cm', 'weight_kg', 'bmi', 'smoking_history', 'alcohol_consumption',
                'fruit_consumption', 'green_vegetables_consumption', 'fried_potato_consumption',
                'message_id', 'source', 'generation_metadata', 'ingested_at'
            ]
            columns_with_db_defaults = {'ingested_at'}
            # Add missing columns as None (exclude columns que Snowflake rellena)
            for col in table_columns:
                if col not in df.columns and col not in columns_with_db_defaults:
                    df[col] = None
            # Remove extra columns not in schema y conserva solo los que vamos a insertar
            columns_for_insert = [col for col in table_columns if col in df.columns]
            df = df[columns_for_insert]

            # Build insert query usando subquery SELECT para poder aplicar PARSE_JSON
            columns = list(df.columns)
            column_names = ", ".join(columns)
            value_aliases = [f"val_{idx}" for idx in range(len(columns))]
            placeholders = ", ".join(["%s"] * len(columns))

            select_projection = []
            for alias, col in zip(value_aliases, columns):
                if col == 'generation_metadata':
                    select_projection.append(f"PARSE_JSON(input_values.{alias})")
                else:
                    select_projection.append(f"input_values.{alias}")
            select_projection_clause = ", ".join(select_projection)

            select_assignments = ", ".join(
                [f"%s AS {alias}" for alias in value_aliases]
            )

            insert_sql = f"""
            INSERT INTO {settings.snowflake_table}
            ({column_names})
            SELECT {select_projection_clause}
            FROM (SELECT {select_assignments}) AS input_values
            """

            # Ejecuta fila por fila para evitar la reescritura multi-row incompatible
            for row in df.itertuples(index=False, name=None):
                self.cursor.execute(insert_sql, row)
            self.connection.commit()

            logger.info(
                f"Successfully inserted {len(records)} records into {settings.snowflake_table}"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to write batch to Snowflake: {e}")

            # Rollback on error
            if self.connection:
                self.connection.rollback()

            return False

    def write_audit_batch(self, records: List[Dict[str, Any]]) -> bool:
        """
        Write a batch of audit log records to Snowflake AUDIT_LOG table

        Args:
            records: List of audit event dictionaries

        Returns:
            True if successful, False otherwise
        """
        if not records:
            logger.warning("No audit records to write")
            return True

        try:
            logger.info(f"Writing {len(records)} audit records to Snowflake...")

            # Insert each record individually using PARSE_JSON in a SELECT statement
            for record in records:
                data_json = json.dumps(record.get("data")) if record.get("data") else None
                
                if data_json:
                    insert_sql = """
                    INSERT INTO AUDIT_LOG (ID, EVENT_TYPE, MESSAGE, DATA, SUCCESS, CREATED_AT)
                    SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s
                    """
                    self.cursor.execute(insert_sql, (
                        record.get("id"),
                        record.get("event_type"),
                        record.get("message"),
                        data_json,
                        record.get("success", True),
                        record.get("created_at"),
                    ))
                else:
                    # If no data, insert NULL
                    insert_sql = """
                    INSERT INTO AUDIT_LOG (ID, EVENT_TYPE, MESSAGE, DATA, SUCCESS, CREATED_AT)
                    VALUES (%s, %s, %s, NULL, %s, %s)
                    """
                    self.cursor.execute(insert_sql, (
                        record.get("id"),
                        record.get("event_type"),
                        record.get("message"),
                        record.get("success", True),
                        record.get("created_at"),
                    ))
            
            self.connection.commit()

            logger.info(
                f"Successfully inserted {len(records)} audit records into AUDIT_LOG"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to write audit batch to Snowflake: {e}")

            # Rollback on error
            if self.connection:
                self.connection.rollback()

            return False

    def write_single(self, record: Dict[str, Any]) -> bool:
        """Write a single record to Snowflake"""
        return self.write_batch([record])

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []

    def get_record_count(self) -> int:
        """Get total record count in the table"""
        try:
            query = f"SELECT COUNT(*) as count FROM {settings.snowflake_table}"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result["count"] if result else 0
        except Exception as e:
            logger.error(f"Failed to get record count: {e}")
            return 0

    def get_latest_records(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest records from the table"""
        query = f"""
        SELECT * FROM {settings.snowflake_table}
        ORDER BY ingested_at DESC
        LIMIT {limit}
        """
        return self.execute_query(query)

    def truncate_table(self):
        """Truncate the table (use with caution!)"""
        try:
            self.cursor.execute(f"TRUNCATE TABLE {settings.snowflake_table}")
            self.connection.commit()
            logger.warning(f"Table {settings.snowflake_table} truncated")
        except Exception as e:
            logger.error(f"Failed to truncate table: {e}")

    def close(self):
        """Close Snowflake connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing Snowflake connection: {e}")

    def health_check(self) -> bool:
        """Check if Snowflake connection is healthy"""
        try:
            self.cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Snowflake health check failed: {e}")
            return False
