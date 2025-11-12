import snowflake.connector
from snowflake.connector import DictCursor
from typing import List, Dict, Any, Optional
from loguru import logger
import pandas as pd

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
            ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            message_id VARCHAR(100),
            source VARCHAR(50) DEFAULT 'synthetic_generator'
        )
        """

        try:
            self.cursor.execute(create_table_sql)
            logger.info(f"Table {settings.snowflake_table} ensured to exist")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
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

            # Build insert query dynamically based on columns
            columns = list(df.columns)
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)

            insert_sql = f"""
            INSERT INTO {settings.snowflake_table} 
            ({column_names})
            VALUES ({placeholders})
            """

            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.values]

            # Execute batch insert
            self.cursor.executemany(insert_sql, data)
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
