#!/usr/bin/env python3
"""
Script to test Snowflake connection and setup
"""

import sys
from dotenv import load_dotenv
import snowflake.connector
from config import get_settings

# Load environment
load_dotenv()
settings = get_settings()


def test_connection():
    """Test connection to Snowflake"""
    print("=" * 60)
    print("Testing Snowflake Connection")
    print("=" * 60)
    
    print(f"\nAccount: {settings.snowflake_account}")
    print(f"User: {settings.snowflake_user}")
    print(f"Warehouse: {settings.snowflake_warehouse}")
    print(f"Database: {settings.snowflake_database}")
    print(f"Schema: {settings.snowflake_schema}")
    print(f"Table: {settings.snowflake_table}")
    
    try:
    print("\n Connecting to Snowflake...")
    
    conn = snowflake.connector.connect(
    account=settings.snowflake_account,
    user=settings.snowflake_user,
    password=settings.snowflake_password,
    warehouse=settings.snowflake_warehouse,
    )
    
    cursor = conn.cursor()
    
    print(" Connection successful!")
    
    # Test query
    print("\n Running test query...")
    cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_WAREHOUSE()")
    result = cursor.fetchone()
    
    print(f" Snowflake Version: {result[0]}")
    print(f" Current User: {result[1]}")
    print(f" Current Warehouse: {result[2]}")
    
    # Check if database exists
    print(f"\n Checking database '{settings.snowflake_database}'...")
    cursor.execute(f"SHOW DATABASES LIKE '{settings.snowflake_database}'")
    databases = cursor.fetchall()
    
    if databases:
    print(f" Database exists")
    
    # Use database
    cursor.execute(f"USE DATABASE {settings.snowflake_database}")
    
    # Check if schema exists
    print(f"\n Checking schema '{settings.snowflake_schema}'...")
    cursor.execute(f"SHOW SCHEMAS LIKE '{settings.snowflake_schema}'")
    schemas = cursor.fetchall()
    
    if schemas:
    print(f" Schema exists")
    
    # Use schema
    cursor.execute(f"USE SCHEMA {settings.snowflake_schema}")
    
    # Check if table exists
    print(f"\n Checking table '{settings.snowflake_table}'...")
    cursor.execute(f"SHOW TABLES LIKE '{settings.snowflake_table}'")
    tables = cursor.fetchall()
    
    if tables:
    print(f" Table exists")
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {settings.snowflake_table}")
    count = cursor.fetchone()[0]
    print(f" Current row count: {count:,}")
    else:
    print(f" ℹ Table doesn't exist yet (will be created automatically)")
    else:
    print(f" Schema '{settings.snowflake_schema}' not found")
    print(f" Create it with: CREATE SCHEMA {settings.snowflake_schema};")
    else:
    print(f" Database '{settings.snowflake_database}' not found")
    print(f" Create it with: CREATE DATABASE {settings.snowflake_database};")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print(" Snowflake is configured correctly!")
    print("=" * 60)
    return True
    
    except snowflake.connector.errors.ProgrammingError as e:
    print(f"\n Snowflake Error: {e}")
    print(f" Error code: {e.errno}")
    print(f" SQL State: {e.sqlstate}")
    return False
    
    except Exception as e:
    print(f"\n Connection failed: {e}")
    print("\n Please check:")
    print(" 1. SNOWFLAKE_ACCOUNT is correct (format: account.region)")
    print(" 2. SNOWFLAKE_USER and SNOWFLAKE_PASSWORD are correct")
    print(" 3. SNOWFLAKE_WAREHOUSE exists and you have access")
    print(" 4. Your IP is not blocked by Snowflake network policies")
    return False


def create_database_and_schema():
    """Helper to create database and schema"""
    print("\n" + "=" * 60)
    print("Creating Database and Schema")
    print("=" * 60)
    
    try:
    conn = snowflake.connector.connect(
    account=settings.snowflake_account,
    user=settings.snowflake_user,
    password=settings.snowflake_password,
    warehouse=settings.snowflake_warehouse,
    )
    
    cursor = conn.cursor()
    
    # Create database
    print(f"\n Creating database '{settings.snowflake_database}'...")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {settings.snowflake_database}")
    print(" Database created")
    
    # Use database
    cursor.execute(f"USE DATABASE {settings.snowflake_database}")
    
    # Create schema
    print(f"\n Creating schema '{settings.snowflake_schema}'...")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.snowflake_schema}")
    print(" Schema created")
    
    cursor.close()
    conn.close()
    
    print("\n Setup complete!")
    return True
    
    except Exception as e:
    print(f"\n Failed to create resources: {e}")
    return False


if __name__ == "__main__":
    print("\n Snowflake Configuration Test")
    
    # Test connection
    if test_connection():
    print("\n You're all set! You can now run the application.")
    else:
    print("\n Would you like to create the database and schema? (y/n)")
    response = input().strip().lower()
    
    if response == 'y':
    if create_database_and_schema():
    print("\n Running connection test again...")
    test_connection()
