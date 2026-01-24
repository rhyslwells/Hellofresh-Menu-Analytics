"""
DATABRICKS SETUP SCRIPT

Purpose
-------
Initialize all necessary Databricks resources before running the data pipeline.
Creates catalogs, schemas, and validates configuration.

Usage
-----
Run once at the beginning in Databricks:
%run ./0_setup
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Databricks environment check
try:
    dbutils
    IN_DATABRICKS = True
except NameError:
    IN_DATABRICKS = False
    raise RuntimeError("This script must run in Databricks!")


# ======================
# Configuration
# ======================

CATALOG = "hfresh_catalog"
BRONZE_SCHEMA = "hfresh_bronze"
SILVER_SCHEMA = "hfresh_silver"
GOLD_SCHEMA = "hfresh_gold"


# ======================
# Setup Functions
# ======================

def setup_databricks():
    """Initialize Databricks environment."""
    spark = SparkSession.builder.appName("hfresh_setup").getOrCreate()
    
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║  HelloFresh Databricks Setup                             ║
    ║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^56} ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Create catalog
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        print(f"✓ Catalog created: {CATALOG}")
    except Exception as e:
        print(f"⚠️  Catalog creation: {e}")
    
    # Create bronze schema
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
        print(f"✓ Bronze schema created: {CATALOG}.{BRONZE_SCHEMA}")
    except Exception as e:
        print(f"⚠️  Bronze schema creation: {e}")
    
    # Create silver schema
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
        print(f"✓ Silver schema created: {CATALOG}.{SILVER_SCHEMA}")
    except Exception as e:
        print(f"⚠️  Silver schema creation: {e}")
    
    # Create gold schema
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
        print(f"✓ Gold schema created: {CATALOG}.{GOLD_SCHEMA}")
    except Exception as e:
        print(f"⚠️  Gold schema creation: {e}")
    
    # Create bronze table
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.api_responses (
                pull_date DATE,
                endpoint STRING,
                locale STRING,
                page_number INT,
                payload STRING,
                ingestion_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (pull_date, endpoint)
        """)
        print(f"✓ Bronze table created: api_responses")
    except Exception as e:
        print(f"⚠️  Bronze table creation: {e}")
    
    # Validate setup
    print(f"\n{'='*60}")
    print("Setup Validation")
    print(f"{'='*60}\n")
    
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{BRONZE_SCHEMA}.api_responses").collect()[0][0]
        print(f"✓ Bronze table accessible: {count} records")
    except Exception as e:
        print(f"✗ Bronze table error: {e}")
    
    # Show schemas
    print(f"\nCreated schemas:")
    spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show()
    
    print(f"\n{'='*60}")
    print("Setup Complete!")
    print(f"{'='*60}\n")
    
    print("Next steps:")
    print("1. Run: %run ./1_bronze")
    print("2. Run: %run ./2_silver")
    print("3. Run: %run ./3_gold_analytics")
    print("4. Run: %run ./6_weekly_report\n")


if __name__ == "__main__":
    setup_databricks()
