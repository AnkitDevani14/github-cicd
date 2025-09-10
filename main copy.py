import os
import glob
import snowflake.connector
import json
from datetime import datetime

# Environment (main -> prod, dev -> dev)
# env = os.getenv("ENV", "prod")
# config_file = f"config/{env}/prod_config.json"
config_file = f"config/prod/prod_config.json"

with open(config_file) as f:
    cfg = json.load(f)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=cfg["username"],
    # password=os.getenv("SNOWFLAKE_PASSWORD"),
    password=cfg["password"],
    account=cfg["account"],
    warehouse=cfg["warehouse"],
    database=cfg["database"],
    role=cfg["role"]
)
cur = conn.cursor()

# Ensure audit table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS MIGRATION.DBO.MIGRATION_LOGS (
    env STRING,
    db_name STRING,
    version STRING,
    file_name STRING,
    status STRING,
    error_message STRING,
    executed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
)
""")

# Get last applied versions from Snowflake
# cur.execute("SELECT DISTINCT version FROM MIGRATION.DBO.MIGRATION_LOGS WHERE env = %s", (env,))
cur.execute("SELECT DISTINCT version FROM MIGRATION.DBO.MIGRATION_LOGS WHERE env = 'prod'")
applied_versions = {row[0] for row in cur.fetchall()}

if not applied_versions:
    applied_versions = 'W_V1_1'
# Migration path
# migration_path = f"source_code/{env}/migration/*"
migration_path = f"source_code/prod/migration/*"
folders = sorted(glob.glob(migration_path))

for folder in folders:
    version = os.path.basename(folder)
    if version in applied_versions:
        continue  # skip already applied versions

    sql_files = sorted(glob.glob(f"{folder}/*.sql"))
    for sql_file in sql_files:
        db_name = sql_file.split("/")[-2]  # you may adjust if DB naming is different
        with open(sql_file, "r") as f:
            sql = f.read()
        try:
            cur.execute(sql)
            status, error_message = "SUCCESS", None
        except Exception as e:
            status, error_message = "FAILED", str(e)

        # Insert log entry
        cur.execute("""
            INSERT INTO MIGRATION_LOGS (env, db_name, version, file_name, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)s
        """, ('prod', db_name, version, os.path.basename(sql_file), status, error_message))

conn.close()
