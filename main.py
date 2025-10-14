import os
import glob
import snowflake.connector
import json
import sys
import traceback
import re,io

# Config
config_file = f"config/prod/prod_config.json"
with open(config_file) as f:
    cfg = json.load(f)

print("✅ Loaded config successfully")

demo = os.environ.get("demo", "Not Set")
print(f"Demo env var: {demo}")
# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=cfg["username"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        role=cfg["role"]
    )
    cur = conn.cursor()
    print("✅ Connected to Snowflake")
except Exception as e:
    print("❌ Failed to connect to Snowflake:", str(e))
    sys.exit(1)

# Ensure audit table exists
try:
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
    print("✅ Verified MIGRATION_LOGS table")
except Exception as e:
    print("❌ Failed creating MIGRATION_LOGS:", str(e))
    traceback.print_exc()
    sys.exit(1)

# Get last applied versions
cur.execute("SELECT DISTINCT version FROM MIGRATION.DBO.MIGRATION_LOGS WHERE env = 'prod' and STATUS = 'SUCCESS'")
applied_versions = {row[0] for row in cur.fetchall()}
print(applied_versions)
if not applied_versions:
    print("⚠️ No previous versions found, starting fresh from first migration")
    applied_versions = set()

# Migration path
migration_path = f"source_code/prod/migration/*"
files = sorted(glob.glob(migration_path))
print(files)
print(f"🔍 Found {len(files)} migration files")

any_failed = False

for file in files:
    version = os.path.basename(file)
    print(version)
    print(applied_versions)
    print(file)

    # Compare
    if version in applied_versions:
        print(f"⏩ Skipping already applied version {version}")
        continue
    
    print(file)
    print(f"🚀 Applying version: {version}")
    sql_file = file
    db_name = sql_file.split("_")[-1]
    print(f"▶️ Running {sql_file} ...")
    with open(sql_file, "r") as f:
        sql = f.read()
        sql = re.sub(r"ASQN_DEV", "ASQN_PRD", sql, flags=re.IGNORECASE)
        print(sql)
        try:
            sql_stream = io.StringIO(sql)

            # Execute SQL statements from stream
            for cs in conn.execute_stream(sql_stream):
                print(f"✅ Executed statement: {cs}")
            status, error_message = "SUCCESS", None
            print(f"✅ Success: {sql_file}")
        except Exception as e:
            status, error_message = "FAILED", str(e)
            print(f"❌ Failed: {sql_file}\n   Error: {error_message}")
            traceback.print_exc()
            any_failed = True

    # Insert log entry
    try:
        cur.execute("""
            INSERT INTO MIGRATION.DBO.MIGRATION_LOGS (env, db_name, version, file_name, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ('prod', db_name, version, os.path.basename(sql_file), status, error_message))
    except Exception as e:
        print(f"❌ Failed to insert log for {sql_file}: {e}")
        traceback.print_exc()
        any_failed = True

conn.close()
print("✅ Migration script completed")

if any_failed:
    print("❌ Some migrations failed, exiting with error")
    sys.exit(1)
else:
    print("🎉 All migrations applied successfully")
