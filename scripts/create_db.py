import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import os

passwords = ["A4apple.", "A4apple"]
target_db = "hyperlocal_demand"
user = "postgres"
host = "localhost"
port = "5432"

working_password = None
conn = None

for pwd in passwords:
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=pwd,
            host=host,
            port=port,
            connect_timeout=3
        )
        working_password = pwd
        print(f"Successfully authenticated with password: '{pwd}'")
        break
    except psycopg2.OperationalError as e:
        print(f"Failed with password '{pwd}': {e}")

if not conn or not working_password:
    print("Error: Could not connect to PostgreSQL with the provided passwords.")
    sys.exit(1)

# Set autocommit to True to create database
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Check if target database exists
cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'")
exists = cursor.fetchone()

if not exists:
    print(f"Creating database '{target_db}'...")
    cursor.execute(f"CREATE DATABASE {target_db}")
    print(f"Database '{target_db}' created successfully.")
else:
    print(f"Database '{target_db}' already exists.")

cursor.close()
conn.close()

# Write the .env file
env_content = f"""# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME={target_db}
DB_USER=postgres
DB_PASSWORD={working_password}

# Airflow configurations
AIRFLOW_HOME={os.path.abspath(os.getcwd())}/airflow
"""
with open(".env", "w") as f:
    f.write(env_content)
print("Created .env file with database credentials.")

# Write .env.example
env_example_content = f"""# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME={target_db}
DB_USER=postgres
DB_PASSWORD=your_password_here

# Airflow configurations
AIRFLOW_HOME=./airflow
"""
with open(".env.example", "w") as f:
    f.write(env_example_content)
print("Created .env.example file.")
