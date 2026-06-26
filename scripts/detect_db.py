import psycopg2
import sys
import os

passwords_to_try = ["postgres", "admin", "123456", "root", "password", ""]
db_name = "postgres"  # default system db to check connectivity
user = "postgres"
host = "localhost"
port = "5432"

working_password = None

print("Checking PostgreSQL connection on localhost:5432...")

for pwd in passwords_to_try:
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=pwd,
            host=host,
            port=port,
            connect_timeout=3
        )
        conn.close()
        working_password = pwd
        print(f"Success! Connected to PostgreSQL with password: '{pwd}'")
        break
    except psycopg2.OperationalError as e:
        print(f"Failed with password '{pwd}': {e}")
        continue

if working_password is not None:
    # Let's write .env and .env.example
    env_content = f"""# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hyperlocal_demand
DB_USER=postgres
DB_PASSWORD={working_password}

# Airflow configurations
AIRFLOW_HOME={os.path.abspath(os.getcwd())}/airflow
"""
    with open(".env", "w") as f:
        f.write(env_content)
    print("Created .env file with database credentials.")

    # Write .env.example
    env_example_content = """# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hyperlocal_demand
DB_USER=postgres
DB_PASSWORD=your_password_here

# Airflow configurations
AIRFLOW_HOME=./airflow
"""
    with open(".env.example", "w") as f:
        f.write(env_example_content)
    print("Created .env.example file.")
    sys.exit(0)
else:
    print("\nCould not connect to PostgreSQL using common default passwords.")
    print("Please create a .env file manually containing:")
    print("DB_HOST=localhost")
    print("DB_PORT=5432")
    print("DB_NAME=hyperlocal_demand")
    print("DB_USER=postgres")
    print("DB_PASSWORD=<your_actual_password>")
    print("AIRFLOW_HOME=./airflow")
    sys.exit(1)
