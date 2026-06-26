import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env variables from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "hyperlocal_demand")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "A4apple")

def get_db_connection():
    """Returns a raw psycopg2 connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_db_engine():
    """Returns a SQLAlchemy engine for the PostgreSQL database."""
    connection_uri = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_uri)

def init_db():
    """Reads data/init_schema.sql and runs it to initialize the database tables."""
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "init_schema.sql")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found at {schema_path}")
        
    print(f"Initializing database schema from {schema_path}...")
    with open(schema_path, "r") as f:
        schema_sql = f.read()
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(schema_sql)
        conn.commit()
        print("Database schema initialized successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing schema: {e}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
