from src.config.db_config import get_connection
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE = os.getenv("DATABASE")

def create_db_if_not_exists():
    conn = get_connection()

    cursor = conn.cursor()

    cursor.execute(f"""
    IF NOT EXISTS (
        SELECT name
        FROM sys.databases
        WHERE name = '{DATABASE}'
    )
    CREATE DATABASE {DATABASE};
    """)

    cursor.close()
    conn.close()