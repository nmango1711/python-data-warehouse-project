import os
from dotenv import load_dotenv
from src.config.db_config import get_connection

load_dotenv()

DATABASE = os.getenv("DATABASE")

def create_db_if_not_exists():
    try:
        conn = get_connection()
        
        cursor = conn.cursor()
        
        cursor.execute(f"""
        IF NOT EXISTS (
            SELECT name FROM sys.databases WHERE name = '{DATABASE}'
        )
        BEGIN
        CREATE DATABASE [{DATABASE}]
        END;
        """)

        cursor.close()
        conn.close()
        return True
    
    except Exception as e:
        print("ERROR CREATING DATABASE:", e)
        return False