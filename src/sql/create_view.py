from src.sql.create_database import create_db_if_not_exists
from src.config.db_config import get_connection

create_db_if_not_exists()

def create_view_if_not_exists(view_name):
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
        print(f"Succesfully created View: {view_name}")
        return True
    
    except Exception as e:
        print("ERROR CREATING VIEW:", e)
        return False