from src.sql.create_database import create_db_if_not_exists
from src.sql.create_table import create_table_if_not_exists
from src.config.db_config import get_connection
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE = os.getenv("DATABASE")

def create_view_if_not_exists(view_name, df_customers, logger):

    table_name = f"stg_{view_name}"

    create_db_if_not_exists()
    create_table_if_not_exists(table_name, df_customers)

    try:
        conn = get_connection(DATABASE)
        cursor = conn.cursor()

        query = f"""
        IF OBJECT_ID('{view_name}', 'V') IS NOT NULL
            DROP VIEW {view_name};

        EXEC('
            CREATE VIEW {view_name} AS
            SELECT *
            FROM dbo.{table_name}
        ');
        """
        cursor.execute(query)

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"GOLD | Successfully created view: {view_name}")
        logger.info(f"GOLD | ------------------")
        return True
    
    except Exception as e:
        print("ERROR CREATING VIEW:", e)
        return False