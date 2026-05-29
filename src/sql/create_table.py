import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("SERVER")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")

def create_table_if_not_exists(table_name, df_customers):

    jdbc_url = f"jdbc:sqlserver://{SERVER}:1433;databaseName={DATABASE};encrypt=true;trustServerCertificate=true"

    try:
        df_customers.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"dbo.{table_name}") \
        .option("user", USERNAME) \
        .option("password", PASSWORD) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()
        return True
    
    except Exception as e:
        print("ERROR CREATING TABLE:", e)
        return False