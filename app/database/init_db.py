import psycopg2
from config.db_settings import settings

def init_database():
    """Initialize database schemas"""
    conn = psycopg2.connect(
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=settings.DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create schemas if they don't exist
    schemas = [
        "auth",
        "master_order",
        "document_management",
        "hr",
        "finance",
        "inventoryv1"
    ]

    for schema in schemas:
        try:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
            print(f"Schema '{schema}' created or already exists")
        except Exception as e:
            print(f"Error creating schema '{schema}': {str(e)}")

    cursor.close()
    conn.close() 