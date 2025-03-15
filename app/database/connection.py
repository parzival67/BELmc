from pony.orm import Database, db_session
from config.db_settings import settings

db = Database()


def connect_to_db():
    db.bind(
        provider='postgres',
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME
    )

    with db_session:
        conn = db.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS hr_schema")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS finance_schema")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS user_schema")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS master_order")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS scheduling")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS inventoryv1")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS document_management")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS auth")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS production")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS EMS")

        conn.commit()  # Ensure changes are saved
    except Exception as e:
        conn.rollback()  # Roll back on failure
        print(f"Error creating schemas: {e}")
    finally:
        cursor.close()

    # Import all models to ensure they're registered with the database
    from ..models import hr_models, finance_models, master_order, user, inventoryv1, production

    db.generate_mapping(create_tables=True)
