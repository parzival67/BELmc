from app.database.connection import connect_to_db

try:
    connect_to_db()
    print("Database connected successfully")

except Exception as e:
    print(f"Setup error: {e}")
