import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    cursor = conn.cursor()

    cursor.execute("SELECT current_database(), current_user;")

    db, user = cursor.fetchone()

    print(f"Connected! Database: {db}, User: {user}")

    cursor.execute("""
        SELECT schema_name
        FROM information_schema.schemata;
    """)

    schemas = [row[0] for row in cursor.fetchall()]

    print(f"Schemas: {schemas}")

    conn.close()

    print("PostgreSQL is working!")

except Exception as e:
    print(f"Connection failed: {e}")
