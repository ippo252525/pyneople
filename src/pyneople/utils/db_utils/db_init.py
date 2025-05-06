from pyneople.utils.db_utils.psql_manager import PSQLConnectionManager
import psycopg2
from pyneople.config.config import Settings

conn = psycopg2.connect(
        dbname=Settings.POSTGRES_DB,
        user=Settings.POSTGRES_USER,
        password=Settings.POSTGRES_PASSWORD,
        host=Settings.POSTGRES_HOST,
        port=Settings.POSTGRES_PORT
    )

with conn.cursor() as cur:
    cur.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'staging%';
    """)
    tables = cur.fetchall()

    for (table_name,) in tables:
        print(f"Truncating {table_name}...")
        # cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;')
        cur.execute(f'DROP TABLE "{table_name}";')
    conn.commit()
    