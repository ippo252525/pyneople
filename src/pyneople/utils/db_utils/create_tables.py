import os
import psycopg2
from pyneople.config.config import Settings

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
folder_path = os.path.join(BASE_DIR, '../../db/schema/staging_tables')
sql_files = [
        os.path.join(folder_path, f) for f in os.listdir(folder_path)
        if f.endswith(".sql")
    ]
def run_sql_file(path, conn):
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    print(f"Executed: {path}")

conn = psycopg2.connect(
    dbname=Settings.POSTGRES_DB,
    user=Settings.POSTGRES_USER,
    password=Settings.POSTGRES_PASSWORD,
    host=Settings.POSTGRES_HOST,
    port=Settings.POSTGRES_PORT
)

for sql_path in sql_files:
    run_sql_file(sql_path, conn)

conn.commit()
conn.close()