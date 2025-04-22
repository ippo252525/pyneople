import asyncpg
from pyneople.config.config import Settings

class PSQLConnectionManager:
    _pool = None

    @classmethod
    async def init_pool(cls):
        if cls._pool is None:
            cls._pool = await asyncpg.create_pool(
                user=Settings.POSTGRES_USER,
                password=Settings.POSTGRES_PASSWORD,
                database=Settings.POSTGRES_DB,
                host=Settings.POSTGRES_HOST,
                port=Settings.POSTGRES_PORT
            )

    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            raise RuntimeError("PSQL Pool not initialized. Call init_pool() first.")
        return cls._pool