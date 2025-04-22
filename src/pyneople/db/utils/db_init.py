from pyneople.db.utils.psql_manager import PSQLConnectionManager
import asyncio

querys = [
    "TRUNCATE TABLE staging_characters;",
    "TRUNCATE TABLE staging_character_timelines;",
]

async def init_db(querys : list):
    await PSQLConnectionManager.init_pool()
    psql_pool = PSQLConnectionManager.get_pool()
    async with psql_pool.acquire() as conn:
        async with conn.transaction():
            for query in querys:
                await conn.execute(query)

asyncio.run(init_db(querys))