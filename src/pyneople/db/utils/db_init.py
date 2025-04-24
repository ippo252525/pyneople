from pyneople.db.utils.psql_manager import PSQLConnectionManager
import asyncio

querys = [
    "TRUNCATE TABLE staging_characters;"
]

async def init_db(querys : list):
    await PSQLConnectionManager.init_pool()
    psql_pool = PSQLConnectionManager.get_pool()
    async with psql_pool.acquire() as conn:
        async with conn.transaction():
            for query in querys:
                try:
                    await conn.execute(query)
                except Exception as e:
                    print(f"Error executing query: {query}")
                    print(e)    

asyncio.run(init_db(querys))