import asyncpg
import asyncio
from utils.query_builder import build_bulk_insert_query

class QueueToPSQLWorker:
    
    def __init__(self, queue : asyncio.Queue, psql_pool : asyncpg.Pool, endpoint : str, table_name : str, preprocess : callable, batch_size : int, timeout : float = 3.0):
        self.queue = queue
        self.psql_pool = psql_pool
        self.endpoint = endpoint
        self.table_name = table_name
        self.preprocess = preprocess
        self.batch_size = batch_size
        self.timeout = timeout
    
    async def run(self):
        buffer = []
        queue_job = 0
        while True:
            try:
                data = await asyncio.wait_for(self.queue.get(), timeout=self.timeout)
            except asyncio.TimeoutError:
                if buffer:
                    await self.flush(buffer)
                    buffer.clear()
                    for _ in range(queue_job):
                        self.queue.task_done()
                    queue_job = 0    
                continue
            # data = await self.queue.get()
            queue_job += 1
            if data is None:
                await self.flush(buffer)
                self.queue.task_done()
                break
            data = self.preprocess(data)
            if isinstance(data, list):
                buffer += data
            else:    
                buffer.append(data)

            if len(buffer) >= self.batch_size:
                await self.flush(buffer)
                buffer.clear()
                for _ in range(queue_job):
                    self.queue.task_done()
                queue_job = 0    
            
            continue            

    async def flush(self, batch : list):
        if not batch:
            return
        query, values = build_bulk_insert_query(
            table_name = self.table_name,
            data = batch
        )
        # print(values)
        # print(query)
        async with self.psql_pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, values)
                print('삽입 완료')