import asyncio
import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient
from workers.mongo_router import MongoRouter
from workers.queue_to_psql_worker import QueueToPSQLWorker
from config.endpoint_mapping import ENDPOINT_TO_STAGING_TABLE_NAME, ENDPOINT_TO_PREPROCESS
from config.config import MONGO_URL, MONGODB_NAME, MONGO_COLLECTION_NAME
from config.METADATA  import ENDPOINTS_WITH_CHARACTER_INFO
NUM_QUEUE_TO_PSQL_WORKER = 4
PSQL_WORKER_BATCH_SIZE = 300
MONGO_ROUTER_BATCH_SIZE = 300

async def main():
    # 1. DB 연결
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    mongo_db = mongo_client[MONGODB_NAME]
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]

    psql_pool = await asyncpg.create_pool(
        user="pyneople",
        password='252525',
        database="dnf",
        host="localhost",
        port=5432
    )
    # 2. 큐 생성 (endpoint별)
    endpoints = await mongo_collection.distinct("endpoint")
    if any(ep in ENDPOINTS_WITH_CHARACTER_INFO for ep in endpoints):
        endpoints.append('character_info')
    endpoint_queues = {ep: asyncio.Queue() for ep in endpoints}
    print(endpoint_queues)
    # 워커생성
    workers = []
    for ep in endpoints:
        for _ in range(NUM_QUEUE_TO_PSQL_WORKER):
            queue = endpoint_queues[ep]
            table_name = ENDPOINT_TO_STAGING_TABLE_NAME[ep]
            preprocess = ENDPOINT_TO_PREPROCESS[ep]

            worker = QueueToPSQLWorker(
                queue=queue,
                psql_pool=psql_pool,
                endpoint=ep,
                table_name=table_name,
                preprocess=preprocess,
                batch_size=PSQL_WORKER_BATCH_SIZE
            )
            workers.append(asyncio.create_task(worker.run()))    
    
    router = MongoRouter(
        mongo_collection=mongo_collection,
        queue_map=endpoint_queues,
        batch_size=MONGO_ROUTER_BATCH_SIZE
    )
    await router.route()

    joins = [queue.join() for queue in endpoint_queues.values()]
    await asyncio.gather(*joins)

    for q in endpoint_queues.values():
        for _ in range(NUM_QUEUE_TO_PSQL_WORKER):
            await q.put(None)
    await asyncio.gather(*workers)
    await psql_pool.close()
    
    # await mongo_collection.delete_many({})

asyncio.run(main())