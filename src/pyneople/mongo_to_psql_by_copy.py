import asyncio
import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient
from pyneople.workers.mongo_router import MongoRouter
from pyneople.workers.queue_to_psql_worker import QueueToPSQLWorkerWithCopy
from pyneople.api.endpoint_mapping import ENDPOINT_TO_STAGING_TABLE_NAME, ENDPOINT_TO_PREPROCESS
from pyneople.config.config import Settings
from pyneople.api.METADATA  import ENDPOINTS_WITH_CHARACTER_INFO
from pyneople.db.utils.psql_manager import PSQLConnectionManager

async def mongo_to_psql(num_queue_to_psql_workers : int = Settings.DEFAULT_NUM_QUEUE_TO_PSQL_WORKERS, 
               mongo_router_batch_size : int = Settings.DEFAULT_MONGO_ROUTER_BATCH_SIZE,
               queue_to_psql_batch_size : int = Settings.DEFAULT_QUEUE_TO_PSQL_BATCH_SIZE
               ):
    # 1. DB 연결
    print("MongoDB와 PSQL DB에 연결합니다.")
    mongo_client = AsyncIOMotorClient(Settings.MONGO_URL)
    mongo_db = mongo_client[Settings.MONGO_DB_NAME]
    mongo_collection = mongo_db[Settings.MONGO_COLLECTION_NAME]
    await PSQLConnectionManager.init_pool()
    psql_pool = PSQLConnectionManager.get_pool()
    # 2. 큐 생성 (endpoint별)
    print("큐를 생성합니다.")
    endpoints = ['character_info','character_timeline']
    endpoint_queues = {ep: asyncio.Queue(maxsize=5000) for ep in endpoints}
    print(endpoint_queues)
    shutdown_event = asyncio.Event()

    # 워커생성
    workers = []
    for ep in endpoints:
        for _ in range(num_queue_to_psql_workers):
            queue = endpoint_queues[ep]
            table_name = ENDPOINT_TO_STAGING_TABLE_NAME[ep]
            preprocess = ENDPOINT_TO_PREPROCESS[ep]
            worker = QueueToPSQLWorkerWithCopy(
                queue=queue,
                psql_pool=psql_pool,
                endpoint=ep,
                table_name=table_name,
                preprocess=preprocess,
                batch_size=queue_to_psql_batch_size,
                shutdown_event=shutdown_event
            )
            workers.append(worker)
    workers = [asyncio.create_task(worker.run()) for worker in workers]
    # 3. MongoRouter 생성
    # MongoRouter는 mongo_collection과 endpoint_queues를 사용하여 데이터를 라우팅합니다.
    # mongo_collection에서 데이터를 가져와서 endpoint_queues에 넣습니다.
    # MongoRouter는 각 endpoint에 대해 별도의 큐를 사용합니다.
    # MongoRouter는 mongo_collection에서 데이터를 가져와서 endpoint_queues에 넣습니다.
    router = MongoRouter(
        mongo_collection=mongo_collection,
        queue_map=endpoint_queues,
        batch_size=mongo_router_batch_size
    )
    filters = await router.get_split_filters(3)
    print(f"filters : {filters}")
    router_tasks = [asyncio.create_task(router.route(id_filter=filter)) for filter in filters]
    print(f"MongoRouter : {router}")    
    await asyncio.gather(*router_tasks)
    # await router.route()

    joins = [queue.join() for queue in endpoint_queues.values()]
    await asyncio.gather(*joins)

    for q in endpoint_queues.values():
        for _ in range(num_queue_to_psql_workers):
            print('none 넣기')
            await q.put(None)
    
    await asyncio.gather(*workers)
    await psql_pool.close()
    
    # await mongo_collection.delete_many({})

asyncio.run(mongo_to_psql())