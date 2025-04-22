import asyncio
import aiohttp

from pyneople.workers.api_fetch_worker import APIFetchWorker
from pyneople.workers.mongo_store_worker import MongoStoreWorker
from motor.motor_asyncio import AsyncIOMotorClient
from pyneople.api.seeder import SEEDERS
from pyneople.config.config import Settings
from pyneople.utils.monitoring import count_requests_per_second
from pyneople.db.utils.psql_manager import PSQLConnectionManager

async def api_to_mongo(endpoints : list[str], 
               num_api_fetch_workers : int = Settings.DEFAULT_NUM_API_FETCH_WORKERS, 
               num_mongo_store_workers : int = Settings.DEFAULT_NUM_MONGO_STORE_WORKERS, 
               **seed_kwargs):
    shutdown_event = asyncio.Event()
    mongo_shutdown_event = asyncio.Event()
    mongo_client = AsyncIOMotorClient(Settings.MONGO_URL)
    mongo_collection = mongo_client[Settings.MONGO_DB_NAME][Settings.MONGO_COLLECTION_NAME]
    error_collection = mongo_client[Settings.MONGO_DB_NAME][Settings.MONGO_ERROR_COLLECTION_NAME]    
    api_request_queue = asyncio.Queue(maxsize=1000)
    data_queue = asyncio.Queue()    

    await PSQLConnectionManager.init_pool()
    psql_pool = PSQLConnectionManager.get_pool()
    
    # SEEDERS = SEEDERS.get(seed_type)(api_request_queue, psql_pool, shutdown_event)
    
    # asyncio.create_task(count_requests_per_second())
    
    async with aiohttp.ClientSession() as session:
        # 여러 개의 워커 생성
        seeders = [SEEDERS.get(endpoint)(endpoint, api_request_queue, psql_pool, shutdown_event) for endpoint in endpoints]
        api_fetch_workers = [APIFetchWorker(api_request_queue, data_queue, session, shutdown_event, error_collection) for _ in range(num_api_fetch_workers)]
        mongo_store_workers = [MongoStoreWorker(data_queue, mongo_collection, mongo_shutdown_event, 2000) for _ in range(num_mongo_store_workers)]
        # 워커 태스크 실행
        seeders_tasks = [asyncio.create_task(seeder.seed(**seed_kwargs)) for seeder in seeders]
        api_fetch_worker_tasks = [asyncio.create_task(worker.run()) for worker in api_fetch_workers]
        mongo_store_worker_tasks = [asyncio.create_task(worker.run()) for worker in mongo_store_workers]
        
        await asyncio.gather(*seeders_tasks)
        print('seeders_tasks gather 완료')

        # 모든 작업이 끝날 때까지 대기
        await api_request_queue.join()
        print('모든 작업 종료 완료')

        for _ in range(num_api_fetch_workers):        
            await api_request_queue.put(None)
        print('api_request_queue None 삽입 완료')
        print("data queue 조인 대기중")
        
        await data_queue.join()
        print("data queue join 완료")
        
        for _ in range(num_mongo_store_workers):
            await data_queue.put(None)        
        print('data_queue None 삽입 완료')    
        
        await asyncio.gather(*api_fetch_worker_tasks)
        print('api_fetch_worker_tasks gather 완료')

        await asyncio.gather(*mongo_store_worker_tasks)
        print('mongo_store_worker_tasks gather 완료')