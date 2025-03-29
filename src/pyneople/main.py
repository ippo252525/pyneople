import asyncio
import aiohttp
from workers.api_fetch_worker import APIFetchWorker
from workers.mongo_store_worker import MongoStoreWorker
from motor.motor_asyncio import AsyncIOMotorClient
from workers.seeder import seed_character_fame_api_request_queue

mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
db = mongo_client["dnf_database"]
NUM_API_FETCH_WORKERS = 100
NUM_MONGO_STORE_WORKERS = 10
async def main():
    api_request_queue = asyncio.Queue()
    data_queue = asyncio.Queue()

    async with aiohttp.ClientSession() as session:
        # 여러 개의 워커 생성
        api_fetch_workers = [APIFetchWorker(api_request_queue, data_queue, session) for _ in range(NUM_API_FETCH_WORKERS)]
        mongo_store_workers = [MongoStoreWorker(data_queue, db) for _ in range(NUM_MONGO_STORE_WORKERS)]
        # 워커 태스크 실행
        api_fetch_worker_tasks = [asyncio.create_task(worker.run()) for worker in api_fetch_workers]
        mongo_store_worker_tasks = [asyncio.create_task(worker.run()) for worker in mongo_store_workers]
        await seed_character_fame_api_request_queue(api_request_queue, 10000)
        print('\r초기 값 put 완료', end="", flush=True)

        # 모든 작업이 끝날 때까지 대기
        await api_request_queue.join()
        print('\r모든 작업 종료 완료', end="", flush=True)

        for _ in range(NUM_API_FETCH_WORKERS):        
            await api_request_queue.put(None)   
        print('api None 삽입 완료')
        
        await data_queue.join()
        print("data queue 완료")
        
        for _ in range(NUM_API_FETCH_WORKERS):        
            await data_queue.put(None)        
        
        await asyncio.gather(*api_fetch_worker_tasks)
        print('api gather 완료')

        await asyncio.gather(*mongo_store_worker_tasks)

asyncio.run(main())