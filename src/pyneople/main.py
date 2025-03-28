import asyncio
import aiohttp
from worker.worker import URLFetcherWorker

NUM_WORKERS = 10
apikey = 1
async def main():
    api_request_queue = asyncio.Queue()
    data_queue = asyncio.Queue()

    async with aiohttp.ClientSession() as session:
        # 여러 개의 워커 생성
        workers = [URLFetcherWorker(api_request_queue, data_queue, session) for _ in range(NUM_WORKERS)]
        
        # 워커 태스크 실행
        worker_tasks = [asyncio.create_task(worker.worker()) for worker in workers]

        await api_request_queue.put({"endpoint": "character_fame", "params": {"maxFame": 1000, 'apikey' : {apikey}}})
        print('초기 값 put 완료')

        # 모든 작업이 끝날 때까지 대기
        await api_request_queue.join()
        print('모든 작업 종료 완료')

        for _ in range(NUM_WORKERS):        
            await api_request_queue.put(None)   
        
        await asyncio.gather(*worker_tasks)

asyncio.run(main())