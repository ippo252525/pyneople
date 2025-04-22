from abc import ABC, abstractmethod
import asyncio

class BaseWorker(ABC):
    """
    Queue에서 특정 스토리지로 데이터를 Batch 처리로 전송하는 기본 Worker 클래스입니다.

    """
    def __init__(self):
        self.queue : asyncio.Queue
        

class QueueToPSQLWorker:
    def __init__(self, 
                 queue : asyncio.Queue, 
                 psql_pool : asyncpg.Pool, 
                 endpoint : str, 
                 table_name : str, 
                 preprocess : callable, 
                 batch_size : int,
                 shutdown_event : asyncio.Event, 
                 timeout : float = Settings.DEFAULT_QUEUE_TO_PSQL_WORKER_TIMEOUT):
        self.queue = queue
        self.psql_pool = psql_pool
        self.endpoint = endpoint
        self.table_name = table_name
        self.preprocess = preprocess
        self.batch_size = batch_size
        self.shutdown_event = shutdown_event
        self.timeout = timeout  

    async def run(self):
        while not self.shutdown_event.is_set():
            
            batch = await self._collect_batch()
            if batch is None:
                continue

            await self._copy_to_psql(batch)

    async def _collect_batch(self):
        start = time.perf_counter()
        batch = []
        while len(batch) < self.batch_size:
            #queue_start = time.perf_counter()
            if self.shutdown_event.is_set():
                print('shutdown_event가 설정됨')
                break
            try:
                data = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(self.timeout)
                continue
                # data = await asyncio.wait_for(self.queue.get(), timeout=self.timeout)
                # print(f'큐에서 가져온 데이터 : {data}')
            except asyncio.TimeoutError:
                continue
            #queue_end = time.perf_counter()
            #print(f'큐에서 가져오는 시간 : {queue_end - queue_start:.2f} seconds')
            if data is None:
                self.shutdown_event.set()
                self.queue.task_done()
                break
            #prepro_start = time.perf_counter()
            data = self.preprocess(data)
            #prepro_end = time.perf_counter()
            #print(f'전처리 시간 : {prepro_end - prepro_start:.2f} seconds')
            # print(f'전처리 완료 : {data}')
            if isinstance(data, list):
                batch.extend(data)
            else:    
                batch.append(data)    

            self.queue.task_done()
        end = time.perf_counter()
        print(f'배치 수집 시간 : {end - start:.2f} seconds')
        # print(batch)
        return batch

    async def _copy_to_psql(self, batch):   
        #print(f'copy_to_psql 진입')
        if not batch:
            #print('batch가 비어있음')
            return
        try:
            #print('테이블 삽입 시도')
            async with self.psql_pool.acquire() as conn:
                async with conn.transaction():
                    start = time.perf_counter()
                    await conn.copy_records_to_table(
                        self.table_name,
                        records=[tuple(row.values()) for row in batch],
                        columns=batch[0].keys(),
                    )
                    end = time.perf_counter()
                    print(f'디비 삽입 시간 : {end - start:.2f} seconds')
        except Exception as e:
            print(f"Error occurred during copy: {e}")
            # Handle the error (e.g., log it, retry, etc.)
            raise e