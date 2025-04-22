import asyncio
from motor.motor_asyncio import AsyncIOMotorCollection
from pyneople.config.config import Settings

# MongoDB에서 엔드포인트 별 collection을 두는 구조에서 사용하는 Worker
# 현재 구조에서 사용하지 않음
class _MongoStoreWorker:
    def __init__(self, data_queue : asyncio.Queue, mongo_db):
        self.queue = data_queue
        self.mongo_db = mongo_db
    
    async def run(self):
        while True:
            data = await self.queue.get()
            if data:
                mongo_collection = self.mongo_db[data['collection']]
                data = data['data']
                try:
                    await mongo_collection.insert_one(data)
                    # print(f"Stored data: {data}")
                except Exception as e:
                    print(f"Failed to store data: {e}")
                finally:
                    self.queue.task_done()
            else:
                print('\rMongo 워커 탈출', end="", flush=True)
                self.queue.task_done()
                break                           

class MongoStoreWorker:
    def __init__(self, data_queue: asyncio.Queue, mongo_collection: AsyncIOMotorCollection, shutdown_event : asyncio.Event, batch_size: int = Settings.DEFAULT_MONGO_STORE_BATCH_SIZE, timeout: float = 3.0):
        self.queue = data_queue
        self.mongo_collection = mongo_collection
        self.batch_size = batch_size
        self.timeout = timeout
        self.shutdown_event = shutdown_event
    
    async def run(self):
        while not self.shutdown_event.is_set():
            
            batch = await self._collect_batch()
            if batch is None:
                continue

            await self._insert_batch(batch)
        print('Mongo Store Worker 종료')
    async def _collect_batch(self):
        batch = []

        while len(batch) < self.batch_size:
            if self.shutdown_event.is_set():
                print('shutdown_event가 설정됨')
                break
            try:
                data = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.01)  # queue가 비어있으면 잠시 대기
                continue
            if data is None:
                self.shutdown_event.set()
                self.queue.task_done()
                break
            batch.append(data)
        return batch    

    async def _insert_batch(self, batch):       
        try:
            await self.mongo_collection.insert_many(batch)
            print(f"Inserted {len(batch)} documents into MongoDB.")
        except Exception as e:
            print(f"Insert failed: {e}")
        finally:
            for _ in batch:
                self.queue.task_done()         