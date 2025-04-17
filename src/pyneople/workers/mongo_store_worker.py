import asyncio
from motor.motor_asyncio import AsyncIOMotorCollection

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
    def __init__(self, data_queue: asyncio.Queue, mongo_collection: AsyncIOMotorCollection, batch_size: int, timeout: float = 3.0):
        self.queue = data_queue
        self.mongo_collection = mongo_collection
        self.batch_size = batch_size
        self.timeout = timeout

    async def run(self):
        batch = []

        while True:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=self.timeout)
            
            except asyncio.TimeoutError:
                # timeout 발생 → 현재 batch 저장
                if batch:
                    try:
                        await self.mongo_collection.insert_many(batch)
                        print(f"\nInserted (timeout flush) {len(batch)} items")
                    except Exception as e:
                        print(f"Insert failed: {e}")
                    finally:
                        for _ in batch:
                            self.queue.task_done()
                        batch.clear()
                continue  # 다음 루프로 진행

            if item is None:
                # 종료 신호 → batch 남은 것 처리하고 종료
                if batch:
                    try:
                        await self.mongo_collection.insert_many(batch)
                    except Exception:
                        pass
                    finally:
                        for _ in batch:
                            self.queue.task_done()
                        batch.clear()

                self.queue.task_done()  # None 자체에 대한 task_done
                print("Mongo 워커 탈출")
                break

            batch.append(item)

            if len(batch) >= self.batch_size:
                try:
                    await self.mongo_collection.insert_many(batch)
                    # print(f"Inserted batch of {len(batch)}")
                except Exception as e:
                    print(f"Insert failed: {e}")
                finally:
                    for _ in batch:
                        self.queue.task_done()
                    batch.clear()            