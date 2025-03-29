import asyncio

class MongoStoreWorker():
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