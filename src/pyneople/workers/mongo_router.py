from motor.motor_asyncio import AsyncIOMotorCollection
from config.METADATA import ENDPOINTS_WITH_CHARACTER_INFO
from api.parser import extract_character_info
class MongoRouter:
    
    def __init__(self, mongo_collection : AsyncIOMotorCollection, queue_map : dict, batch_size):
        self.mongo_collection = mongo_collection
        self.queue_map = queue_map
        self.batch_size = batch_size

    # 커서 기반의 처리가 아니라 _id를 이용해서 가져오는 것을 고려해서 수정할 수 있음
    async def route(self):
        """
        MongoDB에서 데이터를 비동기 커서로 읽어와 endpoint기준으로 queue에 분배
        """
        cursor = self.mongo_collection.find({}).batch_size(self.batch_size)
        async for document in cursor:
            # print(f"도큐먼트 : {document}")
            endpoint = document.get('endpoint')

            if endpoint in ENDPOINTS_WITH_CHARACTER_INFO:
                target_queue = self.queue_map.get('character_info')
                await target_queue.put(extract_character_info(document))
                
            # print(f'엔드포인트 : {endpoint}')
            target_queue = self.queue_map.get(endpoint)
            await target_queue.put(document.get('data'))
            
            
        