from motor.motor_asyncio import AsyncIOMotorCollection
from pyneople.api.METADATA import ENDPOINTS_WITH_CHARACTER_INFO
from pyneople.api.parser import extract_character_info
from pyneople.config.config import Settings
from typing import Optional
class MongoRouterLegacy:
    
    def __init__(self, mongo_collection : AsyncIOMotorCollection, queue_map : dict, batch_size):
        self.mongo_collection = mongo_collection
        self.queue_map = queue_map
        self.batch_size = batch_size

    # 커서 기반의 처리가 아니라 _id를 이용해서 가져오는 것을 고려해서 수정할 수 있음
    async def route(self):
        print('라우터 진입')
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
                #print('put완료')
                
            # print(f'엔드포인트 : {endpoint}')
            target_queue = self.queue_map.get(endpoint)
            await target_queue.put(document.get('data'))
            #print('put완료')
            
            
class MongoRouter:

    def __init__(self, 
        mongo_collection: AsyncIOMotorCollection, 
        queue_map: dict, 
        character_info_endpoints : list,
        batch_size: int = Settings.DEFAULT_MONGO_ROUTER_BATCH_SIZE):
        self.character_info_endpoints = character_info_endpoints
        self.mongo_collection = mongo_collection
        self.queue_map = queue_map
        self.batch_size = batch_size

    async def get_split_filters(self, num_workers: int = Settings.DEFAULT_NUM_MONGO_ROUTERS):
        """
        _id를 기준으로 num_workers개 범위 필터를 계산해서 반환
        """
        total_docs = await self.mongo_collection.estimated_document_count()
        split_points = []

        for i in range(1, num_workers):
            skip = int(total_docs * i / num_workers)
            doc = await self.mongo_collection.find({}).sort('_id', 1).skip(skip).limit(1).to_list(1)
            if doc:
                split_points.append(doc[0]['_id'])

        # ID 범위 필터 만들기
        filters = []
        last_id = None
        for split_id in split_points:
            if last_id is None:
                filters.append({'_id': {'$lt': split_id}})
            else:
                filters.append({'_id': {'$gte': last_id, '$lt': split_id}})
            last_id = split_id
        if last_id:
            filters.append({'_id': {'$gte': last_id}})
        else:
            filters.append({})  # 데이터 적은 경우

        return filters  
    
    async def route(self, id_filter):
        """
        MongoDB에서 특정 범위(id_filter)에 대해 데이터를 읽고, endpoint 별 큐에 분배
        """
        cursor = self.mongo_collection.find(id_filter).batch_size(self.batch_size)
        async for document in cursor:
            endpoint = document.get('endpoint')
            
            target_queue = self.queue_map.get(endpoint)
            if target_queue:
                await target_queue.put(document.get('data'))
            
            if self.character_info_endpoints:
                if endpoint in self.character_info_endpoints:
                    target_queue = self.queue_map.get('character_info')
                    if target_queue:
                        # print(extract_character_info(document))
                        await target_queue.put(extract_character_info(document))            

                
                # print(f"put 완료 : {document.get('data')}")