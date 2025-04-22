import asyncio
from typing import Optional
from pyneople.workers.mongo_router import MongoRouter
from pyneople.config.config import Settings
from pyneople.db.utils.mongo_manager import MongoConnectionManager
from pyneople.db.utils.psql_manager import PSQLConnectionManager
from pyneople.db.utils.get_mongo_endpoints import get_mongo_endpoints
from pyneople.db.utils.get_mongo_split_filters import get_split_filters
from pyneople.api.endpoint_mapping import ENDPOINT_TO_STAGING_TABLE_NAME, ENDPOINT_TO_PREPROCESS
from pyneople.api.endpoints import API_ENDPOINTS
from pyneople.api.METADATA import ENDPOINTS_WITH_CHARACTER_INFO
from pyneople.workers.queue_to_psql_worker import QueueToPSQLWorker

async def mongo_to_psql(
    endpoints: Optional[list] = None,
    character_info_endpoints: Optional[list] = None,
    queue_size : int = Settings.DEFAULT_MONGO_TO_PSQL_QUEUE_SIZE,
    num_queue_to_psql_workers: int = Settings.DEFAULT_NUM_QUEUE_TO_PSQL_WORKERS,
    mongo_router_batch_size: int = Settings.DEFAULT_MONGO_ROUTER_BATCH_SIZE,
    queue_to_psql_batch_size: int = Settings.DEFAULT_QUEUE_TO_PSQL_BATCH_SIZE,
    num_mongo_routers: int = Settings.DEFAULT_NUM_MONGO_ROUTERS,
):
    # 1. DB 연결
    # MongoDB
    await MongoConnectionManager.init_collection()
    mongo_collection = MongoConnectionManager.get_collection()
    # PostgreSQL
    await PSQLConnectionManager.init_pool()
    psql_pool = PSQLConnectionManager.get_pool()

    # 2. MongoDB Collection에 저장 된 endpoint 목록 가져오기
    
    # endpoints가 명시된 경우 사용 endpoint 유효성 검사
    if endpoints:
        for endpoint in endpoints:
            if endpoint not in API_ENDPOINTS.keys():
                raise ValueError(f"Invalid endpoint: {endpoint} 는 지원하지 않는 endpoint 입니다")

    # endpoints 명시 안한 경우 직접 가져옴
    else:
        endpoints = await get_mongo_endpoints(mongo_collection)

    endpoints = set(endpoints)

    # 캐릭터 정보를 추출해서 사용 할 endpoint가 있으면 endpoint : 'character_info' 를 endpoints에 추가함
    if character_info_endpoints:
        # character_info_endpoints에 명시된 endpoint가 캐릭터 정보를 추출할 수 있는 endpoint인지 확인
        for character_info_endpoint in character_info_endpoints:
            if character_info_endpoint not in ENDPOINTS_WITH_CHARACTER_INFO:
                raise ValueError(f"Invalid endpoint: {character_info_endpoint} 는 캐릭터 정보 추출을 지원하지 않습니다")
        # 캐릭터 정보를 추출할 수 있는 endpoint가 명시된 경우 endpoints에 'character_info'를 추가    
        endpoints.add('character_info')
        
    print(f'사용 endpoint : {endpoints}, 캐릭터 정보 추출 endpoint : {character_info_endpoints}')

    # 3. 큐 생성 (endpoint별)
    endpoint_queue_map = {endpoint : asyncio.Queue(maxsize=queue_size) for endpoint in endpoints}

    # 4. MongoRouter 생성 후 실행
    # MongoRouter는 mongo_collection과 endpoint_queue_map을 사용하여 데이터를 라우팅합니다.
    routers = [MongoRouter(mongo_collection, endpoint_queue_map, character_info_endpoints, mongo_router_batch_size) for _ in range(num_mongo_routers)]
    filters = await get_split_filters(mongo_collection, num_mongo_routers)
    router_tasks = [asyncio.create_task(router.route(filter)) for router, filter in zip(routers, filters)]
    print(f"MongoRouter 실행 시작")

    # 5. QueueToPSQLWorker 생성 후 실행
    # queue 하나 당 num_queue_to_psql_workers 개의 QueueToPSQLWorker를 생성합니다.
    shutdown_event = asyncio.Event()
    queue_to_psql_workers = []
    for endpoint in endpoints:
        for _ in range(num_queue_to_psql_workers):
            queue = endpoint_queue_map[endpoint]
            table_name = ENDPOINT_TO_STAGING_TABLE_NAME[endpoint]
            preprocess = ENDPOINT_TO_PREPROCESS[endpoint]
            worker = QueueToPSQLWorker(
                queue=queue,
                psql_pool=psql_pool,
                endpoint=endpoint,
                table_name=table_name,
                preprocess=preprocess,
                batch_size=queue_to_psql_batch_size,
                shutdown_event=shutdown_event
            )
            queue_to_psql_workers.append(worker)
    queue_to_psql_worker_tasks = [asyncio.create_task(queue_to_psql_worker.run()) for queue_to_psql_worker in queue_to_psql_workers]    
    print(f"QueueToPSQLWorker 실행 시작")
    
    # 6. 종료
    await asyncio.gather(*router_tasks)

    joins = [queue.join() for queue in endpoint_queue_map.values()]
    await asyncio.gather(*joins)

    for queue in endpoint_queue_map.values():
        for _ in range(num_queue_to_psql_workers):
            print('none 넣기')
            await queue.put(None)
    
    await asyncio.gather(*queue_to_psql_worker_tasks)
    
    await psql_pool.close()
            
    

# asyncio.run(mongo_to_psql(['character_timeline'], ['character_timeline'])) 