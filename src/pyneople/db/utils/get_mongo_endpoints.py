from motor.motor_asyncio import AsyncIOMotorCollection

async def get_mongo_endpoints(mongo_collection: AsyncIOMotorCollection) -> list:
    """
    MongoDB에서 엔드포인트 목록을 가져오는 함수입니다.
    """
    endpoints = []
    doc_count = await mongo_collection.estimated_document_count()

    if doc_count > 100000:
        # 엔드포인트 목록을 가져오는 데 시간이 걸릴 수 있으므로 비동기적으로 처리합니다.
        cursor = mongo_collection.find({}).batch_size(1000)
        async for document in cursor:
            endpoint = document.get('endpoint')
            if endpoint not in endpoints:
                endpoints.append(endpoint)
    else:
        # 문서 수가 적은 경우에는 직접 가져옵니다.
        endpoints = await mongo_collection.distinct('endpoint')
    return endpoints