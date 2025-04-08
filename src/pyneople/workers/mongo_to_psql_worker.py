import asyncpg

class MongoToPSQLWorker:
    def __init__(self, db, mongodb_collection_name: str, psql_pool: asyncpg.Pool):
        self.mongo_collection = db[mongodb_collection_name]
        self.psql_pool = psql_pool

    def run():
        # 몽고디비에서 데이터 가져와서

        # 전처리

        # psql 삽입
        pass