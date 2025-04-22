import asyncio
from pyneople.api_to_mongo import api_to_mongo
from pyneople.mongo_to_psql import mongo_to_psql

sql = """
SELECT character_id, server_id 
FROM characters;
"""
endpoints = ['character_info']

asyncio.run(api_to_mongo(endpoints, sql = sql))
asyncio.run(mongo_to_psql(endpoints))