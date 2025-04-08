import asyncio
from apikey import APIKEY
from config.METADATA import PARAMS_FOR_SEED_CHARACTER_FAME


async def seed_character_fame_api_request_queue(api_request_queue : asyncio.Queue, max_fame : int):
    for i, seed_params in enumerate(PARAMS_FOR_SEED_CHARACTER_FAME):
        api_request = {
            'endpoint' : 'character_fame', 
            'params' : {
                    'maxFame' : max_fame,
                    'jobId' : seed_params['jobId'],
                    'jobGrowId' : seed_params['jobGrowId'],
                    'serverId' : seed_params['serverId'],
                    'apikey' : APIKEY[i % len(APIKEY)]
            }                                
        }
        await api_request_queue.put(api_request)

async def seed_character_api_request_queue(api_request_queue : asyncio.Queue, ):
    pass


SEEDERS = {
    'character_fame' : seed_character_fame_api_request_queue
}