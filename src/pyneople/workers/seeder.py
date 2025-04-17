import asyncio
import asyncpg
from itertools import cycle
from datetime import datetime, timedelta
from apikey import APIKEY
from config.METADATA import PARAMS_FOR_SEED_CHARACTER_FAME
apikey_cycle = cycle(APIKEY)

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

# 추후 작업 예정
async def seed_character_timeline_api_request_queue(
        api_request_queue : asyncio.Queue,
        psql_pool : asyncpg.Pool,
        sql: str,
        start_date : str = '2025-01-09 12:00',
        end_date : str = datetime.now().strftime("%Y-%m-%d %H:%M"),
        code : str = '',
        limit : int = 100
):
    async with psql_pool.acquire() as conn:
        async with conn.transaction():
            cursor = await conn.cursor(sql)
            while True:
                rows = await cursor.fetch(1000)  # <- 배치 크기 조절 가능
                if not rows:
                    break
                for character_id, server_id in rows:
                    # print(f"end : {type(end_date)}, start : {type(start_date)}")
                    # print('포문진입')
                    # 타임라인 데이터 조회 시작과 끝을 비교하고 90일이 넘어가는 경우 여러개의 api request를 넣어야 한다
                    
                    # 90일이 넘어가는 경우 시간 분리리
                    # print(f"end : {type(end_date)}, start : {type(start_date)}")
                    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M')
                    # print(f"end : {type(end_date)}, start : {type(start_date)}")
                    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M')
                    # print(f"end : {type(end_date)}, start : {type(start_date)}")
                    ranges = []
                    current_start = start_date
                    while current_start < end_date:
                        current_end = min(current_start + timedelta(days=90), end_date)
                        ranges.append((current_start, current_end))
                        current_start = current_end
                    start_date = start_date.strftime('%Y-%m-%d %H:%M')
                    end_date = end_date.strftime('%Y-%m-%d %H:%M')              
                    # print(f"end : {type(end_date)}, start : {type(start_date)}")      
                    for start_dt, end_dt in ranges:
                        api_request = {
                            'endpoint' : 'character_timeline',
                            'params' : {
                                'characterId' : character_id,
                                'serverId' : server_id,
                                'startDate' : start_dt.strftime('%Y-%m-%d %H:%M'),
                                'endDate' : end_dt.strftime('%Y-%m-%d %H:%M'),
                                'code' : code,
                                'limit' : limit,
                                'apikey' : next(apikey_cycle)
                            }
                        }
                        await api_request_queue.put(api_request)


SEEDERS = {
    'character_fame' : seed_character_fame_api_request_queue,
    'character_timeline' : seed_character_timeline_api_request_queue
}