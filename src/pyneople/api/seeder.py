import asyncio
import asyncpg
from typing import Optional
from itertools import cycle
from datetime import datetime, timedelta
from pyneople.config.config import Settings
from pyneople.api.METADATA import PARAMS_FOR_SEED_CHARACTER_FAME
from abc import ABC, abstractmethod

class BaseSeeder(ABC):
    def __init__(self, end_point : str, api_request_queue: asyncio.Queue, psql_pool: asyncpg.Pool, shutdown_event: asyncio.Event):
        self.end_point = end_point
        self.api_request_queue = api_request_queue
        self.shutdown_event = shutdown_event
        self.psql_pool = psql_pool
        self.api_keys = cycle(Settings.API_KEYS)
    
    @abstractmethod
    async def seed(self):
        pass
        # raise NotImplementedError("BaseSeeder를 상속한 클래스에서 seed() 구현 안됨")

class CharacterBaseSeeder(BaseSeeder):
    async def seed(self, **kwargs):
        if kwargs['sql']:
            async with self.psql_pool.acquire() as conn:
                async with conn.transaction():
                    cursor = await conn.cursor(kwargs['sql'])
                    while not self.shutdown_event.is_set():
                        rows_batch = await cursor.fetch(kwargs.get('seeder_batch_size', Settings.DEFAULT_SEEDER_BATCH_SIZE))
                        if not rows_batch:
                            break
                        await self._process_rows(rows_batch, **kwargs)
        elif kwargs['rows']:
            await self._process_rows(kwargs['rows'], **kwargs)
        else:
            raise ValueError("sql 또는 rows 둘 중 하나는 반드시 제공되어야 합니다.")
        
    async def _process_rows(self, rows, **kwargs):
        for character_id, server_id in rows:
            api_requests = self._get_api_requests(character_id, server_id, **kwargs)
            for api_request in api_requests:
                await self.api_request_queue.put(api_request)   

    def _get_api_requests(self, character_id, server_id, **kwargs):       
        api_requests = [{
            'endpoint' : self.end_point,
            'params' : {
                'characterId' : character_id,
                'serverId' : server_id,
                'apikey' : next(self.api_keys)
            }
        }]   
        return api_requests

class CharacterFameSeeder(BaseSeeder):
    async def seed(self, max_fame : int):
        for seed_params in PARAMS_FOR_SEED_CHARACTER_FAME:
            if self.shutdown_event.is_set():
                break
            api_request = {
                'endpoint' : self.end_point, 
                'params' : {
                        'maxFame' : max_fame,
                        'jobId' : seed_params['jobId'],
                        'jobGrowId' : seed_params['jobGrowId'],
                        'serverId' : seed_params['serverId'],
                        'apikey' : next(self.api_keys)
                }
            }
            await self.api_request_queue.put(api_request)

class CharacterTimelineSeeder2(BaseSeeder):
    async def seed(
            self,
            sql: Optional[str] = None,
            rows : Optional[list] = None,
            start_date : str = '2025-01-09 12:00',
            end_date : str = datetime.now().strftime("%Y-%m-%d %H:%M"),
            code : str = '',
            limit : int = 100,
            seeder_batch_size : int = Settings.DEFAULT_SEEDER_BATCH_SIZE
    ):
        if sql:
            async with self.psql_pool.acquire() as conn:
                async with conn.transaction():
                    cursor = await conn.cursor(sql)
                    while not self.shutdown_event.is_set():
                        rows_batch = await cursor.fetch(seeder_batch_size)
                        if not rows_batch:
                            break
                        await self._process_rows(rows_batch, start_date, end_date, code, limit)
        elif rows:
            await self._process_rows(rows, start_date, end_date, code, limit)
        else:
            raise ValueError("sql 또는 rows 둘 중 하나는 반드시 제공되어야 합니다.")
                                  
    async def _process_rows(self, rows, start_date, end_date, code, limit):
        for character_id, server_id in rows:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M')
            start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M')

            ranges = []
            current_start = start_dt
            while current_start < end_dt:
                current_end = min(current_start + timedelta(days=90), end_dt)
                ranges.append((current_start, current_end))
                current_start = current_end

            for range_start, range_end in ranges:
                api_request = {
                    'endpoint': self.end_point,
                    'params': {
                        'characterId': character_id,
                        'serverId': server_id,
                        'startDate': range_start.strftime('%Y-%m-%d %H:%M'),
                        'endDate': range_end.strftime('%Y-%m-%d %H:%M'),
                        'code': code,
                        'limit': limit,
                        'apikey': next(self.api_keys)
                    }
                }
                await self.api_request_queue.put(api_request)

class CharacterTimelineSeeder(CharacterBaseSeeder):
    def _get_api_request(self, character_id, server_id, **kwargs):
        api_requests = []
        start_date = kwargs.get('start_date', '2025-01-09 12:00')
        end_date = kwargs.get('end_date', datetime.now().strftime("%Y-%m-%d %H:%M"))
        code = kwargs.get('code', '')
        limit = kwargs.get('limit', 100)
        
        end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M')
        start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M')

        ranges = []
        current_start = start_dt
        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=90), end_dt)
            ranges.append((current_start, current_end))
            current_start = current_end

            for range_start, range_end in ranges:
                api_request = {
                    'endpoint': self.end_point,
                    'params': {
                        'characterId': character_id,
                        'serverId': server_id,
                        'startDate': range_start.strftime('%Y-%m-%d %H:%M'),
                        'endDate': range_end.strftime('%Y-%m-%d %H:%M'),
                        'code': code,
                        'limit': limit,
                        'apikey': next(self.api_keys)
                    }
                }
                api_requests.append(api_request)
        return api_requests

SEEDERS = {
    'character_fame' : CharacterFameSeeder,
    'character_info' : CharacterBaseSeeder,
    'character_timeline' : CharacterTimelineSeeder
}