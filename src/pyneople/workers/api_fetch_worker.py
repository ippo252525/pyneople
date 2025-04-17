import asyncio
import aiohttp
from datetime import datetime, timezone
from api.url_builder import build_url
from .api_processors import process_api_request, NEXT_ENDPOINT
import config.config as config
class APIFetchWorker:
    
    def __init__(self, api_request_queue: asyncio.Queue, data_queue: asyncio.Queue, session : aiohttp.ClientSession):
        self.api_request_queue = api_request_queue
        self.data_queue = data_queue
        self.session = session

    async def run(self):
        
        while True:
            
            api_request =  await self.api_request_queue.get()
            
            if api_request:
                url = build_url(api_request)
                config.request_count += 1
                try:
                    # print(f'url : {url}')
                    
                    async with self.session.get(url) as response:
                        # print('세션 들어옴')
                        data = await response.json()
                    # print(f"\r{api_request['params']['maxFame']}데이터 확보 완료", end="", flush=True)

                    
                    if api_request['endpoint'] in NEXT_ENDPOINT.keys():
                        next_parameter = process_api_request(data, api_request)
                        # print(next_parameter)
                        # print(f"{next_parameter}다음 url 생성 완료")
                        if next_parameter:
                            await self.api_request_queue.put(next_parameter)
                            # print('추가 url put완료')
                        else:
                            pass
                            # print('추가 url 없음')
                    data.update({'fetched_at' : datetime.now(timezone.utc)})
                    data = {'endpoint' : api_request['endpoint'], 'data' : data}
                    await self.data_queue.put(data)
                
                except Exception as e:
                    print(f'오류발생 {e}')
                    
                finally:
                    self.api_request_queue.task_done()        
            
            else:
                print('\r워커 탈출', end="", flush=True)
                self.api_request_queue.task_done()
                break