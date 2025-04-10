import asyncio
import aiohttp
from datetime import datetime, timezone
from api.url_builder import build_url
from .api_processors import process_api_request, NEXT_ENDPOINT

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
                try:
                    async with self.session.get(url) as response:
                        data = await response.json()
                    print(f"\r{api_request['params']['maxFame']}데이터 확보 완료", end="", flush=True)

                    
                    if api_request['endpoint'] in NEXT_ENDPOINT.keys():
                        # print("들어왔다")
                        # print(api_request)
                        next_parameter = process_api_request(data, api_request)
                        # print(next_parameter)
                        # print(f"{next_parameter}다음 url 생성 완료")
                        if next_parameter:
                            await self.api_request_queue.put(next_parameter)
                            print('\r추가 url put완료', end="", flush=True)
                        else:
                            print('\r추가 url 없음', end="", flush=True)
                    data.update({'fetched_at' : datetime.now(timezone.utc)})
                    data = {'endpoint' : api_request['endpoint'], 'data' : data}
                    await self.data_queue.put(data)
                
                except:
                    print('오류발생')
                    
                finally:
                    self.api_request_queue.task_done()        
            
            else:
                print('\r워커 탈출', end="", flush=True)
                self.api_request_queue.task_done()
                break