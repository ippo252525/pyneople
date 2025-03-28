import asyncio
import aiohttp
from wrapper.url_builder import build_url
from .api_processors import process_api_request, NEXT_ENDPOINT

class URLFetcherWorker:
    
    def __init__(self, api_request_queue: asyncio.Queue, data_queue: asyncio.Queue, session : aiohttp.ClientSession):
        self.api_request_queue = api_request_queue
        self.data_queue = data_queue
        self.session = session

    async def worker(self):
        
        while True:
            
            api_request =  await self.api_request_queue.get()
            
            if api_request:
                url = build_url(api_request)
                async with self.session.get(url) as response:
                    data = await response.json()
                print(f"{api_request['params']['maxFame']}데이터 확보 완료")
                await self.data_queue.put(data)
                
                if api_request['endpoint'] in NEXT_ENDPOINT.keys():
                    # print("들어왔다")
                    # print(api_request)
                    next_parameter = process_api_request(data, api_request)
                    # print(next_parameter)
                    print(f"{next_parameter}다음 url 생성 완료")
                    if next_parameter:
                        await self.api_request_queue.put(next_parameter)
                        print('추가 url put완료')
                    else:
                        print('추가 url 없음')
                
                self.api_request_queue.task_done()        
            
            else:
                print('워커 탈출')
                self.api_request_queue.task_done()
                break