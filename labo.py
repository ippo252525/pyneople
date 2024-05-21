from src.pyneople.METADATA import CHARACTER_INFORMATION_NAME
from src.pyneople.functions import ServerMaintenanceError, NeopleOpenAPIError
import json
import requests
import aiohttp
import asyncio   
import time
from src.pyneople.character import CharacterSearch
# from multiprocessing import Process, Queue, Value
from pymongo import MongoClient
SETTINGS = {
    "request_time_out" : 5,
    "request_time_sleep" : 0.0015
}

MONGO_CLIENT = MongoClient('mongodb://localhost:27017/')
PG_CONNECTION_DICT = {
    'host' : 'localhost', 
    'user' : 'dnfdba', 
    'password':'252525', 
    'database':'dnf'
}
API_KEY_LIST = [
    "bNmjPRpeJ5q45xgJ5R46RLT6apqEa70h",
    "sN01Qsr5CknKLC8FyxAdzNGkaaTQaJdb",
    "icPqja2HwIlw8HJLoRIS1Um4DyGDBkH2"
]

DB_STRING = "postgresql://dnfdba:252525@localhost:5432/dnf"
async def as_get_request(arg_url : str):
    """
    url 입력시 data 가져오는 함수
        Args :
            arg_url(str) : 원하는 url 주소
    """
    async with aiohttp.ClientSession() as session:
        print(f"요청{time.time()}")
        async with session.get(arg_url) as response:
            await asyncio.sleep(0.0015)
            if response.status == 200:
                data = await response.json()
                print("반환")
                return data
            else:
                response.raise_for_status()    

def get_request(arg_url : str):
    """
    url 입력시 data 가져오는 함수
        Args :
            arg_url(str) : 원하는 url 주소
    """
    start_time = time.time()
    print(start_time)
    data = requests.get(arg_url, timeout = SETTINGS['request_time_out'])
    data = json.loads(data.text)
    # Neople Open API 상에서 규정된 에러가 발생할 경우 에러를 발생시킨다.
    if data.get("error"):
        if data.get("error").get('status') == 503:
            raise ServerMaintenanceError
        else:
            raise NeopleOpenAPIError(data.get("error"))
    elapsed_time = time.time() - start_time
    if elapsed_time < SETTINGS['request_time_sleep']:
        time.sleep(SETTINGS['request_time_sleep'] - elapsed_time)
    print("반환")
    return data                
class PyNeople():
    """
    부모 Class로 사용
    """
    def __init__(self, arg_api_key : str):
        """
        클래스 생성 시 Neople Open API key를 입력받는다  
            Args :  
                arg_api_key(str) : Neople Open API key  
        """        
        self._api_key = arg_api_key
# class CharacterFame(PyNeople):
#     """
#     Neople Open API 16. 캐릭터 명성 검색
#     """    
#     def get_data(self, arg_min_fame : int, 
#                   arg_max_fame : int,
#                   arg_job_id : str = "",
#                   arg_job_grow_id : str = "",
#                   arg_is_all_job_grow : bool = False, 
#                   arg_is_buff : bool = "", 
#                   arg_server_id : str = "all",
#                   arg_limit : int = 200):
#         """
#         해당 명성 구간의 캐릭터 정보를 원소로 가지는 list를 반환함
#             Args : 
#                 arg_min_fame(int) : 명성 구간 최소값(최대 명성과의 차이가 2000이상이면 최대명성 - 2000 으로 입력됨)
                
#                 arg_max_fame(int) : 명성 구간 최대값
                
#                 arg_job_id(str) : 캐릭터 직업 고유 코드
                
#                 arg_job_grow_id(str) : 캐릭터 전직 직업 고유 코드(jobId 필요)
                
#                 arg_is_all_job_grow(bool) : jobGrowId 입력 시 연계되는 전체 전직 포함 조회 ex) 검성 -> 웨펀마스터, 검성, 검신, 眞웨펀마스터
                
#                 arg_is_buff(bool) : 버퍼만 조회(true), 딜러만 조회(false), 전체 조회(미 입력)	
                
#                 arg_server_id(str) : 서버 아이디
                
#                 arg_limit(int) : 반환 Row 수
#         """
#         url = f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters-fame?minFame={arg_min_fame}&maxFame={arg_max_fame}&jobId={arg_job_id}&jobGrowId={arg_job_grow_id}&isAllJobGrow={arg_is_all_job_grow}&isBuff={arg_is_buff}&limit={arg_limit}&apikey={self._api_key}"
#         return asyncio.run(as_get_request(url))
        # return get_request(url)
# def store_fame_data_to_mongodb(
#         arg_mongo_client_instance : MongoClient,
#         arg_database_name : str,
#         arg_collection_name : str,
#         arg_api_key_list : list[str],
#         arg_max_fame : int = 100000,
#         arg_min_fame : int = 0):
#     """
#     최근 90일 이내 접속한 110 레벨 이상 캐릭터 전체를 MongoDB에 저장하는 함수
#         Args :
#             arg_mongo_client_instance(MongoClient) : 저장하려는 MongoDB의 pymongo MongoClient 객체  
            
#             arg_database_name(str) : 저장하려는 MongoDB의 database name  
            
#             arg_collection_name(str) : 저장하려는 MongoDB의 collection name  
            
#             arg_api_key_list(list[str]) : Neople Open API 에서 발급된 api key를 원소로 가지는 list  
            
#             arg_max_fame(int) : 조회 하려는 최대 명성

#             arg_min_fame(int) : 조회 하려는 최소 명성  
#     """
#     def task_get_request(character_fame_instance, args_queue, data_queue, completed_tasks_count, tasks_to_be_completed_count):
#         """
#         args_queue에서 인자 정보를 get하고 데이터를 Neople Open API 에서 가져와서 data_queue에 저장
#         """
#         while completed_tasks_count.value != tasks_to_be_completed_count:
#             # print(completed_tasks_count.value, tasks_to_be_completed_count)
#             if not args_queue.empty():
#                 args_dict = args_queue.get()
#                 try:
#                     data = character_fame_instance.get_data(**args_dict)
#                     data_queue.put(data)
#                 except ServerMaintenanceError:
#                     raise Exception("서버 점검중")
#                 except:
#                     args_queue.put(args_dict)

#     def task_store_data(character_search_instance, args_queue, data_queue, mongo_collection, completed_tasks_count, tasks_to_be_completed_count):        
#         """
#         data_queue에서 data를 get하고 MongoDB에 저장 후 다음 인자 정보를 args_queue에 저장
#         """        
#         while completed_tasks_count.value != tasks_to_be_completed_count:
#             # print(completed_tasks_count.value, tasks_to_be_completed_count)
#             # data queue가 비여있지 않다면
#             if not data_queue.empty():
#                 # data queue에서 data get
#                 data = data_queue.get()
#                 # 해당 data MongoDB에 그대로 저장
#                 mongo_collection.insert_one(data)
#                 data = data['rows']

#                 # 데이터가 있다면
#                 if data:

#                     # 다음 인자 정보 반환을 위한 작업
#                     character_search_instance.parse_data(data[0])
#                     max_fame = character_search_instance.fame
#                     character_search_instance.parse_data(data[-1])
#                     min_fame = character_search_instance.fame
                    
#                     # 데이터 수집 여부 확인 하도록 출력
#                     # print(f"max = {max_fame}, min = {min_fame}, 직업 = {character_search_instance.job_grow_name}")
                    
#                     # 모든 캐릭터의 명성이 같다면
#                     if max_fame == min_fame:
#                         # 최대명성을 1만 내림
#                         min_fame = max_fame - 1

#                     # 명성이 최소값이 arg_min_fame보다 작거나 같으면
#                     if min_fame <= arg_min_fame:
#                         # 해당 직업 완료
#                         completed_tasks_count.value += 1
#                         print(f"완료된 직업 개수 {completed_tasks_count.value}")
#                         continue       
                    
#                     # 인자정보 args queue에 저장
#                     args_dict = {
#                         'arg_min_fame' : arg_min_fame,
#                         'arg_max_fame' : min_fame,
#                         'arg_job_id' : character_search_instance.job_id,
#                         'arg_job_grow_id' : character_search_instance.job_grow_id,
#                         'arg_is_all_job_grow' : True
#                     }
#                     args_queue.put(args_dict)
#                 # 데이터가 없다면
#                 else:
#                     # 해당 직업 완료
#                     completed_tasks_count.value += 1
#                     print(f"완료된 직업 개수 {completed_tasks_count.value}")
#                     continue                

    
#     database = arg_mongo_client_instance[arg_database_name]
#     collection = database[arg_collection_name]
#     data = get_request(f"https://api.neople.co.kr/df/jobs?apikey={arg_api_key_list[0]}")
#     data = data['rows']
#     job_id_list = []
#     for job in data:
#         for job_grow in job['rows']:
#             job_id_list.append((job['jobId'], job_grow['jobGrowId']))
#     tasks_to_be_completed_count = len(job_id_list)
#     data_queue = Queue()
#     args_queue = Queue()
#     completed_tasks_count = Value("i", 0)          
#     processes = []  
#     for api_key in arg_api_key_list:
#         character_fame_instance = CharacterFame(api_key)
#         process = Process(target=task_get_request, args=(character_fame_instance, args_queue, data_queue, completed_tasks_count, tasks_to_be_completed_count))
#         processes.append(process)
#     character_search_instance = CharacterSearch(arg_api_key_list[0])    
#     process = Process(target=task_store_data, args=(character_search_instance, args_queue, data_queue, collection, completed_tasks_count, tasks_to_be_completed_count))
#     processes.append(process)
    
#     # 프로세스 시작
#     for process in processes:
#         process.start()
    
#     # arg_queue에 인자 정보 투입
#     for job_id , job_grow_id in job_id_list:
#         max_fame = arg_max_fame
#         args_dict = {
#             'arg_min_fame' : arg_min_fame,
#             'arg_max_fame' : max_fame,
#             'arg_job_id' : job_id,
#             'arg_job_grow_id' : job_grow_id,
#             'arg_is_all_job_grow' : True
#         }
#         args_queue.put(args_dict)
    
#     # 프로세스 모두 종료 대기
#     for process in processes:
#         process.join()    
class PyNeopleAttributeSetter(PyNeople):
    """
    하위 Attribute를 설정 할 수 있는 PyNeople Class 의 부모 Class
    """

    @classmethod
    def set_sub_attributes(cls, arg_new_attribute_list : list[str]):
        for new_attribute_name in arg_new_attribute_list:
            if not new_attribute_name in cls.default_sub_attribute_list:
                raise ValueError("사용할 수 없는 attribute 입니다.")
        cls.sub_attribute_list = arg_new_attribute_list

    @classmethod
    def delete_sub_attributes(cls, arg_delete_attribute_list : list[str]):
        for new_attribute_name in arg_delete_attribute_list:
            if not new_attribute_name in cls.default_sub_attribute_list:
                raise ValueError("제거 할 수 없는 attribute 입니다.")
        cls.sub_attribute_list = [sub_attr for sub_attr in cls.default_sub_attribute_list if sub_attr not in arg_delete_attribute_list]

    @classmethod        
    def init_sub_attributes(cls):
        cls.sub_attribute_list = cls.default_sub_attribute_list
# store_fame_data_to_mongodb(MONGO_CLIENT, "dnf", "test", API_KEY_LIST, 10000)
class CharacterInformation(PyNeopleAttributeSetter):
    """
    Neople Open API 03. 캐릭터 '기본정보' 조회
    """
    default_sub_attribute_list = CHARACTER_INFORMATION_NAME.keys()
    sub_attribute_list = default_sub_attribute_list
    def get_data(self, arg_server_id : str, arg_character_id : str):
        """
        영문 서버 이름과 캐릭터 ID 를 검색하면 기본 정보를 반환
            Args : 
                arg_server_id(str) : 영문 서버 이름  ex) cain  
                
                arg_character_name(str) : 캐릭터 ID ex) d018e5f7e7519e34b8ef21db0c40fd98
        """    
        self._total_id = f"{arg_server_id} {arg_character_id}"
        url = f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}?apikey={self._api_key}"
        return get_request(url)
    def as_get_data(self, arg_server_id : str, arg_character_id : str):
        """
        영문 서버 이름과 캐릭터 ID 를 검색하면 기본 정보를 반환
            Args : 
                arg_server_id(str) : 영문 서버 이름  ex) cain  
                
                arg_character_name(str) : 캐릭터 ID ex) d018e5f7e7519e34b8ef21db0c40fd98
        """    
        self._total_id = f"{arg_server_id} {arg_character_id}"
        url = f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}?apikey={self._api_key}"
        return asyncio.run(as_get_request(url))
cs = CharacterSearch(API_KEY_LIST[0])
data = cs.get_data("디레지에", "알카오존스히")
cs.parse_data(data)
ci = CharacterInformation(API_KEY_LIST[0])

start = time.time()
# ci.as_get_data(cs.server_id, cs.character_id)
# for i in range(10):
ci.as_get_data(cs.server_id, cs.character_id)
end = time.time()
print(end-start)