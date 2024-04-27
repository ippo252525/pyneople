from .character import CharacterFame, CharacterSearch
from .functions import get_request, ServerMaintenanceError, PyneopleError
from multiprocessing import Process, Queue, Value
from typing import Callable
from pymongo import MongoClient
import time
import psycopg2
from psycopg2 import sql

def store_fame_data_to_mongodb(
        arg_mongo_client_instance : MongoClient,
        arg_database_name : str,
        arg_collection_name : str,
        arg_api_key_list : list[str],
        arg_character_search_instance : CharacterSearch,
        arg_max_fame : int):
    # start_time = time.time()
    
    def task_get_request(id, character_fame_instance, args_queue, data_queue, completed_tasks_count, tasks_to_be_completed_count):
        while completed_tasks_count.value != tasks_to_be_completed_count:
            if not args_queue.empty():
                args_dict = args_queue.get()
                try:
                    data = character_fame_instance.get_data(**args_dict)
                    print("task get request", end="\r")
                    data_queue.put(data)
                except ServerMaintenanceError:
                    raise Exception("서버 점검중")
                except:
                    args_queue.put(args_dict)

    def task_store_data(id, args_queue, data_queue, mongo_collection, completed_tasks_count, tasks_to_be_completed_count):                
        
        while completed_tasks_count.value != tasks_to_be_completed_count:
            if not data_queue.empty():
                data = data_queue.get()
                mongo_collection.insert_one(data)
                data = data['rows']
                if data:
                    arg_character_search_instance.parse_data(data[0], ['fame', 'job_id', 'job_grow_id', 'job_grow_name'])
                    max_fame = arg_character_search_instance.fame
                    arg_character_search_instance.parse_data(data[-1], ['fame', 'job_id', 'job_grow_id', 'job_grow_name'])
                    min_fame = arg_character_search_instance.fame
                    job_id = arg_character_search_instance.job_id
                    job_grow_id = arg_character_search_instance.job_grow_id
                    job_grow_name = arg_character_search_instance.job_grow_name
                    print(f"max = {max_fame}, min = {min_fame}, 직업 = {job_grow_name}", end="\r")
                    if max_fame == min_fame:
                        min_fame = max_fame - 1
                    if min_fame <= 0:
                        completed_tasks_count.value += 1
                        print(f"완료된 직업 개수 {completed_tasks_count.value}", end="\r")
                        continue                        
                    args_dict = {
                        'arg_min_fame' : 0,
                        'arg_max_fame' : min_fame,
                        'arg_job_id' : job_id,
                        'arg_job_grow_id' : job_grow_id,
                        'arg_is_all_job_grow' : True
                    }
                    args_queue.put(args_dict)
                else:
                    completed_tasks_count.value += 1
                    print(f"완료된 직업 개수 {completed_tasks_count.value}", end="\r")
                    continue                

    
    database = arg_mongo_client_instance[arg_database_name]
    collection = database[arg_collection_name]
    data = get_request(f"https://api.neople.co.kr/df/jobs?apikey={getattr(arg_character_search_instance, '_api_key')}")
    data = data['rows']
    job_id_list = []
    for job in data:
        for job_grow in job['rows']:
            job_id_list.append((job['jobId'], job_grow['jobGrowId'], job['jobName'], job_grow['jobGrowName']))
    tasks_to_be_completed_count = len(job_id_list)
    data_queue = Queue()
    args_queue = Queue()
    completed_tasks_count = Value("i", 0)          
    processes = []  
    for api_key in arg_api_key_list:
        character_fame_instance = CharacterFame(api_key)
        process = Process(target=task_get_request, args=(1, character_fame_instance, args_queue, data_queue, completed_tasks_count, tasks_to_be_completed_count))
        processes.append(process)
    process = Process(target=task_store_data, args=(2, args_queue, data_queue, collection, completed_tasks_count, tasks_to_be_completed_count))
    processes.append(process)
    for process in processes:
        process.start()
    
    for job_id , job_grow_id, job_name , job_grow_name in job_id_list:
        max_fame = arg_max_fame
        args_dict = {
            'arg_min_fame' : 0,
            'arg_max_fame' : max_fame,
            'arg_job_id' : job_id,
            'arg_job_grow_id' : job_grow_id,
            'arg_is_all_job_grow' : True
        }
        args_queue.put(args_dict)        

    for process in processes:
        process.join()        

class PostgreSQLConnecter():
    
    def __init__(self, arg_database_connection_dict : dict):
        """
        생성자 함수로 database connect의 인자로 전달되는 dict를 입력받는다.
            Args:
                arg_database_connection_dict(dict) : psycopg2.connect 함수의 인자로 사용될 dict
        """
        self.connection = psycopg2.connect(**arg_database_connection_dict)

    def execute(self, arg_sql : str):
        '''
        sql문을 실행시키고 commit까지 완료 시키는 함수
            Args:
                arg_sql(str) : 실행되어야 하는 sql문
        '''
        
        with self.connection.cursor() as cursor:
            cursor.execute(arg_sql)
            self.connection.commit()


    def fetch(self, arg_query : str):
        '''
        query문을 실행시켜 나오는 결과를 반환한다.
            Args:
                arg_query(str) : 실행되어야 하는 query문
        '''

        with self.connection.cursor() as cursor:
            cursor.execute(arg_query)
            return cursor.fetchall()


    def create_table(self, arg_table_name : str, arg_columns : list, arg_drop : bool = False):
        """
        table을 만드는 함수
            Args:
                arg_table_name(str) : 생성하려는 table name
                
                arg_columns(list) : CREATE TABLE {table_name} (); 안에들어가는 문자열 list ex ["characterId VARCHAR(32) PRIMARY KEY", "serverId VARCHAR(32) NOT NULL"]
                
                arg_drop(bool) : {False : 이미 동일한 이름의 table이 있으면 에러발생(default), 
                                True : 이미 동일한 이름의 table이 있으면 삭제하고 만든다}
        """    
        columns_str = ', '.join(arg_columns)
        if arg_drop :
            self.execute(f"DROP TABLE IF EXISTS {arg_table_name};")
            self.execute(f"CREATE TABLE {arg_table_name} ({columns_str});")
        else :
            self.execute(f"CREATE TABLE {arg_table_name} ({columns_str});")


    def get_column_names(self, arg_table_name : str):
        """
        해당 table의 column 들의 이름을 list로 반환하는 함수
            Args:
                arg_table_name(str) : 확인하려는 table 이름
        """
        with self.connection.cursor() as cursor:
            cursor.execute(f"Select * FROM {arg_table_name} LIMIT 0")
            column_names = [desc[0] for desc in cursor.description]
            return column_names    


    def get_table_name_list(self):
        '''
        해당 데이터베이스의 table 이름의 list를 반환하는 함수
        '''
        data = self.fetch( 
        """
        SELECT
            table_schema || '.' || table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE'
        AND
            table_schema NOT IN ('pg_catalog', 'information_schema');
        """
        )            
        table_name_list = [table_name[0].split(".")[1] for table_name in data]
        return table_name_list


    def insert_into_table(self, arg_cursor , arg_table_name : str, arg_columns : list, arg_data : list, arg_ignore_duplication : bool = True):
        """
        table에 데이터를 삽입하는 함수
            Args:
                arg_cursor(cursor) : psycopg2 cursor 객체
                arg_table_name(str) : 데이터를 삽입하려는 table name
                arg_columns(list) : 데이터를 삽입하려는 column들의 list ex) ["characterId", "serverId", "jobName"]
                arg_data(list) : ex [('f2baddf4a296490a4d463cb512a83789', 'anton', '총검사'),
                                    ('87cbd3e834ae89c567a22a98bb2c9911', 'anton', '총검사')]
                                    or
                                    [('f2baddf4a296490a4d463cb512a83789', 'anton', '총검사')] <- data 1개여도 이런식으로 삽입
                arg_ignore_duplication(bool) : {True : 중복되는게 있으면 해당 항목만 넘어가고 계속 저장해라, False : 중복되는게 있으면 에러를 발생시켜라}

        주의사항 : 해당 함수는 connectiom.commit() 을 실행하지 않음
        """
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
            sql.Identifier(arg_table_name),
            sql.SQL(', ').join(map(sql.Identifier, arg_columns)),
            sql.SQL(', ').join(map(sql.Literal, arg_data))
            )
        if arg_ignore_duplication:
            insert_query += sql.SQL(" ON CONFLICT DO NOTHING") 
        arg_cursor.execute(insert_query)                    

def mongodb_to_postgresql(arg_postgresql_connecter : PostgreSQLConnecter, 
                          arg_postgresql_table_name : str,
                          arg_mongo_client : MongoClient, 
                          arg_mongo_database_name : str,
                          arg_mongo_collection_name : str,
                          arg_preprocess_function : Callable, 
                          batch_size : int = 100):
    """
    MomgoDB 에 저장된 데이터를 Postgresql로 전처리 후 batch_size씩 저장하는 함수
        Args :
            arg_postgresql_connecter(PostgreSQLConnecter) : pyneople database connecter
            arg_postgresql_table_name(str) :  저장하려는 PostgreSQL table name
            arg_mongo_client(MongoClient) : pymongo 의 MongoClient 객체
            arg_mongo_database_name(str) : MongoDB의 database name
            arg_mongo_collection_name(str) : MongoDB의 collection name
            arg_preprocess_function(Callable) : 전처리 함수(input으로 MongoDB의 document가 들어가며 tuple 또는 tuple로 이루어진 list를 반환해야 한다.)
            batch_size(int) : 한번에 조회, 저장하는 document 개수
    """
    postgresql_columns = arg_postgresql_connecter.get_column_names(arg_postgresql_table_name)
    postgresql_cursor = arg_postgresql_connecter.connection.cursor()

    mongo_database = arg_mongo_client[arg_mongo_database_name]
    mongo_collection = mongo_database[arg_mongo_collection_name]
    
    # 일괄 처리 크기
    total_data_count = mongo_collection.count_documents({})
    count = 0
    # MongoDB 데이터 조회 및 PostgreSQL에 삽입
    for skip in range(0, total_data_count, batch_size):

        mongo_cursor = mongo_collection.find().skip(skip).limit(batch_size)
        batch_data = list(mongo_cursor)

        if not batch_data:
            break

        # PostgreSQL에 데이터 삽입
        insert_data_list = []
        for document in batch_data:
            if isinstance(arg_preprocess_function(document), tuple):
                insert_data_list.append(arg_preprocess_function(document))
            elif isinstance(arg_preprocess_function(document), list):
                insert_data_list += arg_preprocess_function(document)
            else:
                TypeError("전처리 함수는 tuple 또는 list of tuple을 반환해야 합니다.")
        arg_postgresql_connecter.insert_into_table(postgresql_cursor, arg_postgresql_table_name, postgresql_columns, insert_data_list)
        arg_postgresql_connecter.connection.commit()
        count += batch_size
        print(f"{count}/{total_data_count}", end="\r")

        
    # 연결 종료
    arg_mongo_client.close()
    postgresql_cursor.close()
    arg_postgresql_connecter.connection.close()
    print("done")    

            