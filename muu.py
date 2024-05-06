from src.pyneople.database_connecter import store_fame_data_to_mongodb, mongodb_to_postgresql, PostgreSQLConnecter, store_timeline_data_to_mongodb
from src.pyneople.character import *
from src.pyneople.functions import value_flatten, ServerMaintenanceError
from pymongo import MongoClient
from multiprocessing import Process, Queue, Value
API_KEY_LIST = [
    "bNmjPRpeJ5q45xgJ5R46RLT6apqEa70h",
    "sN01Qsr5CknKLC8FyxAdzNGkaaTQaJdb",
    "icPqja2HwIlw8HJLoRIS1Um4DyGDBkH2"
]
mg = MongoClient('mongodb://localhost:27017/')
# store_fame_data_to_mongodb(mg, 'dnf', 'fame_tb_20240502', API_KEY_LIST)

pg = PostgreSQLConnecter({
    'host' : 'localhost', 
    'user' : 'dnfdba', 
    'password':'252525', 
    'database':'dnf'
})
# cs = CharacterSearch("a")

# def prepro(document):
#     document = document['rows']
#     data = []
#     for character in document:
#         cs.parse_data(character)
#         value_flatten(cs)
#         data.append(tuple(
#             [f"{cs.server_id} {cs.character_id}",
#             cs.character_name,
#             cs.level,
#             cs.job_name,
#             cs.job_grow_name,
#             cs.fame]
#         ))
#     return data    

# pg.create_table("fame_tb_20240502",
#             ["total_id VARCHAR(43) NOT NULL PRIMARY KEY", 
#             "character_name VARCHAR(16)",
#             "level SMALLINT",
#             "job_name VARCHAR(16)",
#             "job_grow_name VARCHAR(16)",
#             "fame INT"], 
#             arg_drop=True)

# mongodb_to_postgresql(pg, 'fame_tb_20240501', mg, 'dnf', 'fame_tb_20240502', prepro)

query = \
"""
SELECT total_id
FROM fame_tb_20240501
WHERE fame > 58087
LIMIT 100
;"""
data = pg.fetch(query)
data = [character[0] for character in data]
store_timeline_data_to_mongodb(mg, 'dnf', 'test', API_KEY_LIST, data, "2024-05-06 10:10","2024-05-06 09:00")

# pg.create_table("timeline_tb_20240502",
#             ["total_id VARCHAR(43)",
#              "timeline_code SMALLINT",
#              "timeline_date TIMESTAMP",
#              "timeline_data TEXT"
#              ], 
#             arg_drop=True)

# def prepro(document):
#     data = []
#     for timeline in document.get("timeline"):
#         data.append((document['total_id'], timeline['code'], timeline['date'], str(timeline['data'])))
#     return data
    
# mongodb_to_postgresql(pg,'timeline_tb_20240502', mg, 'dnf', 'timeline_tb_20240502', prepro)