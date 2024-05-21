from src.pyneople.database_connecter import store_fame_data_to_mongodb
import time
from pymongo import MongoClient

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
start = time.time()
store_fame_data_to_mongodb(MONGO_CLIENT, "dnf", "test", API_KEY_LIST, 2000)
end = time.time()
print(end-start)