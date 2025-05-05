import json
import os
from psycopg2 import sql
from typing import Dict, List
import pyneople.api.registry.endpoint_class
from pyneople.api.registry.endpoint_registry import EndpointRegistry

def create_table_query(
    table_name: str,
    columns: Dict,
    constraints: List[str] = None
):
    """
    PostgreSQL 테이블 생성 쿼리를 반환하는 함수

    Args:
        table_name (str): 테이블 이름
        columns (dict): 컬럼 정의 예: {"id": "SERIAL", "name": "TEXT"}
        constraints (list): 제약조건 리스트 예: ["PRIMARY KEY (id)", "UNIQUE (name)"]
    Returns:
        str: PostgreSQL create table query
            
    """
    indent = sql.SQL("  ")
    newline = sql.SQL("\n")    
    column_defs = [sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(dtype)) for col, dtype in columns.items()]
    if constraints:
        constraint_defs = [sql.SQL(constraint) for constraint in constraints]
        all_defs = column_defs + constraint_defs
    else:
        all_defs = column_defs
    comma_lines = [
        indent + line + sql.SQL(",") for line in all_defs[:-1]
    ] + [indent + all_defs[-1]]    
    query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (\n{}\n);").format(
        sql.Identifier(table_name),
        newline.join(comma_lines)
    )
    return query


import psycopg2
from pyneople.config.config import Settings
if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=Settings.POSTGRES_DB,
        user=Settings.POSTGRES_USER,
        password=Settings.POSTGRES_PASSWORD,
        host=Settings.POSTGRES_HOST,
        port=Settings.POSTGRES_PORT
    )

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(BASE_DIR, f'../../db/endpoint_to_column_dtype_map.json'), 'r', encoding='utf-8') as f:
        endpoint_to_column_dtype_dict = json.load(f)
    endpoints = EndpointRegistry.get_registered_endpoints()
    for endpoint in endpoints:
        if endpoint in ['character_fame', 'character_timeline']:
            continue
        column_dtype_dict = endpoint_to_column_dtype_dict[endpoint]
        endpoint_class = EndpointRegistry.get_class(endpoint)
        staging_table_name = endpoint_class.staging_table_name
        sql_path = os.path.join(BASE_DIR, f'../../db/schema/staging_tables/{staging_table_name}.sql')
        columns = endpoint_class.data_path_map.keys()
        columns = {column : column_dtype_dict[column] for column in columns}
        query = create_table_query(staging_table_name, columns)
        query_str = query.as_string(conn)
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(query_str)

    conn.close()    
        
