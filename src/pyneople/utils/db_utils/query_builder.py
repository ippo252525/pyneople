from typing import Dict, List, Tuple, Any
# Insurt Only Staging Table 도입으로 현재는 사용하지 않는 코드 입니다.

def build_upsert_query(
    table_name: str,
    data: Dict[str, Any],
    conflict_keys: List[str]
) -> Tuple[str, List[Any]]:
    """
    UPSERT 쿼리를 생성. 기존 값과 다를 경우에만 UPDATE 되도록 구성됨.
    
    Returns:
        query: SQL 문자열 (placeholders 포함, 예: $1, $2, ...)
        values: 실제 쿼리에 바인딩할 값 리스트
    """
    columns = list(data.keys())
    values = [data[col] for col in columns]

    col_names = ", ".join(columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))

    # 업데이트 대상 컬럼 (conflict 키 제외)
    update_cols = [col for col in columns if col not in conflict_keys]
    
    update_assignments = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in update_cols
    )
    update_condition = " OR ".join(
        f"{table_name}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in update_cols
    )

    conflict_clause = ", ".join(conflict_keys)

    query = f"""
    INSERT INTO {table_name} ({col_names})
    VALUES ({placeholders})
    ON CONFLICT ({conflict_clause}) DO UPDATE
    SET {update_assignments}
    WHERE {update_condition}
    """

    return query.strip(), values


def build_bulk_upsert_query(
    table_name: str,
    data: list[Dict[str, Any]],
    conflict_keys: List[str]
) -> Tuple[str, List[Any]]:
    """
    다중 UPSERT 쿼리를 생성.
    
    Returns:
        query: SQL 문자열 (placeholders 포함, 예: $1, $2, ...)
        values: 실제 쿼리에 바인딩할 값 리스트
    """
    columns = list(data[0].keys())
    values = [[row.get(col, None) for col in columns] for row in data]

    col_names = ", ".join(columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    
    # confict_keys 없는 경우 insert만 한다.
    if not conflict_keys:
            query = f"""
            INSERT INTO {table_name} ({col_names})
            VALUES ({placeholders})
            """
    else:      
        # 업데이트 대상 컬럼 (conflict 키 제외)
        update_cols = [col for col in columns if col not in conflict_keys]
        
        update_assignments = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in update_cols
        )

        conflict_clause = ", ".join(conflict_keys)
        
      
        query = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause}) DO UPDATE
        SET {update_assignments}
        """

    return query.strip(), values


def build_bulk_insert_query(
    table_name: str,
    data: list[Dict[str, Any]],
) -> Tuple[str, List[Any]]:
    '''
    insert only table인 staging table에 insert하기 위한 용도
    '''
    columns = list(data[0].keys())
    values = [[row.get(col, None) for col in columns] for row in data]

    col_names = ", ".join(columns)
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    query = f"""
            INSERT INTO {table_name} ({col_names})
            VALUES ({placeholders})
            """     
    return query.strip(), values

     