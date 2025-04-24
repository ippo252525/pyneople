from typing import Optional

def process_fame_api_request(data : dict, api_request : dict) -> Optional[dict]:
    """
    캐릭터 명성 데이터를 기준으로 다음 API request parameters를 설정하는 함수

    주어진 데이터가 200개 이상이면 마지막 명성 값을 기준으로 maxFame을 조정하고,
    200개 미만이면 전체 명성 범위의 최소값보다 1 작은 값을 설정함.
    maxFame이 0 이하인 경우 None 반환

    Args:
        data (dict): Neople Open API 응답 데이터.
        api_request (dict): endpoint와 request parameters를 key로 가지는 dict

    Returns:
        Optional[dict]: 수정된 api_request dict 또는 None
    """
    if len(data['rows']) >= 200:
        # 200개는 제일 작은 값부터 다시 조회
        api_request['params']['maxFame'] = data['rows'][-1]['fame']
        
        if data['rows'][0]['fame'] ==  data['rows'][-1]['fame']:
            # 모든 명성 값이 같은 경우는 1만 빼고 다시 조회
            api_request['params']['maxFame'] = data['rows'][0]['fame'] - 1
    else:
        # 200개 미만이면 최소명성 - 1 부터 조회
        api_request['params']['maxFame'] = data['fame']['min'] - 1    
    
    if api_request['params']['maxFame'] <= 0:
        return None
    else:
        return api_request

def process_timeline_api_request(data : dict, api_request : dict) -> Optional[dict]: 
    """
    timeline 응답의 next 값을 기반으로 다음 API request parameters를 설정하는 함수

    next 값이 존재하면 해당 값을 request parameter로 추가하고, 없으면 None 반환

    Args:
        data (dict): Neople Open API 응답 데이터. 'timeline' key와 그 안의 'next' 값을 포함함
        api_request (dict): endpoint와 request parameters를 key로 가지는 dict

    Returns:
        Optional[dict]: 수정된 api_request dict 또는 None
    """
    # next가 있는 경우 next를 추가하고 아니면 None반환
    if data['timeline']['next']:
        api_request['params']['next'] = data['timeline']['next']
        return api_request
    else:
        return None


NEXT_ENDPOINT = {
    'character_fame' : process_fame_api_request,
    'character_timeline' : process_timeline_api_request
}

def process_api_request(data : dict, api_request : dict) -> dict:
    """
    현재 endpoint에 해당하는 후속 API request parameters를 설정하는 함수

    endpoint에 매핑된 처리 함수를 호출해 다음 요청 정보를 생성함

    Args:
        data (dict): Neople Open API 응답 데이터
        api_request (dict): 'endpoint'와 request parameters를 key로 가지는 dict

    Returns:
        dict: 수정된 api_request dict
    """    
    return NEXT_ENDPOINT[api_request['endpoint']](data, api_request)