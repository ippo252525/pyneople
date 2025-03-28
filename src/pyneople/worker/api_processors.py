def process_fame_api_request(data : dict, api_request : dict) -> dict:
    """파라미터dict를 받으면 결과 값에 따라서 다음 파라피터를 넣는 함수"""
    
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

def process_timeline_api_request():     
    return {}

NEXT_ENDPOINT = {
    'character_fame' : process_fame_api_request,
    'character_timeline' : process_timeline_api_request
}

def process_api_request(data : dict, api_request : dict) -> dict:
    return NEXT_ENDPOINT[api_request['endpoint']](data, api_request)