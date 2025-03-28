from .endpoints import API_ENDPOINTS

# def build_url(endpoint: str, params: dict) -> str:
#     template = API_ENDPOINTS.get(endpoint)
#     if not template:
#         raise ValueError(f"Unknown endpoint: {endpoint}")
    
#     filled_params = {**template['default_params'], **params}
#     return template.get('url').format(**filled_params)

def build_url(api_request : dict) -> str:
    template = API_ENDPOINTS.get(api_request['endpoint'])
    if not template:
        raise ValueError(f"Unknown endpoint: {api_request['endpoint']}")
    
    filled_params = {**template['default_params'], **api_request['params']}
    return template.get('url').format(**filled_params)

# 테스트
# print(build_url("character_fame", {'apikey' : "aa"}))