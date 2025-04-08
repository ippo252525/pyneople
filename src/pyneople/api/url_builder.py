from .endpoints import API_ENDPOINTS

def build_url(api_request : dict) -> str:
    template = API_ENDPOINTS.get(api_request['endpoint'])
    if not template:
        raise ValueError(f"Unknown endpoint: {api_request['endpoint']}")
    
    filled_params = {**template['default_params'], **api_request['params']}
    return template.get('url').format(**filled_params)