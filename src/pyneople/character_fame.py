from .functions import get_request

class CharacterFame():
    
    def __init__(self, arg_api_key : str):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """        
        self.__api_key = arg_api_key

    def get_data(self, arg_min_fame : int, 
                  arg_max_fame : int,
                  arg_job_id : str = "",
                  arg_job_grow_id : str = "",
                  arg_is_all_job_grow : bool = False, 
                  arg_is_buff : bool = "", 
                  arg_server_id : str = "all",
                  arg_limit : int = 10):
        url = f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters-fame?minFame={arg_min_fame}&maxFame={arg_max_fame}&jobId={arg_job_id}&jobGrowId={arg_job_grow_id}&isAllJobGrow={arg_is_all_job_grow}&isBuff={arg_is_buff}&limit={arg_limit}&apikey={self.__api_key}"
        return get_request(url)
