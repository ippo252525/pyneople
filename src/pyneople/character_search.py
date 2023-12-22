import urllib.parse
from .functions import get_request
from .METADATA import SERVER_NAME_2_ID

class CharacterSearch():
    
    def __init__(self, arg_api_key : str):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """        
        self.__api_key = arg_api_key
    
    def get_data(self, arg_server_name : str, arg_character_name : str):
        """
        서버명과 캐릭터 이름을 검색하면 기본 정보를 반환
            Args : 
                arg_server_name(str) : 서버 이름 ex) 디레지에
                
                arg_character_name(str) : 캐릭터 이름 ex) 홍길동
        """

        # 한글 서버명을 영문 서버명으로 변환
        arg_server_name = SERVER_NAME_2_ID[arg_server_name]
        
        url = f"https://api.neople.co.kr/df/servers/{arg_server_name}/characters?characterName={urllib.parse.quote(arg_character_name)}&limit=1&apikey={self.__api_key}"
        return get_request(url)
    
    def parse_data(self, arg_data):
        """
        가져온 데이터를 정리해서 하위 attribute에 저장
            Args :
                arg_data(dict) : Neople Open API 를 통해 받은 data
        """
        arg_data = arg_data['rows'][0]

        self.server_id = arg_data.get('serverId')
        self.character_name = arg_data.get('characterName')
        self.character_id = arg_data.get('characterId')
        self.job_name = arg_data.get('jobName')
        self.job_grow_name = arg_data.get('jobGrowName')
        self.level = arg_data.get('level')