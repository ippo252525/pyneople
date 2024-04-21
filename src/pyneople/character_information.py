from .functions import get_request
from .METADATA import CHARACTER_INFORMATION_NAME

class CharacterInformation():
    
    def __init__(self, arg_api_key : str):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """        
        self.__api_key = arg_api_key
    
    def get_data(self, arg_server_id : str, arg_character_id : str):
        """
        영문 서버명과 캐릭터 ID 를 검색하면 기본 정보를 반환
            Args : 
                arg_server_name(str) : 서버 이름  ex) diregie
                
                arg_character_name(str) : 캐릭터 ID ex) 80d9189c86147ab9a7b8c1481be85d95
        """    
        url = f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}?apikey={self.__api_key}"

        return get_request(url)        
    
    def parse_data(self, arg_data, variable_list = CHARACTER_INFORMATION_NAME.keys()):
        """
        가져온 데이터를 정리해서 하위 attribute에 저장
            Args :
                arg_data(dict) : Neople Open API 를 통해 받은 data
        """
        for attribute_name in variable_list:
            setattr(self, attribute_name, arg_data.get(CHARACTER_INFORMATION_NAME[attribute_name]))
