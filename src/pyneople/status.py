from .functions import get_request
from .METADATA import STATUS_NAME

class Status():
    def __init__(self, arg_api_key : str):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """
        self.__api_key = arg_api_key

    def get_data(self, arg_server_id : str, arg_character_id : str):
        """
        캐릭터의 모험단명부터 명성 등 정보를 반환한다
            Args:
                arg_server_id(str) :  서버 ID
                
                arg_character_id(str) : 캐릭터 ID
        """

        url = f'https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/status?apikey={self.__api_key}'
        return get_request(url)
        
    def parse_data(self, arg_data, variable_list = STATUS_NAME.keys()):
        """
        가져온 데이터를 정리해서 하위 attribute에 저장
            Args :
                arg_data(dict) : Neople Open API 를 통해 받은 data
        """
        
        if arg_data.get('buff'):
            for buff in arg_data['buff']:
                if buff.get('name') == '모험단 버프':
                    arg_data['adventure_level'] = buff.get('level')
                elif buff.get('name') == '무제한 길드능력치':
                    arg_data['unlimited_guild_abilities'] = True
                elif buff.get('name') == '기간제 길드능력치':
                    arg_data['limited_guild_abilities'] = True
                else:
                    pass

        if arg_data.get('status'):
            for item in arg_data['status']:
                arg_data[item['name']] = item['value']   

        for attribute_name in variable_list:
            setattr(self, attribute_name, arg_data.get(STATUS_NAME[attribute_name]))

        
        
        
        
