from .functions import get_request, explain_enchant
from .METADATA import EQUIPMENT_LIST, AVATAR_LIST, PLATINUM_AVATAR_LIST

class BuffAvatar():
    def __init__(self):
        self.item_name = None
    def get_buff_avatar_data(self, arg_avatar_dict):
        try: 
            self.item_name = arg_avatar_dict['itemName']
        except:
            pass
class BuffPlatimun(BuffAvatar):
    def __init__(self):
        super().__init__()    
        self.option = None
        self.platinum = None
    
    def get_buff_avatar_data(self, arg_avatar_dict):
        super().get_buff_avatar_data(arg_avatar_dict)
        self.option = arg_avatar_dict.get('optionAbility')
        if arg_avatar_dict.get('emblems'):
            for emblems in arg_avatar_dict.get('emblems'):
                if emblems.get('slotColor') == '플래티넘':
                    self.platinum = emblems.get('itemName')


class Buff():
    def __init__(self, arg_api_key):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """        
        self.__api_key = arg_api_key
        # self.buff_level = None
        # self.buff_desc = None
        # for equipment in EQUIPMENT_LIST:
        #     exec(f"self.buff_equipment_{equipment} = None")
        # for avatar in list(set(AVATAR_LIST) - set(PLATINUM_AVATAR_LIST)):
        #     exec(f"self.buff_avatar_{avatar} = BuffAvatar()")
        # for avatar in PLATINUM_AVATAR_LIST:
        #     exec(f"self.buff_avatar_{avatar} = BuffPlatimun()")
        # self.buff_creature = None           
    
    def get_data(self, arg_server_id : str, arg_character_id : str):
        buff_equipment_data = get_request(f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/skill/buff/equip/equipment?apikey={self.__api_key}")
        buff_avatar_data = get_request(f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/skill/buff/equip/avatar?apikey={self.__api_key}")
        buff_creature_data = get_request(f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/skill/buff/equip/creature?apikey={self.__api_key}")
        return buff_equipment_data, buff_avatar_data, buff_creature_data

    def parse_data(self, arg_buff_equipment_data, arg_buff_avatar_data, arg_buff_creature_data):

        if arg_buff_equipment_data.get("skill", dict()).get('buff'):
            arg_buff_equipment_data = arg_buff_equipment_data.get("skill", dict()).get('buff')
            # 버프 강화 장비
            if arg_buff_equipment_data.get("equipment"):
                for buff_equipment in arg_buff_equipment_data.get("equipment"):
                    setattr(self, f'equipment_{buff_equipment.get("slotId").lower()}', buff_equipment.get('itemName'))
                    if buff_equipment.get("slotId") == 'TITLE':
                        setattr(self, f'equipment_{buff_equipment.get("slotId").lower()}_enchant', explain_enchant(buff_equipment.get('enchant')))
                    else:
                        pass
            else:
                pass
            # 버프 강화 정보
            if arg_buff_equipment_data.get("skillInfo"):
                for index, value in enumerate(arg_buff_equipment_data['skillInfo']['option']['values']):
                    arg_buff_equipment_data['skillInfo']['option']['desc'] = arg_buff_equipment_data['skillInfo']['option']['desc'].replace("{" + f"value{index + 1}" + "}", value)
                self.buff_level = arg_buff_equipment_data['skillInfo']['option']['level']
                self.buff_desc = arg_buff_equipment_data['skillInfo']['option']['desc']              
            
    
        # 버프 강화 아바타
        if arg_buff_avatar_data.get("skill", dict()).get('buff'):
            arg_buff_avatar_data = arg_buff_avatar_data.get("skill", dict()).get('buff')
            if arg_buff_avatar_data.get("avatar"):
                for buff_avatar in arg_buff_avatar_data.get("avatar"):
                    if buff_avatar.get("slotId").lower() in PLATINUM_AVATAR_LIST:
                        setattr(self, f'avatar_{buff_avatar.get("slotId").lower()}', BuffPlatimun())
                        getattr(self, f'avatar_{buff_avatar.get("slotId").lower()}').get_buff_avatar_data(buff_avatar)
                    else:
                        setattr(self, f'avatar_{buff_avatar.get("slotId").lower()}', BuffAvatar())
                        getattr(self, f'avatar_{buff_avatar.get("slotId").lower()}').get_buff_avatar_data(buff_avatar)

        # 버프 강화 크리쳐
        if arg_buff_creature_data.get("skill", dict()).get('buff'):
            arg_buff_creature_data = arg_buff_creature_data.get("skill", dict()).get('buff')
            if arg_buff_creature_data.get('creature'):
                for creature in arg_buff_creature_data.get('creature'):
                    setattr(self, 'creature', creature.get('itemName'))


                


            

        
        # data = data['skill']['buff']

        # if data:
        #     try:
        #         for index, value in enumerate(data['skillInfo']['option']['values']):
        #             data['skillInfo']['option']['desc'] = data['skillInfo']['option']['desc'].replace("{" + f"value{index + 1}" + "}", value)
        #         self.buff_level = data['skillInfo']['option']['level']
        #         self.buff_desc = data['skillInfo']['option']['desc']
        #     except:
        #         pass    
        # if data:
        #     data = data['equipment']
        # if data:
        #     for equipment in EQUIPMENT_LIST:
        #         try:    
        #             exec(f"self.buff_equipment_{equipment} = one_slot(data, '{equipment.upper()}')['itemName']")
        #         except:
        #             pass    
        
        # data = get_request(f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/skill/buff/equip/avatar?apikey={self.__api_key}")
        # data = data['skill']['buff']
        # if data:
        #     data = data['avatar']
        # if data:
        #     for avatar in AVATAR_LIST:            
        #         try:
        #             exec(f"self.buff_avatar_{avatar}.get_buff_avatar_data(one_slot(data, '{avatar.upper()}'))") 
        #         except:
        #             pass    
        # data = get_request(f"https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/skill/buff/equip/creature?apikey={self.__api_key}")
        # data = data['skill']['buff']
        # if data:
        #     try:
        #         self.buff_creature = data['creature'][0]['itemName']
        #     except:
        #         pass    
