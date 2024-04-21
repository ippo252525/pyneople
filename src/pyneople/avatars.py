from .functions import get_request
from .METADATA import AVATAR_LIST, PLATINUM_AVATAR_LIST

class Avatar():
    def __init__(self):
        self.item_name = None
        self.item_rarity = None
        self.option_ability = None
        self.emblem_1 = None
        self.emblem_2 = None
        
    def get_avatar_data(self, arg_avatar_dict):
        self.item_name = arg_avatar_dict.get("itemName")
        self.item_rarity = arg_avatar_dict.get("itemRarity")
        self.option_ability = arg_avatar_dict.get("optionAbility")
        for i, emblem in enumerate(arg_avatar_dict.get('emblems', dict())):
            setattr(self, f'emblem_{i+1}', emblem.get('itemName'))

class PlatinumAvatar(Avatar):
    def __init__(self):
        super().__init__()
        self.emblem_3 = None
    def get_avatar_data(self, arg_avatar_dict):
        super().get_avatar_data(arg_avatar_dict)

class Avatars():
    def __init__(self, arg_api_key):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """        
        self.__api_key = arg_api_key
        # for avatar in AVATAR_LIST:
        #     exec(f"self.{avatar} = Avatar()")  

    def get_data(self, arg_server_id, arg_character_id):    
        url = f'https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/equip/avatar?apikey={self.__api_key}'
        return get_request(url)

    def parse_data(self, arg_data):
        if arg_data.get('avatar'):
            arg_data = arg_data['avatar'] 
            for avatar in arg_data:
                if avatar["slotId"].lower() in PLATINUM_AVATAR_LIST:
                    avatar_data = PlatinumAvatar()    
                else:
                    avatar_data = Avatar()
                avatar_data.get_avatar_data(avatar)
                setattr(self, f'{avatar["slotId"].lower()}', avatar_data)

                #exec(f"self.{avatar}.get_avatar_data(one_slot(data, '{avatar}'.upper()))")
            # except:
            #     pass    
