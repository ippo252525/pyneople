from .functions import get_request, explain_enchant
from .METADATA import EQUIPMENT_LIST

class OptionInfo():
    def __init__(self):
        self.explain = None
    
    def get_option_info_data(self, arg_option_info_dict):
        try:
            self.explain = arg_option_info_dict['explain'].replace("'", "") # 얼음 땡 옵션 예외처리를 위한 replace
        except:
            pass    

class GrowInfo():
    def __init__(self):
        self.level = None       # 장비 성장 레벨
        self.exp_rate = None    # 장비 성장 경험치
        self.transfer = None    # 전송 받은 옵션
        self.option_1 = None    # 1옵션
        self.option_2 = None    # 2옵션
        self.option_3 = None    # 3옵션
        self.option_4 = None    # 4옵션
    def get_grow_info_data(self, arg_grow_info_dict):
        self.level = arg_grow_info_dict.get('level')
        self.exp_rate = arg_grow_info_dict.get('expRate', 0)
        if arg_grow_info_dict.get('options'):
            for i, option in enumerate(arg_grow_info_dict.get('options')):
                if option.get('transfer') == True:
                    setattr(self, 'transfer', i+1)
                else:
                    pass    
                setattr(self, f'option_{i+1}', option.get('explain')) 
            # for i, option in enumerate(arg_grow_info_dict.get('options')):
            #     getattr(self, f'option_{i+1}').get_option_info_data(option)
                
        # exec(f"self.option_{i+1}.get_option_info_data(arg_grow_info_dict['options'][{i}])")

class Equipment():
    def __init__(self):
        self.item_name = None
        self.item_available_level = None
        self.item_rarity = None
        self.reinforce = None
        self.item_grade_name = None
        self.enchant = None
        self.amplification_name = None
        self.refine = None
        self.upgrade_info = None
        self.mist_gear = None
        self.grow_info = GrowInfo()
        # self.pure_mist_gear = None
        # self.refined_mistgear = None
    
    def get_equipment_data(self, arg_equipment_dict):
        self.item_name = arg_equipment_dict.get('itemName') # 이름
        self.item_available_level = arg_equipment_dict.get('itemAvailableLevel') # 레벨 제한
        self.item_rarity = arg_equipment_dict.get('itemRarity') # 레어도
        self.reinforce = arg_equipment_dict.get('reinforce') # 강화수치             
        self.amplification_name = arg_equipment_dict.get('amplificationName') # 차원의 기운 여부 ex 차원의 힘, 차원의 지능, None
        self.refine = arg_equipment_dict.get('refine') # 제련   
        self.item_grade_name = arg_equipment_dict.get('itemGradeName') # 등급(최상~최하)
        self.enchant = explain_enchant(arg_equipment_dict.get('enchant')) # 마법부여
        # self.mistgear = arg_equipment_dict.get('mistGear')# 미스트기어
        # self.pure_mist_gear = arg_equipment_dict.get('pureMistGear') # 순수한 미스트 기어
        # self.refined_mistgear = arg_equipment_dict.get('refinedMistGear') # 정제된 미스트 기어

        # 미스트 기어 정보
        if arg_equipment_dict.get('mistGear'):
            self.mist_gear = 'mist_gear'
        elif arg_equipment_dict.get('pureMistGear'):
            self.mist_gear = 'pure_mist_gear'   
        elif arg_equipment_dict.get('refinedMistGear'):
            self.mist_gear = 'refined_mistgear'                   
        else :
            pass    


        if arg_equipment_dict.get("upgradeInfo"):
            self.upgrade_info = arg_equipment_dict.get("upgradeInfo").get('itemName') # 융합장비
        
        # 105제 성장 장비 정보
        if arg_equipment_dict.get("customOption"):
            self.grow_info.get_grow_info_data(arg_equipment_dict.get('customOption'))
        elif arg_equipment_dict.get("fixedOption"):
            self.grow_info.get_grow_info_data(arg_equipment_dict.get("fixedOption"))
        else :
            pass
        

        # 

class BakalInfo():
    def __init__(self):
        self.option_1 = None
        self.option_2 = None
        self.option_3 = None
    def get_bakal_info_data(self, arg_bakal_info_dict):        
        if arg_bakal_info_dict.get('options'):
            for i, option in enumerate(arg_bakal_info_dict.get('options')):
                setattr(self, f'option_{i+1}',option.get('explain'))
        # try:
        #     self.option_1 = arg_bakal_info_dict['options'][0]['explain']
        # except:
        #     pass

        # try:
        #     self.option_2 = arg_bakal_info_dict['options'][1]['explain']
        # except:
        #     pass

        # try:
        #     self.option_3 = arg_bakal_info_dict['options'][2]['explain']
        # except:
        #     pass                

class Weapon(Equipment):
    def __init__(self):
        super().__init__()
        self.bakal_info = BakalInfo()

    def get_equipment_data(self, arg_equipment_dict):
        super().get_equipment_data(arg_equipment_dict)
        self.bakal_info.get_bakal_info_data(arg_equipment_dict.get("bakalInfo", dict())) # 바칼 무기 융합


class Equipments():
    
    def __init__(self, arg_api_key : str):
        """
        클래스 생성 시 Neople Open API key를 입력받는다
            Args :
                arg_api_key(str) : Neople Open API key
        """
        self.__api_key = arg_api_key

    def get_data(self, arg_server_id : str, arg_character_id : str):
        url = f'https://api.neople.co.kr/df/servers/{arg_server_id}/characters/{arg_character_id}/equip/equipment?apikey={self.__api_key}'
        return get_request(url)

    def parse_data(self, arg_data):
        """
        가져온 데이터를 정리해서 하위 attribute에 저장
            Args :
                arg_data(dict) : Neople Open API 를 통해 받은 data
        """
        if arg_data.get('equipment'):
            for equipment in arg_data.get('equipment'):
                if equipment.get('slotId') != 'WEAPON':
                    equipment_data = Equipment()
                else:
                    equipment_data = Weapon()
                equipment_data.get_equipment_data(equipment)
                setattr(self, equipment['slotId'].lower(), equipment_data)
        if arg_data.get('setItemInfo'):
            set_item_info_list = []
            for set_item_info in arg_data.get('setItemInfo'):
                set_item_info_list.append(f"{set_item_info.get('setItemName')}_{set_item_info.get('activeSetNo')}")
            setattr(self, 'set_item_info', ", ".join(set_item_info_list))
        #         set_item_info
        # for equipment in EQUIPMENT_LIST:
        #     try:
        #         exec(f"self.{equipment}.get_equipment_data(one_slot(data, '{equipment}'.upper()))")
        #     except:
        #         pass                 

    # def parse_data(self, arg_data, variable_list = EQUIPMENT_NAME.keys()):
    #     """
    #     가져온 데이터를 정리해서 하위 attribute에 저장
    #         Args :
    #             arg_data(dict) : Neople Open API 를 통해 받은 data
    #     """
    #     self.character_id = arg_data.get('characterId')
    #     self.character_name = arg_data.get('characterName')
    #     self.level = arg_data.get('level')
    #     self.job_name = arg_data.get('jobName')
    #     self.job_grow_name = arg_data.get('jobGrowName')
    #     self.adventure_name = arg_data.get('adventureName')
    #     self.guild_id = arg_data.get('guildId')
    #     self.guild_name = arg_data.get('guildName')
        