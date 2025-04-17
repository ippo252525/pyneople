import re
import json
from typing import Dict, List, Any
from config.METADATA import CHARACTER_INFO_KEYS
from datetime import datetime
from zoneinfo import ZoneInfo

def extract_character_info(data : dict):
    data = data['data']
    return {key : data.get(key) for key in CHARACTER_INFO_KEYS}

def to_snake_case(s: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()

def convert_selected_keys_to_snake_case(d: Dict[str, Any], target_keys: List[str]) -> Dict[str, Any]:
    result = {}
    for k, v in d.items():
        if k in target_keys:
            result[to_snake_case(k)] = v
    return result

def prepro_chcaracter_fame(data : dict, columns : list = ['characterId', 'serverId', 'characterName', 'level', 'jobName', 'jobGrowName', 'fame']):
    fetched_at = data['fetched_at'].replace(tzinfo=ZoneInfo("UTC"))
    data = data['rows']
    base_info_dict = {'fetched_at' : fetched_at}
    data = [{**convert_selected_keys_to_snake_case(character_fame_dict, columns), **base_info_dict} for character_fame_dict in data]
    return data

def prepro_chcaracter_info(data : dict, columns = ['characterId', 'serverId', 'characterName', 'level', 'jobName', 'jobGrowName', 'fame', 'adventureName', 'guildName', 'fetched_at']):
    data = convert_selected_keys_to_snake_case(data, columns)
    data['fetched_at'] = data['fetched_at'].replace(tzinfo=ZoneInfo("UTC"))
    return data

def prepro_character_timeline(data : dict):
    base_info_dict = {
        'fetched_at' : data['fetched_at'].replace(tzinfo=ZoneInfo("UTC")),
        'character_id' : data['characterId'],
        'server_id' : data['serverId']
    }
    data = data['timeline']['rows']
    data = [{f'timeline_{k}': v for k, v in character_timeline.items() if k != 'name'} for character_timeline in data]
    for character_timeline in data:
        character_timeline['timeline_date'] = datetime.strptime(character_timeline['timeline_date'], '%Y-%m-%d %H:%M').replace(tzinfo=ZoneInfo('Asia/Seoul')).astimezone(ZoneInfo('UTC'))
        character_timeline['timeline_data'] = json.dumps(character_timeline['timeline_data'])
        
    data = [{**character_timeline, **base_info_dict} for character_timeline in data]
    return data