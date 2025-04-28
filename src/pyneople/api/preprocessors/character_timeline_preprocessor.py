from pyneople.utils.api_utils.preprocess_utils import extract_values
from pyneople.api.data_path_map import CHARACTER_TIMELINE_DATA_PATH_MAP
from zoneinfo import ZoneInfo

def preprocess_character_timeline(data : dict, columns : list):
    data = [
            {
                **timeline_data, 
                'fetched_at' : data['fetched_at'].replace(tzinfo=ZoneInfo("UTC")),
                'server_id' : data['serverId'],
                'character_id' : data['characterId']
            } 
            for timeline_data in data['timeline']['rows']
        ]
    return [extract_values(timeline_data, columns, CHARACTER_TIMELINE_DATA_PATH_MAP) for timeline_data in data] 

    