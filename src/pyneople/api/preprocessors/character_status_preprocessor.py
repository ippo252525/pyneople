from pyneople.api.data_path_map import CHARACTER_STATUS_DATA_PATH_MAP
from pyneople.utils.api_utils.preprocess_utils import extract_values
from zoneinfo import ZoneInfo

def preprocess_character_status(data : dict, columns : list):
    data = extract_values(data, columns, CHARACTER_STATUS_DATA_PATH_MAP)
    data['fetched_at'] = data['fetched_at'].replace(tzinfo=ZoneInfo("UTC"))
    return data