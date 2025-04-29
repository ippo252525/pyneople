from pyneople.api.data_path_map import CHARACTER_INFO_DATA_PATH_MAP
from pyneople.utils.api_utils.preprocess_utils import extract_values
from zoneinfo import ZoneInfo

def preprocess_character_info(data : dict, columns : list):
    data = extract_values(data, CHARACTER_INFO_DATA_PATH_MAP, columns)
    data['fetched_at'] = data['fetched_at'].replace(tzinfo=ZoneInfo("UTC"))
    