from pyneople.metadata.metadata_generated import TABLE_COLUMNS_MAP
from pyneople.api.data_path_map import CHARACTER_INFO_DATA_PATH_MAP
from pyneople.api.registry.endpoint_class import CharacterInfo
from pyneople.utils.api_utils.preprocess_utils import extract_values

columns = TABLE_COLUMNS_MAP[CharacterInfo.staging_table_name]

def extract_character_info_data(data : dict):
    data = data['data']
    character_info_keys = [key[0] for key in CHARACTER_INFO_DATA_PATH_MAP.values()]
    return {key : data[key] for key in character_info_keys}
