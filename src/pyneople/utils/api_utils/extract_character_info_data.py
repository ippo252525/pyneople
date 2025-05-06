from pyneople.api.data_path_map import CHARACTER_INFO_DATA_PATH_MAP

def extract_character_info_data(data : dict):
    data = data['data']
    character_info_keys = [key[0] for key in CHARACTER_INFO_DATA_PATH_MAP.values()]
    return {key : data[key] for key in character_info_keys}
