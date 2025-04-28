import re
from functools import lru_cache

@lru_cache(maxsize=1000)
def to_snake_case(s: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()

def convert_selected_keys_to_snake_case(d: dict, target_keys: list) -> dict:
    result = {}
    for k, v in d.items():
        if k in target_keys:
            result[to_snake_case(k)] = v
    return result

def get_value(data: dict, path: tuple):
    current = data
    for step in path:
        if isinstance(step, str):
            current = current.get(step)
        elif isinstance(step, tuple):
            search_key, search_value = step
            found = None
            for item in current:
                if isinstance(item, dict) and item.get(search_key) == search_value:
                    found = item
                    break
            current = found
        else:
            raise ValueError(f"Invalid path step: {step}")
    return current

def extract_values(data : dict, data_path_map : dict, keys: tuple) -> dict:
    return {key: get_value(data, data_path_map[key]) for key in keys}
