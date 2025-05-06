import logging

logger = logging.getLogger(__name__)

def _get_nested_value(data, path):
    current = data
    first_key = path[0]
    current = current[first_key]  

    for step in path[1:]:
        if current is None:
            return current

        if isinstance(step, tuple):
            key, value = step
            if not isinstance(current, list):
                logger.info(f"Expected list at {step}, got {type(current)}")
                # return None
                raise TypeError(f"Expected list at {step}, got {type(current)}")
            for item in current:
                if isinstance(item, dict) and item.get(key) == value:
                    current = item
                    break
            else:
                # raise ValueError(f"No matching item for {step}")
                # logger.info(f"No matching item for {step}")
                return None
        elif isinstance(step, int):

            if isinstance(current, list):
                if 0 <= step < len(current):
                    current = current[step]                
                else:
                    logger.debug('Out of index')
                    return None
        else:
            if not isinstance(current, dict):
                # logger.info(f"Expected dict at {step}, got {type(current)}")
                raise TypeError(f"Expected dict at {step}, got {type(current)}")
            current = current.get(step)  
    return current

def extract_values(data, columns, data_map):
    return {name: _get_nested_value(data, data_map[name]) for name in columns if data_map.get(name)}

def get_data(data, path):
    current = data
    
    for step in path:
        
        if isinstance(step, str):
            if not isinstance(current, dict):
                raise TypeError(f"Expected dict for key '{step}', but got {type(current).__name__}")
            current = current.get(step)
        
        elif isinstance(step, tuple):
            key, value = step
            if not isinstance(current, list):
                raise TypeError(f"Expected list to match ({key}, {value}), but got {type(current).__name__}")
            for item in current:
                if isinstance(item, dict) and item.get(key) == value:
                    current = item
                    break
            else:
                raise ValueError(f"No matching item with {key}={value} in list")
        
        elif isinstance(step, int):
            if not isinstance(current, list):
                raise TypeError(f"Expected list for index {step}, but got {type(current).__name__}")
            try:
                current = current[step]
            except IndexError:
                raise IndexError(f"Index {step} out of range")
        
        else:
            raise TypeError(f"Invalid path step: {step} (must be str, tuple, or int)")
    
    return current

