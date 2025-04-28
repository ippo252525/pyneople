def _get_nested_value(data, path):
    current = data
    first_key = path[0]
    current = current[first_key]  

    for step in path[1:]:
        if isinstance(step, tuple):
            key, value = step
            if not isinstance(current, list):
                raise TypeError(f"Expected list at {step}, got {type(current)}")
            for item in current:
                if isinstance(item, dict) and item.get(key) == value:
                    current = item
                    break
            else:
                raise ValueError(f"No matching item for {step}")
        else:
            if not isinstance(current, dict):
                raise TypeError(f"Expected dict at {step}, got {type(current)}")
            current = current.get(step)  
    return current

def extract_values(data, columns, data_map):
    return {name: _get_nested_value(data, data_map[name]) for name in columns}


