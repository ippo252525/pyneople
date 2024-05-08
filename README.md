# pyneople
Neople Open API wrapper for data analyst

## Documents
[pyneople](https://pyneople.readthedocs.io/ko/latest/index.html).

## Getting Started

### Installation
```bash
pip install pyneople
```

## Simple Usage
```python
from pyneople.character import CharacterSearch
api_key = "Neople Open API 에서 발급받은 API key"
character_search = CharacterSearch(api_key)
data = character_search.get_data("서버이름", "캐릭터이름")
character_search.parse_data(data)

print(character_search.server_id) 
print(character_search.character_name)
```