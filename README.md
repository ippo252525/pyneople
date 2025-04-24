# pyneople

**Neople Open API 기반 데이터 수집 및 저장 라이브러리**

pyneople은 Neople Open API를 사용하여 데이터를 수집하고,  
MongoDB 및 PostgreSQL에 저장할 수 있도록 지원하는 Python 라이브러리입니다.

---

## Installation

```bash
pip install pyneople
```

## Quick Example
```python
from pyneople.api_to_mongo import api_to_mongo

endpoints = ['character_time', 'character_fame']
api_to_mongo(endpoints)
```