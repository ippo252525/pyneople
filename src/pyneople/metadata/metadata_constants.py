# 서버 ID 가 key NAME 이 value
SERVER_ID_TO_NAME = {
    'anton': '안톤',
    'bakal': '바칼',
    'cain': '카인',
    'casillas': '카시야스',
    'diregie': '디레지에',
    'hilder': '힐더',
    'prey': '프레이',
    'siroco': '시로코'
    }

SERVER_NAME_TO_ID = {v : k for k, v in SERVER_ID_TO_NAME.items()}

SERVER_ID_LIST = list(SERVER_ID_TO_NAME.keys())