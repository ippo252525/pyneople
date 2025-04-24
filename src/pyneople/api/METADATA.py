# 초기 설정값
SETTINGS = {
    "request_time_out" : 5,
    "request_time_sleep" : 0.0015
}

# 응답에 캐릭터 기본 정보를 포함하는 endpoints
ENDPOINTS_WITH_CHARACTER_INFO = ['character_timeline']

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

# 서버 ID 가 total_id에 저장되는 값
SERVER_ID_2_TOTAL_ID = {
    'anton': 'a',
    'bakal': 'b',
    'cain': 'c',
    'casillas': 'k',
    'diregie': 'd',
    'hilder': 'h',
    'prey': 'p',
    'siroco': 's'
    }

TOTAL_ID_2_SERVER_ID = {v : k for k , v in SERVER_ID_2_TOTAL_ID.items()}

# 서버 ID 문자열 길이의 최대값
# SERVERLENGTH = max(list(map(lambda x : len(x), list(SERVER_NAME_2_ID.values()))))

JOB_ID_TO_NAME = {
    '41f1cdc2ff58bb5fdc287be0db2a8df3': '귀검사(남)',
    'a7a059ebe9e6054c0644b40ef316d6e9': '격투가(여)',
    'afdf3b989339de478e85b614d274d1ef': '거너(남)',
    '3909d0b188e9c95311399f776e331da5': '마법사(여)',
    'f6a4ad30555b99b499c07835f87ce522': '프리스트(남)',
    '944b9aab492c15a8474f96947ceeb9e4': '거너(여)',
    'ddc49e9ad1ff72a00b53c6cff5b1e920': '도적',
    'ca0f0e0e9e1d55b5f9955b03d9dd213c': '격투가(남)',
    'a5ccbaf5538981c6ef99b236c0a60b73': '마법사(남)',
    '17e417b31686389eebff6d754c3401ea': '다크나이트',
    'b522a95d819a5559b775deb9a490e49a': '크리에이터',
    '1645c45aabb008c98406b3a16447040d': '귀검사(여)',
    '0ee8fa5dc525c1a1f23fc6911e921e4a': '나이트',
    '3deb7be5f01953ac8b1ecaa1e25e0420': '마창사',
    '0c1b401bb09241570d364420b3ba3fd7': '프리스트(여)',
    '986c2b3d72ee0e4a0b7fcfbe786d4e02': '총검사',
    'b9cb48777665de22c006fabaf9a560b3': '아처'
    }

JOB_ID_LIST = list(JOB_ID_TO_NAME.keys())

JOB_INFO_LIST = [
    {'jobId': '41f1cdc2ff58bb5fdc287be0db2a8df3',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '귀검사(남)_웨펀마스터'},
    {'jobId': '41f1cdc2ff58bb5fdc287be0db2a8df3',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '귀검사(남)_소울브링어'},
    {'jobId': '41f1cdc2ff58bb5fdc287be0db2a8df3',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '귀검사(남)_버서커'},
    {'jobId': '41f1cdc2ff58bb5fdc287be0db2a8df3',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '귀검사(남)_아수라'},
    {'jobId': '41f1cdc2ff58bb5fdc287be0db2a8df3',
    'jobGrowId': 'a59ba19824dc3292b6075e29b3862ad3',
    'jobGrowName': '귀검사(남)_검귀'},
    {'jobId': 'a7a059ebe9e6054c0644b40ef316d6e9',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '격투가(여)_넨마스터'},
    {'jobId': 'a7a059ebe9e6054c0644b40ef316d6e9',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '격투가(여)_스트라이커'},
    {'jobId': 'a7a059ebe9e6054c0644b40ef316d6e9',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '격투가(여)_스트리트파이터'},
    {'jobId': 'a7a059ebe9e6054c0644b40ef316d6e9',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '격투가(여)_그래플러'},
    {'jobId': 'afdf3b989339de478e85b614d274d1ef',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '거너(남)_레인저'},
    {'jobId': 'afdf3b989339de478e85b614d274d1ef',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '거너(남)_런처'},
    {'jobId': 'afdf3b989339de478e85b614d274d1ef',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '거너(남)_메카닉'},
    {'jobId': 'afdf3b989339de478e85b614d274d1ef',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '거너(남)_스핏파이어'},
    {'jobId': 'afdf3b989339de478e85b614d274d1ef',
    'jobGrowId': 'a59ba19824dc3292b6075e29b3862ad3',
    'jobGrowName': '거너(남)_어썰트'},
    {'jobId': '3909d0b188e9c95311399f776e331da5',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '마법사(여)_엘레멘탈마스터'},
    {'jobId': '3909d0b188e9c95311399f776e331da5',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '마법사(여)_소환사'},
    {'jobId': '3909d0b188e9c95311399f776e331da5',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '마법사(여)_배틀메이지'},
    {'jobId': '3909d0b188e9c95311399f776e331da5',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '마법사(여)_마도학자'},
    {'jobId': '3909d0b188e9c95311399f776e331da5',
    'jobGrowId': 'a59ba19824dc3292b6075e29b3862ad3',
    'jobGrowName': '마법사(여)_인챈트리스'},
    {'jobId': 'f6a4ad30555b99b499c07835f87ce522',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '프리스트(남)_크루세이더'},
    {'jobId': 'f6a4ad30555b99b499c07835f87ce522',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '프리스트(남)_인파이터'},
    {'jobId': 'f6a4ad30555b99b499c07835f87ce522',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '프리스트(남)_퇴마사'},
    {'jobId': 'f6a4ad30555b99b499c07835f87ce522',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '프리스트(남)_어벤저'},
    {'jobId': '944b9aab492c15a8474f96947ceeb9e4',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '거너(여)_레인저'},
    {'jobId': '944b9aab492c15a8474f96947ceeb9e4',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '거너(여)_런처'},
    {'jobId': '944b9aab492c15a8474f96947ceeb9e4',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '거너(여)_메카닉'},
    {'jobId': '944b9aab492c15a8474f96947ceeb9e4',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '거너(여)_스핏파이어'},
    {'jobId': 'ddc49e9ad1ff72a00b53c6cff5b1e920',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '도적_로그'},
    {'jobId': 'ddc49e9ad1ff72a00b53c6cff5b1e920',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '도적_사령술사'},
    {'jobId': 'ddc49e9ad1ff72a00b53c6cff5b1e920',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '도적_쿠노이치'},
    {'jobId': 'ddc49e9ad1ff72a00b53c6cff5b1e920',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '도적_섀도우댄서'},
    {'jobId': 'ca0f0e0e9e1d55b5f9955b03d9dd213c',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '격투가(남)_넨마스터'},
    {'jobId': 'ca0f0e0e9e1d55b5f9955b03d9dd213c',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '격투가(남)_스트라이커'},
    {'jobId': 'ca0f0e0e9e1d55b5f9955b03d9dd213c',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '격투가(남)_스트리트파이터'},
    {'jobId': 'ca0f0e0e9e1d55b5f9955b03d9dd213c',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '격투가(남)_그래플러'},
    {'jobId': 'a5ccbaf5538981c6ef99b236c0a60b73',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '마법사(남)_엘레멘탈 바머'},
    {'jobId': 'a5ccbaf5538981c6ef99b236c0a60b73',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '마법사(남)_빙결사'},
    {'jobId': 'a5ccbaf5538981c6ef99b236c0a60b73',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '마법사(남)_블러드 메이지'},
    {'jobId': 'a5ccbaf5538981c6ef99b236c0a60b73',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '마법사(남)_스위프트 마스터'},
    {'jobId': 'a5ccbaf5538981c6ef99b236c0a60b73',
    'jobGrowId': 'a59ba19824dc3292b6075e29b3862ad3',
    'jobGrowName': '마법사(남)_디멘션워커'},
    {'jobId': '17e417b31686389eebff6d754c3401ea',
    'jobGrowId': '4fdee159d5aa8874a1459861ced676ec',
    'jobGrowName': '다크나이트_자각1'},
    {'jobId': 'b522a95d819a5559b775deb9a490e49a',
    'jobGrowId': '4fdee159d5aa8874a1459861ced676ec',
    'jobGrowName': '크리에이터_자각1'},
    {'jobId': '1645c45aabb008c98406b3a16447040d',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '귀검사(여)_소드마스터'},
    {'jobId': '1645c45aabb008c98406b3a16447040d',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '귀검사(여)_다크템플러'},
    {'jobId': '1645c45aabb008c98406b3a16447040d',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '귀검사(여)_데몬슬레이어'},
    {'jobId': '1645c45aabb008c98406b3a16447040d',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '귀검사(여)_베가본드'},
    {'jobId': '1645c45aabb008c98406b3a16447040d',
    'jobGrowId': 'a59ba19824dc3292b6075e29b3862ad3',
    'jobGrowName': '귀검사(여)_블레이드'},
    {'jobId': '0ee8fa5dc525c1a1f23fc6911e921e4a',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '나이트_엘븐나이트'},
    {'jobId': '0ee8fa5dc525c1a1f23fc6911e921e4a',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '나이트_카오스'},
    {'jobId': '0ee8fa5dc525c1a1f23fc6911e921e4a',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '나이트_팔라딘'},
    {'jobId': '0ee8fa5dc525c1a1f23fc6911e921e4a',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '나이트_드래곤나이트'},
    {'jobId': '3deb7be5f01953ac8b1ecaa1e25e0420',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '마창사_뱅가드'},
    {'jobId': '3deb7be5f01953ac8b1ecaa1e25e0420',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '마창사_듀얼리스트'},
    {'jobId': '3deb7be5f01953ac8b1ecaa1e25e0420',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '마창사_드래고니안 랜서'},
    {'jobId': '3deb7be5f01953ac8b1ecaa1e25e0420',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '마창사_다크 랜서'},
    {'jobId': '0c1b401bb09241570d364420b3ba3fd7',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '프리스트(여)_크루세이더'},
    {'jobId': '0c1b401bb09241570d364420b3ba3fd7',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '프리스트(여)_이단심판관'},
    {'jobId': '0c1b401bb09241570d364420b3ba3fd7',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '프리스트(여)_무녀'},
    {'jobId': '0c1b401bb09241570d364420b3ba3fd7',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '프리스트(여)_미스트리스'},
    {'jobId': '986c2b3d72ee0e4a0b7fcfbe786d4e02',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '총검사_히트맨'},
    {'jobId': '986c2b3d72ee0e4a0b7fcfbe786d4e02',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '총검사_요원'},
    {'jobId': '986c2b3d72ee0e4a0b7fcfbe786d4e02',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '총검사_트러블 슈터'},
    {'jobId': '986c2b3d72ee0e4a0b7fcfbe786d4e02',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '총검사_스페셜리스트'},
    {'jobId': 'b9cb48777665de22c006fabaf9a560b3',
    'jobGrowId': 'df3870efe8e8754011cd12fa03cd275f',
    'jobGrowName': '아처_뮤즈'},
    {'jobId': 'b9cb48777665de22c006fabaf9a560b3',
    'jobGrowId': '1ea78ae210f681a799feb4403a5c1e85',
    'jobGrowName': '아처_트래블러'},
    {'jobId': 'b9cb48777665de22c006fabaf9a560b3',
    'jobGrowId': 'a9a4ef4552d46e39cf6c874a51126410',
    'jobGrowName': '아처_헌터'},
    {'jobId': 'b9cb48777665de22c006fabaf9a560b3',
    'jobGrowId': '4a1459a4fa3c7f59b6da2e43382ed0b9',
    'jobGrowName': '아처_비질란테'}
    ]

PARAMS_FOR_SEED_CHARACTER_FAME = [
    {**item, 'serverId': server_id}
    for server_id in SERVER_ID_LIST
    for item in JOB_INFO_LIST
]

# 직업명
JOBCLASS = {
    "귀검사(남)" : ["웨펀마스터", "버서커", "소울브링어", "아수라", "검귀"],
    "격투가(남)" : ["넨마스터", "스트리트파이터", "그래플러", "스트라이커"],
    "거너(남)" : ["레인저", "메카닉", "런처", "스핏파이어", "어썰트"],
    "마법사(남)" : ["블러드 메이지", "엘레멘탈 바머", "빙결사", "디멘션워커", "스위프트 마스터"],
    "프리스트(남)" : ["크루세이더", "퇴마사", "인파이터", "어벤저"],
    "귀검사(여)" : ["소드마스터", "데몬슬레이어", "다크템플러", "베가본드", "블레이드"],
    "격투가(여)" : ["넨마스터", "스트리트파이터", "그래플러", "스트라이커"],
    "거너(여)" : ["레인저", "메카닉", "런처", "스핏파이어"],
    "마법사(여)" : ["엘레멘탈마스터", "마도학자", "소환사", "배틀메이지", "인챈트리스"],
    "프리스트(여)" : ["크루세이더", "이단심판관", "미스트리스", "무녀"],
    "도적" : ["로그", "쿠노이치", "섀도우댄서", "사령술사"],
    "나이트" : ["엘븐나이트", "카오스", "드래곤나이트", "팔라딘"],
    "마창사" : ["뱅가드", "듀얼리스트", "다크 랜서", "드래고니안 랜서"],
    "총검사" : ["요원", "트러블 슈터", "히트맨", "스페셜리스트"],
    "외전" : ["다크나이트", "크리에이터"],
    "아처" : ["뮤즈", "트래블러", "헌터", "비질란테"]
}

jobclass_list = [item for sublist in list(JOBCLASS.values()) for item in sublist]

# 1차 전직명 문자열 길이의 최대값
JOB_GROW_NAME_LENGTH = max(list(map(lambda x : len(x), jobclass_list)))

# 직업명 문자열 길이의 최대값
JOB_NAME_LENGTH = max(list(map(lambda x : len(x), list(JOBCLASS.keys()))))

del jobclass_list

# 착용가능 장비
EQUIPMENT_LIST = ['total_id', 'weapon', 'title', 'jacket', 'shoulder', 'pants', 'shoes', 'waist', 'amulet', 'wrist', 'ring', 'support', 'magic_ston', 'earring', 'set_item_info']

# 착용가능 아바타
AVATAR_LIST = ['total_id', 'headgear', 'hair', 'face', 'jacket', 'pants', 'shoes', 'breast', 'waist', 'skin', 'aurora', 'weapon']

# 플래티넘 엠블렘 착용 가능 부위
PLATINUM_AVATAR_LIST = ['jacket', 'pants']

# CharacterSearch 에서 선택 가능한 변수
CHARACTER_SEARCH_NAME = {
    'server_id': 'serverId',
    'character_id': 'characterId',
    'character_name': 'characterName',
    'level': 'level',
    'job_id' : 'jobId',
    'job_grow_id' : 'jobGrowId',
    'job_name': 'jobName',
    'job_grow_name': 'jobGrowName',
    'fame': 'fame',
}

# CharacterInformation 에서 선택 가능한 변수
CHARACTER_INFORMATION_NAME = {
    'total_id' : 'total_id',
    'character_id': 'characterId',
    'character_name': 'characterName',
    'level': 'level',
    'job_name': 'jobName',
    'job_grow_name': 'jobGrowName',
    'adventure_name': 'adventureName',
    'guild_id': 'guildId',
    'guild_name': 'guildName'
}

CHARACTER_INFO_KEYS = ['serverId', 'characterId', 'characterName', 'level', 'jobId', 'jobGrowId', 'jobName', 'jobGrowName', 'fame', 'adventureName', 'guildId', 'guildName', 'fetched_at']

# Status 에서 선택 가능한 변수
STATUS_NAME = {
    'total_id' : 'total_id',
    'character_id': 'characterId',
    'character_name': 'characterName',
    'level': 'level',
    'job_name': 'jobName',
    'job_grow_name': 'jobGrowName',
    'adventure_name': 'adventureName',
    'guild_id': 'guildId',
    'guild_name': 'guildName',    
    'hp': 'HP',
    'mp': 'MP',
    'physical_defense_rate': '물리 방어율',
    'magical_defense_rate': '마법 방어율',
    'strength': '힘',
    'intelligence': '지능',
    'vitality': '체력',
    'spirit': '정신력',
    'physical_attack': '물리 공격',
    'magical_attack': '마법 공격',
    'physical_critical_chance': '물리 크리티컬',
    'magical_critical_chance': '마법 크리티컬',
    'independent_attack': '독립 공격',
    'attack_speed': '공격 속도',
    'casting_speed': '캐스팅 속도',
    'movement_speed': '이동 속도',
    'fame': '모험가 명성',
    'hit_rate': '적중률',
    'evasion_rate': '회피율',
    'hp_recovery': 'HP 회복량',
    'mp_recovery': 'MP 회복량',
    'stiffness': '경직도',
    'hit_recovery': '히트리커버리',
    'fire_element_enhancement': '화속성 강화',
    'fire_element_resistance': '화속성 저항',
    'water_element_enhancement': '수속성 강화',
    'water_element_resistance': '수속성 저항',
    'light_element_enhancement': '명속성 강화',
    'light_element_resistance': '명속성 저항',
    'dark_element_enhancement': '암속성 강화',
    'dark_element_resistance': '암속성 저항',
    'physical_defense': '물리 방어',
    'magical_defense': '마법 방어',
    'attack_power_increase': '공격력 증가',
    'buff_power': '버프력',
    'attack_power_amplification': '공격력 증폭',
    'buff_power_amplification': '버프력 증폭',
    'final_damage_increase': '최종 데미지 증가',
    'cooldown_reduction': '쿨타임 감소',
    'cooldown_recovery_rate': '쿨타임 회복속도',
    'final_cooldown_reduction_rate': '최종 쿨타임 감소율',
    'damage_increase': '데미지 증가',
    'critical_damage_increase': '크리티컬 데미지 증가',
    'additional_damage_increase': '추가 데미지 증가',
    'all_attack_power_increase': '모든 공격력 증가',
    'physical_attack_power_increase': '물리 공격력 증가',
    'magical_attack_power_increase': '마법 공격력 증가',
    'independent_attack_power_increase': '독립 공격력 증가',
    'strength_increase': '힘 증가',
    'intelligence_increase': '지능 증가',
    'damage_over_time': '지속피해',
    'physical_damage_reduction': '물리 피해 감소',
    'magical_damage_reduction': '마법 피해 감소',
    'bleed_damage_conversion': '출혈 피해 전환',
    'poison_damage_conversion': '중독 피해 전환',
    'burn_damage_conversion': '화상 피해 전환',
    'electrocution_damage_conversion': '감전 피해 전환',
    'bleed_resistance': '출혈 내성',
    'poison_resistance': '중독 내성',
    'burn_resistance': '화상 내성',
    'electrocution_resistance': '감전 내성',
    'freeze_resistance': '빙결 내성',
    'slow_resistance': '둔화 내성',
    'stun_resistance': '기절 내성',
    'curse_resistance': '저주 내성',
    'darkness_resistance': '암흑 내성',
    'petrification_resistance': '석화 내성',
    'sleep_resistance': '수면 내성',
    'confusion_resistance': '혼란 내성',
    'restraint_resistance': '구속 내성',
    'fire_element_damage': '화속성 피해',
    'water_element_damage': '수속성 피해',
    'light_element_damage': '명속성 피해',
    'dark_element_damage': '암속성 피해',
    'bleed_damage': '출혈 피해',
    'poison_damage': '중독 피해',
    'burn_damage': '화상 피해',
    'electrocution_damage': '감전 피해'
 }

GROWINFO_NAME = {
        "level" : "level",
        "exp_rate" : "expRate",
        "option" : "options"
}


BASE_EQUIPMENT_NAME = {
    'item_name' :'itemName',
    'item_available_level' :'itemAvailableLevel',
    'item_rarity' :'itemRarity',
    'reinforce' :'reinforce',
    'amplification_name' :'amplificationName',
    'refine' :'refine', 
    'item_grade_name' :'itemGradeName',
    'enchant' : 'enchant'
}

EQUIPMENT_NAME = {
    'upgrade_info' : 'upgrade_info',
    'mist_gear' :  'mist_gear',
    'grow_info' : 'grow_info'
}

WEAPON_NAME = {
    'bakal_info' : 'fusionOption',
    'asrahan_info':'asrahanOption'
}

AVATAR_NAME = {
        'item_name' : "itemName",
        'item_rarity' : "itemRarity",
        'option_ability' : "optionAbility",
        'emblems' : 'emblems'
}

PLATINUM_AVATAR_NAME = {
        'item_name' : "itemName",
        'item_rarity' : "itemRarity",
        'option_ability' : "optionAbility",
        'emblems' : 'emblems',
        'platinum_emblem' : 'emblems'
}

SEED_TYPES_REQUIRING_PSQL_POOL = ['character_timeline']