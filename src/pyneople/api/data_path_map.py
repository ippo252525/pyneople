def standardize_data_map(data_map):
    data_map = {k: v if isinstance(v, tuple) else (v,) for k, v in data_map.items()}
    data_map.update(fetched_at = 'fetched_at')
    return data_map

CHARACTER_FAME_DATA_PATH_MAP = {
    'server_id': 'serverId',
    'character_id': 'characterId',
    'character_name': 'characterName',
    'level': 'level',
    'job_id': 'jobId',
    'job_grow_id': 'jobGrowId',
    'job_name': 'jobName',
    'job_grow_name': 'jobGrowName',
    'fame': 'fame'
}
CHARACTER_FAME_DATA_PATH_MAP = standardize_data_map(CHARACTER_FAME_DATA_PATH_MAP)


CHARACTER_INFO_DATA_PATH_MAP = {
    'server_id': 'serverId',
    'character_id': 'characterId',
    'character_name': 'characterName',
    'level': 'level',
    'job_id': 'jobId',
    'job_grow_id': 'jobGrowId',
    'job_name': 'jobName',
    'job_grow_name': 'jobGrowName',
    'fame': 'fame',
    'adventure_name': 'adventureName',
    'guild_id': 'guildId',
    'guild_name': 'guildName'
}
CHARACTER_INFO_DATA_PATH_MAP = standardize_data_map(CHARACTER_INFO_DATA_PATH_MAP)


CHARACTER_TIMELINE_DATA_PATH_MAP = {
    'timeline_code': 'code',
    'timeline_name': 'name',
    'timeline_date': 'date',
    'timeline_data': 'data'
}