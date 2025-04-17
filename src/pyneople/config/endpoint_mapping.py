from api.parser import prepro_chcaracter_fame, prepro_character_timeline, prepro_chcaracter_info

ENDPOINT_TO_STAGING_TABLE_NAME = {
    "character_fame" : "staging_characters",
    'character_info' : 'staging_characters', 
    'character_timeline' : 'staging_character_timelines'
}

ENDPOINT_TO_PREPROCESS = {
    'character_fame' : prepro_chcaracter_fame,
    'character_info' : prepro_chcaracter_info,
    'character_timeline' : prepro_character_timeline
}

TABLE_TO_CONFLICT_KEYS = {
    "characters" : ['server_id', 'character_id'],
    'character_timelines' : []
}