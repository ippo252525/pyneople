from abc import ABC, abstractmethod

from pyneople.api.registry.endpoint_registry import register_endpoint

from pyneople.api.preprocessors.character_info_preprocessor import preprocess_character_info
from pyneople.api.preprocessors.character_fame_preprocessor import preprocess_character_fame
from pyneople.api.preprocessors.character_timeline_preprocessor import preprocess_character_timeline

from pyneople.api.seeders.character_base_seeder import CharacterBaseSeeder
from pyneople.api.seeders.character_fame_seeder import CharacterFameSeeder
from pyneople.api.seeders.character_timeline_seeder import CharacterTimelineSeeder

from pyneople.api.next_builders.build_next_character_fmae_api_request import build_next_character_fame_api_request
from pyneople.api.next_builders.build_next_character_timeline_api_request import build_next_character_timeline_api_request

class BaseEndpoint(ABC):
    """모든 엔드포인트 클래스가 상속받는 기본 베이스 클래스."""
    staging_table_name: str
    accepts_sql: bool = True
    seeder: type = CharacterBaseSeeder
    has_next: bool = False
    has_character_info_data = True
    
    @staticmethod
    @abstractmethod
    def preprocess(data, columns):
        pass
    
    @staticmethod
    def build_next_api_request(data : dict, api_request : dict):
        """기본 next api request 생성 메서드. (필요한 엔드포인트에서만 오버라이드)"""
        return None


@register_endpoint('character_fame')
class CharacterFame(BaseEndpoint):
    staging_table_name = 'staging_characters'
    accepts_sql = False
    seeder = CharacterFameSeeder
    has_next = True
    has_character_info_data = False

    @staticmethod
    def preprocess(data, columns):
        return preprocess_character_fame(data, columns)

    @staticmethod
    def build_next_api_request(data: dict, api_request : dict):
        return build_next_character_fame_api_request(data, api_request)


@register_endpoint('character_info')
class CharacterInfo(BaseEndpoint):
    staging_table_name = 'staging_characters'
    has_character_info_data = False
    
    @staticmethod
    def preprocess(data, columns):
        return preprocess_character_info(data, columns)

    
@register_endpoint('character_timeline')
class CharacterTimeline(BaseEndpoint):
    staging_table_name = 'staging_character_timelines'
    seeder = CharacterTimelineSeeder

    @staticmethod
    def preprocess(data, columns):
        return preprocess_character_timeline(data, columns)

    @staticmethod
    def build_next_api_request(data: dict, api_request: dict):
        return build_next_character_timeline_api_request(data, api_request)
