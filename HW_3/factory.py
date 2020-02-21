from CS6200_S20_SHARED.url_cleaner import UrlCleaner
from HW_3.filter import UrlFilteringService
from HW_3.frontier import FrontierManager
from utils.singleton import SingletonMeta


class UrlCleanerSingleton(UrlCleaner, metaclass=SingletonMeta):
    pass


class Factory:

    @classmethod
    def create_url_cleaner(cls) -> UrlCleaner:
        return UrlCleanerSingleton()

    @classmethod
    def create_frontier_manager(cls) -> FrontierManager:
        return FrontierManager([])

    @classmethod
    def create_url_filtering_service(cls) -> UrlFilteringService:
        return UrlFilteringService()
