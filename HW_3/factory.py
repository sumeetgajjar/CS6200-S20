from CS6200_S20_SHARED.url_cleaner import UrlCleaner
from utils.singleton import SingletonMeta


class UrlCleanerSingleton(UrlCleaner, metaclass=SingletonMeta):
    pass


class Factory:

    @classmethod
    def create_url_cleaner(cls) -> UrlCleaner:
        return UrlCleanerSingleton()
