from functools import lru_cache
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from constants.constants import Constants


class UrlDistributionService:
    pass


class CrawlingRateLimitingService:

    def __init__(self) -> None:
        pass

    def might_block(self, url_detail: UrlDetail) -> None:
        pass


class RobotsTxtService:
    _ROBOTS_TXT_FILE_NAME = "robots.txt"

    @classmethod
    @lru_cache(maxsize=Constants.ROBOTS_TXT_CACHE_SIZE)
    def get_robot_txt(cls, host: str) -> RobotFileParser:
        rp = RobotFileParser()
        rp.set_url(urljoin(host, cls._ROBOTS_TXT_FILE_NAME))
        rp.read()
        return rp


class Crawler:

    def crawl(self, url_detail: UrlDetail):
        pass
