import time
from datetime import datetime
from functools import lru_cache
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from constants.constants import Constants


class UrlDistributionService:
    pass


class CrawlingRateLimitingService:

    def __init__(self) -> None:
        self.domains_being_crawled = {}

    @classmethod
    def _get_crawl_delay(cls, rp: RobotFileParser):
        try:
            return int(rp.crawl_delay("*"))
        except:
            return Constants.DEFAULT_CRAWL_DELAY

    def might_block(self, url_detail: UrlDetail, rp: RobotFileParser) -> None:
        domain = url_detail.domain
        last_crawling_time = self.domains_being_crawled.get(domain)
        if last_crawling_time:
            crawl_delay = self._get_crawl_delay(rp)
            secs_to_wait = crawl_delay - (datetime.now() - last_crawling_time).total_seconds()
            if secs_to_wait > 0.0001:
                time.sleep(secs_to_wait)

        self.domains_being_crawled[domain] = datetime.now()


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
