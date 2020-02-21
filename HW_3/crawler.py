import logging
import threading
import time
from datetime import datetime
from functools import lru_cache
from typing import Tuple, Optional
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests

from CS6200_S20_SHARED.url_cleaner import UrlDetail, UrlCleaner
from constants.constants import Constants
from utils.singleton import SingletonMeta
from utils.utils import Utils


class CrawlingRateLimitingService(metaclass=SingletonMeta):

    def __init__(self) -> None:
        self.domains_being_crawled = {}
        self.lock = threading.Lock()

    @classmethod
    def _get_crawl_delay(cls, rp: RobotFileParser) -> int:
        crawl_delay = rp.crawl_delay("*")
        if crawl_delay:
            try:
                return int(crawl_delay)
            except:
                logging.error("Error occurred while parsing crawl_delay: {}".format(crawl_delay), exc_info=True)
                pass

        return Constants.DEFAULT_CRAWL_DELAY

    def might_block(self, url_detail: UrlDetail, rp: RobotFileParser) -> None:
        domain = url_detail.domain
        with self.lock:
            last_crawling_time = self.domains_being_crawled.get(domain)
            if last_crawling_time:
                crawl_delay = self._get_crawl_delay(rp)
                secs_to_wait = crawl_delay - (datetime.now() - last_crawling_time).total_seconds()
                if secs_to_wait > 0.0001:
                    logging.info("Blocking {} for {} secs".format(domain, secs_to_wait))
                    time.sleep(secs_to_wait)

            self.domains_being_crawled[domain] = datetime.now()


class RobotsTxtService(metaclass=SingletonMeta):
    _ROBOTS_TXT_FILE_NAME = "robots.txt"

    @lru_cache(maxsize=Constants.ROBOTS_TXT_CACHE_SIZE)
    def get_robot_txt(self, host: str) -> RobotFileParser:
        robots_txt_url = urljoin(host, self._ROBOTS_TXT_FILE_NAME)
        logging.info("Fetching robots.txt: {}".format(robots_txt_url))
        rp = RobotFileParser()
        rp.set_url(robots_txt_url)
        rp.read()
        return rp


class CrawlerResponse:

    def __init__(self, url_detail, raw_html, headers) -> None:
        self.url_detail: UrlDetail = url_detail
        self.raw_html: str = raw_html
        self.headers: dict = headers


class Crawler:

    def __init__(self, robots_txt_service: RobotsTxtService, rate_limiter: CrawlingRateLimitingService) -> None:
        self.robots_txt_service = robots_txt_service
        self.rate_limiter = rate_limiter

    @classmethod
    def _is_html(cls, url_detail: UrlDetail) -> Tuple[bool, str]:
        head_response = requests.head(url_detail.canonical_url)
        head_response.raise_for_status()

        content_type = head_response.headers.get('content-type').strip()
        return content_type == 'text/html', content_type

    def crawl(self, url_detail: UrlDetail) -> Optional[CrawlerResponse]:
        rp = self.robots_txt_service.get_robot_txt(url_detail.host)
        if not rp.can_fetch("*", url_detail.canonical_url):
            logging.info("Crawling not allowed: {}".format(url_detail.canonical_url))
            return None

        self.rate_limiter.might_block(url_detail, rp)

        is_html, content_type = self._is_html(url_detail)
        if not is_html:
            logging.info("Dropping non-html({}) url: {}".format(content_type, url_detail.canonical_url))
            return None

        response = requests.get(url_detail.canonical_url)
        response.raise_for_status()
        
        return CrawlerResponse(url_detail, response.text, response.headers)


if __name__ == '__main__':
    Utils.configure_logging()
    a = UrlCleaner().get_canonical_url("https://docs.python.org/3/library/urllib.request.html")
    c = Crawler(RobotsTxtService(), CrawlingRateLimitingService())
    c.crawl(a)

    a = UrlCleaner().get_canonical_url(
        "https://user-media-prod-cdn.itsre-sumo.mozilla.net/uploads/products/2018-10-03-20-10-50-e35beb.png")
    c.crawl(a)
