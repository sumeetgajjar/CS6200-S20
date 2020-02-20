from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser


class UrlDistributionService:
    pass


class DomainCrawlingRateLimitingService:
    pass


class RobotsTxtService:
    _ROBOTS_TXT_FILE_NAME = "robots.txt"

    def __init__(self) -> None:
        self.robots_txt = {}

    def _fetch_robots_txt(self, host: str) -> RobotFileParser:
        rp = RobotFileParser()
        rp.set_url(urljoin(host, self._ROBOTS_TXT_FILE_NAME))
        rp.read()
        return rp

    def get_robot_txt(self, host: str):
        rp = self.robots_txt.get(host)
        if not rp:
            rp = self._fetch_robots_txt(host)
            self.robots_txt[host] = rp

        return rp


class Crawler:

    def crawl(self, url):
        pass
