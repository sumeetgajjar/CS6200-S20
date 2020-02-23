import logging
import random
from typing import Tuple, Optional

import requests
from requests import HTTPError

from CS6200_S20_SHARED.url_cleaner import UrlDetail, UrlCleaner
from HW_3.beans import CrawlerResponse
from HW_3.filter import CrawlingUtils
from constants.constants import Constants
from utils.utils import Utils


class UserAgent:
    with open(Utils.get_user_agent_file_path(), 'r') as file:
        _USER_AGENTS = file.readlines()

    @classmethod
    def get_random_user_agent(cls) -> str:
        return random.choice(cls._USER_AGENTS)


class Crawler:

    def __init__(self, url_cleaner: UrlCleaner) -> None:
        self.url_cleaner = url_cleaner

    @classmethod
    def _get_request_headers(cls):
        request_headers = requests.utils.default_headers()
        request_headers.update({'User-Agent': UserAgent.get_random_user_agent()})
        return request_headers

    def _is_html(self, url_detail: UrlDetail) -> Tuple[bool, str, UrlDetail]:

        head_response = requests.head(url_detail.canonical_url,
                                      timeout=Constants.CRAWLER_TIMEOUT,
                                      allow_redirects=True,
                                      headers=self._get_request_headers())
        head_response.raise_for_status()

        content_type = head_response.headers.get('content-type').strip()
        new_url_detail = self.url_cleaner.get_canonical_url(head_response.url)

        return 'text/html' in content_type, content_type, new_url_detail

    def _crawl_helper(self, url_detail: UrlDetail):
        rp = Utils.get_robots_txt(url_detail.host)
        if not rp.can_fetch("*", url_detail.canonical_url):
            logging.info("Crawling not allowed: {}".format(url_detail.canonical_url))
            return None

        is_html, content_type, new_url_detail = self._is_html(url_detail)
        if not is_html:
            logging.info("Dropping non-html({}) url: {}".format(content_type, url_detail.canonical_url))
            return None

        crawler_response = CrawlerResponse(url_detail)
        if url_detail.canonical_url != new_url_detail.canonical_url:
            logging.info("Url redirection detected, changing url detail: {}->{}".format(url_detail.canonical_url,
                                                                                        new_url_detail.canonical_url))

            crawler_response.redirected = True
            crawler_response.redirected_url = new_url_detail

            url_detail = new_url_detail
            if CrawlingUtils.is_crawled(url_detail):
                return None

        response = requests.get(url_detail.canonical_url,
                                timeout=Constants.CRAWLER_TIMEOUT,
                                allow_redirects=True,
                                headers=self._get_request_headers())
        response.raise_for_status()

        crawler_response.headers = response.headers
        crawler_response.raw_html = response.text

        return crawler_response

    @classmethod
    def _add_url_to_crawled_list(cls, crawler_response: CrawlerResponse):
        url_details = [crawler_response.url_detail]
        if crawler_response.redirected:
            url_details.append(crawler_response.redirected_url)

        CrawlingUtils.add_urls_to_crawled_list(url_details)

    def crawl(self, url_detail: UrlDetail) -> Optional[CrawlerResponse]:
        try:
            logging.info("Crawling:{}".format(url_detail))
            crawler_response = self._crawl_helper(url_detail)
            if crawler_response:
                logging.info("Crawled:{}".format(url_detail))

            self._add_url_to_crawled_list(crawler_response)

            return crawler_response
        except HTTPError:
            logging.error("HTTPError while crawling: {}".format(url_detail.canonical_url), exc_info=True)
            self._add_url_to_crawled_list(CrawlerResponse(url_detail))
        except:
            logging.error("Error while crawling: {}".format(url_detail.canonical_url), exc_info=True)

        return None


if __name__ == '__main__':
    Utils.configure_logging()
    a = UrlCleaner().get_canonical_url("https://docs.sqlalchemy.org/en/13/core/connections.html")
    c = Crawler(UrlCleaner())
    cr = c.crawl(a)
    print(cr)
