import csv
import json
import logging
import os
import sys
from datetime import datetime
from typing import List
from urllib.robotparser import RobotFileParser

from CS6200_S20_SHARED.url_cleaner import UrlCleaner, UrlDetail
from HW_3.beans import DomainRank, Outlink, FilteredResult
from HW_3.connection_factory import ConnectionFactory
from constants.constants import Constants
from utils.decorators import timing
from utils.singleton import SingletonMeta
from utils.utils import Utils


class DomainRanker:
    _DEFAULT_DOMAIN_RANK = sys.maxsize
    _RAW_DOMAIN_RANK_FILE_PATH = '/home/sumeet/PycharmProjects/CS6200-S20/CS6200_S20_SHARED/domain_rankings.csv'
    _PROCESSED_DOMAIN_RANK_FILE_NAME = 'processed_domain_ranking.json'
    _PROCESSED_DOMAIN_RANK_FILE_PATH = '{}/{}'.format(Utils.get_data_dir_abs_path(), _PROCESSED_DOMAIN_RANK_FILE_NAME)

    def __init__(self, override_old_data=False) -> None:
        self.domain_rankings = {}
        self._init_domain_rankings(override_old_data)

    @timing
    def _init_domain_rankings(self, override_old_data):
        if override_old_data or not os.path.isfile(self._PROCESSED_DOMAIN_RANK_FILE_PATH):
            logging.info("Processing Domain ranks")
            self._read_domain_ranks()

            logging.info("Writing processed domain ranks to json")
            with open(self._PROCESSED_DOMAIN_RANK_FILE_PATH, 'w') as file:
                json.dump(self.domain_rankings, file)

            logging.info("Domain ranks processed")
        else:
            logging.info("Processed Domain rank exists")
            logging.info("Reading the processed domain ranks from json")
            with open(self._PROCESSED_DOMAIN_RANK_FILE_PATH, 'r') as file:
                self.domain_rankings = json.load(file)

    @timing
    def _read_domain_ranks(self):
        with open(self._RAW_DOMAIN_RANK_FILE_PATH, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                domain = UrlCleaner.get_domain_from_url(row['Domain'])
                self.domain_rankings[domain] = [int(row['GlobalRank']), int(row['TldRank'])]

    def get_domain_rank(self, canonical_domain: str) -> DomainRank:
        """
        :param canonical_domain:
        :return: the domain rank
        """
        domain_rank_info = self.domain_rankings.get(canonical_domain)
        if domain_rank_info:
            return DomainRank(canonical_domain, domain_rank_info[0], domain_rank_info[1])
        else:
            return DomainRank(canonical_domain, self._DEFAULT_DOMAIN_RANK, self._DEFAULT_DOMAIN_RANK)


# todo check if singleton required
class UrlFilteringService:

    # TODO contact us and other common stuff
    # TODO ads

    def filter_outlinks(self, outlinks: List[Outlink]) -> FilteredResult:
        # todo do not remove already visited links
        return FilteredResult(outlinks, [])

    def filter_already_crawled_links(self, url_details: List[UrlDetail]) -> FilteredResult:
        filtered_result = FilteredResult([], [])
        for url_detail in url_details:
            if CrawlingUtils.is_crawled(url_detail):
                filtered_result.removed.append(url_detail)
            else:
                filtered_result.filtered.append(url_detail)

        return filtered_result


class CrawlingRateLimitingService(metaclass=SingletonMeta):

    def __init__(self) -> None:
        self.domains_being_crawled = {}

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

    def _is_rate_limited(self, url_detail: UrlDetail) -> bool:
        rate_limited = False
        domain = url_detail.domain
        last_crawling_time = self.domains_being_crawled.get(domain)
        if last_crawling_time:
            rp = Utils.get_robots_txt(url_detail.host)
            crawl_delay = self._get_crawl_delay(rp)
            secs_to_wait = crawl_delay - (datetime.now() - last_crawling_time).total_seconds()
            if secs_to_wait > 0.0001:
                rate_limited = True

        self.domains_being_crawled[domain] = datetime.now()
        return rate_limited

    def filter(self, url_details: List[UrlDetail]) -> FilteredResult:
        filtered_result = FilteredResult([], [])
        for url_detail in url_details:
            if self._is_rate_limited(url_detail):
                filtered_result.removed.append(url_detail)
            else:
                filtered_result.filtered.append(url_detail)

        return filtered_result


class CrawlingUtils:
    with ConnectionFactory.create_redis_connection() as conn:
        if not conn.exists(Constants.CRAWLED_URLS_BF):
            logging.info("Creating Bloomfilter")
            conn.bfCreate(Constants.CRAWLED_URLS_BF,
                          Constants.CRAWLED_URLS_BF_ERROR_RATE,
                          Constants.CRAWLED_URLS_BF_CAPACITY)
            logging.info("Bloomfilter created")
        else:
            logging.info("Bloomfilter already exists")

    @classmethod
    def add_url_to_crawled_list(cls, url_detail: UrlDetail):
        cls.add_urls_to_crawled_list([url_detail])

    @classmethod
    def add_urls_to_crawled_list(cls, url_details: List[UrlDetail]):
        with ConnectionFactory.create_redis_connection() as conn:
            conn.bfMAdd(Constants.CRAWLED_URLS_BF, *[url_detail.canonical_url for url_detail in url_details])

    @classmethod
    def is_crawled(cls, url_detail: UrlDetail) -> bool:
        with ConnectionFactory.create_redis_connection() as conn:
            return conn.bfExists(Constants.CRAWLED_URLS_BF, url_detail.canonical_url) == 1
