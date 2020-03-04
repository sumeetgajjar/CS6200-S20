import logging
import re
from collections import defaultdict
from typing import List, Dict, Tuple

from CS6200_S20_SHARED.url_cleaner import UrlDetail, UrlCleaner
from HW_3.beans import Outlink
from HW_3.connection_factory import ConnectionFactory
from HW_3.filter import DomainRanker
from constants.constants import Constants
from utils.singleton import SingletonMeta
from utils.utils import Utils


class FrontierManager(metaclass=SingletonMeta):
    _URL_SPLIT_REGEX = re.compile("\\W|_")

    def __init__(self, url_cleaner: UrlCleaner, domain_ranker: DomainRanker) -> None:
        self.url_cleaner = url_cleaner
        self.domain_ranker = domain_ranker

    @classmethod
    def _update_inlinks_count(cls, outlinks: List[Outlink]):
        with ConnectionFactory.create_redis_connection() as redis:
            with redis.pipeline() as pipe:
                for outlink in outlinks:
                    pipe.hincrby(Constants.DOMAIN_INLINKS_COUNT_KEY, outlink.url_detail.domain)
                pipe.execute()

                for outlink in outlinks:
                    pipe.hincrby(Constants.URL_INLINKS_COUNT_KEY, outlink.url_detail.canonical_url)
                pipe.execute()

    @classmethod
    def _get_domain_inlinks_count(cls, outlinks: List[Outlink], redis) -> dict:
        result = redis.hmget(Constants.DOMAIN_INLINKS_COUNT_KEY,
                             [outlink.url_detail.domain for outlink in outlinks])

        domain_inlinks = defaultdict(float)
        for i in range(len(outlinks)):
            domain_inlinks[outlinks[i].url_detail.domain] += result[i]

        return domain_inlinks

    @classmethod
    def _get_url_inlinks_count(cls, outlinks: List[Outlink], redis) -> dict:
        result = redis.hmget(Constants.URL_INLINKS_COUNT_KEY,
                             [outlink.url_detail.canonical_url for outlink in outlinks])

        url_inlinks = defaultdict(float)
        for i in range(len(outlinks)):
            url_inlinks[outlinks[i].url_detail.domain] += result[i]

        return url_inlinks

    def _get_domain_ranks(self, outlinks: List[Outlink]) -> dict:
        domain_ranks = {}
        for outlink in outlinks:
            domain_rank = self.domain_ranker.get_domain_rank(outlink.url_detail.domain)
            if domain_rank:
                domain_ranks[outlink.url_detail.domain] = domain_rank

        return domain_ranks

    @classmethod
    def _compute_jacard_similarity_anchor_text(cls, outlink: Outlink) -> float:
        anchor_text_set = set(outlink.anchor_text.split(" "))
        intersection_set = anchor_text_set.intersection(Constants.TOPIC_KEYWORDS)
        union_set_len = len(anchor_text_set) + len(Constants.TOPIC_KEYWORDS) + len(intersection_set)
        return len(intersection_set) / union_set_len

    @classmethod
    def _compute_jacard_similarity_anchor_link(cls, outlink: Outlink) -> float:
        anchor_link_set = set(cls._URL_SPLIT_REGEX.split(outlink.url_detail.canonical_url))
        intersection_set = anchor_link_set.intersection(Constants.TOPIC_KEYWORDS)
        union_set_len = len(anchor_link_set) + len(Constants.TOPIC_KEYWORDS) + len(intersection_set)
        return len(intersection_set) / union_set_len

    @classmethod
    def _get_relevance_from_redis(cls, outlinks: List[Outlink]) -> Tuple[Dict[str, float], Dict[str, float]]:
        url_relevance = defaultdict(float)
        domain_relevance = defaultdict(float)
        with ConnectionFactory.create_redis_connection() as redis:
            domain_relevance_result = redis.hmget(Constants.DOMAIN_RELEVANCE_KEY,
                                                  [outlink.url_detail.domain for outlink in outlinks])
            url_relevance_result = redis.hmget(Constants.URL_RELEVANCE_KEY,
                                               [outlink.url_detail.canonical_url for outlink in outlinks])
        for i in range(len(outlinks)):
            if domain_relevance_result[i]:
                domain_relevance[outlinks[i].url_detail.domain] += domain_relevance_result[i]
            if url_relevance_result[i]:
                url_relevance[outlinks[i].url_detail.canonical_url] += url_relevance_result[i]

        return domain_relevance, url_relevance

    def _update_relevance(self, outlinks: List[Outlink]):
        domain_relevance, url_relevance = defaultdict(float), defaultdict(float)
        for outlink in outlinks:
            anchor_text_relevance = self._compute_jacard_similarity_anchor_text(outlink)
            url_relevance[outlink.url_detail.canonical_url] += (anchor_text_relevance * 1000)
            domain_relevance[outlink.url_detail.domain] += anchor_text_relevance

            anchor_link_relevance = self._compute_jacard_similarity_anchor_link(outlink)
            url_relevance[outlink.url_detail.canonical_url] += (anchor_link_relevance * 1000)
            domain_relevance[outlink.url_detail.domain] += anchor_link_relevance

        with ConnectionFactory.create_redis_connection() as redis_conn:
            with redis_conn.pipeline() as pipe:
                for domain, score in domain_relevance.items():
                    pipe.hincrby(Constants.DOMAIN_RELEVANCE_KEY, domain, score)
                pipe.execute()
                logging.info("Domain relevance updated")

                for url, score in url_relevance.items():
                    pipe.hincrby(Constants.URL_RELEVANCE_KEY, url, score)
                pipe.execute()
                logging.info("Url relevance updated")

        return domain_relevance, url_relevance

    def _generate_outlink_score(self, outlinks: List[Outlink],
                                domain_inlinks, url_inlinks,
                                domain_ranks,
                                domain_relevance, url_relevance) -> Dict[str, float]:
        # TODO add logic to weight score here
        return {}

    def _score_outlinks(self, outlinks: List[Outlink]) -> Dict[str, float]:
        with ConnectionFactory.create_redis_connection() as redis:
            domain_inlinks = self._get_domain_inlinks_count(outlinks, redis)
            url_inlinks = self._get_url_inlinks_count(outlinks, redis)

        domain_ranks = self._get_domain_ranks(outlinks)
        # TODO calculate meta relevance

        return self._generate_outlink_score(outlinks,
                                            domain_inlinks, url_inlinks,
                                            domain_ranks,
                                            domain_relevance, url_relevance)

    def get_urls_to_crawl(self, batch_size=1000) -> List[UrlDetail]:
        with ConnectionFactory.create_redis_connection() as redis:
            serialized_urls = Utils.pop_from_redis_list(Constants.FRONTIER_MANAGER_REDIS_QUEUE, redis, batch_size)

        urls_to_crawl = [Utils.deserialize_url_detail(url) for url in serialized_urls]
        # TODO: check rate limited
        # TODO: add scoring logic here
        return urls_to_crawl

    def add_to_queue(self, outlinks: List[Outlink]):
        logging.info("Adding {} url(s) to frontier".format(len(outlinks)))
        self._update_inlinks_count(outlinks)
        self._update_relevance(outlinks)

        with ConnectionFactory.create_redis_connection() as redis:
            redis.rpush(Constants.FRONTIER_MANAGER_REDIS_QUEUE,
                        *[Utils.serialize_url_detail(outlink.url_detail) for outlink in outlinks])

    @classmethod
    def add_rate_limited_urls(cls, rate_limited_urls: List[UrlDetail]):
        with ConnectionFactory.create_redis_connection() as redis:
            redis.lpush(Constants.FRONTIER_MANAGER_REDIS_QUEUE,
                        *[Utils.serialize_url_detail(url, add_rate_limited=True) for url in rate_limited_urls])
