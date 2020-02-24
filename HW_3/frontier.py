import logging
import re
from collections import defaultdict
from typing import List, Dict

from CS6200_S20_SHARED.url_cleaner import UrlDetail, UrlCleaner
from HW_3.beans import Outlink
from HW_3.connection_factory import ConnectionFactory
from HW_3.filter import DomainRanker
from constants.constants import Constants
from utils.singleton import SingletonMeta


class FrontierManager(metaclass=SingletonMeta):
    _URL_SPLIT_REGEX = re.compile("\\W|_")

    def __init__(self, url_cleaner: UrlCleaner, domain_ranker: DomainRanker) -> None:
        self.url_cleaner = url_cleaner
        self.domain_ranker = domain_ranker

    def get_urls_to_crawl(self, batch_size=20) -> List[UrlDetail]:
        with ConnectionFactory.create_redis_connection() as redis:
            urls = redis.zrevrange(Constants.FRONTIER_MANAGER_REDIS_QUEUE, 0, batch_size - 1)
            if urls:
                redis.zrem(Constants.FRONTIER_MANAGER_REDIS_QUEUE, *urls)
        return [self.url_cleaner.get_canonical_url(url) for url in urls]

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

    def _get_keyword_url_relevance(self, outlinks: List[Outlink]) -> dict:
        relevance = defaultdict(float)
        for outlink in outlinks:
            relevance[outlink.url_detail.canonical_url] += (self._compute_jacard_similarity_anchor_text(outlink)) * 1000
            relevance[outlink.url_detail.canonical_url] += self._compute_jacard_similarity_anchor_link(outlink) * 1000

        return relevance

    def _generate_outlink_score(self, outlinks: List[Outlink],
                                domain_inlinks, url_inlinks, domain_ranks, relevance) -> Dict[str, float]:
        # TODO add logic to weight score here
        return {}

    def _score_outlinks(self, outlinks: List[Outlink]) -> Dict[str, float]:
        with ConnectionFactory.create_redis_connection() as redis:
            domain_inlinks = self._get_domain_inlinks_count(outlinks, redis)
            url_inlinks = self._get_url_inlinks_count(outlinks, redis)

        domain_ranks = self._get_domain_ranks(outlinks)
        relevance = self._get_keyword_url_relevance(outlinks)

        return self._generate_outlink_score(outlinks, domain_inlinks, url_inlinks, domain_ranks, relevance)

    def add_to_queue(self, outlinks: List[Outlink]):
        logging.info("Adding {} url(s) to frontier".format(len(outlinks)))
        self._update_inlinks_count(outlinks)
        urls_scores = self._score_outlinks(outlinks)
        with ConnectionFactory.create_redis_connection() as redis:
            with redis.pipeline() as pipe:
                for url, score in urls_scores.items():
                    pipe.zincrby(Constants.FRONTIER_MANAGER_REDIS_QUEUE, score, url)

                pipe.execute()

    @classmethod
    def add_rate_limited_urls(cls, rate_limited_urls: List[UrlDetail]):
        with ConnectionFactory.create_redis_connection() as redis:
            with redis.pipeline() as pipe:
                for url in rate_limited_urls:
                    pipe.zincrby(Constants.FRONTIER_MANAGER_REDIS_QUEUE, Constants.RATE_LIMITED_URL_WEIGHT,
                                 url.canonical_url)

                pipe.execute()
