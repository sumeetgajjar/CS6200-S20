import logging
import re
from collections import defaultdict
from typing import List, Dict, Tuple

import numpy as np

from CS6200_S20_SHARED.url_cleaner import UrlDetail, UrlCleaner
from HW_3.beans import Outlink, FilteredResult
from HW_3.connection_factory import ConnectionFactory
from HW_3.filter import DomainRanker, UrlFilteringService
from constants.constants import Constants
from utils.singleton import SingletonMeta
from utils.utils import Utils


class FrontierManager(metaclass=SingletonMeta):
    _URL_SPLIT_REGEX = re.compile("\\W|_")

    def __init__(self, url_cleaner: UrlCleaner, domain_ranker: DomainRanker,
                 url_filtering_service: UrlFilteringService) -> None:
        self.url_cleaner = url_cleaner
        self.domain_ranker = domain_ranker
        self.url_filtering_service = url_filtering_service

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
    def _get_domain_inlinks_count(cls, url_details: List[UrlDetail], redis) -> np.array:
        result = redis.hmget(Constants.DOMAIN_INLINKS_COUNT_KEY, [url_detail.domain for url_detail in url_details])

        domain_inlinks = []
        for i in range(len(url_details)):
            domain_inlinks[i] = result[i] if result[i] else 0

        return np.array(domain_inlinks, dtype=np.float)

    @classmethod
    def _get_url_inlinks_count(cls, url_details: List[UrlDetail], redis) -> np.array:
        result = redis.hmget(Constants.URL_INLINKS_COUNT_KEY, [url_detail.canonical_url for url_detail in url_details])

        url_inlinks = []
        for i in range(len(url_details)):
            url_inlinks[i] = result[i] if result[i] else 0

        return np.array(url_inlinks, dtype=np.float)

    def _get_domain_rank_score(self, url_details: List[UrlDetail]) -> np.array:
        domain_ranks = []
        for i in range(len(url_details)):
            domain_rank = self.domain_ranker.get_domain_rank(url_details[i].domain)
            domain_ranks[i] = domain_rank.global_rank if domain_rank else self.domain_ranker.max_rank

        return np.array(domain_ranks, dtype=np.float)

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
    def _get_relevance_from_redis(cls, url_details: List[UrlDetail]) -> Tuple[Dict[str, float], Dict[str, float]]:
        url_relevance = defaultdict(float)
        domain_relevance = defaultdict(float)
        with ConnectionFactory.create_redis_connection() as redis:
            domain_relevance_result = redis.hmget(Constants.DOMAIN_RELEVANCE_KEY,
                                                  [url_detail.domain for url_detail in url_details])
            url_relevance_result = redis.hmget(Constants.URL_RELEVANCE_KEY,
                                               [url_detail.canonical_url for url_detail in url_details])
        for i in range(len(url_details)):
            if domain_relevance_result[i]:
                domain_relevance[url_details[i].domain] += domain_relevance_result[i]
            if url_relevance_result[i]:
                url_relevance[url_details[i].canonical_url] += url_relevance_result[i]

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

    @classmethod
    def _filter_wave_0_1_or_rate_limited_urls(cls, filtered_result: FilteredResult) -> FilteredResult:
        new_filtered_result = FilteredResult([], filtered_result.removed)
        for url_detail in filtered_result.filtered:
            if url_detail.wave == 0 or url_detail.wave == 1 or getattr(url_detail, 'rate_limited'):
                new_filtered_result.filtered.append(url_detail)
            else:
                new_filtered_result.removed.append(url_detail)

        return new_filtered_result

    def _filter_non_relevant_urls(self, filtered_result: FilteredResult) -> FilteredResult:
        domain_relevance, url_relevance = self._get_relevance_from_redis(filtered_result.removed)

        new_filtered_result = FilteredResult(filtered_result.filtered, [])
        for url_detail in filtered_result.removed:
            if url_detail.canonical_url in url_relevance:
                new_filtered_result.filtered.append(url_detail)
            else:
                new_filtered_result.removed.append(url_detail)

        return new_filtered_result

    def _filter_urls_based_on_scores(self, filtered_result: FilteredResult) -> FilteredResult:
        new_filtered_result = FilteredResult(filtered_result.filtered, [])

        urls_still_in_question = filtered_result.removed
        domain_rank_score = self._get_domain_rank_score(urls_still_in_question)
        with ConnectionFactory.create_redis_connection() as redis:
            domain_inlinks = self._get_domain_inlinks_count(urls_still_in_question, redis)
            url_inlinks = self._get_url_inlinks_count(urls_still_in_question, redis)

        normalized_domain_rank_scores = Utils.normalize(domain_rank_score)
        normalized_domain_inlinks = Utils.normalize(domain_inlinks)
        final_score = url_inlinks + normalized_domain_inlinks + normalized_domain_rank_scores
        sorted_ix = np.argsort(final_score)[::-1]

        urls_still_in_question = np.array(urls_still_in_question)
        x = Constants.URLS_TO_CONSIDER_BASED_ON_SCORES
        new_filtered_result.filtered.extend(urls_still_in_question[sorted_ix[:x]])
        new_filtered_result.removed.extend(urls_still_in_question[sorted_ix[x:]])

        return new_filtered_result

    def _filter_urls_for_crawling(self, url_details: List[UrlDetail]) -> FilteredResult:
        filtered_result = self.url_filtering_service.filter_already_crawled_links(url_details)
        filtered_result = self._filter_wave_0_1_or_rate_limited_urls(filtered_result)
        filtered_result = self._filter_non_relevant_urls(filtered_result)
        filtered_result = self._filter_urls_based_on_scores(filtered_result)
        return filtered_result

    def get_urls_to_crawl(self, batch_size=1000) -> List[UrlDetail]:
        with ConnectionFactory.create_redis_connection() as redis_conn:
            serialized_urls = redis_conn.lrange(Constants.FRONTIER_MANAGER_REDIS_QUEUE, 0, batch_size - 1)

            batch_size = min(batch_size, len(serialized_urls))
            urls_to_crawl = [Utils.deserialize_url_detail(url) for url in serialized_urls]
            filtered_result = self._filter_urls_for_crawling(urls_to_crawl)

            redis_conn.ltrim(Constants.FRONTIER_MANAGER_REDIS_QUEUE, batch_size, -1)
            redis_conn.rpush(Constants.FRONTIER_MANAGER_REDIS_QUEUE,
                             *[Utils.serialize_url_detail(url) for url in filtered_result.removed])

        return filtered_result.filtered

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
