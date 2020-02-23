from typing import List, Dict

from CS6200_S20_SHARED.url_cleaner import UrlDetail, UrlCleaner
from HW_3.beans import Outlink
from HW_3.connection_factory import ConnectionFactory
from constants.constants import Constants
from utils.singleton import SingletonMeta


class FrontierManager(metaclass=SingletonMeta):

    def __init__(self, url_cleaner: UrlCleaner) -> None:
        self.url_cleaner = url_cleaner
        self.seed_urls = []

    def get_urls_to_crawl(self, batch_size=20) -> List[UrlDetail]:
        with ConnectionFactory.create_redis_connection() as redis:
            urls = redis.zrevrange(Constants.FRONTIER_MANAGER_REDIS_QUEUE, 0, batch_size - 1)
            redis.zrem(Constants.FRONTIER_MANAGER_REDIS_QUEUE, urls)
        return [self.url_cleaner.get_canonical_url(url) for url in urls]

    def _score_outlinks(self, outlinks: List[Outlink]) -> Dict[str, float]:
        return {outlink.url_detail.canonical_url: 1.0 for outlink in outlinks}

    def add_to_queue(self, outlinks: List[Outlink]):
        urls_scores = self._score_outlinks(outlinks)
        with ConnectionFactory.create_redis_connection() as redis:
            with redis.pipeline() as pipe:
                for url, score in urls_scores.items():
                    pipe.zincrby(Constants.FRONTIER_MANAGER_REDIS_QUEUE, score, url)

                pipe.execute()
