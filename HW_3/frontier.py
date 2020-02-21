from typing import List, Tuple

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from utils.singleton import SingletonMeta


class FrontierManager(metaclass=SingletonMeta):

    def __init__(self, seed_urls: List[UrlDetail]) -> None:
        self.seed_urls = seed_urls
        self.queue = []
        self.queue.extend(seed_urls)

    def get_urls_to_crawl(self, batch_size=20) -> List[UrlDetail]:
        batch = self.queue[:batch_size]
        self.queue = self.queue[len(batch):]
        return batch

    def add_to_queue(self, outlinks: List[Tuple[UrlDetail, str]]):
        self.queue.extend(outlinks)
