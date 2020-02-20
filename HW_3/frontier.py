from typing import List

from CS6200_S20_SHARED.url_cleaner import UrlDetail


class FrontierManager:

    def __init__(self, seed_urls: List[UrlDetail]) -> None:
        self.seed_urls = seed_urls

    def get_urls_to_crawl(self, batch_size=20) -> List[UrlDetail]:
        pass
