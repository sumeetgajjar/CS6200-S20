import logging
import time
from typing import List, Tuple

from bs4 import BeautifulSoup

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from HW_3.connection_factory import ConnectionFactory
from HW_3.crawler import Crawler, RobotsTxtService, CrawlingRateLimitingService, CrawlerResponse
from HW_3.factory import Factory
from HW_3.link_graph import LinkGraph
from constants.constants import Constants
from utils.utils import Utils


class UrlProcessor:

    def __init__(self, processor_id, redis_queue_name) -> None:
        self.processor_id = processor_id
        self.redis_queue_name = redis_queue_name
        self.redis_processing_queue = '{}{}{}'.format(redis_queue_name, Constants.REDIS_SEPARATOR, 'PROCESSING')
        self.crawler = Crawler(RobotsTxtService(), CrawlingRateLimitingService())
        self.url_cleaner = Factory.create_url_cleaner()
        self.frontier_manager = Factory.create_frontier_manager()
        self.url_filtering_service = Factory.create_url_filtering_service()

    @classmethod
    def _clean_html(cls, soup: BeautifulSoup):
        for tag_to_remove in Constants.TAGS_TO_REMOVE:
            for element in soup.find_all(tag_to_remove):
                element.clear()

    def _extract_outlinks(self, soup: BeautifulSoup) -> List[Tuple[UrlDetail, str]]:
        return [(self.url_cleaner.get_canonical_url(a_element['href']), a_element.text)
                for a_element in soup.find_all('a') if a_element['href']]

    @classmethod
    def _update_link_graph(cls, curr_url_detail: UrlDetail, outlinks: List[Tuple[UrlDetail, str]]) -> None:
        graph = LinkGraph()
        for outlink_tup in outlinks:
            graph.add_edge(curr_url_detail, outlink_tup[0])

    def _filter_outlinks(self, outlinks: List[Tuple[UrlDetail, str]]) -> List[Tuple[UrlDetail, str]]:
        filtered_urls, removed_urls = self.url_filtering_service.filter_outlinks(outlinks)
        # TODO log removed urls
        return filtered_urls

    # TODO add the retryer here
    def _process_crawler_response(self, crawler_response: CrawlerResponse):
        try:
            soup = BeautifulSoup(crawler_response.raw_html, features=Constants.HTML_PARSER)
            cleaned_text = soup.text
            outlinks = self._extract_outlinks(soup)
            self._update_link_graph(crawler_response.url_detail, outlinks)
            filtered_outlinks = self._filter_outlinks(outlinks)
            self.frontier_manager.add_to_queue(filtered_outlinks)


        except Exception:
            logging.error("Error occurred while crawling: {}".format(crawler_response.url_detail.canonical_url),
                          exc_info=True)

    def start(self):
        while True:
            with ConnectionFactory.create_redis_connection() as redis_conn:
                urls_batch_size = Utils.int(redis_conn.get(Constants.URLS_BATCH_SIZE_KEY), Constants.URLS_BATCH_SIZE)
                urls_batch_size -= 1

                urls_to_process = redis_conn.zrevrange(self.redis_queue_name, 0, urls_batch_size, withscores=True)
                if urls_to_process:
                    url_details = [self.url_cleaner.get_canonical_url(url) for url in urls_to_process]
                    if url_details:
                        crawler_responses = Utils.run_tasks_parallelly(self._process_crawler_response,
                                                                       url_details,
                                                                       Constants.NO_OF_THREADS_PER_URL_PROCESSOR)

                        for crawler_response in crawler_responses:
                            self._process_crawler_response(crawler_response)

                else:
                    logging.info('No urls to process, {} sleeping for 10 sec'.format(self.processor_id))
                    time.sleep(Constants.URL_PROCESSOR_SLEEP_TIME)


if __name__ == '__main__':
    Utils.configure_logging()
    UrlProcessor(1, "test_queue").start()
