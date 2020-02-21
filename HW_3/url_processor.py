import logging
import time
from typing import List
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from HW_3.beans import Outlink, CrawlerResponse
from HW_3.connection_factory import ConnectionFactory
from HW_3.crawler import Crawler, RobotsTxtService, CrawlingRateLimitingService
from HW_3.factory import Factory
from HW_3.link_graph import LinkGraph
from constants.constants import Constants
from utils.utils import Utils


class UrlMapper:

    def __init__(self) -> None:
        self.frontier_manager = Factory.create_frontier_manager()

    def start(self):
        with ConnectionFactory.create_redis_connection() as redis_conn:
            url_processor_batch_size = UrlProcessor.get_batch_size(redis_conn)
            url_to_crawl_batch_size = url_processor_batch_size * Constants.NO_OF_URL_PROCESSORS
            url_details = self.frontier_manager.get_urls_to_crawl(url_to_crawl_batch_size)

            # TODO write logic to map url_details to processor


class UrlProcessor:

    def __init__(self, processor_id, redis_queue_name) -> None:
        self.processor_id = processor_id
        self.redis_queue_name = redis_queue_name
        self.redis_processing_queue = '{}{}{}'.format(redis_queue_name, Constants.REDIS_SEPARATOR, 'PROCESSING')
        self.url_cleaner = Factory.create_url_cleaner()
        self.url_filtering_service = Factory.create_url_filtering_service()
        self.crawler = Crawler(RobotsTxtService(), CrawlingRateLimitingService(),
                               self.url_cleaner, self.url_filtering_service)
        self.frontier_manager = Factory.create_frontier_manager()

    @classmethod
    def _clean_html(cls, soup: BeautifulSoup):
        for tag_to_remove in Constants.TAGS_TO_REMOVE:
            for element in soup.find_all(tag_to_remove):
                element.clear()

    @classmethod
    def _is_absolute(cls, url):
        return bool(urlparse(url).netloc)

    def _extract_outlinks(self, in_link: UrlDetail, soup: BeautifulSoup) -> List[Outlink]:
        outlinks = []
        for a_element in soup.find_all('a'):
            outlink_url = a_element['href']
            if self._is_absolute(outlink_url):
                outlink_url_detail = self.url_cleaner.transform_relative_url_to_absolute_url(in_link.canonical_url,
                                                                                             outlink_url)
            else:
                outlink_url_detail = self.url_cleaner.get_canonical_url(outlink_url)

            outlink = Outlink(outlink_url_detail, a_element.text)
            outlinks.append(outlink)

        return outlinks

    @classmethod
    def _update_link_graph(cls, curr_url_detail: UrlDetail, outlinks: List[Outlink]) -> None:
        # TODO added edge between old url and new url if there is any redirection
        graph = LinkGraph()
        for outlink_tup in outlinks:
            pass
            # graph.add_edge(curr_url_detail, outlink_tup[0])

    def _filter_outlinks(self, outlinks: List[Outlink]) -> List[Outlink]:
        filtered_urls, removed_urls = self.url_filtering_service.filter_outlinks(outlinks)
        # TODO log removed urls
        return filtered_urls

    # TODO add the retryer here
    def _process_crawler_response(self, crawler_response: CrawlerResponse):
        try:
            soup = BeautifulSoup(crawler_response.raw_html, features=Constants.HTML_PARSER)
            cleaned_text = soup.text
            outlinks = self._extract_outlinks(crawler_response.url_detail, soup)
            self._update_link_graph(crawler_response.url_detail, outlinks)
            filtered_outlinks = self._filter_outlinks(outlinks)
            self.frontier_manager.add_to_queue(filtered_outlinks)


        except Exception:
            logging.error("Error occurred while crawling: {}".format(crawler_response.url_detail.canonical_url),
                          exc_info=True)

    @classmethod
    def get_batch_size(cls, redis_conn) -> int:
        return Utils.int(redis_conn.get(Constants.URL_PROCESSOR_BATCH_SIZE_KEY),
                         Constants.URL_PROCESSOR_DEFAULT_BATCH_SIZE)

    def _remove_crawled_urls_from_redis_queue(self, crawler_responses: List[CrawlerResponse], redis_conn):
        crawled_urls = []
        for crawler_response in crawler_responses:
            if crawler_response:
                crawled_urls.append(crawler_response.url_detail.canonical_url)

        if crawled_urls:
            redis_conn.zrem(self.redis_queue_name, crawled_urls)

    def start(self):
        while True:
            with ConnectionFactory.create_redis_connection() as redis_conn:
                urls_batch_size = self.get_batch_size(redis_conn)
                urls_batch_size -= 1

                urls_to_process = redis_conn.zrevrange(self.redis_queue_name, 0, urls_batch_size, withscores=True)
                if urls_to_process:
                    url_details = [self.url_cleaner.get_canonical_url(url) for url in urls_to_process]
                    filtered_result = self.url_filtering_service.filter_already_crawled_links(url_details)
                    filtered_url_details = filtered_result.filtered
                    if filtered_url_details:
                        crawler_responses = Utils.run_tasks_parallelly(self._process_crawler_response,
                                                                       filtered_url_details,
                                                                       Constants.NO_OF_THREADS_PER_URL_PROCESSOR)

                        self._remove_crawled_urls_from_redis_queue(crawler_responses, redis_conn)

                        for crawler_response in crawler_responses:
                            self._process_crawler_response(crawler_response)

                else:
                    logging.info('No urls to process, {} sleeping for 10 sec'.format(self.processor_id))
                    time.sleep(Constants.URL_PROCESSOR_SLEEP_TIME)


if __name__ == '__main__':
    Utils.configure_logging()
    UrlProcessor(1, "test_queue").start()
