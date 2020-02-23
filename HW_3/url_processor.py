import hashlib
import json
import logging
import os
import time
from collections import defaultdict, Counter
from typing import List
from urllib.parse import urlparse

import redis
from bs4 import BeautifulSoup
from retrying import retry

from CS6200_S20_SHARED.url_cleaner import UrlDetail
from HW_3.beans import Outlink, CrawlerResponse
from HW_3.connection_factory import ConnectionFactory
from HW_3.crawler import Crawler
from HW_3.factory import Factory
from HW_3.filter import CrawlingUtils
from HW_3.link_graph import LinkGraph
from constants.constants import Constants
from utils.utils import Utils


class UrlMapper:

    def __init__(self, url_processor_queue_names: List[str]) -> None:
        self.frontier_manager = Factory.create_frontier_manager()
        self.crawling_rate_limiting_service = Factory.create_crawling_rate_limiter_service()
        self.url_processor_queue_names = url_processor_queue_names
        self.url_processor_queue_names_set = set(url_processor_queue_names)
        self.domain_url_processor_mapping = {}
        self.next_queue_to_assign_url = 0

    def _get_queue_for_domain(self, domain: str) -> str:
        assigned_queue = self.domain_url_processor_mapping.get(domain)
        if not assigned_queue:
            assigned_queue_index = self.next_queue_to_assign_url % len(self.url_processor_queue_names)
            assigned_queue = self.url_processor_queue_names[assigned_queue_index]
            self.next_queue_to_assign_url += 1

        return assigned_queue

    @classmethod
    @retry(stop_max_attempt_number=Constants.URL_MAPPER_QUEUE_TO_REDIS_RETRY)
    def _queue_urls(cls, queue_name, urls_to_queue: dict, redis_conn: redis.Redis):
        with redis_conn.pipeline() as pipe:
            for url_to_queue, score in urls_to_queue.items():
                pipe.zincrby(queue_name, score, url_to_queue)

            pipe.execute()

    def _generate_urls_queue_mapping(self, url_details) -> dict:
        urls_queue_mapping = defaultdict(Counter)
        for url_detail in url_details:
            assigned_queue = self._get_queue_for_domain(url_detail.domain)
            if assigned_queue in self.url_processor_queue_names_set:
                urls_queue_mapping[assigned_queue][url_detail.canonical_url] += 1
            else:
                raise RuntimeError("Invalid Queue name found: {}".format(assigned_queue))

        return urls_queue_mapping

    def start(self):
        rate_limited_url_details = []

        while True:
            try:
                with ConnectionFactory.create_redis_connection() as redis_conn:
                    url_details = rate_limited_url_details

                    url_processor_batch_size = UrlProcessor.get_batch_size(redis_conn)
                    urls_batch_size = (url_processor_batch_size * Constants.NO_OF_URL_PROCESSORS) - len(url_details)

                    if urls_batch_size > 0:
                        url_details.extend(self.frontier_manager.get_urls_to_crawl(urls_batch_size))

                    filtered_result = self.crawling_rate_limiting_service.filter(url_details)
                    filtered_url_details = filtered_result.filtered
                    rate_limited_url_details = filtered_result.removed

                    if len(filtered_url_details) > 0:
                        urls_queue_mapping = self._generate_urls_queue_mapping(filtered_url_details)
                        for queue_name, urls_to_queue in urls_queue_mapping:
                            self._queue_urls(queue_name, urls_to_queue, redis_conn)
                    else:
                        logging.info('No urls to queue, url mapper sleeping for 10 sec')
                        time.sleep(Constants.URL_MAPPER_SLEEP_TIME)

            except:
                logging.error("Error occurred while queueing urls to url processor", exc_info=True)


class UrlProcessor:
    if not os.path.isdir(Utils.get_crawled_response_dir()):
        logging.info("Creating the crawled response dir")
        os.makedirs(Utils.get_crawled_response_dir())
        logging.info("Crawled response dir created")

    def __init__(self, processor_id, redis_queue_name) -> None:
        self.processor_id = processor_id
        self.redis_queue_name = redis_queue_name
        self.url_cleaner = Factory.create_url_cleaner()
        self.url_filtering_service = Factory.create_url_filtering_service()
        self.crawler = Crawler(self.url_cleaner)
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
            if outlink_url:
                if self._is_absolute(outlink_url):
                    outlink_url_detail = self.url_cleaner.transform_relative_url_to_absolute_url(in_link.canonical_url,
                                                                                                 outlink_url)
                else:
                    outlink_url_detail = self.url_cleaner.get_canonical_url(outlink_url)

                outlink = Outlink(outlink_url_detail, a_element.text)
                outlinks.append(outlink)

        logging.info("Extracted {} outlink(s)".format(len(outlinks)))
        return outlinks

    @classmethod
    def _update_link_graph(cls, crawler_response: CrawlerResponse, outlinks: List[Outlink]) -> None:
        logging.info("Updating link graph")
        src_url_detail = crawler_response.url_detail
        if crawler_response.redirected:
            LinkGraph.add_edge(src_url_detail, crawler_response.redirected_url)
            src_url_detail = crawler_response.redirected_url

        LinkGraph.add_edges(src_url_detail, outlinks)
        logging.info("Link graph updated")

    def _filter_outlinks(self, outlinks: List[Outlink]) -> List[Outlink]:
        logging.info("Filtering outlinks")
        filtered_result = self.url_filtering_service.filter_outlinks(outlinks)
        logging.info("Filtered {} outlink(s)".format(len(filtered_result.removed)))
        return filtered_result.filtered

    @classmethod
    def _save_crawled_response(cls, crawler_response: CrawlerResponse, title: str, cleaned_text: str):
        logging.info("Persisting crawled response")
        data = {
            'title': title,
            'cleaned_text': cleaned_text,
            'headers': crawler_response.headers,
            'raw_html': crawler_response.raw_html,
            'url': crawler_response.url_detail.canonical_url,
            'org_url': crawler_response.url_detail.org_url,
        }

        if crawler_response.redirected:
            data['redirected_org_url'] = crawler_response.redirected_url.org_url
            data['redirected_url'] = crawler_response.redirected_url.canonical_url

        file_name = '{}.json'.format(hashlib.md5(crawler_response.url_detail.canonical_url.encode()).hexdigest())
        file_path = '{}/{}'.format(Utils.get_crawled_response_dir(), file_name)
        with open(file_path, 'w') as file:
            json.dump(data, file)

        logging.info("Crawled response persisted:{}".format(file_path))

    def _process_crawler_response(self, crawler_response: CrawlerResponse):
        try:
            soup = BeautifulSoup(crawler_response.raw_html, features=Constants.HTML_PARSER)
            title = ''
            if soup.title:
                title = soup.title.text.strip() if soup.title.text else ''
            cleaned_text = soup.text.strip()

            outlinks = self._extract_outlinks(crawler_response.url_detail, soup)
            self._update_link_graph(crawler_response, outlinks)
            filtered_outlinks = self._filter_outlinks(outlinks)
            self.frontier_manager.add_to_queue(filtered_outlinks)

            self._save_crawled_response(crawler_response, title, cleaned_text)
        except:
            logging.error("Error occurred while processing: {}".format(crawler_response.url_detail.canonical_url),
                          exc_info=True)

    @classmethod
    def get_batch_size(cls, redis_conn) -> int:
        return Utils.int(redis_conn.get(Constants.URL_PROCESSOR_BATCH_SIZE_KEY),
                         Constants.URL_PROCESSOR_DEFAULT_BATCH_SIZE)

    def _remove_crawled_urls_from_redis_queue(self, url_details: List[UrlDetail], redis_conn):
        crawled_urls = [url_detail.canonical_url for url_detail in url_details]
        if crawled_urls:
            redis_conn.zrem(self.redis_queue_name, *crawled_urls)

    @classmethod
    def _add_url_to_crawled_list(cls, crawler_responses: List[CrawlerResponse]):
        url_details = []

        for crawler_response in crawler_responses:
            url_details.append(crawler_response.url_detail)
            if crawler_response.redirected:
                url_details.append(crawler_response.redirected_url)

        CrawlingUtils.add_urls_to_crawled_list(url_details)

    def start(self):
        while True:
            with ConnectionFactory.create_redis_connection() as redis_conn:
                urls_batch_size = self.get_batch_size(redis_conn)

                urls_to_process = redis_conn.zrevrange(self.redis_queue_name, 0, urls_batch_size - 1)
                logging.info("Fetched {} url(s) to process".format(len(urls_to_process)))
                if urls_to_process:
                    url_details = [self.url_cleaner.get_canonical_url(url) for url in urls_to_process]
                    filtered_result = self.url_filtering_service.filter_already_crawled_links(url_details)
                    filtered_url_details = filtered_result.filtered
                    if filtered_url_details:
                        crawler_responses = Utils.run_tasks_parallelly(self.crawler.crawl,
                                                                       filtered_url_details,
                                                                       Constants.NO_OF_THREADS_PER_URL_PROCESSOR)

                        for crawler_response in crawler_responses:
                            self._process_crawler_response(crawler_response)

                        self._add_url_to_crawled_list(crawler_responses)

                    self._remove_crawled_urls_from_redis_queue(url_details, redis_conn)

                else:
                    logging.info('No urls to process, Url Processor:{} sleeping for 10 sec'.format(self.processor_id))
                    time.sleep(Constants.URL_PROCESSOR_SLEEP_TIME)
