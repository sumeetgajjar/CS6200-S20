import concurrent.futures
import csv
import glob
import json
import logging
import signal
import sys
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Set

from CS6200_S20_SHARED.es_inserter import LinkGraphReader, EsInserter
from CS6200_S20_SHARED.shared_beans import ElasticSearchInput
from CS6200_S20_SHARED.url_cleaner import UrlCleaner
from HW_3.filter import CrawlingUtils
from utils.decorators import timing
from utils.utils import Utils

Utils.configure_logging(enable_logging_to_file=True, filepath='hw_3_crawler.log')

from HW_3.url_processor import UrlMapper, UrlProcessor
from constants.constants import Constants


class HW3:
    _URL_MAPPER_POOL = concurrent.futures.ProcessPoolExecutor(1)
    _URL_PROCESSOR_POOL = concurrent.futures.ProcessPoolExecutor(Constants.NO_OF_URL_PROCESSORS)
    _URL_MAPPER: Optional[UrlMapper] = None

    @classmethod
    def _add_signal_handler(cls):
        signal.signal(signal.SIGINT, cls._exit_gracefully)
        signal.signal(signal.SIGTERM, cls._exit_gracefully)

    @classmethod
    def _exit_gracefully(cls, signum, frame):
        if cls._URL_MAPPER:
            cls._URL_MAPPER.queue_rate_limited_urls_to_frontier()
        sys.exit(1)

    @classmethod
    def start_url_processor(cls, url_processor_id: int, url_processor_queue_name: str):
        logging.info('Starting Url Processor, Id:{}, Queue:"{}"'.format(url_processor_id, url_processor_queue_name))
        try:
            url_processor = UrlProcessor(url_processor_id, url_processor_queue_name)
            url_processor.start()
        except:
            logging.critical("Exiting Url Processor: {}".format(url_processor_id), exc_info=True)
            raise

    @classmethod
    def url_processor_init_wrapper(cls, url_processor_init_infos):
        futures = []
        for url_processor_init_info in url_processor_init_infos:
            futures.append(cls._URL_PROCESSOR_POOL.submit(cls.start_url_processor, *url_processor_init_info))
        return futures

    @classmethod
    def start_url_mapper(cls, url_processor_queue_names: List[str]):
        logging.info("Starting Url Mapper")
        try:
            cls._URL_MAPPER = UrlMapper(url_processor_queue_names)
            cls._URL_MAPPER.start()
        except:
            logging.critical("Exiting Url Mapper", exc_info=True)
            raise

    @classmethod
    def url_mapper_init_wrapper(cls, url_processor_queue_names):
        return cls._URL_MAPPER_POOL.submit(cls.start_url_mapper, url_processor_queue_names)

    @classmethod
    def init_crawling(cls):
        cls._add_signal_handler()
        CrawlingUtils.init_bloomfilter()
        url_processor_init_infos = [(i, Constants.URL_PROCESSOR_QUEUE_NAME_TEMPLATE.format(i))
                                    for i in range(1, Constants.NO_OF_URL_PROCESSORS + 1)]
        url_processor_queue_names = [queue_name for _, queue_name in url_processor_init_infos]

        processor = True
        mapper = False

        url_processor_futures = []
        if processor:
            url_processor_futures = cls.url_processor_init_wrapper(url_processor_init_infos)

        url_mapper_future = []
        if mapper:
            url_mapper_future = cls.url_mapper_init_wrapper(url_processor_queue_names)

        if processor:
            cls._URL_PROCESSOR_POOL.shutdown(wait=True)
            result = [future.result() for future in url_processor_futures]

        if mapper:
            cls._URL_MAPPER_POOL.shutdown(wait=True)
            url_mapper_future.result()

    @classmethod
    @timing
    def _create_link_graph_csv(cls, crawled_url_set: Set[str]):
        link_graph_csv_path = Utils.get_link_graph_csv_path()
        outlinks = defaultdict(set)

        url_cleaner = UrlCleaner()
        for src_csv_path in ['/home/sumeet/PycharmProjects/CS6200-S20/data/link_graph_edges_madhur.csv',
                             '/home/sumeet/PycharmProjects/CS6200-S20/data/link_graph_edges_sumeet.csv',
                             '/home/sumeet/PycharmProjects/CS6200-S20/data/link_graph_edges_saurabha.csv']:
            logging.info('Reading: {}'.format(src_csv_path))
            i = 0
            with open(src_csv_path, 'r') as src_csv:
                csv_reader = csv.reader(src_csv)
                for row in csv_reader:
                    try:
                        src_url_detail = url_cleaner.get_canonical_url(row[0])
                        src = src_url_detail.canonical_url
                        dest_url_detail = url_cleaner.get_canonical_url(row[2])
                        dest = dest_url_detail.canonical_url
                        if dest not in crawled_url_set:
                            dest = dest_url_detail.domain

                        outlinks[src].add(dest)

                        if i % 1000000 == 100:
                            logging.info("Processed {} edges".format(i))
                            logging.info("Outlinks dict size:{}".format(len(outlinks)))

                        i += 1
                    except:
                        logging.critical("Error in line: {}, {}".format(i, row), exc_info=True)

        logging.info("Writing link graph to TSV")
        with open(link_graph_csv_path, 'w') as output_file:
            csv_writer = csv.writer(output_file, delimiter='\t')
            for src, dests in outlinks.items():
                csv_writer.writerow([src, *dests])

    @classmethod
    def _get_crawled_file_paths(cls) -> List[str]:
        crawled_file_paths = glob.glob('{}/*.json'.format(Utils.get_crawled_response_dir()))
        logging.info("{} crawled file(s)".format(len(crawled_file_paths)))
        return crawled_file_paths

    @classmethod
    def _get_crawled_data(cls, crawled_file_paths, link_graph_reader: LinkGraphReader):
        url_cleaner = UrlCleaner()
        for path in crawled_file_paths:
            with open(path, 'r') as file:
                data = json.load(file)
                yield ElasticSearchInput(
                    url_detail=url_cleaner.get_canonical_url(data['url']),
                    org_url=data['org_url'],
                    raw_html=data['raw_html'],
                    headers=data['headers'],
                    is_redirected=data['is_redirected'],
                    redirected_url=url_cleaner.get_canonical_url(data['redirected_url']) if data[
                        'is_redirected'] else None,
                    title=data['title'],
                    cleaned_text=data['cleaned_text'],
                    crawled_time=datetime.strptime(data['crawled_time'], Constants.TIME_FORMAT),
                    crawled_by='sumeet',
                    link_info=link_graph_reader.get_linkinfo(data['url']),
                    meta_keywords=data['meta_keywords'],
                    meta_description=data['meta_description'],
                    wave=data['wave']
                )

    @classmethod
    def _insert_data_into_es_helper(cls, crawled_file_paths: List[str], es_inserter: EsInserter):
        link_graph_reader = LinkGraphReader(Utils.get_link_graph_csv_path())
        crawled_data = cls._get_crawled_data(crawled_file_paths, link_graph_reader)
        es_inserter.bulk_insert(crawled_data, chunk_size=100)

    @classmethod
    @timing
    def _read_crawled_urls_csv(cls) -> Set[str]:
        url_set = set()

        url_cleaner = UrlCleaner()
        for path in [('/home/sumeet/PycharmProjects/CS6200-S20/data/crawled_urls_saurabha.csv', 0),
                     ('/home/sumeet/PycharmProjects/CS6200-S20/data/crawled_urls_sumeet.csv', 1),
                     ('/home/sumeet/PycharmProjects/CS6200-S20/data/crawled_urls_madhur.csv', 0)]:

            with open(path[0], 'r') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    url_set.add(url_cleaner.get_canonical_url(row[path[1]]).canonical_url)

        logging.info("Unique urls crawled: {}".format(len(url_set)))
        return url_set

    @classmethod
    def merge_crawled_urls(cls):
        crawled_url_set = cls._read_crawled_urls_csv()
        cls._create_link_graph_csv(crawled_url_set)

    @classmethod
    def insert_data_into_es(cls):
        crawled_file_paths = cls._get_crawled_file_paths()

        es_inserter = EsInserter("localhost", 9200, Constants.CRAWLED_DATA_INDEX_NAME, Constants.ES_TIMEOUT)
        es_inserter.init_index(True)
        Utils.run_tasks_parallelly_in_chunks(cls._insert_data_into_es_helper, crawled_file_paths, 8,
                                             es_inserter=es_inserter)


if __name__ == '__main__':
    # HW3.init_crawling()
    # HW3.insert_data_into_es()
    HW3.merge_crawled_urls()
