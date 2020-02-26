# TODO write stuff to add to es
# TODO write stuff to create link graph once processing is done
import concurrent.futures
import logging
import signal
import sys
from typing import List, Optional

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


if __name__ == '__main__':
    HW3.init_crawling()
