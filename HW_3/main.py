# TODO make seed urls https
# TODO write stuff to add to es
# TODO write stuff to create link graph once processing is done
import concurrent.futures
import logging
from typing import List

from HW_3.url_processor import UrlMapper, UrlProcessor
from constants.constants import Constants
from utils.utils import Utils


class HW3:
    _URL_MAPPER_POOL = concurrent.futures.ProcessPoolExecutor(1)
    _URL_PROCESSOR_POOL = concurrent.futures.ProcessPoolExecutor(Constants.NO_OF_URL_PROCESSORS)

    @classmethod
    def start_url_processor(cls, url_processor_id: int, url_processor_queue_name: str):
        logging.info('Starting Url Processor, Id:{}, Queue:"{}"'.format(url_processor_id, url_processor_queue_name))
        url_processor = UrlProcessor(url_processor_id, url_processor_queue_name)
        url_processor.start()

    @classmethod
    def url_processor_init_wrapper(cls, url_processor_init_infos):
        futures = []
        for url_processor_init_info in url_processor_init_infos:
            futures.append(cls._URL_PROCESSOR_POOL.submit(cls.start_url_processor, *url_processor_init_info))
        return futures

    @classmethod
    def start_url_mapper(cls, url_processor_queue_names: List[str]):
        logging.info("Starting Url Mapper")
        url_mapper = UrlMapper(url_processor_queue_names)
        url_mapper.start()

    @classmethod
    def url_mapper_init_wrapper(cls, url_processor_queue_names):
        return cls._URL_MAPPER_POOL.submit(cls.start_url_mapper, url_processor_queue_names)

    @classmethod
    def init_crawling(cls):
        Utils.configure_logging()
        url_processor_init_infos = [(i, Constants.URL_PROCESSOR_QUEUE_NAME_TEMPLATE.format(i))
                                    for i in range(1, Constants.NO_OF_URL_PROCESSORS + 1)]
        url_processor_futures = cls.url_processor_init_wrapper(url_processor_init_infos)

        # url_processor_queue_names = [queue_name for _, queue_name in url_processor_init_infos]
        # url_mapper_future = cls.url_mapper_init_wrapper(url_processor_queue_names)

        # cls._URL_MAPPER_POOL.shutdown(wait=True)
        cls._URL_PROCESSOR_POOL.shutdown(wait=True)
        result = [future.result() for future in url_processor_futures]
        # url_mapper_future.result()


if __name__ == '__main__':
    HW3.init_crawling()
