# TODO make seed urls https
# TODO create url id services
# TODO check where retry is required
# TODO check the entire flow
# TODO add logging wherever necessary
# TODO write stuff to add to es
# TODO write stuff to create link graph once processing is done
import logging
from multiprocessing.context import Process
from typing import List

from HW_3.url_processor import UrlMapper, UrlProcessor
from constants.constants import Constants
from utils.utils import Utils


def init_url_processor(url_processor_id: int, url_processor_queue_name: str):
    logging.info("Starting Url Processor, Id:{}, Queue:{}".format(url_processor_id, url_processor_queue_name))
    url_processor = UrlProcessor(url_processor_id, url_processor_queue_name)
    url_processor.start()


def url_processor_init_wrapper(url_processor_init_infos):
    url_processor_processes = []
    for url_processor_init_info in url_processor_init_infos:
        url_processor_start = Process(target=init_url_processor, args=url_processor_init_info)
        url_processor_start.start()

        url_processor_processes.append(url_processor_start)

    return url_processor_processes


def init_url_mapper(url_processor_queue_names: List[str]):
    url_mapper = UrlMapper(url_processor_queue_names)
    url_mapper.start()


def url_mapper_init_wrapper(url_processor_queue_names):
    logging.info("Starting Url Mapper")
    url_mapper_process = Process(target=init_url_mapper, args=url_processor_queue_names)
    url_mapper_process.start()
    return url_mapper_process


def init_crawling():
    url_processor_init_infos = [(i, Constants.URL_PROCESSOR_QUEUE_NAME_TEMPLATE.format(i))
                                for i in range(1, Constants.NO_OF_URL_PROCESSORS + 1)]
    url_processor_processes = url_processor_init_wrapper(url_processor_init_infos)

    url_processor_queue_names = [queue_name for _, queue_name in url_processor_init_infos]
    url_mapper_process = url_mapper_init_wrapper(url_processor_queue_names)
    url_mapper_process.join()

    for url_processor in url_processor_processes:
        url_processor.join()


if __name__ == '__main__':
    Utils.configure_logging()
    init_crawling()
