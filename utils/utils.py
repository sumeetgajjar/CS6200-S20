import concurrent.futures
import logging
import math
from logging import getLogger, Formatter, StreamHandler

from HW_1.es_index_config import EsIndexConfig
from HW_1.es_utils import EsUtils
from constants.constants import Constants
from utils.decorators import timing


class Utils:

    @classmethod
    def get_data_dir_abs_path(cls):
        return '{}/{}'.format(Constants.PROJECT_ROOT, Constants.DATA_DIR)

    @classmethod
    def get_ap_data_path(cls):
        return '{}/{}'.format(cls.get_data_dir_abs_path(), Constants.AP_DATA_PATH)

    @classmethod
    def get_ap89_collection_abs_path(cls, ):
        return '{}/{}'.format(cls.get_ap_data_path(), Constants.AP89_COLLECTION)

    @classmethod
    def create_ap_data_index(cls, ):
        EsUtils.create_es_index(name=Constants.AP_DATA_INDEX_NAME,
                                index_config=EsIndexConfig.get_ap_data_index_config())

    @classmethod
    def delete_ap_data_index(cls, ignore_unavailable=False):
        EsUtils.delete_es_index(name=Constants.AP_DATA_INDEX_NAME, ignore_unavailable=ignore_unavailable)

    @classmethod
    def configure_logging(cls, level=logging.INFO):
        log = getLogger()
        log.setLevel(level)

        log_formatter = Formatter("[%(process)d] %(asctime)s [%(levelname)s] %(name)s: %(message)s")

        console_handler = StreamHandler()
        console_handler.setFormatter(log_formatter)
        log.addHandler(console_handler)

    @classmethod
    def split_list_into_sub_lists(cls, list_to_split, no_of_sub_lists: int = None, sub_list_size: int = None):
        if no_of_sub_lists is not None and sub_list_size is not None:
            raise ValueError("Both no_of_sub_lists and no_of_items_per_list should be passed as argument")

        if no_of_sub_lists is None and sub_list_size is None:
            raise ValueError("Both no_of_sub_lists and no_of_items_per_list cannot be passed as argument")

        if no_of_sub_lists is not None:
            sub_list_size = math.ceil(len(list_to_split) / no_of_sub_lists)

        for i in range(0, len(list_to_split), sub_list_size):
            yield list_to_split[i:i + sub_list_size]

    @classmethod
    @timing
    def run_tasks_parallelly_in_chunks(cls, func, tasks: list, no_of_parallel_tasks: int, **kwargs):
        with concurrent.futures.ProcessPoolExecutor(no_of_parallel_tasks) as pool:
            futures = []
            for sub_tasks in cls.split_list_into_sub_lists(tasks, no_of_parallel_tasks):
                futures.append(pool.submit(func, sub_tasks, **kwargs))

            results = []
            for future in futures:
                results.append(future.result())

        return results

    @classmethod
    @timing
    def run_tasks_parallelly(cls, func, tasks: list, no_of_parallel_tasks: int, **kwargs):
        with concurrent.futures.ProcessPoolExecutor(no_of_parallel_tasks) as pool:
            futures = []
            for task in tasks:
                futures.append(pool.submit(func, task, **kwargs))

            results = []
            for future in futures:
                results.append(future.result())

        return results

    @classmethod
    def write_results_to_file(cls, file_path, results):
        logging.info("Writing results to: {}".format(file_path))
        lines = ["{query_number} Q0 {doc_no} {rank} {score} Exp\n".format(**result) for result in results]
        with open(file_path, 'w') as file:
            file.writelines(lines)
        logging.info("Results written")

    @classmethod
    def get_stopwords_file_path(cls):
        return "{}/{}".format(cls.get_ap_data_path(), 'stoplist.txt')
