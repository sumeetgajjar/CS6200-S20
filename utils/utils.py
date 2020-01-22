import concurrent.futures
import logging
import math
import sys

from HW_1.constants import Constants
from HW_1.es_index_config import EsIndexConfig
from HW_1.es_utils import EsUtils
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
        logging.basicConfig(stream=sys.stdout, level=level)

    @classmethod
    def split_list_into_sub_lists(cls, list_to_split, no_of_sub_lists):
        sub_list_size = math.ceil(len(list_to_split) / no_of_sub_lists)
        for i in range(0, len(list_to_split), sub_list_size):
            yield list_to_split[i:i + sub_list_size]

    @classmethod
    @timing
    def run_task_parallelly(cls, func, tasks: list, no_of_parallel_tasks: int, **kwargs):
        with concurrent.futures.ProcessPoolExecutor(no_of_parallel_tasks) as pool:
            futures = []
            for sub_tasks in cls.split_list_into_sub_lists(tasks, no_of_parallel_tasks):
                futures.append(pool.submit(func, sub_tasks, **kwargs))

            results = []
            for future in futures:
                results.append(future.result())

        return results
