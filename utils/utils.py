import logging
import sys

from HW_1.constants import Constants
from HW_1.es_index_config import EsIndexConfig
from HW_1.es_utils import EsUtils


class Utils:

    @classmethod
    def get_data_dir_abs_path(cls):
        return '{}/{}'.format(Constants.PROJECT_ROOT, Constants.DATA_DIR)

    @classmethod
    def get_ap89_collection_abs_path(cls, ):
        return '{}/{}'.format(cls.get_data_dir_abs_path(), Constants.AP89_COLLECTION)

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
