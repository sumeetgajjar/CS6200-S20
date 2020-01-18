import logging
import sys

from HW_1.Constants import PROJECT_ROOT, DATA_DIR, AP89_COLLECTION, AP_DATA_INDEX_NAME
from HW_1.es_index_config import get_ap_data_index_config
from HW_1.es_utils import create_es_index, delete_es_index


def get_data_dir_abs_path():
    return '{}/{}'.format(PROJECT_ROOT, DATA_DIR)


def get_ap89_collection_abs_path():
    return '{}/{}'.format(get_data_dir_abs_path(), AP89_COLLECTION)


def create_ap_data_index():
    create_es_index(name=AP_DATA_INDEX_NAME, index_config=get_ap_data_index_config())


def delete_ap_data_index(ignore_unavailable=False):
    delete_es_index(name=AP_DATA_INDEX_NAME, ignore_unavailable=ignore_unavailable)


def configure_logging(level=logging.INFO):
    logging.basicConfig(stream=sys.stdout, level=level)
