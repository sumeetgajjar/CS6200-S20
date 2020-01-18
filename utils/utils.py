import logging
import sys

PROJECT_ROOT = '/home/sumeet/PycharmProjects/CS6200-S20/'
DATA_DIR = 'data'
AP89_COLLECTION = 'AP_DATA/ap89_collection'


def get_data_dir_abs_path():
    return '{}/{}'.format(PROJECT_ROOT, DATA_DIR)


def get_ap89_collection_abs_path():
    return '{}/{}'.format(get_data_dir_abs_path(), AP89_COLLECTION)


def configure_logging(level=logging.INFO):
    logging.basicConfig(stream=sys.stdout, level=level)
