import logging
import os

from HW_1.parser import TRECParser
from utils.utils import get_ap89_collection_abs_path, configure_logging


def get_file_paths_to_parse(dir_path: str) -> list:
    return list(map(lambda file: '{}/{}'.format(dir_path, file), os.listdir(dir_path)))


def get_parsed_documents(file_paths: list):
    parsed_documents = []
    parser = TRECParser()
    for file_path in file_paths:
        parsed_documents.extend(parser.parse(file_path))

    logging.info('total documents parsed: {}'.format(len(parsed_documents)))
    return parsed_documents


def demo_hw_1():
    dir_path = get_ap89_collection_abs_path()
    file_paths = get_file_paths_to_parse(dir_path)
    parsed_documents = get_parsed_documents(file_paths)


if __name__ == '__main__':
    configure_logging()
    demo_hw_1()
