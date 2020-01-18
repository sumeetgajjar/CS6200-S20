import logging
import os

from HW_1.es_utils import bulk_add_document_to_ap_data_index
from HW_1.parser import TRECParser
from utils.utils import get_ap89_collection_abs_path, configure_logging, delete_ap_data_index, create_ap_data_index


def get_file_paths_to_parse(dir_path: str) -> list:
    return list(map(lambda file: '{}/{}'.format(dir_path, file), os.listdir(dir_path)))


def get_parsed_documents(file_paths: list):
    parsed_documents = []
    parser = TRECParser()
    for file_path in file_paths:
        parsed_documents.extend(parser.parse(file_path))

    logging.info('total documents parsed: {}'.format(len(parsed_documents)))
    return parsed_documents


def create_ap_data_index_and_insert_documents():
    dir_path = get_ap89_collection_abs_path()
    file_paths = get_file_paths_to_parse(dir_path)
    parsed_documents = get_parsed_documents(file_paths)

    delete_ap_data_index(ignore_unavailable=True)
    create_ap_data_index()
    bulk_add_document_to_ap_data_index(parsed_documents)


if __name__ == '__main__':
    configure_logging()
    # create_ap_data_index_and_insert_documents()
