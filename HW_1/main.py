import logging
import os

from HW_1.constants import Constants
from HW_1.es_utils import EsUtils
from HW_1.parser import TRECParser
from utils.decorators import timing
from utils.utils import Utils


def get_file_paths_to_parse(dir_path: str) -> list:
    return list(map(lambda file: '{}/{}'.format(dir_path, file), os.listdir(dir_path)))


@timing
def get_parsed_documents(file_paths: list):
    logging.info("Parsing documents")
    parsed_documents = []
    parser = TRECParser()
    for file_path in file_paths:
        parsed_documents.extend(parser.parse(file_path))

    logging.info('total documents parsed: {}'.format(len(parsed_documents)))
    logging.info("Documents parsed")
    return parsed_documents


def create_ap_data_index_and_insert_documents():
    dir_path = Utils.get_ap89_collection_abs_path()
    file_paths = get_file_paths_to_parse(dir_path)
    parsed_documents = get_parsed_documents(file_paths)

    Utils.delete_ap_data_index(ignore_unavailable=True)
    Utils.create_ap_data_index()
    EsUtils.bulk_add_document_to_ap_data_index(parsed_documents)


if __name__ == '__main__':
    Utils.configure_logging()
    # create_ap_data_index_and_insert_documents()
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, all_document_ids, 10000)
    logging.info(len(term_vectors))
