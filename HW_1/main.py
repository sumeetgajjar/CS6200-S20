import logging
import os

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


def parse_queries():
    dir_path = Utils.get_ap_data_path()
    queries_file_path = '{}/query_desc.51-100.short.txt'.format(dir_path)
    queries = {}
    with open(queries_file_path, 'r') as file:
        for line in file:
            line = line.strip().lower()
            if line:
                splits = line.split(".   ")
                query_id = splits[0]
                raw_query = splits[1]
                queries[query_id] = {'raw_query': raw_query}

    return queries


def clean_queries(queries):
    for query_id, query in queries.items():
        raw_query: str = query['raw_query']
        cleaned_query = raw_query.replace("document", "").replace("will", "")
        query['cleaned_query'] = cleaned_query.strip()

    return queries


if __name__ == '__main__':
    Utils.configure_logging()
    # create_ap_data_index_and_insert_documents()
    # all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    # term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, all_document_ids, 10000)
    # logging.info(len(term_vectors))
    print(clean_queries(parse_queries()))
