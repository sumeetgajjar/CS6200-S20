import logging
import math
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


def parse_queries():
    dir_path = Utils.get_ap_data_path()
    queries_file_path = '{}/query_desc.51-100.short-edited.txt'.format(dir_path)
    queries = []
    with open(queries_file_path, 'r') as file:
        for line in file:
            line = line.strip().lower()
            if line:
                splits = line.split(".   ")
                query_id = splits[0]
                raw_query = splits[1]
                queries.append({
                    'raw': raw_query,
                    'id': query_id
                })

    return queries


def clean_queries(queries):
    es_indices_client = EsUtils.get_indices_client()

    for query in queries:
        raw_query = query['raw']
        response = es_indices_client.analyze(index=Constants.AP_DATA_INDEX_NAME, body={
            'text': raw_query,
            'analyzer': 'stopped'
        })

        analyzed_query = " ".join([token['token'] for token in response['tokens']])
        query['cleaned'] = analyzed_query.strip()
        query['tokens'] = [token['token'] for token in response['tokens']]

    return queries


def get_queries():
    import sys
    queries = parse_queries()
    # queries = [queries[int(sys.argv[1])]]
    queries = clean_queries(queries)
    return queries


def query_es(query):
    es_client = EsUtils.get_es_client()
    response = es_client.search(index=Constants.AP_DATA_INDEX_NAME, body={
        "query": {
            "match": {
                "text": query['cleaned']
            }
        },
        "size": 1000,
        "stored_fields": []
    })
    results = []
    for ix, doc in enumerate(response['hits']['hits']):
        results.append({
            'doc_no': doc['_id'],
            'rank': ix + 1,
            'score': doc['_score'],
            'query_number': query['id']
        })
    return results


@timing
def find_scores_using_es_builtin():
    results = []
    queries = get_queries()
    for query in queries:
        results.extend(query_es(query))

    Utils.write_results_to_file('results/es_builtin.txt', results)


def calculate_okapi_tf_scores(document_ids, query):
    term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, document_ids, 10000)
    avg_doc_len = EsUtils.get_average_doc_length(Constants.AP_DATA_INDEX_NAME)
    scores = []
    for term_vector in term_vectors:
        if term_vector['term_vectors']:
            score = 0.0
            for token in query['tokens']:
                if token in term_vector['term_vectors']['text']['terms']:
                    tf = term_vector['term_vectors']['text']['terms'][token]['term_freq']
                    temp = tf / (tf + 0.5 + (
                            1.5 * (len(term_vector['term_vectors']['text']['terms']) / avg_doc_len)))
                    score += temp
            scores.append((score, term_vector['_id']))
    return scores


@timing
def find_scores_using_okapi_tf():
    temp = []
    queries = get_queries()
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    for query in queries:
        results = Utils.run_task_parallelly(calculate_okapi_tf_scores, all_document_ids, 8, query=query)
        scores = []
        for result in results:
            scores.extend(result)
        scores.sort(reverse=True)
        for ix, score in enumerate(scores[:1000]):
            temp.append({
                'doc_no': score[1],
                'rank': ix + 1,
                'score': score[0],
                'query_number': query['id']
            })

    Utils.write_results_to_file('results/okapi_tf.txt', temp)


def calculate_okapi_tf_idf_scores(document_ids, query, total_documents):
    term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, document_ids, 10000)
    avg_doc_len = EsUtils.get_average_doc_length(Constants.AP_DATA_INDEX_NAME)
    scores = []
    for term_vector in term_vectors:
        if term_vector['term_vectors']:
            score = 0.0
            for token in query['tokens']:
                if token in term_vector['term_vectors']['text']['terms']:
                    tf = term_vector['term_vectors']['text']['terms'][token]['term_freq']
                    temp = tf / (tf + 0.5 + (
                            1.5 * (len(term_vector['term_vectors']['text']['terms']) / avg_doc_len)))
                    doc_freq = term_vector['term_vectors']['text']['terms'][token]['doc_freq']
                    score += (temp * math.log(total_documents / doc_freq))
            scores.append((score, term_vector['_id']))
    return scores


@timing
def find_scores_using_okapi_tf_idf():
    temp = []
    queries = get_queries()
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    for query in queries:
        results = Utils.run_task_parallelly(calculate_okapi_tf_idf_scores, all_document_ids, 8,
                                            query=query,
                                            total_documents=len(all_document_ids))
        scores = []
        for result in results:
            scores.extend(result)
        scores.sort(reverse=True)
        for ix, score in enumerate(scores[:1000]):
            temp.append({
                'doc_no': score[1],
                'rank': ix + 1,
                'score': score[0],
                'query_number': query['id']
            })

    Utils.write_results_to_file('results/okapi_tf_idf.txt', temp)


if __name__ == '__main__':
    Utils.configure_logging()
    # create_ap_data_index_and_insert_documents()
    find_scores_using_es_builtin()
    find_scores_using_okapi_tf()
    find_scores_using_okapi_tf_idf()
