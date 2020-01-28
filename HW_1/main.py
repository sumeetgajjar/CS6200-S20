import logging
import math
import os
from collections import Counter

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
    queries = parse_queries()
    # import sys
    # queries = [queries[int(sys.argv[1])]]
    queries = clean_queries(queries)
    return queries


def transform_scores_for_writing_to_file(scores, query):
    results = []
    for ix, score in enumerate(scores[:1000]):
        results.append({
            'doc_no': score[1],
            'rank': ix + 1,
            'score': score[0],
            'query_number': query['id']
        })
    return results


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
def find_scores_using_es_builtin(queries):
    results = []
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
                    doc_length = len(term_vector['term_vectors']['text']['terms'])
                    temp = tf / (tf + 0.5 + (
                            1.5 * (doc_length / avg_doc_len)))
                    score += temp
            scores.append((score, term_vector['_id']))
    return scores


def find_scores_parallelly_and_write_to_file(queries,
                                             score_calculator,
                                             file_name,
                                             result_sub_dir=None,
                                             **kwargs):
    results_to_write = []
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    for query in queries:
        results = Utils.run_task_parallelly(score_calculator, all_document_ids, 8, query=query, **kwargs)
        scores = []
        for result in results:
            scores.extend(result)
        scores.sort(reverse=True)
        results_to_write.extend(transform_scores_for_writing_to_file(scores, query))

    file_path = 'results'
    if result_sub_dir:
        file_path = '{}/{}'.format(file_path, result_sub_dir)
    file_path = '{}/{}.txt'.format(file_path, file_name)

    Utils.write_results_to_file(file_path, results_to_write)


def add_es_significant_terms_to_queries(queries):
    for query in queries:
        token_set = set(query['tokens'])
        related_query_tokens = []
        for token in query['tokens']:
            related_tokens = []
            significant_terms = EsUtils.get_significant_terms(Constants.AP_DATA_INDEX_NAME, token)
            for term in significant_terms:
                if term['key'] in token_set:
                    continue
                if term['score'] < 0.7:
                    continue
                if len(related_tokens) == 2:
                    break

                related_tokens.append(term['key'])
            related_query_tokens.extend(related_tokens)

        query['tokens'].extend(related_query_tokens)

    return queries


def find_scores_parallelly_apply_es_significant_terms_feedback_and_write_to_file(queries,
                                                                                 score_calculator,
                                                                                 file_name,
                                                                                 **kwargs):
    queries = add_es_significant_terms_to_queries(queries)
    find_scores_parallelly_and_write_to_file(queries,
                                             score_calculator,
                                             file_name,
                                             result_sub_dir='pseudo-relevance-feedback/es-significant',
                                             **kwargs)


def find_scores_parallelly_apply_custom_feedback_and_write_to_file(queries,
                                                                   score_calculator,
                                                                   file_name,
                                                                   k,
                                                                   **kwargs):
    results_to_write_before_feedback = []
    results_to_write_after_feedback = []
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    for query in queries:
        results = Utils.run_task_parallelly(score_calculator, all_document_ids, 8, query=query, **kwargs)
        old_scores = []
        for result in results:
            old_scores.extend(result)
        old_scores.sort(reverse=True)
        results_to_write_before_feedback.extend(transform_scores_for_writing_to_file(old_scores, query))

        top_doc_ids = [tup[1] for tup in old_scores[:k]]
        termvectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, top_doc_ids)
        ttf = Counter()
        doc_freq = Counter()
        for termvector in termvectors:
            if termvector['term_vectors']:
                for term, value in termvector['term_vectors']['text']['terms'].items():
                    ttf[term] += value['term_freq']
                    doc_freq[term] += value['doc_freq']

        tf_idf = Counter()
        for term, tf in ttf.items():
            tf_idf[term] = tf * math.log(len(all_document_ids) / doc_freq[term])

        temp_tokens = [tup[0] for tup in tf_idf.most_common(10)]
        query['tokens'].extend(temp_tokens)

        results = Utils.run_task_parallelly(score_calculator, all_document_ids, 8, query=query, **kwargs)
        new_scores = []
        for result in results:
            new_scores.extend(result)
        new_scores.sort(reverse=True)

        results_to_write_after_feedback.extend(transform_scores_for_writing_to_file(new_scores, query))

    Utils.write_results_to_file('results/{}.txt'.format(file_name), results_to_write_before_feedback)
    Utils.write_results_to_file('results/pseudo-relevance-feedback/custom-heuristic/{}.txt'.format(file_name),
                                results_to_write_after_feedback)


@timing
def find_scores_using_okapi_tf(queries):
    find_scores_parallelly_and_write_to_file(queries,
                                             calculate_okapi_tf_scores,
                                             'okapi_tf')


@timing
def find_scores_using_okapi_tf_with_custom_feedback(queries):
    find_scores_parallelly_apply_custom_feedback_and_write_to_file(queries,
                                                                   calculate_okapi_tf_scores,
                                                                   'okapi_tf',
                                                                   10)


@timing
def find_scores_using_okapi_tf_with_es_significant_feedback(queries):
    find_scores_parallelly_apply_es_significant_terms_feedback_and_write_to_file(queries,
                                                                                 calculate_okapi_tf_scores,
                                                                                 'okapi_tf')


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
                    doc_length = len(term_vector['term_vectors']['text']['terms'])
                    temp = tf / (tf + 0.5 + (
                            1.5 * (doc_length / avg_doc_len)))
                    doc_freq = term_vector['term_vectors']['text']['terms'][token]['doc_freq']
                    score += (temp * math.log(total_documents / doc_freq))
            scores.append((score, term_vector['_id']))
    return scores


@timing
def find_scores_using_okapi_tf_idf(queries):
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_and_write_to_file(queries,
                                             calculate_okapi_tf_idf_scores,
                                             'okapi_tf_idf',
                                             total_documents=len(all_document_ids))


@timing
def find_scores_using_okapi_tf_idf_with_custom_feedback(queries):
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_custom_feedback_and_write_to_file(queries,
                                                                   calculate_okapi_tf_idf_scores,
                                                                   'okapi_tf_idf',
                                                                   10,
                                                                   total_documents=len(all_document_ids))


@timing
def find_scores_using_okapi_tf_idf_with_es_significant_feedback(queries):
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_es_significant_terms_feedback_and_write_to_file(queries,
                                                                                 calculate_okapi_tf_idf_scores,
                                                                                 'okapi_tf_idf',
                                                                                 total_documents=len(all_document_ids))


def calculate_okapi_bm25_scores(document_ids, query, total_documents, k_1=1.2, k_2=500, b=0.75):
    term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, document_ids, 10000)
    avg_doc_len = EsUtils.get_average_doc_length(Constants.AP_DATA_INDEX_NAME)
    scores = []
    query_term_freq = Counter(query['tokens'])
    for term_vector in term_vectors:
        if term_vector['term_vectors']:
            score = 0.0
            for token in query['tokens']:
                if token in term_vector['term_vectors']['text']['terms']:
                    tf = term_vector['term_vectors']['text']['terms'][token]['term_freq']
                    query_tf = query_term_freq.get(token)
                    doc_freq = term_vector['term_vectors']['text']['terms'][token]['doc_freq']
                    doc_length = len(term_vector['term_vectors']['text']['terms'])
                    temp_1 = math.log((total_documents + 0.5) / (doc_freq + 0.5))
                    temp_2 = (tf + (k_1 * tf)) / (tf + (k_1 * ((1 - b) + (b * (doc_length / avg_doc_len)))))
                    temp_3 = (query_tf + (k_2 * query_tf)) / (query_tf + k_2)
                    score += (temp_1 * temp_2 * temp_3)

            scores.append((score, term_vector['_id']))
    return scores


@timing
def find_scores_using_okapi_bm25(queries):
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_and_write_to_file(queries,
                                             calculate_okapi_bm25_scores,
                                             'okapi_bm25',
                                             total_documents=len(all_document_ids))


@timing
def find_scores_using_okapi_bm25_with_custom_feedback(queries):
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_custom_feedback_and_write_to_file(queries,
                                                                   calculate_okapi_bm25_scores,
                                                                   'okapi_bm25',
                                                                   10,
                                                                   total_documents=len(all_document_ids))


@timing
def find_scores_using_okapi_bm25_with_es_significant_feedback(queries):
    all_document_ids = EsUtils.get_all_document_ids(Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_es_significant_terms_feedback_and_write_to_file(queries,
                                                                                 calculate_okapi_bm25_scores,
                                                                                 'okapi_bm25',
                                                                                 total_documents=len(all_document_ids))


def calculate_unigram_lm_with_laplace_smoothing_scores(document_ids, query, vocabulary_size):
    term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, document_ids, 10000)
    scores = []
    for term_vector in term_vectors:
        if term_vector['term_vectors']:
            score = 0.0
            for token in query['tokens']:
                if token in term_vector['term_vectors']['text']['terms']:
                    tf = term_vector['term_vectors']['text']['terms'][token]['term_freq']
                else:
                    tf = 0

                doc_length = len(term_vector['term_vectors']['text']['terms'])
                temp = (tf + 1.0) / (doc_length + vocabulary_size)
                score += math.log(temp)

            scores.append((score, term_vector['_id']))
    return scores


@timing
def find_scores_using_unigram_lm_with_laplace_smoothing(queries):
    vocabulary_size = EsUtils.get_vocabulary_size(index_name=Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_and_write_to_file(queries,
                                             calculate_unigram_lm_with_laplace_smoothing_scores,
                                             'unigram_lm_with_laplace_smoothing',
                                             vocabulary_size=vocabulary_size)


@timing
def find_scores_using_unigram_lm_with_laplace_smoothing_with_custom_feedback(queries):
    vocabulary_size = EsUtils.get_vocabulary_size(index_name=Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_custom_feedback_and_write_to_file(queries,
                                                                   calculate_unigram_lm_with_laplace_smoothing_scores,
                                                                   'unigram_lm_with_laplace_smoothing',
                                                                   10,
                                                                   vocabulary_size=vocabulary_size)


@timing
def find_scores_using_unigram_lm_with_laplace_smoothing_with_es_significant_feedback(queries):
    vocabulary_size = EsUtils.get_vocabulary_size(index_name=Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_es_significant_terms_feedback_and_write_to_file(queries,
                                                                                 calculate_unigram_lm_with_laplace_smoothing_scores,
                                                                                 'unigram_lm_with_laplace_smoothing',
                                                                                 vocabulary_size=vocabulary_size)


def calculate_unigram_lm_with_jelinek_mercer_smoothing_scores(document_ids, query, vocabulary_size, lam=0.8):
    term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, document_ids, 10000)
    scores = []

    ttf = {}
    for term_vector in term_vectors:
        if term_vector['term_vectors']:
            for term, value in term_vector['term_vectors']['text']['terms'].items():
                ttf[term] = value['ttf']

    for term_vector in term_vectors:
        if term_vector['term_vectors']:
            score = 0.0
            for token in query['tokens']:
                if token in term_vector['term_vectors']['text']['terms']:
                    tf = term_vector['term_vectors']['text']['terms'][token]['term_freq']
                    token_ttf = term_vector['term_vectors']['text']['terms'][token]['ttf']
                else:
                    tf = 0
                    token_ttf = ttf.get(token, 0)

                doc_length = len(term_vector['term_vectors']['text']['terms'])
                temp_1 = lam * (tf / doc_length)
                temp_2 = (1 - lam) * (token_ttf / vocabulary_size)
                try:
                    score += math.log(temp_1 + temp_2)
                except:
                    pass

            scores.append((score, term_vector['_id']))
    return scores


@timing
def find_scores_using_unigram_lm_with_jelinek_mercer_smoothing(queries):
    vocabulary_size = EsUtils.get_vocabulary_size(index_name=Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_and_write_to_file(queries,
                                             calculate_unigram_lm_with_jelinek_mercer_smoothing_scores,
                                             'unigram_lm_with_jelinek_mercer_smoothing',
                                             vocabulary_size=vocabulary_size)


@timing
def find_scores_using_unigram_lm_with_jelinek_mercer_smoothing_with_custom_feedback(queries):
    vocabulary_size = EsUtils.get_vocabulary_size(index_name=Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_custom_feedback_and_write_to_file(queries,
                                                                   calculate_unigram_lm_with_jelinek_mercer_smoothing_scores,
                                                                   'unigram_lm_with_jelinek_mercer_smoothing',
                                                                   10,
                                                                   vocabulary_size=vocabulary_size)


@timing
def find_scores_using_unigram_lm_with_jelinek_mercer_smoothing_with_es_significant_feedback(queries):
    vocabulary_size = EsUtils.get_vocabulary_size(index_name=Constants.AP_DATA_INDEX_NAME)
    find_scores_parallelly_apply_es_significant_terms_feedback_and_write_to_file(queries,
                                                                                 calculate_unigram_lm_with_jelinek_mercer_smoothing_scores,
                                                                                 'unigram_lm_with_jelinek_mercer_smoothing',
                                                                                 vocabulary_size=vocabulary_size)


if __name__ == '__main__':
    Utils.configure_logging()
    # create_ap_data_index_and_insert_documents()
    _queries = get_queries()
    # find_scores_using_es_builtin(_queries)
    # find_scores_using_okapi_tf(_queries)
    # find_scores_using_okapi_tf_idf(_queries)
    # find_scores_using_okapi_bm25(_queries)
    # find_scores_using_unigram_lm_with_laplace_smoothing(_queries)
    # find_scores_using_unigram_lm_with_jelinek_mercer_smoothing(_queries)

    # find_scores_using_okapi_tf_with_custom_feedback(_queries)
    # find_scores_using_okapi_tf_idf_with_custom_feedback(_queries)
    # find_scores_using_okapi_bm25_with_custom_feedback(_queries)
    # find_scores_using_unigram_lm_with_laplace_smoothing_with_custom_feedback(_queries)
    # find_scores_using_unigram_lm_with_jelinek_mercer_smoothing_with_custom_feedback(_queries)

    find_scores_using_okapi_tf_with_es_significant_feedback(_queries)
    # find_scores_using_okapi_tf_idf_with_es_significant_feedback(_queries)
    # find_scores_using_okapi_bm25_with_es_significant_feedback(_queries)
    # find_scores_using_unigram_lm_with_laplace_smoothing_with_es_significant_feedback(_queries)
    # find_scores_using_unigram_lm_with_jelinek_mercer_smoothing_with_es_significant_feedback(_queries)
