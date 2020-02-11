import gc
import logging
import math
import sys
from collections import Counter, defaultdict

from HW_1.main import get_file_paths_to_parse, get_parsed_documents, parse_queries, \
    transform_scores_for_writing_to_file
from HW_2.factory import Factory
from HW_2.indexer import CustomIndex
from utils.decorators import timing
from utils.utils import Utils


class HW2:

    @classmethod
    def create_files_to_read_batches(cls):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        logging.info("Total File to read: {}".format(len(file_paths)))
        return Utils.split_list_into_sub_lists(file_paths, no_of_sub_lists=8)

    @classmethod
    def add_documents_to_index(cls, index_head, enable_stemming):
        dir_path = Utils.get_ap89_collection_abs_path()
        file_paths = get_file_paths_to_parse(dir_path)
        logging.info("Total File to read: {}".format(len(file_paths)))
        parsed_documents = get_parsed_documents(file_paths)

        custom_index = Factory.create_custom_index()
        metadata, metadata_file_path = custom_index.index_documents(parsed_documents, index_head, enable_stemming)
        return custom_index, metadata_file_path

    @classmethod
    def calculate_okapi_bm25_scores(cls, query, custom_index, avg_doc_len, total_documents, k_1=1.2, k_2=500, b=0.75):
        document_score = defaultdict(float)
        query_term_freq = Counter(query['tokens'])

        for query_token in query['tokens']:
            termvector = custom_index.get_termvector(query_token)
            if termvector:
                doc_freq = len(termvector['tf'])
                for doc_id, tf_info in termvector['tf'].items():
                    score = 0.0
                    tf = tf_info['tf']
                    query_tf = query_term_freq.get(query_token)
                    doc_length = custom_index.get_doc_length(doc_id)
                    temp_1 = math.log((total_documents + 0.5) / (doc_freq + 0.5))
                    temp_2 = (tf + (k_1 * tf)) / (tf + (k_1 * ((1 - b) + (b * (doc_length / avg_doc_len)))))
                    temp_3 = (query_tf + (k_2 * query_tf)) / (query_tf + k_2)
                    score += (temp_1 * temp_2 * temp_3)

                    document_score[doc_id] += score

        scores = [(score, doc_id) for doc_id, score in document_score.items()]
        return scores

    @classmethod
    def calculate_okapi_tf_idf_scores(cls, query, custom_index, avg_doc_len, total_documents):
        document_score = defaultdict(float)

        for query_token in query['tokens']:
            termvector = custom_index.get_termvector(query_token)
            if termvector:
                doc_freq = len(termvector['tf'])
                for doc_id, tf_info in termvector['tf'].items():
                    score = 0.0
                    tf = tf_info['tf']
                    doc_length = custom_index.get_doc_length(doc_id)
                    temp = tf / (tf + 0.5 + (1.5 * (doc_length / avg_doc_len)))
                    score += (temp * math.log(total_documents / doc_freq))

                    document_score[doc_id] += score

        scores = [(score, doc_id) for doc_id, score in document_score.items()]
        return scores

    @classmethod
    def calculate_unigram_lm_with_laplace_smoothing_scores(cls, query, custom_index, vocabulary_size):

        def _calculate_score(_tf, _doc_id):
            _doc_length = custom_index.get_doc_length(_doc_id)
            _temp = (_tf + 1.0) / (_doc_length + vocabulary_size)
            _score = math.log(_temp)
            return _score

        document_score = defaultdict(float)
        document_ids = custom_index.get_all_document_ids()
        for query_token in query['tokens']:
            termvector = custom_index.get_termvector(query_token)
            if termvector:
                for doc_id, tf_info in termvector['tf'].items():
                    document_score[doc_id] += _calculate_score(tf_info['tf'], doc_id)

                for doc_id in document_ids:
                    if doc_id not in termvector['tf']:
                        document_score[doc_id] += _calculate_score(0, doc_id)
            else:
                for doc_id in document_ids:
                    document_score[doc_id] += _calculate_score(0, doc_id)

        scores = [(score, doc_id) for doc_id, score in document_score.items()]
        return scores

    @classmethod
    def calculate_unigram_lm_with_jelinek_mercer_smoothing_scores(cls, query, custom_index, vocabulary_size, lam=0.9):

        def _calculate(_tf, _ttf, _doc_id):
            _doc_length = custom_index.get_doc_length(_doc_id)
            _score = 0.0
            _temp_1 = lam * (_tf / _doc_length)
            _temp_2 = (1 - lam) * (_ttf / vocabulary_size)
            try:
                _score += math.log(_temp_1 + _temp_2)
                return _score
            except:
                return 0

        document_score = defaultdict(float)
        document_ids = custom_index.get_all_document_ids()

        for query_token in query['tokens']:
            termvector = custom_index.get_termvector(query_token)
            if termvector:
                token_ttf = termvector['ttf']
                for doc_id, tf_info in termvector['tf'].items():
                    tf = tf_info['tf']
                    document_score[doc_id] += _calculate(tf, token_ttf, doc_id)

                for doc_id in document_ids:
                    if doc_id not in termvector['tf']:
                        document_score[doc_id] += _calculate(0, token_ttf, doc_id)
            else:
                for doc_id in document_ids:
                    document_score[doc_id] += _calculate(0, 0, doc_id)

        scores = [(score, doc_id) for doc_id, score in document_score.items()]
        return scores

    @classmethod
    def compute_minimum_span(cls, ngram_tokens, positions):
        min_ix, max_ix = 0, 0
        min_pos, max_pos = sys.maxsize, -sys.maxsize
        pointers = [0] * len(ngram_tokens)
        min_span = sys.maxsize
        while True:
            for ix, token in enumerate(ngram_tokens):
                token_positions = positions[token]
                curr_pos = token_positions[pointers[ix]]
                if min_pos > curr_pos:
                    min_ix = ix
                    min_pos = curr_pos
                elif max_pos < curr_pos:
                    max_pos = curr_pos

            curr_span = max_pos - min_pos
            if min_span > curr_span:
                min_span = curr_span

            pointers[min_ix] += 1

            min_pos_token = ngram_tokens[min_ix]
            if pointers[min_ix] >= len(positions[min_pos_token]):
                if 0 < min_span < 500:
                    return min_span
                else:
                    return 500

            min_pos = positions[min_pos_token][pointers[min_ix]]

    @classmethod
    def compute_doc_id_term_positions(cls, query, custom_index):
        doc_id_term_positions = {}
        for query_token in query['tokens']:
            termvector = custom_index.get_termvector(query_token)
            if termvector:
                for doc_id, tf_info in termvector['tf'].items():
                    if doc_id not in doc_id_term_positions:
                        doc_id_term_positions[doc_id] = {}

                    doc_id_term_positions[doc_id][query_token] = tf_info['pos']

        return doc_id_term_positions

    @classmethod
    def compute_min_span_score(cls, query_ngrams, doc_id_term_positions, alpha):
        document_score = defaultdict(float)
        for doc_id in doc_id_term_positions.keys():
            for ngram_tokens in query_ngrams:
                positions = {}
                for ngram_token in ngram_tokens:
                    pos = doc_id_term_positions[doc_id].get(ngram_token)
                    if pos:
                        positions[ngram_token] = pos

                if len(ngram_tokens) == len(positions):
                    min_span = cls.compute_minimum_span(ngram_tokens, positions)
                else:
                    min_span = 287

                document_score[doc_id] += math.log(alpha + math.exp(-min_span))

        return document_score

    @classmethod
    def generate_query_ngrams(cls, query, ngram_length):
        query_ngrams = []
        for i in range(len(query['tokens']) - ngram_length + 1):
            query_ngrams.append(query['tokens'][i:i + ngram_length])

        return query_ngrams

    @classmethod
    def calculate_scores_using_proximity_search(cls, query, custom_index, avg_doc_len, total_documents, ngram_length=2,
                                                alpha=1.3):

        doc_id_term_positions = cls.compute_doc_id_term_positions(query, custom_index)
        query_ngrams = cls.generate_query_ngrams(query, ngram_length)
        document_score = cls.compute_min_span_score(query_ngrams, doc_id_term_positions, alpha)

        for query_token in query['tokens']:
            termvector = custom_index.get_termvector(query_token)
            if termvector:
                doc_freq = len(termvector['tf'])
                for doc_id, tf_info in termvector['tf'].items():
                    score = 0.0
                    tf = tf_info['tf']
                    doc_length = custom_index.get_doc_length(doc_id)
                    temp = tf / (tf + 0.5 + (1.5 * (doc_length / avg_doc_len)))
                    score += (temp * math.log(total_documents / doc_freq))

                    document_score[doc_id] += score

        scores = [(score, doc_id) for doc_id, score in document_score.items()]
        return scores

    @classmethod
    @timing
    def find_scores_and_write_to_file(cls, queries,
                                      score_calculator,
                                      file_name,
                                      result_sub_dir=None,
                                      **kwargs):
        results_to_write = []
        for query in queries:
            scores = score_calculator(query, **kwargs)
            scores.sort(reverse=True)

            results_to_write.extend(transform_scores_for_writing_to_file(scores, query))

        file_path = 'results'
        if result_sub_dir:
            file_path = '{}/{}'.format(file_path, result_sub_dir)
        file_path = '{}/{}.txt'.format(file_path, file_name)

        Utils.write_results_to_file(file_path, results_to_write)

    @classmethod
    def clean_queries(cls, queries, custom_index):
        for query in queries:
            raw_query = query['raw']
            tokens = custom_index.analyze(raw_query, True)
            query['tokens'] = [token[0] for token in tokens]
            analyzed_query = " ".join(query['tokens'])
            query['cleaned'] = analyzed_query.strip()

        return queries

    @classmethod
    @timing
    def get_queries(cls, custom_index):
        queries = parse_queries()
        queries = cls.clean_queries(queries, custom_index)
        return queries

    @classmethod
    def run_models(cls, results_sub_dir, metadata_file_path):
        gc.collect()
        custom_index = Factory.create_custom_index()
        custom_index.init_index(metadata_file_path)

        queries = cls.get_queries(custom_index)
        cls.find_scores_and_write_to_file(queries, cls.calculate_okapi_tf_idf_scores, 'okapi_tf_idf',
                                          results_sub_dir,
                                          custom_index=custom_index,
                                          avg_doc_len=custom_index.get_average_doc_length(),
                                          total_documents=custom_index.get_total_documents()
                                          )

        cls.find_scores_and_write_to_file(queries, cls.calculate_okapi_bm25_scores, 'okapi_bm25',
                                          results_sub_dir,
                                          custom_index=custom_index,
                                          avg_doc_len=custom_index.get_average_doc_length(),
                                          total_documents=custom_index.get_total_documents()
                                          )

        # cls.find_scores_and_write_to_file(queries, cls.calculate_unigram_lm_with_laplace_smoothing_scores,
        #                                   'unigram_lm_with_laplace_smoothing',
        #                                   results_sub_dir,
        #                                   custom_index=custom_index,
        #                                   vocabulary_size=custom_index.get_vocabulary_size()
        #                                   )

        cls.find_scores_and_write_to_file(queries, cls.calculate_unigram_lm_with_jelinek_mercer_smoothing_scores,
                                          'unigram_lm_with_jelinek_mercer_smoothing',
                                          results_sub_dir,
                                          custom_index=custom_index,
                                          vocabulary_size=custom_index.get_vocabulary_size()
                                          )

        cls.find_scores_and_write_to_file(queries, cls.calculate_scores_using_proximity_search,
                                          'proximity_search',
                                          results_sub_dir,
                                          custom_index=custom_index,
                                          avg_doc_len=281.71925743100417,
                                          total_documents=custom_index.get_total_documents()
                                          )

    @classmethod
    @timing
    def generate_indexes(cls):
        for index_head in [True, False]:
            for stemming_enabled in [True, False]:
                gc.collect()
                custom_index, metadata_file_path = cls.add_documents_to_index(index_head, stemming_enabled)
                logging.info(
                    'Index Head: {}, Stemming Enabled: {}, Metadata file: {}'.format(index_head, stemming_enabled,
                                                                                     metadata_file_path))

    @classmethod
    def _get_absolute_metadata_file_path(cls, metadata_file_name):
        return '{}/{}'.format(CustomIndex.get_metadata_dir(), metadata_file_name)

    @classmethod
    @timing
    def main(cls):
        Utils.configure_logging()
        # Utils.set_gc_debug_flags()
        # cls.generate_indexes()

        # uncompressed indexes
        # cls.run_models('non-stemmed/head-text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:11:12-22a12732-fc24-40d7-98ff-587a31439dd0.txt'))
        # cls.run_models('non-stemmed/text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:17:41-60170caf-3317-4cd3-97f0-473cee8ac778.txt'))
        # cls.run_models('stemmed/head-text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:08:13-9e26ba96-ba2e-42e7-991d-5dfa9a468c59.txt'))
        # cls.run_models('stemmed/text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:14:42-f6e26542-5858-4248-8be1-611a45fd721f.txt'))

        # compressed indexes
        # change the compressor to Gzip compressor in Factory.create_custom_index()
        # cls.run_models('non-stemmed/head-text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:51:46-0e909aca-bd79-4214-bfe1-edc734ad3389.txt'))
        # cls.run_models('non-stemmed/text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-16:02:06-d0f6bbbb-b4d1-4e86-8fb5-672ab8d9568b.txt'))
        # cls.run_models('stemmed/head-text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:46:56-fea8cc14-a710-4cc7-9a4c-e3aff6aa30b9.txt'))
        # cls.run_models('stemmed/text', cls._get_absolute_metadata_file_path(
        #     '02-08-2020-15:56:55-3c539457-a6ac-415d-aeac-c88710b68edd.txt'))


if __name__ == '__main__':
    HW2.main()
