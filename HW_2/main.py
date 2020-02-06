import logging
import math
from collections import Counter, defaultdict

from HW_1.main import get_file_paths_to_parse, get_parsed_documents, parse_queries, transform_scores_for_writing_to_file
from HW_2.factory import Factory
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
        metadata = custom_index.index_documents(parsed_documents, index_head, enable_stemming)
        return custom_index

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
            doc_freq = len(termvector['tf'])
            if termvector:
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
                                      **kwargs):
        results_to_write = []
        for query in queries:
            scores = score_calculator(query, **kwargs)
            scores.sort(reverse=True)

            results_to_write.extend(transform_scores_for_writing_to_file(scores, query))

        file_path = 'results/{}.txt'.format(file_name)
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
        # import sys
        # queries = [queries[int(sys.argv[1])]]
        queries = cls.clean_queries(queries, custom_index)
        return queries

    @classmethod
    @timing
    def main(cls):
        Utils.configure_logging()
        # custom_index = cls.add_documents_to_index(False, True)

        custom_index = Factory.create_custom_index()
        custom_index.init_index(
            '/home/sumeet/PycharmProjects/CS6200-S20/data/custom-index/metadata/02-05-2020-22:31:22-ccf762e5-3e26-4b37-9a6c-257d9159c3be.txt')
        # term = 'alexand'
        # print(custom_index.get_termvector('%s' % term))
        # print(len(custom_index.get_termvector(term)['tf']))
        # print(custom_index.get_average_doc_length())
        # print(custom_index.analyze("The car was in the car wash.", True))
        # print(custom_index.get_doc_length('AP891218-0001
        # '))
        # results_to_write = []
        queries = cls.get_queries(custom_index)
        cls.find_scores_and_write_to_file(queries, cls.calculate_okapi_tf_idf_scores, 'okapi_tf_idf',
                                          custom_index=custom_index,
                                          avg_doc_len=custom_index.get_average_doc_length(),
                                          total_documents=custom_index.get_total_documents()
                                          )

        # cls.find_scores_and_write_to_file(queries, cls.calculate_okapi_bm25_scores, 'okapi_bm25',
        #                                   custom_index=custom_index,
        #                                   avg_doc_len=custom_index.get_average_doc_length(),
        #                                   total_documents=custom_index.get_total_documents()
        #                                   )


if __name__ == '__main__':
    HW2.main()
    # tokens = Factory.create_tokenizer(Constants.CUSTOM_TOKENIZER_NAME).tokenize("The car was in the car wash.")
    # print(tokens)
    # filtered_tokens = Factory.create_stopwords_filter(Constants.STOPWORDS_FILTER_NAME).filter(tokens)
    # print(filtered_tokens)
