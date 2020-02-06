import logging

from HW_1.es_utils import EsUtils
from HW_1.main import get_file_paths_to_parse, get_parsed_documents
from HW_2.factory import Factory
from constants.constants import Constants
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

    # @classmethod
    # def calculate_okapi_bm25_scores(cls, query, total_documents, k_1=1.2, k_2=500, b=0.75):
    #     term_vectors = EsUtils.get_termvectors(Constants.AP_DATA_INDEX_NAME, document_ids, 10000)
    #     avg_doc_len = EsUtils.get_average_doc_length(Constants.AP_DATA_INDEX_NAME)
    #     scores = []
    #     query_term_freq = Counter(query['tokens'])
    #     for term_vector in term_vectors:
    #         if term_vector['term_vectors']:
    #             score = 0.0
    #             for token in query['tokens']:
    #                 if token in term_vector['term_vectors']['text']['terms']:
    #                     tf = term_vector['term_vectors']['text']['terms'][token]['term_freq']
    #                     query_tf = query_term_freq.get(token)
    #                     doc_freq = term_vector['term_vectors']['text']['terms'][token]['doc_freq']
    #                     doc_length = len(term_vector['term_vectors']['text']['terms'])
    #                     temp_1 = math.log((total_documents + 0.5) / (doc_freq + 0.5))
    #                     temp_2 = (tf + (k_1 * tf)) / (tf + (k_1 * ((1 - b) + (b * (doc_length / avg_doc_len)))))
    #                     temp_3 = (query_tf + (k_2 * query_tf)) / (query_tf + k_2)
    #                     score += (temp_1 * temp_2 * temp_3)
    #
    #             scores.append((score, term_vector['_id']))
    #     return scores

    @classmethod
    @timing
    def main(cls):
        Utils.configure_logging()
        custom_index = cls.add_documents_to_index(False, True)

        custom_index = Factory.create_custom_index()
        # custom_index.init_index(
        #     '/home/sumeet/PycharmProjects/CS6200-S20/data/custom-index/metadata/02-05-2020-21:50:31-ec57b3c8-0169-4e82-b03d-ad77c4ba9ad9.txt')
        term = 'alexand'
        print(custom_index.get_termvector('%s' % term))
        print(len(custom_index.get_termvector(term)['tf']))
        print(custom_index.get_average_doc_length())
        print(EsUtils.get_average_doc_length(Constants.AP_DATA_INDEX_NAME))


if __name__ == '__main__':
    HW2.main()
    # tokens = Factory.create_tokenizer(Constants.CUSTOM_TOKENIZER_NAME).tokenize("The car was in the car wash.")
    # print(tokens)
    # filtered_tokens = Factory.create_stopwords_filter(Constants.STOPWORDS_FILTER_NAME).filter(tokens)
    # print(filtered_tokens)
