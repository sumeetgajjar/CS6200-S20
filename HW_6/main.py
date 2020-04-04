import logging
import re
from collections import defaultdict
from typing import Dict

import sklearn

from HW_1.main import parse_queries
from constants.constants import Constants
from utils.utils import Utils


class HW6:
    _TREQ_FILE_SPLIT_REGEX = '\\s+'

    @classmethod
    def _get_qrel_doc_ids(cls) -> Dict[str, Dict[str, int]]:
        logging.info('Parsing QREL for doc ids')
        qrel_file_path = '{}/{}'.format(Utils.get_ap_data_path(), 'qrels.adhoc.51-100.AP89.txt')
        query_doc_id_mapping = defaultdict(dict)
        with open(qrel_file_path, 'r') as file:
            for line in file:
                parts = line.strip().split(" ")
                query_id = parts[0].strip()
                doc_id = parts[2].strip()
                relevance = int(parts[3].strip())

                query_doc_id_mapping[query_id][doc_id] = relevance

        logging.info("QREL file parsed")
        return query_doc_id_mapping

    @classmethod
    def _parse_treq_file(cls, treq_file_path) -> Dict:
        logging.info("Parsing Treq file: {}".format(treq_file_path))
        treq = defaultdict(dict)
        with open(treq_file_path, 'r') as file:
            for line in file:
                query_id, _, doc_id, _, score, _ = re.split(cls._TREQ_FILE_SPLIT_REGEX, line.strip())
                treq[query_id][doc_id] = float(score)

        logging.info("Treq file parsed")
        return treq

    @classmethod
    def _add_non_relevant_documents(cls, query_id, query_document_mapping, treq_query_doc_id_mappings):
        if len(query_document_mapping[query_id]['non_relevant']) < 1000:
            tups = treq_query_doc_id_mappings[query_id].items()

            for tup in sorted(tups, key=lambda x: x[1], reverse=True):
                if len(query_document_mapping[query_id]['non_relevant']) == 1000:
                    break

                query_document_mapping[query_id]['non_relevant'].append(tup[0])

    @classmethod
    def _get_document_set_for_queries(cls, queries, bm25_treq_file_path) -> Dict:
        # dir_path = Utils.get_ap89_collection_abs_path()
        # file_paths = get_file_paths_to_parse(dir_path)
        # all_documents = {doc['id']: doc for doc in get_parsed_documents(file_paths)}

        qrel_query_doc_id_mappings = cls._get_qrel_doc_ids()
        treq_query_doc_id_mappings = cls._parse_treq_file(bm25_treq_file_path)

        query_document_mapping = defaultdict(lambda: defaultdict(list))
        for query in queries:
            query_id = query['id']
            for doc_id, relevance in qrel_query_doc_id_mappings[query_id].items():
                if relevance > 0:
                    query_document_mapping[query_id]['relevant'].append(doc_id)
                else:
                    query_document_mapping[query_id]['non_relevant'].append(doc_id)

            cls._add_non_relevant_documents(query_id, query_document_mapping, treq_query_doc_id_mappings)

        return query_document_mapping

    @classmethod
    def _generate_features(cls, query_document_mapping):
        """
        bm25 score
        and all other relevance models
        doc length


        :param query_document_mapping:
        :return:
        """
        return [], []

    @classmethod
    def _split_training_testing_data(cls, queries, feature_matrix, labels):
        query_ids = [query['id'] for query in queries]
        train_query_ids, test_query_ids = sklearn.model_selection.train_test_split(query_ids, test_size=0.8)
        X_train = [feature_matrix[query_id] for query_id in train_query_ids]
        Y_train = [labels[query_id] for query_id in train_query_ids]
        X_test = [feature_matrix[query_id] for query_id in test_query_ids]
        Y_test = [labels[query_id] for query_id in test_query_ids]

        return X_train, X_test, Y_train, Y_test

    @classmethod
    def _train_model(cls, X_train, Y_train):
        pass

    @classmethod
    def _train_model_and_predict(cls, X_train, X_test, Y_train, Y_test):
        cls._train_model(X_train, Y_train)

        # TODO predict

    @classmethod
    def main(cls):
        # TODO try with edited queries
        queries = parse_queries(parse_original=True)
        bm25_file_path = '{}/HW_1/results/okapi_bm25_all.txt'.format(Constants.PROJECT_ROOT)
        query_document_mapping = cls._get_document_set_for_queries(queries, bm25_file_path)
        feature_matrix, labels = cls._generate_features(query_document_mapping)
        X_train, X_test, Y_train, Y_test = cls._split_training_testing_data(queries, feature_matrix, labels)
        cls._train_model_and_predict(X_train, X_test, Y_train, Y_test)


if __name__ == '__main__':
    Utils.configure_logging()
    HW6.main()
